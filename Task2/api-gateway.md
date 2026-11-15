# Дизайн API Gateway для DSP-интеграции (Edge RTB Gateway)

Диаграммы: [diagrams/api-gateway-component.puml](diagrams/api-gateway-component.puml), [diagrams/bid-request-sequence.puml](diagrams/bid-request-sequence.puml)

## 1. Роль и требования

Edge RTB Gateway — единственная точка входа bid request от DSP/SSP-партнёров.

| Требование | Значение |
|---|---|
| Пропускная способность | 18 000 RPS (3 мес.) → 50 000 RPS (1 год), запас ×1,5 |
| Собственная задержка | P99 ≤ 2 мс (без учёта апстрима) |
| Доступность | 99.95%, без SPOF: ≥ 3 инстанса в разных зонах на регион |
| Изоляция | отдельное развёртывание от API Gateway кабинета |

Технология — **Envoy** (обоснование в [interaction.md](interaction.md#5-нужен-ли-api-gateway-да--какой-именно)). Конфигурация доставляется через xDS из control plane, источник истины — Partner Config Service + Git (GitOps).

## 2. Цепочка фильтров

```
TLS (mTLS) → IP allowlist (RBAC) → аутентификация партнёра → local rate limit (партнёр)
  → adaptive concurrency (load shedding) → валидация размера/формата → проставление дедлайна
  → маршрутизация (партнёр / canary / mirror) → circuit breaker + outlier detection → upstream
  → метрики и трассировка (на всём пути)
```

## 3. Маршрутизация

| Маршрут | Назначение | Апстрим | Route timeout |
|---|---|---|---|
| `POST /openrtb/2.6/bid` с `x-partner-id: dsp-new` | новый DSP | `bidding-v2` (100%) | 70 мс |
| `POST /openrtb/2.5/bid` текущие DSP | текущие партнёры | `bidding-v2` (weight N%) / `monolith-adserver` (100−N%) | 70 мс |
| то же, **mirror** | shadow-трафик на новый сервис (ответ игнорируется) | `bidding-v2-shadow` | — |
| `GET /win`, `GET /bill` | win/billing notice | `event-collector` | 200 мс |
| `GET /status` | проверка доступности для партнёра | локальный ответ Envoy | — |

Принципы:
- **Партнёр определяется на этапе аутентификации** (по сертификату или ключу), Gateway проставляет заголовок `x-partner-id` и удаляет одноимённый входящий — партнёр не может выдать себя за другого.
- **Canary по весам**: `weighted_clusters` для постепенного переключения 1% → 10% → 50% → 100% (Strangler Fig).
- **Shadow**: `request_mirror_policies` с `runtime_fraction` (например, 10% трафика) — сравнение решений старого и нового аукциона без влияния на ответ.
- **Мультиплатформенность**: адаптер версии протокола (2.5 ↔ 2.6) выполняется в Bidding Service, Gateway лишь маршрутизирует по версии в пути.
- **Региональность (через год)**: GeoDNS/Anycast направляет партнёра в ближайший регион; при недоступности региона — в другой.

## 4. Аутентификация и авторизация

| Механизм | Назначение |
|---|---|
| **mTLS** (клиентский сертификат партнёра, выпускается нашим CA, ротация раз в 90 дней) | основной способ аутентификации; без вычислений на каждый запрос после установления TLS-сессии |
| **IP allowlist** (RBAC-фильтр Envoy) | дополнительный уровень: принимаем только с объявленных подсетей партнёра |
| **API-ключ** в заголовке `x-api-key` (хранится как SHA-256 в конфигурации, ключи — в Vault) | для партнёров, не поддерживающих mTLS |
| **HMAC-подпись** в `nurl`/`burl` | защита win/billing notice от подделки; проверяется в Event Collector |

JWT/OAuth2 на RTB-пути не используются: проверка подписи токена на каждый из 50 000 запросов/с дороже, чем mTLS, и не принята в OpenRTB-интеграциях.

## 5. Ограничение частоты запросов (rate limiting)

| Уровень | Механизм | Параметры (пример для нового DSP) | Ответ при превышении |
|---|---|---|---|
| Квота партнёра | **local rate limit** (token bucket) на каждом инстансе Envoy: квота / число инстансов | квота 25 000 QPS, burst 20% | `204 No Content` (no-bid) + заголовок `x-adscale-throttled: 1` |
| Защита от перегрузки | **adaptive concurrency** (gradient controller) | цель — P90 апстрима ≤ 30 мс | `503` |
| Глобальный предохранитель | общий лимит на кластер (overload manager Envoy по памяти/соединениям) | 120% от расчётной мощности | `503` |
| Размер запроса | `max_request_bytes` | 64 КБ | `413` |

Почему **local**, а не global rate limit: глобальный лимит требует обращения к внешнему сервису (ratelimit + Redis) на каждый запрос, это +1–2 мс и ещё одна зависимость на hot path. Квота делится на число инстансов; при изменении числа инстансов control plane пересчитывает её.

Почему **no-bid (204)**, а не 429: для DSP в OpenRTB 204 — штатный ответ «ставки нет», не считается ошибкой и не ухудшает метрики доступности, а 429/5xx — считаются.

## 6. Circuit Breaker и защита апстримов

```yaml
clusters:
- name: bidding-v2
  connect_timeout: 0.05s
  type: STRICT_DNS
  lb_policy: LEAST_REQUEST
  typed_extension_protocol_options:
    envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
      explicit_http_config: { http2_protocol_options: {} }
  circuit_breakers:
    thresholds:
    - priority: DEFAULT
      max_connections: 20000
      max_pending_requests: 2000      # очередь короткая: ждать нет смысла при дедлайне 70 мс
      max_requests: 20000
      max_retries: 0                  # retry на hot path запрещены
  outlier_detection:
    consecutive_5xx: 5
    consecutive_gateway_failure: 3
    interval: 2s
    base_ejection_time: 15s
    max_ejection_percent: 30          # не выбрасываем больше 30% подов
    success_rate_minimum_hosts: 5
  health_checks:
  - timeout: 0.05s
    interval: 2s
    unhealthy_threshold: 2
    healthy_threshold: 2
    http_health_check: { path: /readyz }
```

Поведение:
- **Размыкание по нагрузке**: при превышении `max_pending_requests` Envoy сразу отвечает (переводим в 204 через `local_reply_config`), вместо того чтобы копить очередь и опаздывать.
- **Outlier detection**: «больной» под исключается из балансировки на 15 с (с экспоненциальным ростом при повторах), но не более 30% подов одновременно.
- **Fallback-маршрут**: если в кластере `bidding-v2` нет здоровых хостов, а монолит ещё работает (переходный период), трафик переключается на `monolith-adserver` (`aggregate cluster` с приоритетами).
- **Retry отсутствуют**: повтор в бюджете 80 мс невозможен и создаёт «шторм» при деградации.

## 7. Мониторинг времени отклика

| Метрика | Источник | Срез | Алерт |
|---|---|---|---|
| `envoy_cluster_upstream_rq_time` (гистограмма) | Envoy | партнёр, апстрим, зона | P95 > 70 мс в течение 2 мин (warning), > 80 мс (critical) |
| Downstream latency (полное время в Gateway) | Envoy access log → Loki / метрики | партнёр | P95 > 75 мс |
| Доля no-bid по причинам (throttled, deadline, overload) | Envoy + Bidding | партнёр | throttled > 5% |
| Доля 5xx | Envoy | партнёр | > 0,1% за 5 мин |
| Ejected hosts, открытые circuit breakers | Envoy | кластер | > 0 в течение 1 мин |
| QPS vs квота | Envoy | партнёр | > 90% квоты |
| Трейсы | OpenTelemetry (sampling 1% + все медленные > 60 мс) | — | — |

Дашборд **«SLA по партнёрам»** в Grafana: P50/P95/P99, доступность, error budget за 30 дней, win rate. Эти же данные отдаются партнёру в ежемесячном отчёте (FR-12) — это доказательство соблюдения SLA при спорах о штрафах.

## 8. Развёртывание

- Kubernetes Deployment, ≥ 3 реплики на регион (anti-affinity по зонам), HPA по CPU и активным соединениям.
- Перед Envoy — L4 load balancer облака (TCP passthrough, TLS терминируется в Envoy для mTLS).
- Размер: ~1 vCPU на 8–10 тыс. RPS для HTTP/1.1 keep-alive с TLS → для 50 000 RPS с запасом — 8–10 инстансов по 2 vCPU на регион.
- Конфигурация — GitOps (Argo CD), изменение квоты/маршрута партнёра — pull request, применение — без перезапуска (xDS).

## 9. Подключение нового партнёра (runbook)

1. Запись в Partner Config: `partner_id`, версия OpenRTB, подсети, сертификат/ключ, квота, `tmax`, модель аукциона.
2. Выпуск клиентского сертификата, обмен эндпоинтами (staging).
3. Интеграционные тесты на staging, затем prod с квотой 10% от целевой.
4. Мониторинг дашборда партнёра 48 ч → увеличение квоты до 100%.
