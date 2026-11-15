# Паттерны надёжности

Диаграммы: [diagrams/circuit-breaker-state.puml](diagrams/circuit-breaker-state.puml), [diagrams/finance-idempotency-sequence.puml](diagrams/finance-idempotency-sequence.puml)

## 0. Общие принципы

1. **Hot path: тайм-аут → fallback, без retry.** Бюджет 80 мс не позволяет повторять вызовы; повторы при деградации усиливают нагрузку («retry storm»).
2. **Асинхронные потоки: retry с экспоненциальной задержкой + идемпотентность + DLQ.**
3. **Безопасные значения по умолчанию**: при неопределённости аукцион выбирает no-bid или исключает кампанию, а не рискует перерасходом бюджета.
4. **Bulkhead**: отдельные пулы соединений и потоков на каждую зависимость; отказ одной не исчерпывает ресурсы для других.

## 1. Circuit Breaker для вызовов зависимых сервисов

### 1.1. Состояния

```
CLOSED ──(доля ошибок/тайм-аутов > порога в окне)──► OPEN
OPEN ──(истёк wait duration)──► HALF_OPEN
HALF_OPEN ──(N пробных вызовов успешны)──► CLOSED
HALF_OPEN ──(любой пробный вызов неуспешен)──► OPEN (wait duration × 2, до максимума)
```

- В **OPEN** вызов не выполняется, сразу срабатывает fallback — это экономит время (не ждём тайм-аут) и даёт зависимости восстановиться.
- Тайм-аут считается ошибкой. Медленные вызовы (выше `slow_call_threshold`) учитываются отдельно.

### 1.2. Параметры по зависимостям

| Вызов | Где реализован | Тайм-аут | Окно / порог размыкания | Wait в OPEN | Fallback |
|---|---|---|---|---|---|
| Edge Gateway → Bidding | Envoy (circuit breakers + outlier detection) | 70 мс | 5 подряд 5xx на хост; `max_pending_requests` | 15 с (растёт) | другой под; при отсутствии здоровых — монолит (переход) или 204 |
| Bidding → Redis (сегменты, caps) | клиентская библиотека (Go: `sony/gobreaker`; C++: собственная обёртка) | 5 мс | скользящее окно 1 с, ошибки+тайм-ауты > 20% при ≥ 100 вызовах | 2 с | торговля без сегментов; исключение кампаний со строгим frequency cap |
| Bidding → User Profile (gRPC) | gRPC interceptor | 8 мс | окно 1 с, > 20% | 2 с | то же |
| Bidding → Budget & Pacing (фоновое продление lease) | клиент Budget | 50 мс | окно 10 с, > 50% | 5 с | работа на остатке lease; по истечении — кампания выбывает |
| BFF → Campaign / Finance / Analytics | gRPC interceptor + Resilience4j-подобная логика в Node.js (`opossum`) | 300 мс / 1 с / 2 с | окно 10 с, > 50% при ≥ 20 вызовах | 10 с | кешированный ответ, частичная страница («данные временно недоступны») |
| Finance → Payment Adapter → шлюз | Go (`gobreaker`) | 5 с | окно 60 с, > 30% | 30 с | платёж ставится в очередь `pending`, пользователю — статус «в обработке» |
| Consumers → ClickHouse / PostgreSQL | consumer | 5 с | 5 подряд ошибок | 10 с | пауза потребления партиции (offset не коммитится) |

## 2. Повторные попытки с экспоненциальной задержкой

### 2.1. Где применяются и где нет

| Взаимодействие | Retry | Почему |
|---|---|---|
| DSP → Gateway → Bidding | **нет** | дедлайн 70 мс; повтор выполняет сам DSP следующим запросом |
| Bidding → Redis / Profile | **нет** | бюджет 5–8 мс; fallback дешевле |
| Bidding → Budget (продление lease, фоново) | да, 3 попытки | не на пути запроса |
| Kafka producer (Bidding, Collector, Outbox) | да, встроенные (`retries`, `delivery.timeout.ms=30s`) + idempotent producer | дубли исключены на уровне брокера |
| Kafka consumers → хранилища | да, 5 попыток → DLQ | at-least-once + идемпотентная запись |
| BFF → сервисы (GET) | 1 повтор | только идемпотентные чтения |
| Finance → платёжный шлюз | да, до 6 попыток / до 30 мин, затем ручная сверка | с одним и тем же idempotency key |

### 2.2. Алгоритм: exponential backoff + full jitter

```
delay(n) = random(0, min(cap, base × 2^n))
```

| Поток | base | cap | Попыток | После исчерпания |
|---|---|---|---|---|
| Продление lease | 20 мс | 200 мс | 3 | работа на остатке lease |
| Consumers | 100 мс | 10 с | 5 | DLQ `<topic>.dlq` + алерт |
| Платёжный шлюз | 1 с | 5 мин | 6 | статус `NEEDS_RECONCILIATION`, задача на сверку |

```go
func backoff(attempt int, base, max time.Duration) time.Duration {
    d := base << attempt              // base * 2^attempt
    if d <= 0 || d > max { d = max }  // защита от переполнения и cap
    return time.Duration(rand.Int63n(int64(d)))  // full jitter
}
```

Jitter обязателен: без него все инстансы повторяют одновременно и создают пики нагрузки на восстанавливающуюся зависимость.

**Retry budget**: доля повторных запросов не более 10% от основных на клиента — при превышении повторы отключаются (защита от retry storm).

Повторяются только **идемпотентные** операции или операции с idempotency key; ошибки `4xx` (кроме 408/429) не повторяются.

## 3. Идемпотентность финансовых операций

Kafka и HTTP-ретраи дают семантику **at-least-once**: одно и то же событие показа или win notice может прийти несколько раз (повтор DSP, ребалансировка консьюмера, ретрай продюсера). Списание должно выполниться ровно один раз.

### 3.1. Уровни защиты

| Уровень | Механизм |
|---|---|
| Event Collector | дедупликация на входе: `SET NX dedup:{auction_id}:{type}` в Redis с TTL 24 ч (отсекает 99% дублей дёшево) |
| Kafka producer | `enable.idempotence=true` — брокер отбрасывает дубли ретраев продюсера |
| Ключ события | `event_id` детерминирован: для win/billing — `hash(auction_id, 'bill')`, для клика — `hash(auction_id, 'click', click_ts_bucket)` |
| **Finance consumer (Inbox)** | таблица `processed_events` с первичным ключом `event_id`; вставка и проводка — в одной транзакции |
| **Леджер** | уникальный ключ `(source, source_id)` в `ledger_entries` — повторная проводка невозможна даже при ошибке в коде |
| Пополнения | idempotency key от клиента (`Idempotency-Key` заголовок, UUID) + тот же ключ передаётся платёжному шлюзу |
| Публикация результатов | **Transactional Outbox**: событие `finance.charges` пишется в таблицу `outbox` в той же транзакции, Debezium публикует его в Kafka |

### 3.2. Схема данных (Finance DB, фрагмент)

```sql
CREATE TABLE processed_events (
    event_id      UUID PRIMARY KEY,
    consumer      TEXT NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ledger_entries (
    entry_id      BIGSERIAL PRIMARY KEY,
    tx_id         UUID NOT NULL,              -- одна бизнес-операция = 2+ проводки
    account_id    BIGINT NOT NULL,
    amount_micros BIGINT NOT NULL,            -- дебет < 0, кредит > 0; сумма по tx_id = 0
    currency      CHAR(3) NOT NULL,
    source        TEXT NOT NULL,              -- 'impression_batch', 'click', 'topup', 'refund'
    source_id     TEXT NOT NULL,              -- event_id / batch_id / payment_id
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_id, account_id)
);

CREATE TABLE idempotency_keys (
    key           UUID PRIMARY KEY,
    request_hash  BYTEA NOT NULL,             -- защита от повторного ключа с другим телом
    status        TEXT NOT NULL,              -- IN_PROGRESS / COMPLETED / FAILED
    response      JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### 3.3. Обработка оплачиваемого события

```sql
BEGIN;
INSERT INTO processed_events(event_id, consumer) VALUES ($1, 'finance-billing')
  ON CONFLICT DO NOTHING;
-- если вставлено 0 строк → дубль: COMMIT и коммит offset без проводки
INSERT INTO ledger_entries(...) VALUES (... дебет рекламодателя ...), (... кредит выручки ...);
UPDATE account_balances SET balance_micros = balance_micros - $amount, version = version + 1
  WHERE account_id = $adv;
INSERT INTO outbox(aggregate_id, type, payload) VALUES ($adv, 'ChargeApplied', ...);
COMMIT;
-- после COMMIT — commit offset в Kafka
```

Если процесс упал после `COMMIT`, но до коммита offset, событие придёт повторно и будет отброшено по `processed_events`. Это даёт **effectively-once** обработку поверх at-least-once доставки.

Для снижения нагрузки показы списываются **батчами**: Statistics агрегирует CPM-показы по кампании в окнах по 5 минут и публикует `billing.batches` с детерминированным `batch_id = hash(campaign_id, window_start, partition, offset_range)`, а Finance проводит один батч как одну операцию.

### 3.4. Сверка (reconciliation)

Ежечасно и ежесуточно: сумма списаний в леджере сравнивается с агрегатами ClickHouse и с billing-отчётами DSP. Расхождение > 0,1% → инцидент и корректирующие проводки (никогда не UPDATE/DELETE проводок — только сторно).

## 4. Резервные стратегии (fallback)

| Ситуация | Стратегия | Влияние на бизнес |
|---|---|---|
| Campaign Service / CDC недоступны | Аукцион работает на **последнем снапшоте кампаний** в памяти; при старте нового пода — снапшот из S3 (сохраняется каждые 5 мин). При отставании > 5 мин кампании, изменённые в монолите/Campaign, но не дошедшие до снапшота, могут торговаться по старым правилам → для пауз кампаний есть «быстрый канал» (Redis `paused:{campaign_id}`) | минимальное |
| Redis (сегменты/caps) недоступен | Торговля по контекстному таргетингу (гео, устройство, площадка); кампании с обязательными сегментами/строгими caps исключаются | снижение ставок на часть трафика, без нарушения SLA |
| Budget & Pacing недоступен | Работа на текущих lease; по истечении lease кампания исключается | ограниченный перерасход (≤ сумма lease) либо недорасход |
| Профиль пользователя неизвестен | **Дефолтная ставка**: базовая ставка кампании × коэффициент для «холодного» пользователя (настраивается, по умолчанию 0,7) | ниже win rate для неизвестных |
| pCTR-модель/сложные правила не успевают | **Кешированная ставка**: последний рассчитанный eCPM для пары (кампания, сегмент площадки), обновляется каждые 30 с | небольшая потеря точности |
| Осталось < 10 мс до дедлайна | немедленный **no-bid (204)** | потерян один аукцион, но SLA не нарушен |
| Kafka недоступна | События — в локальный ограниченный буфер (память → диск, до 15 мин); ответ DSP и редирект пользователя не блокируются. При переполнении буфера — алерт и отказ от неоплачиваемых логов (no-bid логи), оплачиваемые события сохраняются | задержка статистики |
| ClickHouse недоступен | Консьюмеры ставят партиции на паузу, данные остаются в Kafka (retention 7 дней), догоняют после восстановления | задержка отчётов |
| Перегрузка (трафик > мощности) | Load shedding в Gateway (adaptive concurrency) → 204 для части запросов; приоритет — партнёрам с SLA | контролируемая потеря части трафика вместо деградации всех |
| Платёжный шлюз недоступен | Пополнение в статусе «в обработке», повтор с backoff; кампании продолжают работать на текущем балансе | задержка пополнения |

## 5. Дополнительные паттерны

| Паттерн | Применение |
|---|---|
| **Timeout budget / deadline propagation** | Gateway проставляет `x-request-deadline`, gRPC-вызовы получают остаток дедлайна |
| **Bulkhead** | отдельные пулы соединений Redis/gRPC; отдельные Kafka-продюсеры для оплачиваемых и логовых событий; отдельные node pool в Kubernetes для hot path |
| **Load shedding** | adaptive concurrency в Envoy; приоритизация по партнёрам |
| **Health checks и readiness** | под не получает трафик, пока не загружен снапшот |
| **Graceful shutdown** | `preStop` → снятие с балансировки → доработка запросов (≤ 5 с) → flush Kafka-буфера |
| **Dead Letter Queue** | `<topic>.dlq` для сообщений, не обработанных после ретраев, с алертом и инструментом переотправки |
| **Chaos testing** | ежемесячно на staging (и ежеквартально в проде в низкий трафик): убийство подов, отключение Redis, задержка сети к Kafka |
