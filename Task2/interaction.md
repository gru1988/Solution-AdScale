# Схема взаимодействия и выбор протоколов

Диаграммы:
- [diagrams/interaction-component.puml](diagrams/interaction-component.puml) — компоненты и протоколы
- [diagrams/bid-request-sequence.puml](diagrams/bid-request-sequence.puml) — обработка bid request
- [diagrams/event-flow-sequence.puml](diagrams/event-flow-sequence.puml) — win, показ, клик → статистика и финансы
- [diagrams/campaign-update-sequence.puml](diagrams/campaign-update-sequence.puml) — изменение кампании → аукцион

## 1. Три класса взаимодействий

| Класс | Требование | Выбор |
|---|---|---|
| **Внешнее RTB** (DSP ↔ AdScale) | отраслевой стандарт, совместимость с любым партнёром, ≤ 80 мс | **OpenRTB 2.5/2.6 поверх HTTPS (HTTP/1.1 keep-alive или HTTP/2), JSON**; Protobuf-вариант OpenRTB — по договорённости с партнёром |
| **Внутреннее синхронное** (нужен ответ в рамках запроса) | минимальная задержка и накладные расходы, строгий контракт между языками C++/Go/Python/Node.js, дедлайны | **gRPC (HTTP/2 + Protobuf)** |
| **Внутреннее асинхронное** (факты, которые обрабатываются позже и несколькими потребителями) | высокий поток, надёжность, replay, развязка | **Apache Kafka + Avro (Schema Registry)** |
| **Публичный API кабинета** (браузер ↔ backend) | удобство фронтенда, кешируемость, простая отладка | **REST/JSON** через API Gateway |

## 2. Сравнение вариантов для внутреннего взаимодействия

| Критерий | REST/JSON | gRPC/Protobuf | Асинхронно (Kafka) |
|---|---|---|---|
| Задержка сериализации и размер сообщения | выше (текст, JSON-парсинг 0,1–1 мс на запрос) | в 3–10 раз меньше размер, быстрая (де)сериализация | не важна для hot path, но добавляет задержку доставки (мс–с) |
| Транспорт | HTTP/1.1 (обычно), соединение на запрос в пуле | HTTP/2, мультиплексирование, долгоживущие соединения | TCP к брокеру, батчи |
| Контракт | OpenAPI (опционально, часто нарушается) | `.proto` обязателен, кодогенерация для C++/Go/Python/Node.js | схема Avro в Schema Registry |
| Дедлайны и отмена | вручную | **встроенные deadline propagation и cancellation** | нет (асинхронно) |
| Стриминг | нет (SSE/WebSocket отдельно) | server/client/bidirectional streaming | естественный |
| Нужен немедленный ответ | да | да | **нет** |
| Связность во времени | обе стороны должны быть доступны | обе стороны должны быть доступны | **нет**: продюсер не зависит от потребителя |
| Наблюдаемость и отладка | проще (curl, браузер) | нужны grpcurl, reflection | lag, offsets |

## 3. Обоснование выбора по взаимодействиям

### 3.1. Почему gRPC для Bidding и Campaign

1. **Bidding → User Profile / Budget** находится на hot path: каждые сэкономленные 0,5–1 мс важны при бюджете 80 мс. Protobuf и HTTP/2-мультиплексирование дают меньше задержки и CPU, чем JSON поверх HTTP/1.1.
2. **Дедлайны**: gRPC передаёт оставшийся дедлайн вниз по цепочке. Если у Bidding осталось 8 мс, Profile Service не будет работать дольше — это прямо реализует тайм-аут-бюджет.
3. **Мультиязычный стек**: Bidding (C++/Go), Campaign (Go), Analytics (Python), BFF (Node.js) получают типизированных клиентов из одного `.proto`; контракт ломается на этапе компиляции, а не в проде.
4. **Campaign → Bidding (снапшот при старте)**: server-streaming позволяет отдать десятки тысяч кампаний потоком без пагинации и без нагрузки на БД.
5. Почему **не REST** для Bidding и Campaign: на 50 000 RPS JSON-сериализация и отсутствие встроенных дедлайнов означают лишние миллисекунды и сложность. REST остаётся там, где он удобнее: внешнее API кабинета и OpenRTB (стандарт).
6. Почему **не асинхронно**: Bidding нужен ответ здесь и сейчас — асинхронная модель не подходит для запроса-ответа в 80 мс. Но изменения кампаний доставляются в Bidding **асинхронно** через Kafka (`campaign.changes`), чтобы аукцион не зависел от доступности Campaign Service.

### 3.2. Почему Kafka для событий кликов и показов

1. **Нет необходимости в синхронном ответе**: DSP не ждёт, пока событие запишется в статистику и финансы.
2. **Высокий поток** (десятки тысяч событий/с) — Kafka держит его на нескольких брокерах, а потребители пишут в хранилища батчами (ClickHouse, PostgreSQL), снимая write-нагрузку.
3. **Много потребителей**: одно событие показа нужно Statistics, Finance, Budget & Pacing, User Profile (frequency caps), Analytics и антифроду. С Kafka каждый читает со своей скоростью через свою consumer group.
4. **Надёжность и replay**: при сбое потребителя события не теряются (retention 7 дней), их можно перечитать и пересчитать агрегаты.
5. **Порядок по ключу** `campaign_id` упрощает агрегаты и учёт бюджета.
6. Подробно: [ADR-003](../Task1/adr/ADR-003-event-streaming-technology.md), [Task3/event-streaming.md](../Task3/event-streaming.md).

## 4. Матрица взаимодействий

| # | Источник → Получатель | Протокол | Стиль | Тайм-аут | Гарантия | Fallback |
|---|---|---|---|---|---|---|
| 1 | DSP → Edge RTB Gateway | HTTPS, OpenRTB 2.6 JSON | sync | `tmax` партнёра (обычно 100 мс) | — | 204 no-bid |
| 2 | Edge Gateway → Bidding | HTTP/2 (h2c внутри кластера) | sync | 70 мс (route timeout) | — | 204 no-bid |
| 3 | Bidding → User Profile | gRPC | sync | 8 мс | best effort | торговля без сегментов |
| 4 | Bidding → Budget & Pacing | gRPC (фоново, продление lease) | async to request | 50 мс | at-least-once с backoff | остаток lease |
| 5 | Bidding → Kafka `auction.results` | Kafka producer | async | — | at-least-once, idempotent producer | локальный буфер |
| 6 | DSP → Event Collector (win/bill) | HTTPS GET (`nurl`/`burl`) | sync, быстрый 200/204 | 200 мс | — | DSP повторяет по своей политике; дедупликация по `auction_id` |
| 7 | Пользователь → Event Collector (показ/клик) | HTTPS (пиксель, 302-редирект) | sync | 100 мс | — | редирект всегда, даже если Kafka недоступна (событие в локальный буфер) |
| 8 | Event Collector → Kafka | Kafka producer | async | — | `acks=all` | локальный буфер |
| 9 | Kafka → Statistics / Finance / Budget / Profile / Analytics | Kafka consumer groups | async | — | at-least-once + идемпотентность потребителя | DLQ |
| 10 | Campaign → Kafka `campaign.changes` | Transactional Outbox + Debezium | async | — | at-least-once, compacted topic | — |
| 11 | Bidding → Campaign (старт) | gRPC server-streaming `StreamSnapshot` | sync | 30 с | — | снапшот из S3 |
| 12 | Dashboard → API Gateway → BFF | HTTPS REST/JSON | sync | 2 с | — | частичный ответ, кеш |
| 13 | BFF → Campaign / Finance / Analytics | gRPC | sync | 300 мс–2 с | retry для идемпотентных GET | кеш BFF, деградация виджетов |
| 14 | Finance → Payment Adapter → Платёжный шлюз | gRPC → HTTPS REST + webhooks | sync + callback | 5 с | idempotency key | повтор по расписанию, ручная сверка |
| 15 | Edge Gateway ← Partner Config | xDS (Envoy control plane) | push конфигурации | — | — | последняя применённая конфигурация |

## 5. Нужен ли API Gateway? Да — какой именно

### Зачем

- **Единая точка входа для DSP** с аутентификацией, rate limiting по партнёру, тайм-аутами, circuit breaking и метриками latency на партнёра — без дублирования этой логики в Bidding Service.
- **Фасад Strangler Fig**: маршрутизация трафика между монолитом и новым Bidding Service по партнёру или проценту (canary, shadow/mirroring).
- **Мультиплатформенность**: новый партнёр — новая запись конфигурации (маршрут, квота, ключи), а не изменение кода.
- **Публичный API кабинета** — JWT/OAuth2, CORS, лимиты, маршрутизация к BFF.

### Какой

| Вариант | Latency overhead | Rate limit / CB / outlier detection | gRPC / HTTP/2 | Traffic mirroring (shadow) | Сложность / стоимость |
|---|---|---|---|---|---|
| **Envoy (Edge RTB) + Envoy Gateway (API)** | **< 1 мс** (C++) | встроены: local/global rate limit, circuit breakers, outlier detection, adaptive concurrency | нативно | **да** (`request_mirror_policies`) | open-source; одна технология на оба входа; хорошо ложится на Kubernetes Gateway API |
| Kong (Nginx + Lua) | 1–3 мс | плагины (часть — Enterprise) | да | ограниченно | open-source + Enterprise |
| NGINX | < 1 мс | базовый rate limit, без outlier detection в OSS | частично | `mirror` | OSS, остальное — Plus |
| Облачный API Gateway (AWS/GCP) | 5–20 мс | да | частично | нет | дорого на 50 000 RPS (оплата за запрос), vendor lock-in |
| Spring Cloud Gateway | 2–5 мс (JVM) | через Resilience4j | да | вручную | новый стек для команды |

**Решение: Envoy.** Два независимых развёртывания одной технологии:

1. **Edge RTB Gateway** — только трафик DSP, оптимизирован под latency, без «тяжёлых» плагинов; масштабируется вместе с Bidding.
2. **API Gateway (Envoy Gateway)** — трафик кабинета и публичного API, JWT/OAuth2, CORS.

Разделение защищает hot path: всплеск запросов от кабинета или ошибка в конфигурации публичного API не влияют на RTB. Детальный дизайн — в [api-gateway.md](api-gateway.md).
