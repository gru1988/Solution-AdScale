# Архитектура потоковой обработки событий (Kafka)

Диаграмма: [diagrams/kafka-topology.puml](diagrams/kafka-topology.puml). Выбор технологии: [ADR-003](../Task1/adr/ADR-003-event-streaming-technology.md).

## 1. Кластер

| Параметр | 3 месяца | 1 год | Обоснование |
|---|---|---|---|
| Брокеры | 3 (по одному в AZ) | 6 на регион | RF = 3 с размещением по AZ (`broker.rack`) |
| Режим метаданных | KRaft | KRaft | без ZooKeeper — меньше компонентов |
| `default.replication.factor` | 3 | 3 | переживаем потерю брокера/AZ |
| `min.insync.replicas` | 2 | 2 | с `acks=all` — запись подтверждена минимум двумя репликами |
| `unclean.leader.election.enable` | false | false | не теряем подтверждённые данные |
| Schema Registry | 2 инстанса | 2 на регион | совместимость схем |
| Межрегиональная репликация | — | MirrorMaker 2 | события → центральный регион для Finance/Analytics; `campaign.changes` → регионы |
| Архив | Kafka Connect S3 Sink (Parquet) | то же | хранение сырых событий 1 год дёшево, источник для пересчёта |

## 2. Топики

| Топик | Продюсер | Ключ | Партиции (3 мес. → 1 год) | Retention | Cleanup | Назначение |
|---|---|---|---|---|---|---|
| `rtb.auction.results.v1` | Bidding | `campaign_id` (для no-bid — `request_id`) | 48 → 96 | 48 ч | delete | факт каждого аукциона: ставка/no-bid, цена, latency |
| `rtb.ad.impressions.v1` | Event Collector | `campaign_id` | 24 → 48 | 7 дней | delete | показы, win/billing notice (оплачиваемые) |
| `rtb.ad.clicks.v1` | Event Collector | `campaign_id` | 12 → 24 | 7 дней | delete | клики (оплачиваемые для CPC) |
| `campaign.changes.v1` | Campaign (Outbox + Debezium); на переходе — CDC таблиц монолита | `campaign_id` | 12 | бессрочно | **compact** | последнее состояние каждой кампании; снапшот для Bidding/Analytics |
| `partner.changes.v1` | Partner Config | `partner_id` | 3 | бессрочно | compact | конфигурация партнёров |
| `budget.spend.v1` | Budget & Pacing | `campaign_id` | 24 | 3 дня | delete | расход по кампаниям (минутные дельты), сигналы pacing |
| `billing.batches.v1` | Statistics | `campaign_id` | 24 | 14 дней | delete | агрегированные оплачиваемые события за 5 мин → Finance |
| `finance.charges.v1` | Finance (Outbox) | `advertiser_id` | 12 | 30 дней | delete | проведённые списания и пополнения |
| `finance.balance.v1` | Finance (Outbox) | `advertiser_id` | 12 | бессрочно | compact | текущее состояние баланса / `BalanceExhausted` |
| `payments.events.v1` | Payment Adapter | `payment_id` | 6 | 30 дней | delete | `PaymentReceived`, `PaymentReversed` |
| `*.dlq` | любой консьюмер | исходный | 6 | 14 дней | delete | необработанные сообщения после ретраев |

**Именование:** `<домен>.<сущность>.<версия>`. Несовместимое изменение схемы → новый топик `v2` и период параллельной публикации.

**Почему ключ `campaign_id`:** порядок событий кампании сохраняется в партиции, Statistics и Budget агрегируют по кампании локально, без shuffle. Для крупных кампаний в `auction.results` — составной ключ `campaign_id#bucket(0..7)` против перекоса.

**Оценка объёма (1 год):** `auction.results` ≈ 50 000 × 400 Б ≈ 20 МБ/с → ≈ 1,7 ТБ/сутки, × RF 3 ≈ 5,2 ТБ/сутки, при retention 48 ч ≈ 10,4 ТБ; показы ≈ 7 500 × 500 Б ≈ 3,8 МБ/с → ≈ 0,33 ТБ/сутки × 3 × 7 дней ≈ 6,9 ТБ. Для экономии — tiered storage (S3) для данных старше 12 ч.

## 3. Схема событий: Avro

### Почему Avro, а не JSON

| Критерий | Avro + Schema Registry | JSON |
|---|---|---|
| Размер сообщения | в 3–5 раз меньше (бинарный, без имён полей) | большой |
| Скорость (де)сериализации | высокая | ниже |
| Контракт и эволюция схем | Schema Registry, проверка совместимости при публикации | нет гарантий, ошибки в рантайме |
| Типы (timestamp, decimal, enum) | строгие | строки и числа |
| Читаемость при отладке | нужен инструмент (kcat + registry) | удобно |
| Интеграция с ClickHouse/Kafka Connect | нативная (`AvroConfluent`) | да |

На 50 000 msg/s экономия трафика, дисков и CPU существенна, а строгая схема защищает 6+ потребителей от ломающих изменений. Режим совместимости — **BACKWARD** (новые поля — только с default). JSON используется только во внешних API (OpenRTB, кабинет).

### Общий конверт

Каждое событие содержит обязательные поля: `event_id` (UUID v7, идемпотентность), `event_type`, `event_ts`, `producer`, `schema_version`, `trace_id` (сквозная трассировка), `region`.

### Пример: показ

```json
{
  "type": "record",
  "name": "AdImpression",
  "namespace": "io.adscale.events.v1",
  "fields": [
    {"name": "event_id",        "type": {"type": "string", "logicalType": "uuid"}},
    {"name": "event_ts",        "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "trace_id",        "type": ["null", "string"], "default": null},
    {"name": "region",          "type": "string"},
    {"name": "auction_id",      "type": "string"},
    {"name": "partner_id",      "type": "string"},
    {"name": "campaign_id",     "type": "string"},
    {"name": "advertiser_id",   "type": "string"},
    {"name": "creative_id",     "type": "string"},
    {"name": "user_id",         "type": ["null", "string"], "default": null},
    {"name": "price_micros",    "type": "long", "doc": "цена клиринга из ${AUCTION_PRICE}"},
    {"name": "currency",        "type": "string", "default": "USD"},
    {"name": "billing_model",   "type": {"type": "enum", "name": "BillingModel", "symbols": ["CPM", "CPC"]}},
    {"name": "source",          "type": {"type": "enum", "name": "ImpSource", "symbols": ["BURL", "NURL", "PIXEL"]}},
    {"name": "geo_country",     "type": ["null", "string"], "default": null},
    {"name": "device_type",     "type": ["null", "string"], "default": null}
  ]
}
```

### Пример: клик

```json
{
  "type": "record",
  "name": "AdClick",
  "namespace": "io.adscale.events.v1",
  "fields": [
    {"name": "event_id",      "type": {"type": "string", "logicalType": "uuid"}},
    {"name": "event_ts",      "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "trace_id",      "type": ["null", "string"], "default": null},
    {"name": "auction_id",    "type": "string"},
    {"name": "campaign_id",   "type": "string"},
    {"name": "advertiser_id", "type": "string"},
    {"name": "creative_id",   "type": "string"},
    {"name": "user_id",       "type": ["null", "string"], "default": null},
    {"name": "cpc_micros",    "type": ["null", "long"], "default": null},
    {"name": "ip_hash",       "type": "string", "doc": "для антифрода, без PII"},
    {"name": "user_agent_hash", "type": "string"}
  ]
}
```

`auction.results` описан в [Task2/bidding-service.md](../Task2/bidding-service.md#53-исходящее-событие-auctionresults-avro).

## 4. Продюсеры

| Продюсер | Настройки | Комментарий |
|---|---|---|
| Bidding (`auction.results`) | `acks=all`, `enable.idempotence=true`, `linger.ms=5`, `batch.size=256KB`, `compression.type=lz4`, `max.block.ms=0` | `max.block.ms=0` — продюсер никогда не блокирует обработку запроса; при переполнении буфера — локальный spill |
| Event Collector (оплачиваемые) | `acks=all`, `enable.idempotence=true`, `linger.ms=1`, `delivery.timeout.ms=150` (win/bill) / `50` (клики) | отдельный продюсер (bulkhead). **Ответ DSP отправляется только после подтверждения записи**; метка дедупликации в Redis ставится после подтверждения, а не до него ([Task2/reliability.md, 3.2](../Task2/reliability.md#32-event-collector-подтверждение-только-после-надёжной-фиксации)) |
| Outbox (Debezium) | exactly-once source connector | гарантированная публикация после коммита транзакции |

## 5. Группы потребителей

| Consumer group | Топики | Что делает | Куда пишет | Параллелизм | Семантика |
|---|---|---|---|---|---|
| `statistics-aggregator` | impressions, clicks, auction.results | дедупликация, антифрод-фильтры, минутные агрегаты, реалтайм-счётчики, батчи биллинга | ClickHouse, Redis (mgmt), `billing.batches` | = партиций | at-least-once + идемпотентные вставки (`ReplacingMergeTree` по `event_id`) |
| `clickhouse-raw-loader` | auction.results, impressions, clicks | сырые события батчами (100 000 строк или 1 с) | ClickHouse `events_raw` | 12–24 пода | at-least-once, дедупликация `ReplacingMergeTree` |
| `budget-pacing` | impressions, clicks, auction.results (только BID) | фактический расход, корректировка lease и pacing | Redis (Budget Store), `budget.spend` | = партиций | at-least-once, идемпотентно по `event_id` (множество обработанных за окно) |
| `profile-updater` | impressions, clicks | frequency caps, обновление сегментов | Redis / Aerospike (Profile) | 12 | at-least-once (caps допускают небольшую погрешность) |
| `finance-billing` | billing.batches, clicks (CPC), payments.events | проводки в леджер | Finance DB | 12 | **effectively-once** (Inbox + уникальные ключи) |
| `bidding-snapshot-<pod>` | campaign.changes, partner.changes | обновление L1 снапшота | память пода | 1 на под (каждый под читает всё) | — |
| `analytics-dictionaries` | campaign.changes | справочники кампаний для отчётов | ClickHouse dictionaries | 1–2 | — |
| `fraud-detector` (план) | impressions, clicks | выявление аномалий | `fraud.signals` | 6 | — |
| `s3-archiver` | все событийные топики | архив Parquet | S3 | Kafka Connect | at-least-once |

Правила:
- Offset коммитится **только после** успешной записи в целевое хранилище.
- Ошибка обработки → retry с backoff (до 5 попыток) → `<topic>.dlq` с заголовками причины; DLQ разбирается инструментом переотправки.
- Ребалансировка — `CooperativeStickyAssignor`, статическое членство (`group.instance.id`) для подов Kubernetes, чтобы рестарты не вызывали лавину ребалансировок.

## 6. Политика хранения (retention)

| Категория | Retention | Почему |
|---|---|---|
| Лог аукционов | 48 ч (+ tiered storage) | большой объём; основное хранение — ClickHouse (90 дней сырые) и S3 (1 год) |
| Оплачиваемые события (показы, клики) | 7 дней | окно для replay после аварии ClickHouse/Finance и разбора инцидентов (выходные + запас) |
| Финансовые события | 14–30 дней | сверки и повторная обработка за расчётный период |
| Состояния (`campaign.changes`, `finance.balance`, `partner.changes`) | compaction, бессрочно | последнее состояние ключа = снапшот для новых потребителей |
| DLQ | 14 дней | время на разбор |
| Архив в S3 | 1 год (сырые), 5 лет (финансовые агрегаты) | пересчёт статистики, аудит, требования к финансовой отчётности |

## 7. Мониторинг

| Метрика | Алерт |
|---|---|
| Consumer lag (сообщения и секунды) по группам | `finance-billing` > 5 мин, `statistics-aggregator` > 60 с, `bidding-snapshot` > 30 с |
| Under-replicated partitions | > 0 дольше 5 мин |
| Offline partitions | > 0 — critical |
| Ошибки продюсеров, заполненность буфера Bidding/Collector | > 50% буфера |
| Сообщения в DLQ | > 0 для финансовых топиков, > 100/ч для остальных |
| Диск брокеров | > 70% |
