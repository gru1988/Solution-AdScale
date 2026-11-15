# Задание 3. Данные, масштабирование и отказоустойчивость

## Что сделано

| Артефакт | Содержание |
|---|---|
| [database-strategy.md](database-strategy.md) | Database per Service: тип хранилища и обоснование для каждого сервиса, владение данными, согласованность (Outbox, Saga), план миграции с общей PostgreSQL |
| [scaling.md](scaling.md) | Оценка нагрузки, master–slave репликация (где и в каком режиме), ключи шардирования, read replicas для аналитики, CQRS, горизонтальное масштабирование |
| [caching.md](caching.md) | Уровни кеша (L1 в процессе, Redis hot path / management, CDN), TTL и инвалидация по каждому типу данных, защита от stampede, прогрев |
| [event-streaming.md](event-streaming.md) | Кластер Kafka, топики (ключи, партиции, retention), схемы Avro, продюсеры, группы потребителей, мониторинг |
| [failover.md](failover.md) | RPO/RTO по сервисам, резервное копирование, сценарии отказов, мультирегион, соответствие 12-factor |
| [diagrams/](diagrams/) | Архитектура данных, репликация и шардирование, CQRS, кеширование (sequence), топология Kafka, мультирегиональный failover |

## Сводка решений

| Сервис | Хранилище | Масштабирование | RPO / RTO |
|---|---|---|---|
| Bidding | память процесса (L1) + Redis Cluster | stateless, HPA; Redis шардирован по `campaign_id` / `user_id` | — / ≤ 30 с |
| Campaign | PostgreSQL + Patroni | master–slave (sync + async), read replica; без шардирования; CQRS | ≤ 5 с / ≤ 1 мин |
| Statistics | Kafka → ClickHouse + Redis-счётчики | ClickHouse шарды по `cityHash64(campaign_id)` × 2 реплики | ≤ 1 мин / ≤ 30 мин |
| Analytics | ClickHouse (выделенные реплики) | read replicas; на переходе — PostgreSQL read replica | ≤ 24 ч / ≤ 4 ч |
| Finance | PostgreSQL, синхронная репликация | master–slave (ANY 1 sync), партиционирование леджера по месяцам; шардирование по `advertiser_id` — в резерве | 0 / ≤ 15 мин |

Главный принцип: **hot path читает только из памяти и Redis, пишет только в Kafka**; реляционные БД обслуживают управление и деньги, ClickHouse — всё аналитическое.
