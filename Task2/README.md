# Задание 2. Проектирование RTB и интеграции с DSP

## Что сделано

| Артефакт | Содержание |
|---|---|
| [bidding-service.md](bidding-service.md) | Границы Bidding Service, внешний (OpenRTB) и внутренний (gRPC) API, зависимости и fallback, внутренние компоненты, модель данных (снапшот в памяти, ключи Redis, событие `auction.results`), бюджет времени, масштабирование |
| [interaction.md](interaction.md) | Сравнение REST / gRPC / асинхронного подхода, обоснование gRPC для Bidding и Campaign и Kafka для событий, матрица взаимодействий, выбор API Gateway |
| [api-gateway.md](api-gateway.md) | Edge RTB Gateway на Envoy: маршрутизация (canary, shadow), аутентификация (mTLS, allowlist, ключи), rate limiting, circuit breaker и outlier detection, мониторинг latency, runbook подключения партнёра |
| [reliability.md](reliability.md) | Circuit Breaker по зависимостям, retry с экспоненциальной задержкой и jitter, идемпотентность финансовых операций (Inbox, Ledger, Outbox), резервные стратегии |
| [diagrams/](diagrams/) | Component: Bidding, Edge Gateway, взаимодействие; Sequence: bid request, поток событий, обновление кампании, идемпотентное списание; State: circuit breaker |

## Ключевые решения

1. **Bidding Service = Ad Server + Auction Engine** в одном stateless-процессе: один ограниченный контекст, без лишнего сетевого перехода. Данные кампаний — в памяти (copy-on-write снапшот, обновляемый из Kafka), бюджеты — локальные lease + Redis. **PostgreSQL на пути запроса нет.**
2. **Протоколы**: OpenRTB 2.6/HTTPS — наружу; gRPC — внутренние синхронные вызовы (дедлайны, Protobuf, мультиязычность); Kafka + Avro — события; REST — кабинет.
3. **API Gateway нужен — Envoy**: два отдельных развёртывания (RTB и кабинет). На RTB-пути — mTLS, local rate limit с ответом 204, adaptive concurrency, outlier detection, **без retry**, route timeout 70 мс.
4. **Надёжность**: на hot path — тайм-аут → fallback/no-bid; в асинхронных потоках — retry с full jitter, DLQ и идемпотентные потребители; финансы — effectively-once через `processed_events` + уникальные ключи леджера + Transactional Outbox.

## Бюджет времени (P95)

| Участок | мс |
|---|---|
| Edge Gateway | 2–3 |
| Bidding Service (парсинг → ответ) | 15–25 |
| Сеть DSP ↔ AdScale | 10–25 |
| **Итого** | **≈ 30–55** при SLA ≤ 80 |
