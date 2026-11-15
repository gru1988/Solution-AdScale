# Solution-AdScale

Проектная работа №2: эволюция архитектуры SaaS-платформы цифровой рекламы **AdScale** под интеграцию с крупной DSP-площадкой.

## Контекст в двух словах

| Параметр | Сейчас (AS-IS) | Через 3 месяца (Transition) | Через год (TO-BE) |
|---|---|---|---|
| Пиковая нагрузка | 15 000 RPS | ≥ 18 000 RPS (+ запас до 25 000) | 50 000 RPS |
| P95 аукциона | 110–120 мс | ≤ 100 мс (≤ 80 мс для нового DSP) | ≤ 80 мс |
| Доступность | не измеряется | 99.5% hot path | 99.9% (SLA) |
| Архитектура | гибридный монолит, одна PostgreSQL | монолит + выделенный Bidding Service, Kafka, Redis | микросервисы, event-driven, мультирегион |
| Деплой | вручную | Docker + CI/CD для hot path | Kubernetes, GitOps, canary |

## Структура репозитория

```
Solution-AdScale/
├── Task1/  Архитектурная диагностика и целевое видение
│   ├── README.md          краткое описание задания и решения
│   ├── AS-IS.md           анализ текущей архитектуры
│   ├── drivers.md         архитектурные драйверы
│   ├── TO-BE.md           целевая архитектура и дорожная карта
│   ├── diagrams/          C4 (AS-IS, Transition, TO-BE) в PlantUML
│   └── adr/               ADR-001, ADR-002, ADR-003
├── Task2/  Проектирование RTB и интеграции с DSP
│   ├── README.md
│   ├── bidding-service.md спецификация сервиса ставок
│   ├── interaction.md     протоколы и паттерны взаимодействия
│   ├── api-gateway.md     дизайн API Gateway для DSP
│   ├── reliability.md     паттерны надёжности
│   └── diagrams/          sequence / component / state
└── Task3/  Данные, масштабирование и отказоустойчивость
    ├── README.md
    ├── database-strategy.md  Database per Service
    ├── scaling.md            репликация, шардирование, CQRS
    ├── caching.md            Redis и локальные кеши
    ├── event-streaming.md    архитектура Kafka
    ├── failover.md           RPO/RTO, бэкапы, 12-factor
    └── diagrams/             диаграммы данных
```

## Ключевые решения

1. **Strangler Fig** поверх монолита вместо Big Bang-переписывания ([ADR-001](Task1/adr/ADR-001-evolution-strategy.md)).
2. **Сервис ставок (Bidding Service) выделяется первым**: именно он определяет SLA перед DSP ([ADR-002](Task1/adr/ADR-002-bidding-service-first.md)).
3. **Apache Kafka** для потока событий показов, кликов и ставок ([ADR-003](Task1/adr/ADR-003-event-streaming-technology.md)).
4. **Hot path без синхронных обращений к реляционной БД**: кампании и бюджеты читаются из памяти процесса и Redis, события пишутся асинхронно.
5. **Разделение OLTP и OLAP**: аналитика уходит с production-PostgreSQL сначала на read replica, затем в ClickHouse.

## Как смотреть диаграммы

Все диаграммы написаны на PlantUML (C4-PlantUML). Их можно открыть:
- в IDE с плагином PlantUML (IntelliJ, VS Code);
- на https://www.plantuml.com/plantuml, вставив содержимое `.puml`-файла.
