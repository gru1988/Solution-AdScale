# Спецификация сервиса ставок (Bidding Service)

Диаграммы: [diagrams/bidding-component.puml](diagrams/bidding-component.puml), [diagrams/bid-request-sequence.puml](diagrams/bid-request-sequence.puml)

## 1. Назначение и границы

**Bidding Service** принимает bid request от DSP-партнёров (через Edge RTB Gateway), подбирает подходящие кампании, проводит внутренний аукцион и возвращает bid response или no-bid в пределах жёсткого тайм-аута.

Сервис выделяется из монолита первым ([ADR-002](../Task1/adr/ADR-002-bidding-service-first.md)) и объединяет логику **Ad Server** и **Auction Engine**.

### Входит в границы (ответственность)

| Функция | Пришло из монолита |
|---|---|
| Парсинг и валидация OpenRTB 2.5/2.6 (`imp`, `site/app`, `device`, `user`, `tmax`, `bidfloor`) | Ad Server |
| Идентификация пользователя (cookie/device ID → профиль) | Ad Server |
| Подбор кандидатов по таргетингу: гео, устройство, ОС, площадка, сегменты, расписание, frequency caps | Ad Server |
| Фильтрация по бюджету и pacing (локальный lease) | Auction Engine |
| Бизнес-правила, расчёт eCPM (CPM/CPC × pCTR), floor price, выбор победителя и цены | Auction Engine |
| Формирование bid response: `price`, `adm` (из предрендеренного шаблона), `nurl`, `burl`, `crid`, `adomain` | Auction Engine + Delivery (шаблоны) |
| Публикация факта аукциона в Kafka | новое |
| Поддержание снапшота кампаний в памяти | новое |

### Не входит в границы

| Функция | Где живёт |
|---|---|
| CRUD кампаний, креативов, таргетинга | Campaign Service (на переходе — монолит) |
| Учёт показов, кликов, win notice | Event Collector → Kafka → Statistics |
| Списание денег, счета, балансы | Finance Service |
| Глобальный учёт расхода и выдача lease бюджета | Budget & Pacing Service (на переходе — модуль внутри Bidding + Redis) |
| Хранение и обновление профилей пользователей | User Profile Service (на переходе — Redis, обновляемый консьюмером) |
| Аутентификация DSP, rate limiting, TLS | Edge RTB Gateway |
| Хранение и раздача креативов | Creative Delivery + CDN |

## 2. API

### 2.1. Внешний (через Edge RTB Gateway)

| Метод | Путь | Описание | Ответ |
|---|---|---|---|
| `POST` | `/openrtb/2.6/bid` | Bid request в формате OpenRTB. Заголовки: `x-openrtb-version`, `x-partner-id` (проставляет Gateway после аутентификации), `x-request-deadline` (абсолютный дедлайн) | `200 OK` + BidResponse; `204 No Content` — no-bid; `400` — невалидный запрос |

Пример ответа (сокращённо):

```json
{
  "id": "req-8c1f",
  "bidid": "b-5512a",
  "cur": "USD",
  "seatbid": [{
    "seat": "adscale",
    "bid": [{
      "id": "1", "impid": "1",
      "price": 1.42,
      "adid": "cmp-1042",
      "crid": "cr-77",
      "adomain": ["brand.example"],
      "adm": "<div>…</div>",
      "nurl": "https://ev.adscale.io/win?a=${AUCTION_ID}&p=${AUCTION_PRICE}&t=…",
      "burl": "https://ev.adscale.io/bill?a=${AUCTION_ID}&p=${AUCTION_PRICE}&t=…",
      "w": 300, "h": 250
    }]
  }]
}
```

Ссылки `nurl`/`burl` указывают на **Event Collector**, параметр `t` — подписанный токен (HMAC) с `auction_id`, `campaign_id`, `bid_price`, `expires_at` для защиты от подделки событий.

### 2.2. Внутренний (gRPC)

Используется другими внутренними источниками трафика (например, собственные SSP-адаптеры, тестовый трафик) и для внутренних проверок. Контракт:

```protobuf
syntax = "proto3";
package adscale.bidding.v1;

service BiddingService {
  // Принять решение о ставке. Дедлайн задаётся через gRPC deadline.
  rpc Bid(BidRequest) returns (BidResponse);
}

message BidRequest {
  string request_id   = 1;
  string partner_id   = 2;
  repeated Impression imps = 3;
  Device device       = 4;
  User   user         = 5;
  Context context     = 6;   // site/app, страна, язык
  uint32 tmax_ms      = 7;
}

message BidResponse {
  string request_id   = 1;
  repeated Bid bids   = 2;   // пусто = no-bid
  NoBidReason no_bid_reason = 3;
}

message Bid {
  string imp_id = 1; string campaign_id = 2; string creative_id = 3;
  int64  price_micros = 4; string adm = 5; string nurl = 6; string burl = 7;
}

enum NoBidReason {
  NO_BID_REASON_UNSPECIFIED = 0; NO_MATCH = 1; BUDGET_EXHAUSTED = 2;
  DEADLINE = 3; BELOW_FLOOR = 4; OVERLOADED = 5; INVALID_REQUEST = 6;
}
```

### 2.3. Служебный API

| Путь | Назначение |
|---|---|
| `GET /healthz` | liveness: процесс жив |
| `GET /readyz` | readiness: снапшот кампаний загружен, отставание от `campaign.changes` < 30 с, Redis доступен или включён degraded-режим |
| `GET /metrics` | Prometheus: `bid_requests_total{partner,result}`, `bid_latency_seconds` (гистограмма по шагам), `snapshot_lag_seconds`, `budget_lease_remaining` |
| `POST /admin/snapshot/reload` | ручная перезагрузка снапшота (защищён, только из внутренней сети) |

## 3. Зависимости

| Зависимость | Тип | На пути запроса? | Тайм-аут | Поведение при отказе |
|---|---|---|---|---|
| Снапшот кампаний (память процесса) | локально | да | — | всегда доступен; при старте без снапшота → `readyz=false`, под не получает трафик |
| Redis: сегменты и frequency caps | синхронно | да | 5 мс | circuit breaker → торгуем без сегментов, кампании с обязательным сегментным таргетингом или строгим frequency cap исключаются |
| User Profile Service (через год) | gRPC | да | 8 мс | то же, что выше |
| Budget lease (локальный счётчик) | локально | да | — | при исчерпании lease кампания выбывает из аукциона |
| Budget & Pacing Service / Redis (продление lease) | gRPC/Redis, **фоново** | нет | 50 мс, retry с backoff | работаем на остатке lease; при истечении lease → кампания выбывает (safe default) |
| Kafka (`auction.results`) | async producer | нет | буфер в памяти | при недоступности — локальный буфер (ограниченный), затем spill на диск; ответ DSP не блокируется |
| Kafka (`campaign.changes`) | consumer | нет | — | снапшот «замораживается», алерт при lag > 30 с, при lag > 5 мин — кампании с изменённым статусом исключаются по TTL снапшота |
| Campaign Service / read replica | gRPC / SQL при старте | нет | 30 с | старт с последнего снапшота из S3 |

## 4. Внутренние компоненты

| Компонент | Ответственность |
|---|---|
| **Request Handler** | HTTP/2-сервер, парсинг OpenRTB, контроль дедлайна |
| **Deadline Controller** | вычисляет остаток времени: `min(tmax − сеть, 70 мс)`; если на шаге не хватает бюджета времени — no-bid |
| **User Resolver** | ID пользователя → сегменты и frequency caps (Redis/Profile) |
| **Candidate Selector** | инвертированные индексы в памяти: гео → кампании, устройство → кампании, сегмент → кампании; пересечение битовых множеств |
| **Budget Guard** | атомарный локальный счётчик lease по кампании; фоновое продление |
| **Auction Core (C++)** | eCPM, бизнес-правила, floor, second-price/first-price логика по настройке партнёра |
| **Response Builder** | заполнение шаблона `adm`, подписанные `nurl`/`burl` |
| **Snapshot Manager** | загрузка снапшота, применение событий `campaign.changes`, атомарная замена индексов (copy-on-write) |
| **Event Publisher** | асинхронная публикация `auction.results` батчами |

## 5. Модель данных

### 5.1. Снапшот кампаний (в памяти, read-only, copy-on-write)

```text
CampaignSnapshot
  version            : int64          -- offset последнего применённого события
  campaigns          : map<campaign_id, Campaign>
  index_geo          : map<country/region, Bitset<campaign_idx>>
  index_device       : map<device_type/os, Bitset>
  index_segment      : map<segment_id, Bitset>
  index_placement    : map<site/app category, Bitset>

Campaign
  campaign_id        : string
  advertiser_id      : string
  status             : ACTIVE | PAUSED | ARCHIVED
  schedule           : [ {days, from, to, tz} ]
  bid_strategy       : CPM | CPC
  max_bid_micros     : int64
  targeting          : {geo[], devices[], os[], segments_include[], segments_exclude[], placements[]}
  frequency_cap      : {impressions, period_sec}
  creatives          : [Creative]
  pacing             : EVEN | ASAP

Creative
  creative_id, format (banner/video/native), w, h, adm_template, adomain[], categories[]
```

Объём: ~50 000 кампаний × ~5 КБ ≈ 250 МБ + индексы — укладывается в память пода (лимит 2 ГБ).

### 5.2. Данные в Redis

| Ключ | Тип | Содержимое | TTL |
|---|---|---|---|
| `seg:{user_id}` | SET | сегменты пользователя | 30 дней |
| `fcap:{user_id}` | HASH | `campaign_id → показов за период` | 24 ч |
| `budget:{campaign_id}` | HASH | `daily_limit`, `spent`, `leased`, `day` | до конца суток + 1 ч |
| `lease:{campaign_id}:{instance_id}` | STRING | размер выданной порции | 60 с |
| `pacing:{campaign_id}:{minute}` | STRING | целевой расход на минуту | 2 мин |

Механизм lease: инстанс запрашивает у Budget Guard (Lua-скрипт в Redis, атомарно) порцию бюджета, например `min(1% дневного бюджета, 60 с прогнозного расхода)`. Решения принимаются по локальному счётчику, без сетевого вызова. Невыбранный остаток возвращается при истечении TTL. Максимальный перерасход = сумма активных lease.

### 5.3. Исходящее событие `auction.results` (Avro)

| Поле | Тип | Описание |
|---|---|---|
| `event_id` | string (UUID v7) | идемпотентный ключ |
| `auction_id` | string | ID аукциона (= `bidid`) |
| `request_id` | string | ID запроса DSP |
| `partner_id` | string | DSP |
| `campaign_id` | string, nullable | победитель, `null` для no-bid |
| `creative_id` | string, nullable | |
| `bid_price_micros` | long | наша ставка |
| `floor_micros` | long | |
| `result` | enum `BID`, `NO_BID` | |
| `no_bid_reason` | enum, nullable | |
| `latency_us` | int | время обработки |
| `geo`, `device_type` | string | для аналитики |
| `ts` | timestamp-millis | |

## 6. Бюджет времени и SLO

| Шаг | Цель P95 |
|---|---|
| Парсинг и валидация | ≤ 2 мс |
| User Resolver (Redis) | ≤ 4 мс |
| Candidate Selector | ≤ 8 мс |
| Budget Guard | < 1 мс |
| Auction Core | ≤ 8 мс |
| Response Builder | ≤ 2 мс |
| **Обработка в сервисе** | **P95 ≤ 25 мс, P99 ≤ 40 мс** |
| Жёсткий дедлайн от входа в Gateway | 70 мс → no-bid |

SLO сервиса: доступность 99.95% (доля ответов 2xx/204 из всех запросов), доля ответов в дедлайн ≥ 99.5%.

## 7. Масштабирование и развёртывание

- Stateless, Kubernetes Deployment; HPA по CPU (цель 60%) и метрике `bid_requests_per_second` на под.
- Оценка: 2 500–3 000 RPS на под (4 vCPU), в расчётах используется нижняя граница 2 500. Для 18 000 RPS с запасом 50% → **10–12 подов** (один регион).
- Для 50 000 RPS в двух регионах — **27 подов в каждом регионе** (`minReplicas = 27`, по 9 на AZ): каждый регион в одиночку держит 50 000 RPS при загрузке ≈ 74%, поэтому отказ региона не требует автоскейлинга в момент аварии. Расчёт и план переключения — [Task1/TO-BE.md, раздел 3.5](../Task1/TO-BE.md#35-мультирегиональное-развёртывание).
- `PodDisruptionBudget` — не менее 80% подов доступны; anti-affinity по зонам доступности.
- Прогрев: под получает трафик только после загрузки снапшота и прогрева соединений к Redis (readiness).
- Релизы: Argo Rollouts canary 5% → 25% → 100% с автоматическим анализом P95 и доли ошибок.
