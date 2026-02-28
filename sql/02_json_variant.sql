-- What you learn:
-- - VARIANT に JSON を入れる
-- - JSON path で値を取り出す
-- - LATERAL FLATTEN で配列を行に展開する

use warehouse LEARN_WH;
use database LEARN_DB;
use schema RAW;

create or replace table RAW.RAW_EVENTS (
  raw variant,
  loaded_at timestamp_ntz default current_timestamp()
);

-- 8件を一括で INSERT（複数 UNION ALL → 1回の INSERT にまとめて効率化）
-- u001, u003, u005 は複数購入（Sports 多め）、u002 は Food のヘビーユーザー
insert into RAW.RAW_EVENTS(raw)
select parse_json(v) from (values
  ('{
    "event_id": "e001",
    "user_id": "u001",
    "event_type": "purchase",
    "event_time": "2026-02-01T09:00:00Z",
    "device": {"os": "iOS", "app_version": "2.0.0"},
    "review_text": "Fast delivery and good quality. I would buy again.",
    "items": [
      {"sku": "A001", "product_name": "Trail Shoes", "category": "Sports", "qty": 1, "price": 12000},
      {"sku": "B001", "product_name": "Coffee Beans", "category": "Food", "qty": 2, "price": 900}
    ]
  }'),
  ('{
    "event_id": "e002",
    "user_id": "u002",
    "event_type": "purchase",
    "event_time": "2026-02-01T11:30:00Z",
    "device": {"os": "Android", "app_version": "2.1.0"},
    "review_text": "Coffee aroma was strong, but the packaging was slightly damaged.",
    "items": [
      {"sku": "B001", "product_name": "Coffee Beans", "category": "Food", "qty": 3, "price": 900}
    ]
  }'),
  ('{
    "event_id": "e003",
    "user_id": "u003",
    "event_type": "purchase",
    "event_time": "2026-02-02T14:15:00Z",
    "device": {"os": "PC", "app_version": "2.0.1"},
    "items": [
      {"sku": "C001", "product_name": "Desk Lamp", "category": "Home", "qty": 1, "price": 4500},
      {"sku": "D001", "product_name": "USB Hub", "category": "Electronics", "qty": 1, "price": 3200}
    ]
  }'),
  ('{
    "event_id": "e004",
    "user_id": "u001",
    "event_type": "purchase",
    "event_time": "2026-02-03T10:00:00Z",
    "device": {"os": "iOS", "app_version": "2.0.0"},
    "review_text": "Exactly as described. Will order again.",
    "items": [
      {"sku": "A002", "product_name": "Yoga Mat", "category": "Sports", "qty": 1, "price": 4800}
    ]
  }'),
  ('{
    "event_id": "e005",
    "user_id": "u004",
    "event_type": "purchase",
    "event_time": "2026-02-04T16:45:00Z",
    "device": {"os": "Android", "app_version": "1.2.0"},
    "items": [
      {"sku": "E001", "product_name": "Cotton Tote", "category": "Fashion", "qty": 2, "price": 1500}
    ]
  }'),
  ('{
    "event_id": "e006",
    "user_id": "u002",
    "event_type": "purchase",
    "event_time": "2026-02-05T09:30:00Z",
    "device": {"os": "Android", "app_version": "2.1.0"},
    "review_text": "Good value for money. Quick delivery.",
    "items": [
      {"sku": "B002", "product_name": "Protein Bar", "category": "Food", "qty": 5, "price": 350},
      {"sku": "B003", "product_name": "Green Tea", "category": "Food", "qty": 2, "price": 600}
    ]
  }'),
  ('{
    "event_id": "e007",
    "user_id": "u005",
    "event_type": "purchase",
    "event_time": "2026-02-06T20:00:00Z",
    "device": {"os": "iOS", "app_version": "2.0.1"},
    "review_text": "The speaker quality exceeded my expectations.",
    "items": [
      {"sku": "D003", "product_name": "Bluetooth Speaker", "category": "Electronics", "qty": 1, "price": 12000}
    ]
  }'),
  ('{
    "event_id": "e008",
    "user_id": "u003",
    "event_type": "purchase",
    "event_time": "2026-02-07T13:20:00Z",
    "device": {"os": "PC", "app_version": "2.0.1"},
    "items": [
      {"sku": "C003", "product_name": "Air Purifier", "category": "Home", "qty": 1, "price": 18000},
      {"sku": "A003", "product_name": "Running Cap", "category": "Sports", "qty": 2, "price": 2500}
    ]
  }')
) v(v);

-- JSON path で値を取り出す
-- 構文: raw:<キー名>::<型>
-- raw:event_id         → JSON の "event_id" フィールド（VARIANT 型）
-- raw:event_id::string → string 型にキャスト（:: は型変換演算子）
-- raw:device.os        → ネストしたオブジェクトにはドットでアクセス
select
  raw:event_id::string as event_id,
  raw:user_id::string as user_id,
  raw:event_type::string as event_type,
  raw:device.os::string as device_os,
  raw:device.app_version::string as app_version,
  raw:review_text::string as review_text,
  to_timestamp_ntz(raw:event_time::string) as event_time
from RAW.RAW_EVENTS
order by event_id;

-- LATERAL FLATTEN で配列（items）を行に展開する
--
-- 【展開イメージ】
-- 元のデータ（1イベントに2商品の items 配列）:
--   event_id="e001"
--     items[0] = {sku:"A001", price:12000}
--     items[1] = {sku:"B005", price:900}
--
-- LATERAL FLATTEN 後（1商品 = 1行 に展開される）:
--   event_id | sku  | price
--   ---------|------|------
--   e001     | A001 | 12000
--   e001     | B005 |   900
--   e002     | B005 |   900
--
-- 構文の読み方:
--   lateral flatten(input => raw:items) item
--     └─ raw:items      : 展開対象の配列フィールド
--     └─ item           : 展開後の各要素を参照するエイリアス
--
-- item.value:<フィールド>::<型> の構造:
--   item.value          : 展開された1要素（オブジェクト）を参照
--   item.value:price    : その要素内の "price" フィールド
--   ::number(10,2)      : 数値型にキャスト（精度10桁、小数2桁）
select
  raw:event_id::string as event_id,
  item.value:sku::string as sku,
  item.value:product_name::string as product_name,
  item.value:category::string as category,
  item.value:qty::number as qty,
  item.value:price::number(10,2) as price,
  item.value:qty::number * item.value:price::number(10,2) as line_amount
from RAW.RAW_EVENTS,
lateral flatten(input => raw:items) item
order by event_id, sku;

use schema STAGING;

create or replace table STAGING.STG_EVENTS as
select
  raw:event_id::string as event_id,
  raw:user_id::string as user_id,
  raw:event_type::string as event_type,
  raw:device.os::string as device_os,
  raw:device.app_version::string as app_version,
  raw:review_text::string as review_text,
  to_timestamp_ntz(raw:event_time::string) as event_time,
  loaded_at
from RAW.RAW_EVENTS;

create or replace table STAGING.STG_EVENT_ITEMS as
select
  raw:event_id::string as event_id,
  raw:user_id::string as user_id,
  to_timestamp_ntz(raw:event_time::string) as event_time,
  item.value:sku::string as sku,
  item.value:product_name::string as product_name,
  item.value:category::string as category,
  item.value:qty::number as qty,
  item.value:price::number(10,2) as price,
  item.value:qty::number * item.value:price::number(10,2) as line_amount
from RAW.RAW_EVENTS,
lateral flatten(input => raw:items) item;

-- Check.
select * from STAGING.STG_EVENTS order by event_id;
select * from STAGING.STG_EVENT_ITEMS order by event_id, sku;

-- Try this:
-- event_type = 'purchase' だけを抽出する where 条件を足してください。
