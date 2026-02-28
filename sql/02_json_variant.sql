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

insert into RAW.RAW_EVENTS(raw)
select parse_json('
{
  "event_id": "e001",
  "user_id": "u001",
  "event_type": "purchase",
  "event_time": "2026-02-28T10:00:00Z",
  "device": {
    "os": "iOS",
    "app_version": "1.2.0"
  },
  "review_text": "Fast delivery and good quality. I would buy again.",
  "items": [
    {"sku": "A001", "product_name": "Trail Shoes", "category": "Sports", "qty": 1, "price": 12000},
    {"sku": "B005", "product_name": "Coffee Beans", "category": "Food", "qty": 2, "price": 900}
  ]
}');

insert into RAW.RAW_EVENTS(raw)
select parse_json('
{
  "event_id": "e002",
  "user_id": "u002",
  "event_type": "purchase",
  "event_time": "2026-02-28T11:30:00Z",
  "device": {
    "os": "Android",
    "app_version": "1.3.1"
  },
  "review_text": "Coffee aroma was strong, but the packaging was slightly damaged.",
  "items": [
    {"sku": "B005", "product_name": "Coffee Beans", "category": "Food", "qty": 1, "price": 900}
  ]
}');

-- JSON path extraction
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

-- 配列を展開
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
