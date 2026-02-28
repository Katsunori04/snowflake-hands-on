-- What you learn:
-- - stream で差分を取る
-- - task で定期実行する
-- - Snowflake 内でバッチ的な処理を作る

use warehouse LEARN_WH;
use database LEARN_DB;
use schema RAW;

create or replace stream RAW.RAW_EVENTS_STREAM
  on table RAW.RAW_EVENTS_PIPE;

use schema MART;

create or replace table MART.FACT_PURCHASE_EVENTS (
  event_id string,
  user_id string,
  event_time timestamp_ntz,
  sku string,
  product_name string,
  category string,
  qty number,
  price number(10,2),
  line_amount number(12,2),
  src_filename string,
  inserted_at timestamp_ntz default current_timestamp()
);

-- まずは手動で増分反映
merge into MART.FACT_PURCHASE_EVENTS tgt
using (
  select
    s.raw:event_id::string as event_id,
    s.raw:user_id::string as user_id,
    to_timestamp_ntz(s.raw:event_time::string) as event_time,
    item.value:sku::string as sku,
    item.value:product_name::string as product_name,
    item.value:category::string as category,
    item.value:qty::number as qty,
    item.value:price::number(10,2) as price,
    item.value:qty::number * item.value:price::number(10,2) as line_amount,
    s.src_filename
  from RAW.RAW_EVENTS_STREAM s,
  lateral flatten(input => s.raw:items) item
  where s.metadata$action = 'INSERT'
) src
on tgt.event_id = src.event_id
and tgt.sku = src.sku
when matched then update set
  tgt.user_id = src.user_id,
  tgt.event_time = src.event_time,
  tgt.product_name = src.product_name,
  tgt.category = src.category,
  tgt.qty = src.qty,
  tgt.price = src.price,
  tgt.line_amount = src.line_amount,
  tgt.src_filename = src.src_filename
when not matched then insert (
  event_id,
  user_id,
  event_time,
  sku,
  product_name,
  category,
  qty,
  price,
  line_amount,
  src_filename
) values (
  src.event_id,
  src.user_id,
  src.event_time,
  src.sku,
  src.product_name,
  src.category,
  src.qty,
  src.price,
  src.line_amount,
  src.src_filename
);

select * from MART.FACT_PURCHASE_EVENTS order by event_time, event_id, sku;

-- 定期実行用 task
create or replace task STAGING.LOAD_FACT_PURCHASE_EVENTS
  warehouse = LEARN_WH
  schedule = 'USING CRON 0/5 * * * * Asia/Tokyo'
as
merge into MART.FACT_PURCHASE_EVENTS tgt
using (
  select
    s.raw:event_id::string as event_id,
    s.raw:user_id::string as user_id,
    to_timestamp_ntz(s.raw:event_time::string) as event_time,
    item.value:sku::string as sku,
    item.value:product_name::string as product_name,
    item.value:category::string as category,
    item.value:qty::number as qty,
    item.value:price::number(10,2) as price,
    item.value:qty::number * item.value:price::number(10,2) as line_amount,
    s.src_filename
  from RAW.RAW_EVENTS_STREAM s,
  lateral flatten(input => s.raw:items) item
  where s.metadata$action = 'INSERT'
) src
on tgt.event_id = src.event_id
and tgt.sku = src.sku
when matched then update set
  tgt.user_id = src.user_id,
  tgt.event_time = src.event_time,
  tgt.product_name = src.product_name,
  tgt.category = src.category,
  tgt.qty = src.qty,
  tgt.price = src.price,
  tgt.line_amount = src.line_amount,
  tgt.src_filename = src.src_filename
when not matched then insert (
  event_id,
  user_id,
  event_time,
  sku,
  product_name,
  category,
  qty,
  price,
  line_amount,
  src_filename
) values (
  src.event_id,
  src.user_id,
  src.event_time,
  src.sku,
  src.product_name,
  src.category,
  src.qty,
  src.price,
  src.line_amount,
  src.src_filename
);

-- task の開始と確認
alter task STAGING.LOAD_FACT_PURCHASE_EVENTS resume;
show tasks like 'LOAD_FACT_PURCHASE_EVENTS' in schema STAGING;

-- 必要に応じて停止
-- alter task STAGING.LOAD_FACT_PURCHASE_EVENTS suspend;

-- Check.
select * from RAW.RAW_EVENTS_STREAM;
select * from MART.FACT_PURCHASE_EVENTS order by event_time, event_id, sku;

-- Try this:
-- schedule を毎日 01:00 実行に変えるならどう書くか考えてみてください。
