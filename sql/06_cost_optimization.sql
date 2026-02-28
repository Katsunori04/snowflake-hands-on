-- What you learn:
-- - 初学者向けの基本的なコスト最適化
-- - warehouse 設定とクエリ設計の関係
-- - raw を毎回直接読む欠点

use warehouse LEARN_WH;
use database LEARN_DB;

-- 1. 学習用 warehouse の基本設定
show warehouses like 'LEARN_WH';

-- 2. auto suspend / auto resume の確認
alter warehouse LEARN_WH set
  auto_suspend = 60,
  auto_resume = true;

-- 3. raw JSON を毎回展開するクエリ
select
  raw:event_id::string as event_id,
  item.value:sku::string as sku,
  item.value:qty::number as qty,
  item.value:price::number as price
from RAW.RAW_EVENTS,
lateral flatten(input => raw:items) item;

-- 4. すでに整形したテーブルを読むクエリ
select
  event_id,
  sku,
  qty,
  price
from STAGING.STG_EVENT_ITEMS;

-- 5. 最近のクエリを確認
select
  query_id,
  query_text,
  warehouse_name,
  total_elapsed_time,
  bytes_scanned
from table(information_schema.query_history_by_warehouse(
  warehouse_name => 'LEARN_WH',
  end_time_range_start => dateadd('hour', -1, current_timestamp()),
  result_limit => 20
))
order by start_time desc;

-- Cost memo:
-- - warehouse はまず XSMALL から始める
-- - auto_suspend は短くする
-- - raw JSON の直接参照を常用しない
-- - よく使う列は STAGING / MART に展開する
-- - task の実行頻度を細かくしすぎない

-- Check.
show parameters like 'AUTO_SUSPEND' in warehouse LEARN_WH;

-- Try this:
-- 5分おき task を 1時間おきに変えると何が良くて何が悪いか考えてみてください。
