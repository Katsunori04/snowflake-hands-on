-- What you learn:
-- - 学習用の warehouse / database / schema を一度で用意する
-- - RAW / STAGING / MART の役割を固定する

-- Run this first.
create or replace warehouse LEARN_WH
  warehouse_size = 'XSMALL'
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true;

create or replace database LEARN_DB;

create or replace schema LEARN_DB.RAW;
create or replace schema LEARN_DB.STAGING;
create or replace schema LEARN_DB.MART;

use warehouse LEARN_WH;
use database LEARN_DB;
use schema RAW;

-- Check.
show warehouses like 'LEARN_WH';
show schemas in database LEARN_DB;

-- Try this:
-- auto_suspend の秒数を 300 に変えた場合に何が変わるか説明してみてください。
