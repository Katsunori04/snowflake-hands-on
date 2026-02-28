-- What you learn:
-- - fact table と dimension table を作る
-- - スタースキーマで集計する
-- - MART 層で分析しやすい形を作る

use warehouse LEARN_WH;
use database LEARN_DB;
use schema MART;

create or replace table MART.DIM_USERS as
select distinct
  user_id,
  case
    when user_id = 'u001' then 'Aki'
    when user_id = 'u002' then 'Mina'
    else 'Unknown'
  end as user_name,
  case
    when user_id = 'u001' then 'Tokyo'
    when user_id = 'u002' then 'Osaka'
    else 'Unknown'
  end as prefecture
from MART.FACT_PURCHASE_EVENTS;

create or replace table MART.DIM_PRODUCTS as
select distinct
  sku,
  product_name,
  category
from MART.FACT_PURCHASE_EVENTS;

create or replace table MART.DIM_DATE as
select distinct
  cast(event_time as date) as date_key,
  year(event_time) as year_num,
  month(event_time) as month_num,
  day(event_time) as day_num
from MART.FACT_PURCHASE_EVENTS;

-- fact + dimension の利用例
select
  d.category,
  u.prefecture,
  sum(f.line_amount) as sales_amount
from MART.FACT_PURCHASE_EVENTS f
join MART.DIM_PRODUCTS d
  on f.sku = d.sku
join MART.DIM_USERS u
  on f.user_id = u.user_id
group by d.category, u.prefecture
order by sales_amount desc;

select
  dd.year_num,
  dd.month_num,
  d.category,
  sum(f.qty) as total_qty,
  sum(f.line_amount) as sales_amount
from MART.FACT_PURCHASE_EVENTS f
join MART.DIM_PRODUCTS d
  on f.sku = d.sku
join MART.DIM_DATE dd
  on cast(f.event_time as date) = dd.date_key
group by dd.year_num, dd.month_num, d.category
order by dd.year_num, dd.month_num, d.category;

-- Check.
select * from MART.DIM_USERS order by user_id;
select * from MART.DIM_PRODUCTS order by sku;
select * from MART.DIM_DATE order by date_key;

-- Try this:
-- user_name ごとの購入金額を集計してください。
