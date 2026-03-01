-- What you learn:
-- - View を分析の入口として使う
-- - FACT と DIM の JOIN を View に閉じ込める
-- - Secure View の基本を確認する
--
-- Prerequisite:
-- - 06_star_schema.sql が完了していること

use warehouse LEARN_WH;
use database LEARN_DB;
use schema MART;

-- 1. 明細粒度の分析用 View
create or replace view MART.V_SALES_DETAIL as
select
  f.event_id,
  f.event_time,
  cast(f.event_time as date) as purchase_date,
  dd.year_num,
  dd.month_num,
  dd.day_num,
  f.user_id,
  u.user_name,
  u.prefecture,
  f.sku,
  p.product_name,
  p.category,
  f.qty,
  f.price,
  f.line_amount
from MART.FACT_PURCHASE_EVENTS f
join MART.DIM_USERS u
  on f.user_id = u.user_id
join MART.DIM_PRODUCTS p
  on f.sku = p.sku
join MART.DIM_DATE dd
  on cast(f.event_time as date) = dd.date_key;

-- 2. View を使った集計
select
  category,
  prefecture,
  sum(line_amount) as sales_amount
from MART.V_SALES_DETAIL
group by category, prefecture
order by sales_amount desc;

-- 3. 月次カテゴリ売上 View
create or replace view MART.V_CATEGORY_MONTHLY_SALES as
select
  year_num,
  month_num,
  category,
  sum(qty) as total_qty,
  sum(line_amount) as sales_amount
from MART.V_SALES_DETAIL
group by year_num, month_num, category;

select *
from MART.V_CATEGORY_MONTHLY_SALES
order by year_num, month_num, sales_amount desc;

-- 4. Secure View
create or replace secure view MART.V_SALES_PUBLIC as
select
  purchase_date,
  prefecture,
  category,
  qty,
  line_amount
from MART.V_SALES_DETAIL;

-- Check.
show views in schema MART;
select * from MART.V_SALES_DETAIL order by event_time, event_id, sku;
select * from MART.V_CATEGORY_MONTHLY_SALES order by year_num, month_num, category;

-- Try this:
-- MART.V_SALES_DETAIL を使って、ユーザーごとの累計購入金額 View
-- MART.V_USER_LTV を作ってみてください。
