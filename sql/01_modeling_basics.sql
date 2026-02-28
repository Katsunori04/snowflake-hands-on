-- What you learn:
-- - 正規化、非正規化、スタースキーマをサンプルで比べる
-- - Snowflake では RAW / STAGING / MART にどう置くかを掴む

use warehouse LEARN_WH;
use database LEARN_DB;
use schema STAGING;

-- 【正規化 vs スタースキーマの比較】
-- 正規化（以下の1〜2）: テーブルを分割して重複を排除する。更新しやすいがクエリに JOIN が増える。
-- スタースキーマ（以下の3〜4）: FACT（計測値）と DIM（属性）に分ける分析向けの設計。
--   JOIN が定型化され BI ツールとの相性が良いが、データ量が増えると DIM に重複が生じうる。
--
-- この章では「STAGING 層で正規化、MART 層でスタースキーマ」という使い分けのイメージを掴む。

-- 1. アプリ寄りの正規化例
create or replace table STAGING.USERS_NORM (
  user_id string,
  user_name string,
  prefecture string
);

create or replace table STAGING.PRODUCTS_NORM (
  sku string,
  product_name string,
  category string
);

create or replace table STAGING.ORDERS_NORM (
  order_id string,
  user_id string,
  order_date date
);

create or replace table STAGING.ORDER_ITEMS_NORM (
  order_id string,
  sku string,
  qty number,
  price number(10,2)
);

insert into STAGING.USERS_NORM values
  ('u001', 'Aki', 'Tokyo'),
  ('u002', 'Mina', 'Osaka');

insert into STAGING.PRODUCTS_NORM values
  ('A001', 'Trail Shoes', 'Sports'),
  ('B005', 'Coffee Beans', 'Food');

insert into STAGING.ORDERS_NORM values
  ('o1001', 'u001', '2026-02-27'),
  ('o1002', 'u002', '2026-02-28');

insert into STAGING.ORDER_ITEMS_NORM values
  ('o1001', 'A001', 1, 12000),
  ('o1001', 'B005', 2, 900),
  ('o1002', 'B005', 1, 900);

-- 2. 正規化データを join して見る
-- 正規化の欠点: product_name・user_name を取るために毎回 4テーブル JOIN が必要。
-- 分析クエリではこの JOIN コストが積み上がる。
select
  o.order_id,
  o.order_date,
  u.user_name,
  p.product_name,
  p.category,
  i.qty,
  i.price,
  i.qty * i.price as line_amount
from STAGING.ORDERS_NORM o
join STAGING.USERS_NORM u
  on o.user_id = u.user_id
join STAGING.ORDER_ITEMS_NORM i
  on o.order_id = i.order_id
join STAGING.PRODUCTS_NORM p
  on i.sku = p.sku
order by o.order_id, p.sku;

-- 3. 分析寄りのスタースキーマ例
-- FACT に product_name を「持たない」設計（DIM に分離するメリット）:
-- - 商品名が変わっても DIM_PRODUCTS を 1 箇所更新すれば済む
-- - BI ツールは DIM を JOIN するだけで属性を取れる
-- ただし FACT テーブルが大きいと JOIN コストが生じるため、
-- FACT に非正規化（商品名を直接持たせる）するケースもある（04章の FACT_PURCHASE_EVENTS がその例）。
create or replace table MART.DIM_USERS (
  user_id string,
  user_name string,
  prefecture string
);

create or replace table MART.DIM_PRODUCTS (
  sku string,
  product_name string,
  category string
);

create or replace table MART.FACT_ORDER_LINES (
  order_id string,
  order_date date,
  user_id string,
  sku string,
  qty number,
  price number(10,2),
  line_amount number(12,2)
);

insert into MART.DIM_USERS
select * from STAGING.USERS_NORM;

insert into MART.DIM_PRODUCTS
select * from STAGING.PRODUCTS_NORM;

insert into MART.FACT_ORDER_LINES
select
  o.order_id,
  o.order_date,
  o.user_id,
  i.sku,
  i.qty,
  i.price,
  i.qty * i.price as line_amount
from STAGING.ORDERS_NORM o
join STAGING.ORDER_ITEMS_NORM i
  on o.order_id = i.order_id;

-- 4. fact + dimension で集計
select
  d.category,
  sum(f.line_amount) as sales_amount
from MART.FACT_ORDER_LINES f
join MART.DIM_PRODUCTS d
  on f.sku = d.sku
group by d.category
order by sales_amount desc;

-- Check.
select * from MART.FACT_ORDER_LINES order by order_id, sku;
select * from MART.DIM_USERS order by user_id;
select * from MART.DIM_PRODUCTS order by sku;

-- Try this:
-- prefecture 別の売上を出してください。
