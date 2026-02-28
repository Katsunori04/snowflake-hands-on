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

-- USERS_NORM: 15件（5地域に分散）
insert into STAGING.USERS_NORM values
  ('u001', '田中 太郎',   '東京'),
  ('u002', '山田 花子',   '大阪'),
  ('u003', '佐藤 健',     '愛知'),
  ('u004', '鈴木 恵',     '福岡'),
  ('u005', '高橋 誠',     '北海道'),
  ('u006', '伊藤 めぐみ', '東京'),
  ('u007', '渡辺 拓哉',   '神奈川'),
  ('u008', '中村 さくら', '大阪'),
  ('u009', '小林 竜',     '愛知'),
  ('u010', '加藤 玲奈',   '東京'),
  ('u011', '吉田 浩',     '福岡'),
  ('u012', '山本 綾',     '兵庫'),
  ('u013', '松本 隆',     '京都'),
  ('u014', '井上 みき',   '埼玉'),
  ('u015', '木村 剛',     '千葉');

-- PRODUCTS_NORM: 15件（5カテゴリ各3商品）
insert into STAGING.PRODUCTS_NORM values
  ('A001', 'Trail Shoes',       'Sports'),
  ('A002', 'Yoga Mat',           'Sports'),
  ('A003', 'Running Cap',        'Sports'),
  ('B001', 'Coffee Beans',       'Food'),
  ('B002', 'Protein Bar',        'Food'),
  ('B003', 'Green Tea',          'Food'),
  ('C001', 'Desk Lamp',          'Home'),
  ('C002', 'Candle Set',         'Home'),
  ('C003', 'Air Purifier',       'Home'),
  ('D001', 'USB Hub',            'Electronics'),
  ('D002', 'Webcam',             'Electronics'),
  ('D003', 'Bluetooth Speaker',  'Electronics'),
  ('E001', 'Cotton Tote',        'Fashion'),
  ('E002', 'Wool Scarf',         'Fashion'),
  ('E003', 'Leather Wallet',     'Fashion');

-- ORDERS_NORM: 20件（2025-12〜2026-02 の期間に分散）
insert into STAGING.ORDERS_NORM values
  ('o1001', 'u001', '2025-12-03'),
  ('o1002', 'u002', '2025-12-07'),
  ('o1003', 'u003', '2025-12-10'),
  ('o1004', 'u004', '2025-12-14'),
  ('o1005', 'u005', '2025-12-18'),
  ('o1006', 'u001', '2025-12-22'),
  ('o1007', 'u006', '2025-12-25'),
  ('o1008', 'u007', '2025-12-28'),
  ('o1009', 'u002', '2026-01-05'),
  ('o1010', 'u008', '2026-01-09'),
  ('o1011', 'u003', '2026-01-13'),
  ('o1012', 'u009', '2026-01-17'),
  ('o1013', 'u001', '2026-01-20'),
  ('o1014', 'u010', '2026-01-24'),
  ('o1015', 'u004', '2026-01-28'),
  ('o1016', 'u011', '2026-02-03'),
  ('o1017', 'u002', '2026-02-07'),
  ('o1018', 'u012', '2026-02-12'),
  ('o1019', 'u001', '2026-02-18'),
  ('o1020', 'u013', '2026-02-25');

-- ORDER_ITEMS_NORM: 40件程度（20注文 × 平均2商品）
insert into STAGING.ORDER_ITEMS_NORM values
  ('o1001', 'A001', 1, 12000),
  ('o1001', 'B001', 2,   900),
  ('o1002', 'C001', 1,  4500),
  ('o1002', 'B002', 3,   350),
  ('o1003', 'D001', 1,  3200),
  ('o1003', 'E001', 2,  1500),
  ('o1004', 'A002', 1,  4800),
  ('o1004', 'B003', 2,   600),
  ('o1005', 'C002', 1,  2800),
  ('o1005', 'D002', 1,  8500),
  ('o1006', 'A001', 1, 12000),
  ('o1006', 'E002', 1,  3600),
  ('o1007', 'B001', 3,   900),
  ('o1007', 'C003', 1, 18000),
  ('o1008', 'D003', 1, 12000),
  ('o1008', 'A003', 2,  2500),
  ('o1009', 'E003', 1,  6800),
  ('o1009', 'B002', 2,   350),
  ('o1010', 'C001', 1,  4500),
  ('o1010', 'A002', 1,  4800),
  ('o1011', 'D001', 2,  3200),
  ('o1011', 'B003', 3,   600),
  ('o1012', 'E001', 1,  1500),
  ('o1012', 'A003', 2,  2500),
  ('o1013', 'C002', 2,  2800),
  ('o1013', 'B001', 1,   900),
  ('o1014', 'D002', 1,  8500),
  ('o1014', 'E002', 1,  3600),
  ('o1015', 'A001', 1, 12000),
  ('o1015', 'C003', 1, 18000),
  ('o1016', 'B002', 4,   350),
  ('o1016', 'D001', 1,  3200),
  ('o1017', 'E003', 1,  6800),
  ('o1017', 'A002', 2,  4800),
  ('o1018', 'C001', 1,  4500),
  ('o1018', 'B003', 2,   600),
  ('o1019', 'D003', 1, 12000),
  ('o1019', 'E001', 3,  1500),
  ('o1020', 'A003', 1,  2500),
  ('o1020', 'C002', 1,  2800);

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
