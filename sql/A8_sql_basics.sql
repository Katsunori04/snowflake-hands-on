-- ============================================================
-- 付録A8: SQL の基本（SELECT〜ウィンドウ関数・DDL）
-- 本編を始める前・途中で構文を確認したいときのリファレンス
-- ============================================================
-- 実行前提: 06_star_schema.sql を完了していること
--   （MART.FACT_PURCHASE_EVENTS / DIM_PRODUCTS / DIM_USERS / DIM_DATE が存在）
-- 使用ロール: SYSADMIN
-- ============================================================

USE ROLE SYSADMIN;
USE DATABASE HANDS_ON_DB;
USE SCHEMA MART;
USE WAREHOUSE LEARN_WH;

-- ============================================================
-- Section 1: SELECT / FROM
-- ============================================================
-- What you learn: 列の指定・別名・計算列・データ型確認

-- 基本: 全列取得
SELECT * FROM MART.FACT_PURCHASE_EVENTS LIMIT 5;

-- 列を指定して取得
SELECT event_id, user_id, category, line_amount
FROM MART.FACT_PURCHASE_EVENTS
LIMIT 10;

-- 別名（AS）: 列名を分かりやすく変える
SELECT
  event_id   AS id,
  user_id    AS uid,
  line_amount AS 購入金額
FROM MART.FACT_PURCHASE_EVENTS
LIMIT 5;

-- 計算列: 数値演算ができる
SELECT
  event_id,
  qty,
  price,
  qty * price           AS 計算金額,   -- 乗算
  line_amount,
  line_amount * 1.1     AS 税込金額    -- 10%消費税
FROM MART.FACT_PURCHASE_EVENTS
LIMIT 10;

-- 文字列結合: CONCAT または || 演算子
SELECT
  user_id || ' - ' || product_name AS 購入サマリ,
  CONCAT(category, '（', product_name, '）') AS カテゴリ付き商品名
FROM MART.FACT_PURCHASE_EVENTS
LIMIT 10;

-- 型確認: TYPEOF でデータ型を調べる
SELECT
  TYPEOF(event_id)    AS event_id_type,
  TYPEOF(qty)         AS qty_type,
  TYPEOF(line_amount) AS line_amount_type,
  TYPEOF(event_time)  AS event_time_type
FROM MART.FACT_PURCHASE_EVENTS
LIMIT 1;

-- Check: 以下のクエリで列の意味を確認しよう
SELECT event_id, user_id, category, product_name, qty, price, line_amount
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY line_amount DESC
LIMIT 5;

-- Try this: FACT_PURCHASE_EVENTS から event_id・product_name・「単価×数量」の計算列を出力してみる

-- ============================================================
-- Section 2: WHERE
-- ============================================================
-- What you learn: 行のフィルタリング（比較・IN・LIKE・BETWEEN・IS NULL・AND/OR/NOT）

-- 比較演算子: = / <> / > / < / >= / <=
SELECT event_id, category, line_amount
FROM MART.FACT_PURCHASE_EVENTS
WHERE line_amount >= 10000;

-- 複数条件: AND（両方を満たす）
SELECT event_id, category, line_amount
FROM MART.FACT_PURCHASE_EVENTS
WHERE category = 'Electronics'
  AND line_amount >= 5000;

-- 複数条件: OR（どちらかを満たす）
SELECT event_id, category, line_amount
FROM MART.FACT_PURCHASE_EVENTS
WHERE category = 'Electronics'
   OR category = 'Sports';

-- IN: 複数の値のどれかに一致（OR の短縮）
SELECT event_id, category, line_amount
FROM MART.FACT_PURCHASE_EVENTS
WHERE category IN ('Electronics', 'Sports', 'Books');

-- NOT IN: 除外
SELECT event_id, category, line_amount
FROM MART.FACT_PURCHASE_EVENTS
WHERE category NOT IN ('Electronics');

-- LIKE: パターンマッチング（% = 任意の文字列、_ = 任意の1文字）
SELECT event_id, product_name
FROM MART.FACT_PURCHASE_EVENTS
WHERE product_name LIKE '%Phone%';    -- "Phone" を含む商品名

SELECT event_id, product_name
FROM MART.FACT_PURCHASE_EVENTS
WHERE product_name LIKE 'Smart%';    -- "Smart" で始まる商品名

-- BETWEEN: 範囲指定（両端を含む）
SELECT event_id, line_amount
FROM MART.FACT_PURCHASE_EVENTS
WHERE line_amount BETWEEN 1000 AND 5000;

-- IS NULL / IS NOT NULL: NULL の判定（= NULL は常に FALSE のため IS NULL を使う）
SELECT event_id, src_filename
FROM MART.FACT_PURCHASE_EVENTS
WHERE src_filename IS NOT NULL;

-- NOT: 条件の否定
SELECT event_id, category
FROM MART.FACT_PURCHASE_EVENTS
WHERE NOT category = 'Electronics';  -- category <> 'Electronics' と同義

-- Check: Electronics カテゴリで line_amount が 3000 以上の行数を確認
SELECT COUNT(*) AS electronics_high_value
FROM MART.FACT_PURCHASE_EVENTS
WHERE category = 'Electronics'
  AND line_amount >= 3000;

-- Try this: product_name に 'Pro' を含み、qty が 2 以上の行を出力してみる

-- ============================================================
-- Section 3: ORDER BY / LIMIT / DISTINCT
-- ============================================================
-- What you learn: ソート・件数制限・重複排除

-- ORDER BY: 並び替え（ASC=昇順 デフォルト, DESC=降順）
SELECT event_id, category, line_amount
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY line_amount DESC;   -- 高い順

-- 複数列ソート: 第1キーが同じ場合に第2キーで比較
SELECT event_id, category, line_amount
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY category ASC, line_amount DESC;

-- LIMIT: 先頭N件だけ取得（FETCH FIRST N ROWS ONLY も同義）
SELECT * FROM MART.FACT_PURCHASE_EVENTS
ORDER BY event_time DESC
LIMIT 10;

-- DISTINCT: 重複を排除（ユニークな値のみ返す）
SELECT DISTINCT category
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY category;

SELECT DISTINCT user_id, category
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY user_id, category;

-- Count DISTINCT: ユニークな値の件数
SELECT COUNT(DISTINCT category) AS unique_categories
FROM MART.FACT_PURCHASE_EVENTS;

-- Check: ユニークなユーザー数とカテゴリ数を1クエリで確認
SELECT
  COUNT(DISTINCT user_id)  AS unique_users,
  COUNT(DISTINCT category) AS unique_categories
FROM MART.FACT_PURCHASE_EVENTS;

-- Try this: DIM_PRODUCTS から category をユニークに抽出して件数も確認してみる

-- ============================================================
-- Section 4: GROUP BY + 集計関数 / HAVING
-- ============================================================
-- What you learn: グループ集計・HAVING と WHERE の使い分け

-- 集計関数: COUNT / SUM / AVG / MAX / MIN
SELECT
  COUNT(*)          AS total_rows,     -- NULL 含む全行数
  COUNT(event_id)   AS non_null_rows,  -- NULL を除く件数
  SUM(line_amount)  AS total_sales,
  AVG(line_amount)  AS avg_sales,
  MAX(line_amount)  AS max_sale,
  MIN(line_amount)  AS min_sale
FROM MART.FACT_PURCHASE_EVENTS;

-- GROUP BY: カテゴリ別集計
SELECT
  category,
  COUNT(*)         AS transaction_count,
  SUM(line_amount) AS total_sales,
  AVG(line_amount) AS avg_sale,
  MAX(line_amount) AS max_sale
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY category
ORDER BY total_sales DESC;

-- 複数列でグループ化
SELECT
  category,
  user_id,
  COUNT(*)         AS transaction_count,
  SUM(line_amount) AS total_sales
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY category, user_id
ORDER BY total_sales DESC;

-- HAVING: GROUP BY 後のフィルタリング（集計後の条件に使う）
-- WHERE は集計前・HAVING は集計後 — この評価タイミングの違いが重要
SELECT
  category,
  COUNT(*)         AS transaction_count,
  SUM(line_amount) AS total_sales
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY category
HAVING SUM(line_amount) >= 50000   -- 集計後の条件 → HAVING
ORDER BY total_sales DESC;

-- WHERE と HAVING の組み合わせ
-- WHERE で行を絞った後 → GROUP BY → HAVING で集計結果を絞る
SELECT
  category,
  COUNT(*)         AS transaction_count,
  SUM(line_amount) AS total_sales
FROM MART.FACT_PURCHASE_EVENTS
WHERE line_amount >= 1000                 -- 集計前: 1000円未満の行を除外
GROUP BY category
HAVING COUNT(*) >= 3                      -- 集計後: 3件未満のカテゴリを除外
ORDER BY total_sales DESC;

-- Check: ユーザー別の購入件数と合計金額（件数が多い順）
SELECT
  user_id,
  COUNT(*)         AS purchase_count,
  SUM(line_amount) AS total_amount
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY user_id
ORDER BY purchase_count DESC;

-- Try this: カテゴリ別に最大単価（price）と平均数量（qty）を集計してみる

-- ============================================================
-- Section 5: DDL（CREATE TABLE / CTAS / ALTER TABLE / DROP / TRUNCATE / CREATE VIEW）
-- ============================================================
-- What you learn: テーブル・ビューの作成・変更・削除

-- DDL①: CREATE TABLE — 列定義とデータ型
-- Snowflakeでよく使うデータ型: STRING / NUMBER / BOOLEAN / TIMESTAMP_NTZ / VARIANT / DATE
CREATE OR REPLACE TABLE HANDS_ON_DB.STAGING.SAMPLE_PRODUCTS (
  product_id   NUMBER        NOT NULL,      -- 整数
  product_name STRING(200)   NOT NULL,      -- 可変長文字列
  price        NUMBER(10,2)  NOT NULL,      -- 小数点あり数値
  in_stock     BOOLEAN       DEFAULT TRUE,  -- 真偽値
  created_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()  -- タイムゾーンなしタイムスタンプ
);

-- DDL②: CREATE TABLE AS SELECT（CTAS）
-- 既存テーブルの集計結果を新テーブルとして保存する（本編06章で多用）
CREATE OR REPLACE TABLE HANDS_ON_DB.STAGING.CATEGORY_SUMMARY AS
SELECT
  category,
  COUNT(*)         AS item_count,
  SUM(line_amount) AS total_amount,
  AVG(line_amount) AS avg_amount
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY category;

-- 確認
SELECT * FROM HANDS_ON_DB.STAGING.CATEGORY_SUMMARY ORDER BY total_amount DESC;

-- DDL③: ALTER TABLE — 列の追加・削除・名前変更・テーブル名変更
-- 列の追加
ALTER TABLE HANDS_ON_DB.STAGING.SAMPLE_PRODUCTS
  ADD COLUMN category STRING(100);

-- 列の削除
ALTER TABLE HANDS_ON_DB.STAGING.SAMPLE_PRODUCTS
  DROP COLUMN in_stock;

-- 列名の変更
ALTER TABLE HANDS_ON_DB.STAGING.SAMPLE_PRODUCTS
  RENAME COLUMN product_name TO name;

-- テーブル名の変更
ALTER TABLE HANDS_ON_DB.STAGING.SAMPLE_PRODUCTS
  RENAME TO HANDS_ON_DB.STAGING.SAMPLE_PRODUCTS_V2;

-- DDL④: TRUNCATE TABLE — データだけ消す（テーブル定義は残る）
-- DROP TABLE との違い: テーブル構造は残るためすぐ再利用できる
TRUNCATE TABLE HANDS_ON_DB.STAGING.SAMPLE_PRODUCTS_V2;

-- 確認（件数が0になる）
SELECT COUNT(*) FROM HANDS_ON_DB.STAGING.SAMPLE_PRODUCTS_V2;

-- DDL⑤: DROP TABLE IF EXISTS
-- IF EXISTS を付けると存在しない場合でもエラーにならない
DROP TABLE IF EXISTS HANDS_ON_DB.STAGING.SAMPLE_PRODUCTS_V2;
DROP TABLE IF EXISTS HANDS_ON_DB.STAGING.CATEGORY_SUMMARY;

-- DDL⑥: CREATE OR REPLACE VIEW
-- ビューはSELECTの「名前付き保存」。データは持たない。
CREATE OR REPLACE VIEW HANDS_ON_DB.MART.V_HIGH_VALUE_ORDERS AS
SELECT
  event_id, user_id, category, product_name, line_amount
FROM MART.FACT_PURCHASE_EVENTS
WHERE line_amount >= 5000;

-- 確認（ビューを普通のテーブルと同様にSELECT）
SELECT * FROM HANDS_ON_DB.MART.V_HIGH_VALUE_ORDERS LIMIT 10;

-- ビューの削除
DROP VIEW IF EXISTS HANDS_ON_DB.MART.V_HIGH_VALUE_ORDERS;

-- Check: SHOW TABLES / SHOW VIEWS でオブジェクト一覧を確認
SHOW TABLES IN SCHEMA HANDS_ON_DB.STAGING;
SHOW VIEWS  IN SCHEMA HANDS_ON_DB.MART;

-- Try this: FACT_PURCHASE_EVENTS のカテゴリ別サマリーを CTAS で新テーブルに保存してみる

-- ============================================================
-- Section 6: JOIN（INNER / LEFT / RIGHT / FULL OUTER）
-- ============================================================
-- What you learn: 複数テーブルの結合・4種類のJOINの違い

-- INNER JOIN: 両テーブルにキーが存在する行のみ返す（最も基本）
SELECT
  f.event_id,
  f.user_id,
  u.user_name,
  u.prefecture,
  f.category,
  f.line_amount
FROM MART.FACT_PURCHASE_EVENTS f
INNER JOIN MART.DIM_USERS u
  ON f.user_id = u.user_id
ORDER BY f.line_amount DESC
LIMIT 10;

-- LEFT JOIN: 左テーブル（FACT）の全行 + 右テーブルにキーがあれば結合
-- 右テーブルにキーがない場合は NULL を返す
SELECT
  f.event_id,
  f.user_id,
  u.user_name,   -- DIM_USERS にない user_id の場合は NULL
  f.line_amount
FROM MART.FACT_PURCHASE_EVENTS f
LEFT JOIN MART.DIM_USERS u
  ON f.user_id = u.user_id
ORDER BY f.line_amount DESC
LIMIT 10;

-- RIGHT JOIN: 右テーブル（DIM）の全行 + 左テーブルにキーがあれば結合
-- 実務では LEFT JOIN で書き換えることが多い（可読性のため）
SELECT
  f.event_id,
  u.user_id,
  u.user_name,
  f.line_amount   -- FACTにない場合は NULL
FROM MART.FACT_PURCHASE_EVENTS f
RIGHT JOIN MART.DIM_USERS u
  ON f.user_id = u.user_id;

-- FULL OUTER JOIN: 両テーブルの全行（マッチしない行もNULLで返す）
-- どちらか片方にしか存在しないレコードを確認するときに使う
SELECT
  f.event_id,
  u.user_id      AS dim_user_id,
  u.user_name,
  f.line_amount
FROM MART.FACT_PURCHASE_EVENTS f
FULL OUTER JOIN MART.DIM_USERS u
  ON f.user_id = u.user_id
WHERE f.event_id IS NULL OR u.user_id IS NULL;  -- 片方にしかない行

-- 3テーブルJOIN: FACT + DIM_PRODUCTS + DIM_USERS（本編06章のパターン）
SELECT
  f.event_id,
  u.user_name,
  u.prefecture,
  d.category,
  d.product_name,
  f.qty,
  f.line_amount
FROM MART.FACT_PURCHASE_EVENTS f
INNER JOIN MART.DIM_USERS u
  ON f.user_id = u.user_id
INNER JOIN MART.DIM_PRODUCTS d
  ON f.sku = d.sku
ORDER BY f.line_amount DESC
LIMIT 10;

-- LEFT JOIN と INNER JOIN の件数比較
-- 件数が変わる場合は DIM にないキーが存在することを示す
SELECT 'INNER JOIN' AS join_type, COUNT(*) AS row_count
FROM MART.FACT_PURCHASE_EVENTS f
INNER JOIN MART.DIM_USERS u ON f.user_id = u.user_id
UNION ALL
SELECT 'LEFT JOIN', COUNT(*)
FROM MART.FACT_PURCHASE_EVENTS f
LEFT JOIN MART.DIM_USERS u ON f.user_id = u.user_id;

-- Check: category ごとの都道府県別売上（FACT + DIM_USERS + DIM_PRODUCTS の3テーブルJOIN）
SELECT
  u.prefecture,
  d.category,
  SUM(f.line_amount) AS total_sales
FROM MART.FACT_PURCHASE_EVENTS f
INNER JOIN MART.DIM_USERS u ON f.user_id = u.user_id
INNER JOIN MART.DIM_PRODUCTS d ON f.sku = d.sku
GROUP BY u.prefecture, d.category
ORDER BY total_sales DESC;

-- Try this: DIM_DATE も JOIN して年月別カテゴリ売上を集計してみる

-- ============================================================
-- Section 7: サブクエリと CTE
-- ============================================================
-- What you learn: クエリのネスト・CTEによる可読性向上・多段変換パターン

-- ------------------------------------------------
-- サブクエリ①: FROM句（インラインビュー）
-- カテゴリ別合計金額を集計してから上位3件を取得
-- ------------------------------------------------
SELECT category, total_sales
FROM (
  SELECT
    category,
    SUM(line_amount) AS total_sales
  FROM MART.FACT_PURCHASE_EVENTS
  GROUP BY category
) AS cat_sales          -- サブクエリには必ず別名が必要
ORDER BY total_sales DESC
LIMIT 3;

-- ------------------------------------------------
-- サブクエリ②: WHERE句（IN）
-- DIM_PRODUCTS で 'Sports' を含むカテゴリに絞って FACT の明細を取得
-- ------------------------------------------------
SELECT event_id, product_name, line_amount
FROM MART.FACT_PURCHASE_EVENTS
WHERE category IN (
  SELECT DISTINCT category
  FROM MART.DIM_PRODUCTS
  WHERE category LIKE '%Sports%'
)
ORDER BY line_amount DESC;

-- ------------------------------------------------
-- サブクエリ③: スカラーサブクエリ（SELECT句）
-- 各行に「全体平均との差分」を付与する
-- ------------------------------------------------
SELECT
  event_id,
  category,
  line_amount,
  (SELECT AVG(line_amount) FROM MART.FACT_PURCHASE_EVENTS) AS global_avg,
  line_amount - (SELECT AVG(line_amount) FROM MART.FACT_PURCHASE_EVENTS) AS diff_from_avg
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY diff_from_avg DESC
LIMIT 10;

-- ------------------------------------------------
-- CTE①: 単一CTE（上記サブクエリ①をWITH句で書き直し）
-- CTE = WITH句で名前を付けたサブクエリ。後から名前で参照できる。
-- ------------------------------------------------
WITH cat_sales AS (
  SELECT
    category,
    SUM(line_amount) AS total_sales
  FROM MART.FACT_PURCHASE_EVENTS
  GROUP BY category
)
SELECT category, total_sales
FROM cat_sales
ORDER BY total_sales DESC
LIMIT 3;

-- ------------------------------------------------
-- CTE②: 複数CTEの連結
-- 月次集計 → カテゴリ内ランク付け
-- ------------------------------------------------
WITH monthly_sales AS (
  -- Step1: 月別・カテゴリ別の売上を集計
  SELECT
    DATE_TRUNC('month', event_time) AS sale_month,
    category,
    SUM(line_amount)                AS monthly_total
  FROM MART.FACT_PURCHASE_EVENTS
  GROUP BY DATE_TRUNC('month', event_time), category
),
ranked AS (
  -- Step2: 月ごとにカテゴリをランク付け
  SELECT
    sale_month,
    category,
    monthly_total,
    RANK() OVER (PARTITION BY sale_month ORDER BY monthly_total DESC) AS rank_in_month
  FROM monthly_sales
)
SELECT *
FROM ranked
WHERE rank_in_month <= 3       -- 各月の上位3カテゴリのみ
ORDER BY sale_month, rank_in_month;

-- ------------------------------------------------
-- CTE③: 多段変換パターン（raw → staging → mart の考え方をSQLで体験）
-- with_tier → tier_summary の2段変換
-- ------------------------------------------------
WITH with_tier AS (
  -- Step1（staging相当）: 金額帯フラグを付ける
  SELECT
    event_id,
    category,
    line_amount,
    CASE
      WHEN line_amount >= 10000 THEN 'high'
      WHEN line_amount >= 3000  THEN 'mid'
      ELSE                           'low'
    END AS price_tier
  FROM MART.FACT_PURCHASE_EVENTS
),
tier_summary AS (
  -- Step2（mart相当）: 金額帯×カテゴリで集計
  SELECT
    price_tier,
    category,
    COUNT(*)         AS transaction_count,
    SUM(line_amount) AS total_sales
  FROM with_tier
  GROUP BY price_tier, category
)
SELECT *
FROM tier_summary
ORDER BY price_tier, total_sales DESC;

-- Check: CTE を使ってユーザー別合計金額を出し、合計が最も高いユーザーを1件取得
WITH user_totals AS (
  SELECT
    user_id,
    SUM(line_amount) AS total_amount
  FROM MART.FACT_PURCHASE_EVENTS
  GROUP BY user_id
)
SELECT user_id, total_amount
FROM user_totals
ORDER BY total_amount DESC
LIMIT 1;

-- Try this: CTE を使って「カテゴリ別平均単価」を計算し、平均単価が5000円以上のカテゴリを出力してみる

-- ============================================================
-- Section 8: ウィンドウ関数
-- ============================================================
-- What you learn: 行を保ちながら集計値を付加・ランキング・累計・前後行参照

-- ------------------------------------------------
-- 概念確認: GROUP BY との違い
-- GROUP BY → 行数が減る（カテゴリ数分）
-- ウィンドウ関数 → 行数は変わらない（元の行数のまま）
-- ------------------------------------------------

-- GROUP BY（行数が減る）
SELECT category, SUM(line_amount) AS total
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY category;

-- SUM() OVER()（行数は変わらない）
SELECT
  event_id,
  category,
  line_amount,
  SUM(line_amount) OVER () AS grand_total   -- 全体合計を各行に付ける
FROM MART.FACT_PURCHASE_EVENTS
LIMIT 10;

-- ------------------------------------------------
-- SUM OVER: 全体合計・カテゴリ内合計・カテゴリ内シェアを同一クエリで
-- OVER() の PARTITION BY でグループを指定する
-- ------------------------------------------------
SELECT
  event_id,
  category,
  line_amount,
  SUM(line_amount) OVER ()                              AS grand_total,       -- 全体合計
  SUM(line_amount) OVER (PARTITION BY category)         AS category_total,    -- カテゴリ内合計
  ROUND(
    line_amount / SUM(line_amount) OVER (PARTITION BY category) * 100, 2
  )                                                     AS share_in_category  -- カテゴリ内シェア%
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY category, line_amount DESC;

-- ------------------------------------------------
-- ROW_NUMBER: 重複なし連番
-- 全体ランキング → PARTITION BY でカテゴリ内ランキングに変える
-- ------------------------------------------------

-- 全体ランキング（line_amount 降順）
SELECT
  event_id,
  category,
  line_amount,
  ROW_NUMBER() OVER (ORDER BY line_amount DESC) AS overall_rank
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY overall_rank
LIMIT 10;

-- カテゴリ内ランキング（PARTITION BY を加えるだけ）
SELECT
  event_id,
  category,
  line_amount,
  ROW_NUMBER() OVER (PARTITION BY category ORDER BY line_amount DESC) AS rank_in_category
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY category, rank_in_category;

-- ------------------------------------------------
-- RANK vs DENSE_RANK: 同順位の扱いの違い
-- RANK:       1, 1, 3（同順位の次は飛ぶ）
-- DENSE_RANK: 1, 1, 2（同順位の次は続く）
-- ------------------------------------------------
SELECT
  event_id,
  category,
  line_amount,
  RANK()       OVER (PARTITION BY category ORDER BY line_amount DESC) AS rank_with_gap,
  DENSE_RANK() OVER (PARTITION BY category ORDER BY line_amount DESC) AS rank_dense
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY category, rank_with_gap;

-- ------------------------------------------------
-- SUM OVER（累計）: ユーザー別購入累計
-- ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
-- = 最初の行から現在行までの合計
-- ------------------------------------------------
SELECT
  user_id,
  event_time,
  line_amount,
  SUM(line_amount) OVER (
    PARTITION BY user_id
    ORDER BY event_time
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_amount   -- ユーザー別累計購入金額
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY user_id, event_time;

-- ------------------------------------------------
-- LAG / LEAD: 前後の行の値を参照
-- LAG  → 前の行の値（デフォルト1行前）
-- LEAD → 次の行の値（デフォルト1行後）
-- ------------------------------------------------
SELECT
  user_id,
  event_time,
  line_amount,
  LAG(line_amount)  OVER (PARTITION BY user_id ORDER BY event_time) AS prev_amount,
  LEAD(line_amount) OVER (PARTITION BY user_id ORDER BY event_time) AS next_amount,
  line_amount - LAG(line_amount) OVER (PARTITION BY user_id ORDER BY event_time) AS diff_from_prev
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY user_id, event_time;

-- ------------------------------------------------
-- 応用: ROW_NUMBER + CTE でカテゴリ別1位のみ抽出
-- ウィンドウ関数はWHERE句に直接書けない → CTEでラップしてから絞る
-- ------------------------------------------------
WITH ranked AS (
  SELECT
    event_id,
    category,
    product_name,
    line_amount,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY line_amount DESC) AS rn
  FROM MART.FACT_PURCHASE_EVENTS
)
SELECT event_id, category, product_name, line_amount
FROM ranked
WHERE rn = 1   -- ← CTEを経由することでウィンドウ関数の結果をフィルタできる
ORDER BY category;

-- Check: カテゴリ内の line_amount の累計と全体に占めるシェア（%）を確認
SELECT
  event_id,
  category,
  line_amount,
  SUM(line_amount) OVER (PARTITION BY category ORDER BY line_amount DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_in_cat,
  ROUND(
    SUM(line_amount) OVER (PARTITION BY category ORDER BY line_amount DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    / SUM(line_amount) OVER (PARTITION BY category) * 100, 1
  ) AS cumulative_share_pct
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY category, line_amount DESC;

-- Try this: user_id ごとに最初の購入（event_time 昇順 ROW_NUMBER=1）を抽出してみる

-- ============================================================
-- Section 9: CASE WHEN と NULL処理
-- ============================================================
-- What you learn: 条件分岐列・グループ集計への応用・NULLの特殊な振る舞い

-- ------------------------------------------------
-- CASE WHEN①: SELECT句で条件分岐列（高額/中額/少額）
-- ------------------------------------------------
SELECT
  event_id,
  category,
  line_amount,
  CASE
    WHEN line_amount >= 10000 THEN '高額'
    WHEN line_amount >= 3000  THEN '中額'
    ELSE                           '少額'
  END AS price_tier
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY line_amount DESC;

-- ------------------------------------------------
-- CASE WHEN②: GROUP BY と組み合わせて金額帯ごとに集計
-- GROUP BY の列番号参照（列名の代わりに SELECT 内の位置を使う）
-- ------------------------------------------------
SELECT
  CASE
    WHEN line_amount >= 10000 THEN '高額'
    WHEN line_amount >= 3000  THEN '中額'
    ELSE                           '少額'
  END AS price_tier,
  COUNT(*)         AS transaction_count,
  SUM(line_amount) AS total_sales
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY 1          -- 1 = SELECT 句の1列目（price_tier）を指す
ORDER BY total_sales DESC;

-- ------------------------------------------------
-- CASE WHEN③: 0/1フラグを作ってSUMに活用
-- 「高額注文の件数」と「全体件数」を1クエリで取得
-- ------------------------------------------------
SELECT
  category,
  COUNT(*)                                                AS total_count,
  SUM(CASE WHEN line_amount >= 10000 THEN 1 ELSE 0 END) AS high_value_count,
  ROUND(
    SUM(CASE WHEN line_amount >= 10000 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
  )                                                       AS high_value_pct
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY category
ORDER BY high_value_pct DESC;

-- ------------------------------------------------
-- CASE WHEN④: IFF との比較（Snowflake固有の2択ショートカット）
-- IFF(条件, 真の値, 偽の値) = CASE WHEN 条件 THEN 真 ELSE 偽 END の短縮形
-- ------------------------------------------------
SELECT
  event_id,
  line_amount,
  IFF(line_amount >= 5000, '高額', '通常') AS tier_iff,
  CASE WHEN line_amount >= 5000 THEN '高額' ELSE '通常' END AS tier_case
FROM MART.FACT_PURCHASE_EVENTS
LIMIT 10;

-- ------------------------------------------------
-- NULL処理①: IS NULL / IS NOT NULL
-- = NULL は常に FALSE → 必ず IS NULL を使う
-- ------------------------------------------------

-- NG: この条件は常に FALSE（行が返らない）
-- SELECT * FROM MART.FACT_PURCHASE_EVENTS WHERE src_filename = NULL;

-- OK: IS NULL で正しく判定
SELECT COUNT(*) AS null_src_count
FROM MART.FACT_PURCHASE_EVENTS
WHERE src_filename IS NULL;

SELECT COUNT(*) AS non_null_src_count
FROM MART.FACT_PURCHASE_EVENTS
WHERE src_filename IS NOT NULL;

-- COUNT(*) と COUNT(列) の差がNULL行数
SELECT
  COUNT(*)          AS total_rows,
  COUNT(src_filename) AS non_null_rows,
  COUNT(*) - COUNT(src_filename) AS null_rows  -- この差がNULL件数
FROM MART.FACT_PURCHASE_EVENTS;

-- ------------------------------------------------
-- NULL処理②: COALESCE — 最初のNULLでない値を返す
-- LEFT JOINでNULLになる列を安全に置換する
-- ------------------------------------------------

-- DIM_USERS にない user_id は user_name が NULL になる
-- → COALESCE で '不明' に置換
SELECT
  f.event_id,
  f.user_id,
  COALESCE(u.user_name, '不明')    AS user_name,   -- NULLなら'不明'
  COALESCE(u.prefecture, '不明')   AS prefecture,
  f.line_amount
FROM MART.FACT_PURCHASE_EVENTS f
LEFT JOIN MART.DIM_USERS u ON f.user_id = u.user_id
ORDER BY f.line_amount DESC
LIMIT 10;

-- COALESCE は複数引数を取れる（最初のNULLでない値を返す）
SELECT COALESCE(NULL, NULL, 'first_non_null', 'ignored');  -- → 'first_non_null'

-- ------------------------------------------------
-- NULL処理③: NULLIF — ゼロ除算防止パターン
-- NULLIF(a, b): a = b のとき NULL を返す（そうでなければ a を返す）
-- ゼロ除算を防ぐ: qty = 0 のときに NULL を返して除算エラーを回避
-- ------------------------------------------------
SELECT
  event_id,
  line_amount,
  qty,
  NULLIF(qty, 0)                          AS safe_qty,    -- qty=0 なら NULL
  line_amount / NULLIF(qty, 0)            AS unit_price   -- ゼロ除算を回避
FROM MART.FACT_PURCHASE_EVENTS
LIMIT 10;

-- ------------------------------------------------
-- NULL処理④: NVL — Snowflake固有の2引数 COALESCE
-- NVL(value, default_value) = COALESCE(value, default_value)
-- ------------------------------------------------
SELECT
  event_id,
  NVL(src_filename, 'unknown')  AS safe_src_filename
FROM MART.FACT_PURCHASE_EVENTS
LIMIT 10;

-- Check: COALESCE と CASE WHEN を組み合わせて「user_nameがNULLなら'ゲスト'、そうでなければuser_name」を表示
SELECT
  f.event_id,
  f.user_id,
  CASE
    WHEN u.user_name IS NULL THEN 'ゲスト'
    ELSE u.user_name
  END AS display_name,
  -- COALESCE のほうが簡潔
  COALESCE(u.user_name, 'ゲスト') AS display_name_v2,
  f.line_amount
FROM MART.FACT_PURCHASE_EVENTS f
LEFT JOIN MART.DIM_USERS u ON f.user_id = u.user_id
ORDER BY f.line_amount DESC
LIMIT 10;

-- Try this: CASE WHEN で qty を「まとめ買い(3以上)」「通常(2)」「単品(1)」に分類し、
--           分類ごとの件数と合計金額を集計してみる

-- ============================================================
-- Section 10: Snowflake オブジェクト階層とネームスペース
-- ============================================================
-- What you learn: Account > Database > Schema > Table の階層構造・USE コマンド・
--                 完全修飾名・INFORMATION_SCHEMA によるメタデータ確認

-- オブジェクト階層の確認: SHOW コマンドで存在するオブジェクトを一覧表示
SHOW WAREHOUSES;
SHOW DATABASES;
SHOW SCHEMAS IN DATABASE HANDS_ON_DB;
SHOW TABLES IN SCHEMA HANDS_ON_DB.MART;

-- USE: アクティブなコンテキストを切り替える
-- SQL 実行時は「USE で設定した DB/Schema/WH」が既定として使われる
USE WAREHOUSE LEARN_WH;
USE DATABASE HANDS_ON_DB;
USE SCHEMA MART;

-- SELECT CURRENT_*: 現在のコンテキストを確認
SELECT
  CURRENT_WAREHOUSE()  AS current_wh,
  CURRENT_DATABASE()   AS current_db,
  CURRENT_SCHEMA()     AS current_schema,
  CURRENT_ROLE()       AS current_role,
  CURRENT_USER()       AS current_user;

-- 完全修飾名（Fully Qualified Name）: database.schema.table の形式
-- USE で設定していなくてもどこからでも参照できる
SELECT COUNT(*) FROM HANDS_ON_DB.MART.FACT_PURCHASE_EVENTS;    -- 完全修飾
SELECT COUNT(*) FROM MART.FACT_PURCHASE_EVENTS;                -- DB は USE で設定済みなら省略可
SELECT COUNT(*) FROM FACT_PURCHASE_EVENTS;                     -- DB + Schema 両方 USE 済みなら省略可

-- INFORMATION_SCHEMA: データベース内のメタデータを確認するシステムビュー
SELECT table_name, table_type, row_count, bytes
FROM HANDS_ON_DB.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'MART'
ORDER BY table_name;

-- Check: 現在のコンテキストと MART スキーマのテーブル一覧を確認
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE();
SHOW TABLES IN SCHEMA HANDS_ON_DB.MART;

-- Try this: INFORMATION_SCHEMA で STAGING スキーマのテーブル一覧を出力してみる

-- ============================================================
-- Section 11: ユーザー・ロール・権限の基本
-- ============================================================
-- What you learn: RBAC の考え方・システムロール・GRANT/REVOKE・FUTURE GRANTS
-- 実行ロール: SYSADMIN（カスタムロールの作成に必要）

-- ロールとは: 権限の「まとめ」。ユーザーに直接権限を付けず、ロールに権限を付けてユーザーに付与する
-- Snowflake システム定義ロール（よく使う3つ）
--   ACCOUNTADMIN: 最上位。全権限。普段は使わない
--   SYSADMIN:     WH・DB・Schema・Table の作成/操作。本編で使うロール
--   PUBLIC:       全ユーザーが自動で持つ最低限ロール

-- 現在のロールと付与されたロール一覧を確認
SELECT CURRENT_ROLE();
SHOW ROLES;

-- カスタムロールの作成例（SYSADMIN 権限が必要）
USE ROLE SYSADMIN;
CREATE ROLE IF NOT EXISTS ANALYST_ROLE;
-- カスタムロールは SYSADMIN 配下に置くのが Snowflake の推奨パターン
GRANT ROLE ANALYST_ROLE TO ROLE SYSADMIN;

-- 権限の付与: GRANT <権限> ON <オブジェクト> TO ROLE <ロール名>
-- DATABASE への USAGE（スキーマを「見る」権限）
GRANT USAGE ON DATABASE HANDS_ON_DB TO ROLE ANALYST_ROLE;

-- SCHEMA への USAGE（テーブルを「見る」権限）
GRANT USAGE ON SCHEMA HANDS_ON_DB.MART TO ROLE ANALYST_ROLE;

-- TABLE への SELECT（データを読む権限）
GRANT SELECT ON TABLE HANDS_ON_DB.MART.FACT_PURCHASE_EVENTS TO ROLE ANALYST_ROLE;

-- スキーマ内の全テーブルに一括付与
GRANT SELECT ON ALL TABLES IN SCHEMA HANDS_ON_DB.MART TO ROLE ANALYST_ROLE;

-- 将来作成されるテーブルにも自動で付与（FUTURE GRANTS）
GRANT SELECT ON FUTURE TABLES IN SCHEMA HANDS_ON_DB.MART TO ROLE ANALYST_ROLE;

-- 権限の確認
SHOW GRANTS TO ROLE ANALYST_ROLE;
SHOW GRANTS ON TABLE HANDS_ON_DB.MART.FACT_PURCHASE_EVENTS;

-- ユーザーへのロール付与（参考: 実際に試す場合は ACCOUNTADMIN が必要）
-- CREATE USER IF NOT EXISTS analyst_user PASSWORD='...' DEFAULT_ROLE=ANALYST_ROLE;
-- GRANT ROLE ANALYST_ROLE TO USER analyst_user;

-- 権限の取り消し: REVOKE
REVOKE SELECT ON ALL TABLES IN SCHEMA HANDS_ON_DB.MART FROM ROLE ANALYST_ROLE;

-- ロールの削除（後片付け）
DROP ROLE IF EXISTS ANALYST_ROLE;

-- Check: 自分のロールに付与されている権限を確認
SHOW GRANTS TO ROLE SYSADMIN;

-- Try this: ANALYST_ROLE を作成し、STAGING スキーマの全テーブルに SELECT を付与してみる
