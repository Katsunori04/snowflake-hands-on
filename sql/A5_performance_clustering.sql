-- ============================================================
-- 付録A5: パフォーマンス詳細（キャッシュ・クラスタリング・Query Profile）
-- SnowPro Core 対策 — Domain 4: Performance Concepts（15%）
-- ============================================================
-- 実行前提: 04_streams_tasks.sql・06_star_schema.sql を完了していること
-- 使用ロール: SYSADMIN
-- ============================================================

USE ROLE SYSADMIN;
USE DATABASE LEARN_DB;
USE SCHEMA MART;
USE WAREHOUSE LEARN_WH;

-- ============================================================
-- Step 1: Result Cache を体感する
-- 同じ SQL を2回実行し、2回目が「キャッシュから」返ることを確認する
-- ============================================================

-- 1回目: Warehouse でデータをスキャン（時間がかかる）
SELECT category, SUM(line_amount) AS sales, COUNT(*) AS transactions
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY category
ORDER BY sales DESC;

-- ↑ 同じ SQL をもう一度実行してください
-- Snowsight の「Query Details」→「Profile Overview」で
--   "Bytes scanned: 0 B" になっていれば Result Cache がヒットしています

-- ============================================================
-- Step 2: Result Cache をバイパスして強制的に再スキャン
-- USE_CACHED_RESULT = FALSE でキャッシュを無効化できる
-- ============================================================

-- Result Cache を無効化
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- キャッシュなしで実行（1回目と同じ時間がかかるはず）
SELECT category, SUM(line_amount) AS sales, COUNT(*) AS transactions
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY category
ORDER BY sales DESC;

-- Result Cache を有効化（デフォルトに戻す）
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

-- ============================================================
-- Step 3: マイクロパーティション情報の確認
-- SYSTEM$CLUSTERING_INFORMATION で現在のパーティション状態を確認する
-- ============================================================

-- event_time::DATE での Pruning 適性を確認
SELECT SYSTEM$CLUSTERING_INFORMATION(
  'MART.FACT_PURCHASE_EVENTS',
  '(event_time::DATE)'
);
-- 返り値の JSON の見方:
--   total_partition_count     : 総パーティション数
--   total_constant_partition_count: 同一値のみのパーティション数（Pruning しやすい）
--   average_overlaps          : パーティション間の重複度（0 が理想）
--   average_depth             : 重複の深さ（1 が理想）

-- category での Pruning 適性を確認
SELECT SYSTEM$CLUSTERING_INFORMATION(
  'MART.FACT_PURCHASE_EVENTS',
  '(category)'
);

-- ============================================================
-- Step 4: Clustering Key を設定してパフォーマンスを改善する
-- ============================================================

-- ①新しいテーブルをクラスタリングキー付きで作成
--   CLUSTER BY を指定すると自動クラスタリングが有効になる
CREATE OR REPLACE TABLE MART.FACT_PURCHASE_EVENTS_CLUSTERED
  CLUSTER BY (event_time::DATE, category)
AS
  SELECT * FROM MART.FACT_PURCHASE_EVENTS;

-- クラスタリング後のパーティション状態を確認
-- average_overlaps が元テーブルより低ければ Pruning 効率が改善している
SELECT SYSTEM$CLUSTERING_INFORMATION(
  'MART.FACT_PURCHASE_EVENTS_CLUSTERED',
  '(event_time::DATE, category)'
);

-- ②既存テーブルに Clustering Key を追加（ALTER TABLE で後から設定も可能）
--   注意: 設定後、自動クラスタリングがバックグラウンドで実行されるまで
--         クラスタリング効果は出ない（サービスクレジットが消費される）
ALTER TABLE MART.FACT_PURCHASE_EVENTS
  CLUSTER BY (event_time::DATE);

-- テーブルのクラスタリング設定を確認
SHOW TABLES LIKE 'FACT_PURCHASE_EVENTS' IN SCHEMA MART;

-- ③既存テーブルのクラスタリングキーを削除（コスト抑制のため）
ALTER TABLE MART.FACT_PURCHASE_EVENTS
  DROP CLUSTERING KEY;

-- ============================================================
-- Step 5: クラスタリングのコストを確認
-- Automatic Clustering がどのくらいのクレジットを消費しているかを確認
-- ============================================================

SELECT
  TABLE_NAME,
  START_TIME,
  END_TIME,
  CREDITS_USED,
  NUM_BYTES_RECLUSTERED
FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
WHERE TABLE_NAME ILIKE 'FACT_PURCHASE_EVENTS%'
ORDER BY START_TIME DESC
LIMIT 10;

-- ============================================================
-- Step 6: Pruning の効果を比較するクエリ
-- 同じデータを Clustered / Non-Clustered で検索して差を確認する
-- ============================================================

-- ① Clustering Key なしのテーブルで検索
SELECT SUM(line_amount) AS sales
FROM MART.FACT_PURCHASE_EVENTS
WHERE event_time::DATE = CURRENT_DATE() - 7;
-- Query Profile で "Partitions Scanned / Partitions Total" を確認

-- ② Clustering Key ありのテーブルで同じ検索
SELECT SUM(line_amount) AS sales
FROM MART.FACT_PURCHASE_EVENTS_CLUSTERED
WHERE event_time::DATE = CURRENT_DATE() - 7;
-- Clustering されていれば Partitions Scanned が大幅に少ないはず

-- ============================================================
-- Step 7: Warehouse サイズの確認
-- クエリのスループットとコストのトレードオフ
-- ============================================================

-- 現在の Warehouse サイズを確認
SHOW WAREHOUSES LIKE 'LEARN_WH';

-- Warehouse サイズ変更（サイズアップするとクレジット消費も倍増）
-- XS = 1 クレジット/時間, S = 2, M = 4, L = 8, XL = 16 ...
-- ALTER WAREHOUSE LEARN_WH SET WAREHOUSE_SIZE = 'SMALL';

-- クエリの実行履歴と処理時間を確認
SELECT
  QUERY_ID,
  QUERY_TEXT,
  WAREHOUSE_SIZE,
  TOTAL_ELAPSED_TIME / 1000 AS elapsed_sec,
  BYTES_SCANNED,
  PARTITIONS_SCANNED,
  PARTITIONS_TOTAL
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE WAREHOUSE_NAME = 'LEARN_WH'
  AND START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC
LIMIT 20;

-- ============================================================
-- 後片付け: 練習で作成したテーブルを削除
-- ============================================================

DROP TABLE IF EXISTS MART.FACT_PURCHASE_EVENTS_CLUSTERED;
