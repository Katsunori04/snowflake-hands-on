-- ============================================================
-- 付録A1: Snowflake アーキテクチャ詳細
-- SnowPro Core 対策 — Domain 1: Snowflake Data Cloud Features & Architecture
-- ============================================================
-- 実行前提: 00_setup.sql〜06_star_schema.sql を完了していること
-- 使用ロール: SYSADMIN（ACCOUNT_USAGE は ACCOUNTADMIN が必要）
-- ============================================================

USE ROLE SYSADMIN;
USE DATABASE LEARN_DB;
USE SCHEMA MART;
USE WAREHOUSE LEARN_WH;

-- ============================================================
-- Step 1: マイクロパーティション情報の確認
-- 「どの列でどのくらいクラスタされているか」を確認する関数
-- ============================================================

-- FACT_PURCHASE_EVENTS のパーティション状態を確認
-- 返り値の JSON キーの意味:
--   total_partition_count  : パーティション総数
--   total_constant_partition_count: 単一値のみのパーティション数（Pruning しやすい）
--   average_overlaps       : パーティション間の重複度（低いほど Pruning が効く）
--   average_depth          : 重複の深さ（1に近いほど良い）
SELECT SYSTEM$CLUSTERING_INFORMATION('MART.FACT_PURCHASE_EVENTS');

-- event_time 列での Pruning 適性を確認
-- 列を指定することで「その列についての」クラスタリング情報を取得できる
SELECT SYSTEM$CLUSTERING_INFORMATION(
  'MART.FACT_PURCHASE_EVENTS',
  '(event_time::DATE)'
);

-- ============================================================
-- Step 2: テーブルのストレージ情報を確認
-- 各テーブルの実使用バイト数・Time Travel 保持バイト数などが分かる
-- ============================================================

SELECT
  TABLE_SCHEMA,
  TABLE_NAME,
  ACTIVE_BYTES,          -- 現在のアクティブデータのバイト数
  TIME_TRAVEL_BYTES,     -- Time Travel で保持しているバイト数
  FAILSAFE_BYTES,        -- Fail-safe で保持しているバイト数
  RETAINED_FOR_CLONE_BYTES  -- クローンのために保持しているバイト数
FROM TABLE(INFORMATION_SCHEMA.TABLE_STORAGE_METRICS(
  DATABASE_NAME => 'LEARN_DB'
))
ORDER BY ACTIVE_BYTES DESC;

-- ============================================================
-- Step 3: Virtual Warehouse のメタデータを確認
-- SIZE, STATE（起動中/停止中）, クエリキューの状態などが分かる
-- ============================================================

SHOW WAREHOUSES;

-- 現在の Warehouse サイズを確認
SELECT CURRENT_WAREHOUSE();

-- Warehouse を一時的に XS に変更（本番環境では注意して実施）
-- ALTER WAREHOUSE LEARN_WH SET WAREHOUSE_SIZE = 'XSMALL';

-- ============================================================
-- Step 4: Cloud Services 層のコスト（Warehouse 使用履歴）を確認
-- ※ ACCOUNTADMIN または SNOWFLAKE データベース権限が必要
-- ============================================================

-- Warehouse ごとのクレジット消費履歴（Cloud Services は Compute とは別に記録される）
SELECT
  WAREHOUSE_NAME,
  START_TIME,
  END_TIME,
  CREDITS_USED,              -- Compute 層のクレジット消費
  CREDITS_USED_CLOUD_SERVICES -- Cloud Services 層のクレジット消費
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'LEARN_WH'
ORDER BY START_TIME DESC
LIMIT 10;

-- ============================================================
-- Step 5: データベース・スキーマの構造確認
-- Information Schema でオブジェクト一覧を取得
-- ============================================================

-- 現在のデータベース内の全テーブルを確認
SELECT
  TABLE_SCHEMA,
  TABLE_NAME,
  TABLE_TYPE,
  ROW_COUNT,
  BYTES,
  CREATED
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- ============================================================
-- 確認クエリ: 3層アーキテクチャの動作を実感する
-- ============================================================

-- クエリを2回実行し、2回目が Result Cache から返ることを確認（付録A5 で詳しく解説）
SELECT category, COUNT(*) AS cnt, SUM(line_amount) AS total
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY category
ORDER BY total DESC;

-- ↑ 同じクエリをもう一度実行 → Snowsight の「Query Details」で
--   "Bytes scanned" が 0 になっていれば Result Cache が効いている
