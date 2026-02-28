-- ============================================================
-- 付録A2: Time Travel / Fail-safe / Zero-Copy Cloning
-- SnowPro Core 対策 — Domain 1・Domain 6: Data Protection & Recovery
-- ============================================================
-- 実行前提: 04_streams_tasks.sql を完了していること（FACT_PURCHASE_EVENTS が存在）
-- 使用ロール: SYSADMIN
-- ============================================================

USE ROLE SYSADMIN;
USE DATABASE LEARN_DB;
USE SCHEMA MART;
USE WAREHOUSE LEARN_WH;

-- ============================================================
-- 事前準備: バックアップ用テーブルを作成（DROP → UNDROP の練習用）
-- ============================================================

-- 練習用にテーブルを複製しておく（クローン機能で作成）
CREATE TABLE MART.FACT_PURCHASE_EVENTS_BACKUP
  CLONE MART.FACT_PURCHASE_EVENTS;

SELECT COUNT(*) FROM MART.FACT_PURCHASE_EVENTS_BACKUP;

-- ============================================================
-- Step 1: Time Travel — OFFSET（秒単位で過去に遡る）
-- ============================================================

-- 10分前（600秒前）の状態を参照
-- ※ テーブルが作成されたばかりの場合、データが少ない可能性がある
SELECT *
FROM MART.FACT_PURCHASE_EVENTS
  AT (OFFSET => -60 * 10)  -- 10分前
LIMIT 5;

-- 1時間前の状態を参照
SELECT COUNT(*) AS row_count_1hr_ago
FROM MART.FACT_PURCHASE_EVENTS
  AT (OFFSET => -3600);  -- 3600秒 = 1時間

-- ============================================================
-- Step 2: Time Travel — TIMESTAMP（特定時刻を絶対指定）
-- ============================================================

-- ※ 実際に存在する時刻（テーブル作成後）を指定してください
--   下記の日時は例です。適宜書き換えてください
SELECT *
FROM MART.FACT_PURCHASE_EVENTS
  AT (TIMESTAMP => '2026-02-28 00:00:00'::TIMESTAMP_NTZ)
LIMIT 5;

-- AT（その時刻を含む）と BEFORE（その直前）の違いを確認
-- BEFORE: 特定クエリを実行する直前の状態（クエリIDが必要）
-- SELECT * FROM MART.FACT_PURCHASE_EVENTS
--   BEFORE (STATEMENT => '<クエリIDをここに貼る>');

-- ============================================================
-- Step 3: UNDROP — テーブルを削除して復元する
-- ============================================================

-- ① バックアップテーブルを削除
DROP TABLE MART.FACT_PURCHASE_EVENTS_BACKUP;

-- 削除されたことを確認（エラーになれば OK）
-- SELECT COUNT(*) FROM MART.FACT_PURCHASE_EVENTS_BACKUP;

-- ② UNDROP で復元（Time Travel 期間内であれば復元可能）
UNDROP TABLE MART.FACT_PURCHASE_EVENTS_BACKUP;

-- 復元されたことを確認
SELECT COUNT(*) FROM MART.FACT_PURCHASE_EVENTS_BACKUP;

-- ============================================================
-- Step 4: Zero-Copy Cloning — テーブルをゼロコピー複製
-- ============================================================

-- ① テーブルをクローン（メタデータのみコピー・データは共有）
--   実行は即時完了する（データ量に関わらず）
CREATE OR REPLACE TABLE MART.FACT_PURCHASE_EVENTS_CLONE
  CLONE MART.FACT_PURCHASE_EVENTS;

-- クローンのデータ件数を確認（元テーブルと同じはず）
SELECT COUNT(*) AS clone_count FROM MART.FACT_PURCHASE_EVENTS_CLONE;
SELECT COUNT(*) AS original_count FROM MART.FACT_PURCHASE_EVENTS;

-- ② データベース丸ごとクローン（開発環境の複製に有効）
-- ※ 実行すると LEARN_DB_CLONE が作成されます。不要な場合はコメントアウトしてください
-- CREATE DATABASE LEARN_DB_CLONE
--   CLONE LEARN_DB;

-- ============================================================
-- Step 5: クローンのストレージ情報を確認
-- クローン直後は ACTIVE_BYTES がほぼ 0 であることを確認する
-- ============================================================

SELECT
  TABLE_SCHEMA,
  TABLE_NAME,
  ACTIVE_BYTES,
  TIME_TRAVEL_BYTES
FROM TABLE(INFORMATION_SCHEMA.TABLE_STORAGE_METRICS(
  DATABASE_NAME => 'LEARN_DB'
))
WHERE TABLE_NAME ILIKE '%CLONE%' OR TABLE_NAME ILIKE '%BACKUP%'
ORDER BY TABLE_NAME;

-- ============================================================
-- Step 6: Time Travel 期間の確認と変更
-- ============================================================

-- 現在の Data Retention 期間を確認
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS'
  IN TABLE MART.FACT_PURCHASE_EVENTS;

-- Time Travel 期間を 7 日に変更（Enterprise 以上で最大 90 日）
-- ※ Standard エディションは最大 1 日のため、1以上を設定するとエラーになる場合あり
ALTER TABLE MART.FACT_PURCHASE_EVENTS
  SET DATA_RETENTION_TIME_IN_DAYS = 1;  -- Standard では 1 が上限

-- 設定を元に戻す（デフォルトはアカウント設定に従う）
ALTER TABLE MART.FACT_PURCHASE_EVENTS
  UNSET DATA_RETENTION_TIME_IN_DAYS;

-- ============================================================
-- Step 7: 過去の時点をクローン（バグ修正前の状態を退避するユースケース）
-- ============================================================

-- 1時間前のデータをクローンとして保存
-- ※ 本番でのデータ修正前のスナップショット取得に有効
CREATE OR REPLACE TABLE MART.FACT_PURCHASE_EVENTS_SNAPSHOT
  CLONE MART.FACT_PURCHASE_EVENTS
  AT (OFFSET => -3600);  -- 1時間前の状態

SELECT COUNT(*) FROM MART.FACT_PURCHASE_EVENTS_SNAPSHOT;

-- ============================================================
-- 後片付け: 練習で作成したテーブルを削除
-- ============================================================

DROP TABLE IF EXISTS MART.FACT_PURCHASE_EVENTS_BACKUP;
DROP TABLE IF EXISTS MART.FACT_PURCHASE_EVENTS_CLONE;
DROP TABLE IF EXISTS MART.FACT_PURCHASE_EVENTS_SNAPSHOT;
-- DROP DATABASE IF EXISTS LEARN_DB_CLONE;  -- DB クローンを作った場合
