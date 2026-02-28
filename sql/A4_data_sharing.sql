-- ============================================================
-- 付録A4: Secure Data Sharing
-- SnowPro Core 対策 — Domain 1・Domain 6: Data Protection & Recovery
-- ============================================================
-- 実行前提: 04_streams_tasks.sql・06_star_schema.sql を完了していること
-- 使用ロール: ACCOUNTADMIN（SHARE の作成・Consumer 追加に必要）
--             SYSADMIN（Secure View の作成は可能）
-- ============================================================

-- ============================================================
-- Step 1: SHARE オブジェクトの作成
-- SHARE = 共有のコンテナ。ここにオブジェクトを追加して Consumer に渡す
-- ============================================================

USE ROLE ACCOUNTADMIN;  -- SHARE の作成には ACCOUNTADMIN が必要

-- Share オブジェクトを作成
CREATE SHARE IF NOT EXISTS LEARN_DB_SHARE
  COMMENT = 'LEARN_DB のサンプルデータを共有するShare（ハンズオン用）';

-- Share の一覧確認
SHOW SHARES;

-- ============================================================
-- Step 2: Share に共有するオブジェクトを追加
-- USAGE → SELECT の順で付与が必要（オブジェクト権限と同じ考え方）
-- ============================================================

-- データベースへのアクセスを許可
GRANT USAGE ON DATABASE LEARN_DB TO SHARE LEARN_DB_SHARE;

-- スキーマへのアクセスを許可
GRANT USAGE ON SCHEMA LEARN_DB.MART TO SHARE LEARN_DB_SHARE;

-- テーブルへの SELECT を許可（通常テーブルは直接共有可能）
GRANT SELECT ON TABLE LEARN_DB.MART.FACT_PURCHASE_EVENTS TO SHARE LEARN_DB_SHARE;

-- Share に含まれるオブジェクトの確認
SHOW GRANTS TO SHARE LEARN_DB_SHARE;

-- ============================================================
-- Step 3: Consumer アカウントを Share に追加
-- ※ 実際の Consumer アカウント識別子が必要。ハンズオンでは概念確認のみ
-- ============================================================

-- 実際のアカウント識別子（例: myorg-myaccount）に変更してください
-- ALTER SHARE LEARN_DB_SHARE ADD ACCOUNTS = 'myorg-myaccount';

-- Share の詳細確認（Consumer アカウントが設定されているか）
-- DESCRIBE SHARE LEARN_DB_SHARE;

-- ============================================================
-- Step 4: Secure View の作成（個人情報を除外して共有）
-- 通常の VIEW は Share に追加できないため SECURE VIEW を使う
-- Secure View では Consumer から内部 SQL が見えない（実装が隠蔽される）
-- ============================================================

USE ROLE SYSADMIN;
USE DATABASE LEARN_DB;
USE SCHEMA MART;
USE WAREHOUSE LEARN_WH;

-- Secure View の作成（user_id を除外してプライバシーを保護）
CREATE OR REPLACE SECURE VIEW MART.SECURE_FACT_PURCHASE AS
SELECT
  event_id,
  event_time,
  sku,
  category,
  qty,
  line_amount
  -- user_id は除外（個人情報保護のため Consumer には見せない）
FROM MART.FACT_PURCHASE_EVENTS;

-- Secure View の内容確認
SELECT * FROM MART.SECURE_FACT_PURCHASE LIMIT 5;

-- Secure View が「SECURE」として作成されているかを確認
SHOW VIEWS LIKE 'SECURE_FACT_PURCHASE' IN SCHEMA MART;

-- ============================================================
-- Step 5: Secure View を Share に追加
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- Secure View へのアクセスを Share に付与
GRANT SELECT ON VIEW LEARN_DB.MART.SECURE_FACT_PURCHASE TO SHARE LEARN_DB_SHARE;

-- Share の内容確認
SHOW GRANTS TO SHARE LEARN_DB_SHARE;

-- ============================================================
-- Step 6: Consumer 側の操作（別アカウントで実行するコマンドの参考）
-- ※ 同一アカウントでは実行できません（参考コードとして確認してください）
-- ============================================================

-- Consumer 側: Share からデータベースを作成
-- CREATE DATABASE SHARED_LEARN_DB
--   FROM SHARE <provider_account_identifier>.LEARN_DB_SHARE;

-- Consumer 側: 通常の SELECT でデータを参照
-- SELECT * FROM SHARED_LEARN_DB.MART.SECURE_FACT_PURCHASE LIMIT 10;

-- ============================================================
-- Step 7: Reader Account の作成（Consumer が Snowflake を持っていない場合）
-- ※ 実際に作成するとコストが発生するため、概念確認のみ
-- ============================================================

-- Reader Account を作成（Provider が Consumer のアカウントを代理作成）
-- CREATE MANAGED ACCOUNT reader_account_for_partner
--   ADMIN_NAME = 'partner_admin'
--   ADMIN_PASSWORD = '<secure_password>'
--   TYPE = READER;

-- Reader Account の一覧
-- SHOW MANAGED ACCOUNTS;

-- ============================================================
-- 後片付け: Share を削除
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- Share を削除（Consumer がいない場合のみ削除可能）
DROP SHARE IF EXISTS LEARN_DB_SHARE;

-- Secure View を削除
USE ROLE SYSADMIN;
DROP VIEW IF EXISTS MART.SECURE_FACT_PURCHASE;
