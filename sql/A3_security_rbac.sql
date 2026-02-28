-- ============================================================
-- 付録A3: セキュリティ・RBAC・データマスキング
-- SnowPro Core 対策 — Domain 2: Account Access and Security（20%）
-- ============================================================
-- 実行前提: 04_streams_tasks.sql を完了していること（FACT_PURCHASE_EVENTS が存在）
-- 使用ロール: SYSADMIN（Network Policy は ACCOUNTADMIN が必要）
-- ============================================================

USE ROLE SYSADMIN;
USE DATABASE LEARN_DB;
USE WAREHOUSE LEARN_WH;

-- ============================================================
-- Step 1: カスタムロールの作成とロール階層の設定
-- ============================================================

-- アナリスト向けロールとデータエンジニア向けロールを作成
CREATE ROLE IF NOT EXISTS ANALYST_ROLE;
CREATE ROLE IF NOT EXISTS DATA_ENGINEER_ROLE;

-- ロール階層に組み込む（SYSADMIN 配下に置くことで ACCOUNTADMIN からも管理可能）
-- ※ これをしないと ACCOUNTADMIN がオブジェクトを所有するロールを継承できない
GRANT ROLE ANALYST_ROLE TO ROLE SYSADMIN;
GRANT ROLE DATA_ENGINEER_ROLE TO ROLE SYSADMIN;

-- ロール階層の確認
SHOW ROLES;

-- ※ ユーザーにロールを付与する場合は <your_user> を実際のユーザー名に変更
-- GRANT ROLE ANALYST_ROLE TO USER <your_user>;

-- ============================================================
-- Step 2: オブジェクト権限の付与（最小権限の原則）
-- USAGE → SELECT の順で付与が必要
-- ============================================================

-- データベースへのアクセスを許可
GRANT USAGE ON DATABASE LEARN_DB TO ROLE ANALYST_ROLE;

-- スキーマへのアクセスを許可
GRANT USAGE ON SCHEMA LEARN_DB.MART TO ROLE ANALYST_ROLE;
GRANT USAGE ON SCHEMA LEARN_DB.STAGING TO ROLE ANALYST_ROLE;

-- 特定テーブルへの SELECT 権限を付与
GRANT SELECT ON TABLE LEARN_DB.MART.FACT_PURCHASE_EVENTS TO ROLE ANALYST_ROLE;

-- Warehouse の使用権限を付与
GRANT USAGE ON WAREHOUSE LEARN_WH TO ROLE ANALYST_ROLE;

-- 付与された権限を確認
SHOW GRANTS TO ROLE ANALYST_ROLE;

-- DATA_ENGINEER_ROLE には書き込み権限も付与
GRANT USAGE ON DATABASE LEARN_DB TO ROLE DATA_ENGINEER_ROLE;
GRANT USAGE ON SCHEMA LEARN_DB.MART TO ROLE DATA_ENGINEER_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE LEARN_DB.MART.FACT_PURCHASE_EVENTS
  TO ROLE DATA_ENGINEER_ROLE;
GRANT USAGE ON WAREHOUSE LEARN_WH TO ROLE DATA_ENGINEER_ROLE;

-- ============================================================
-- Step 3: Dynamic Data Masking（列単位のアクセス制御）
-- ロールに応じて user_id の表示内容を変える
-- ============================================================

USE SCHEMA STAGING;

-- マスキングポリシーを作成
-- 引数: val（マスク対象の列の値）
-- 戻り値: ロールに応じてマスクするかそのまま返すかを決定
CREATE OR REPLACE MASKING POLICY STAGING.MASK_USER_ID
  AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE', 'SYSADMIN', 'ACCOUNTADMIN') THEN val
    ELSE REGEXP_REPLACE(val, '.', '*')  -- ANALYST_ROLE 等には全文字を '*' でマスク
  END;

-- マスキングポリシーを FACT_PURCHASE_EVENTS の user_id 列に適用
ALTER TABLE MART.FACT_PURCHASE_EVENTS
  MODIFY COLUMN user_id
  SET MASKING POLICY STAGING.MASK_USER_ID;

-- 適用状況の確認
-- Information Schema でポリシーの適用先を確認
SELECT *
FROM INFORMATION_SCHEMA.POLICY_REFERENCES(
  POLICY_NAME => 'LEARN_DB.STAGING.MASK_USER_ID'
);

-- SYSADMIN では user_id が見える（マスクされない）
USE ROLE SYSADMIN;
SELECT event_id, user_id, sku FROM MART.FACT_PURCHASE_EVENTS LIMIT 5;

-- ANALYST_ROLE に切り替えると user_id がマスクされる
-- USE ROLE ANALYST_ROLE;
-- SELECT event_id, user_id, sku FROM MART.FACT_PURCHASE_EVENTS LIMIT 5;

USE ROLE SYSADMIN;  -- SYSADMIN に戻す

-- ============================================================
-- Step 4: Row Access Policy（行単位のアクセス制御）
-- ロールに応じてアクセスできる行（sku）を制限する
-- ============================================================

USE SCHEMA STAGING;

-- Row Access Policy を作成
-- sku_col の値を引数に取り、アクセスを許可する行かどうかを BOOLEAN で返す
CREATE OR REPLACE ROW ACCESS POLICY STAGING.ROW_POLICY_BY_SKU
  AS (sku_col STRING) RETURNS BOOLEAN ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN', 'DATA_ENGINEER_ROLE')
      THEN TRUE                              -- 全行アクセス可
    WHEN CURRENT_ROLE() = 'ANALYST_ROLE'
      THEN sku_col LIKE 'A%'               -- 'A' で始まる SKU のみ表示
    ELSE FALSE                              -- その他のロールは全行非表示
  END;

-- Row Access Policy をテーブルに追加
ALTER TABLE MART.FACT_PURCHASE_EVENTS
  ADD ROW ACCESS POLICY STAGING.ROW_POLICY_BY_SKU ON (sku);

-- 適用状況の確認
SELECT *
FROM INFORMATION_SCHEMA.POLICY_REFERENCES(
  POLICY_NAME => 'LEARN_DB.STAGING.ROW_POLICY_BY_SKU'
);

-- SYSADMIN では全行が見える
USE ROLE SYSADMIN;
SELECT sku, COUNT(*) AS cnt FROM MART.FACT_PURCHASE_EVENTS GROUP BY sku;

-- ANALYST_ROLE に切り替えると A始まりのSKUのみ表示される
-- USE ROLE ANALYST_ROLE;
-- SELECT sku, COUNT(*) AS cnt FROM MART.FACT_PURCHASE_EVENTS GROUP BY sku;

USE ROLE SYSADMIN;  -- SYSADMIN に戻す

-- ============================================================
-- Step 5: Network Policy の作成（概念確認）
-- IP アドレスで接続元を制限する
-- ※ 実際にアカウントへ適用すると、この IP レンジ外からのアクセスがすべてブロックされます
-- ============================================================

-- Network Policy の作成（SYSADMIN で作成可能、適用は ACCOUNTADMIN が必要）
CREATE OR REPLACE NETWORK POLICY ALLOW_OFFICE_IPS
  ALLOWED_IP_LIST = ('203.0.113.0/24')   -- オフィスのIPレンジ（例: RFC 5737 のドキュメント用アドレス）
  BLOCKED_IP_LIST = ()                   -- ブロックリスト（空でも可）
  COMMENT = 'オフィスIPレンジからのみ接続を許可するポリシー（サンプル）';

-- Network Policy の一覧確認
SHOW NETWORK POLICIES;

-- ユーザー単位での適用（SECURITYADMIN 以上が必要）
-- ALTER USER <your_user> SET NETWORK_POLICY = ALLOW_OFFICE_IPS;

-- アカウント全体への適用（ACCOUNTADMIN が必要 ※実行注意）
-- ALTER ACCOUNT SET NETWORK_POLICY = ALLOW_OFFICE_IPS;

-- ============================================================
-- 後片付け: 練習で作成したポリシーとロールを削除
-- ============================================================

-- Row Access Policy を先に解除してからポリシーを削除
ALTER TABLE MART.FACT_PURCHASE_EVENTS
  DROP ROW ACCESS POLICY STAGING.ROW_POLICY_BY_SKU;

-- Masking Policy を先に解除してからポリシーを削除
ALTER TABLE MART.FACT_PURCHASE_EVENTS
  MODIFY COLUMN user_id
  UNSET MASKING POLICY;

-- ポリシーの削除
DROP MASKING POLICY IF EXISTS STAGING.MASK_USER_ID;
DROP ROW ACCESS POLICY IF EXISTS STAGING.ROW_POLICY_BY_SKU;
DROP NETWORK POLICY IF EXISTS ALLOW_OFFICE_IPS;

-- ロールの削除（権限の REVOKE 後に削除）
REVOKE ROLE ANALYST_ROLE FROM ROLE SYSADMIN;
REVOKE ROLE DATA_ENGINEER_ROLE FROM ROLE SYSADMIN;
DROP ROLE IF EXISTS ANALYST_ROLE;
DROP ROLE IF EXISTS DATA_ENGINEER_ROLE;
