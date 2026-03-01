-- What you learn:
-- - Semantic View でビジネス定義（メトリクス・ディメンション）をデータベースに登録する
-- - Cortex Analyst で自然言語の質問を SQL に変換する
-- - Cortex Search でテキストのハイブリッド検索サービスを構築する
--
-- Prerequisite:
--   06_star_schema.sql が完了していること（MART.FACT_PURCHASE_EVENTS, DIM_USERS, DIM_PRODUCTS）
--   09_ai_sql.sql が完了していること（STAGING.REVIEWS）
--   grant database role SNOWFLAKE.CORTEX_USER to role <your_role>;

use warehouse LEARN_WH;
use database LEARN_DB;

-- ============================================================
-- 1. Semantic View
-- ============================================================
-- ビジネスのメトリクスとディメンションを、物理テーブルとは別に定義する。
-- Cortex Analyst はこの定義を読んで自然言語の質問を SQL に変換する。
--
-- TABLES       : 対象テーブルとエイリアス・主キーを登録する
-- RELATIONSHIPS: テーブル間の結合条件（FK → PK）を宣言する
-- DIMENSIONS   : 集計の「切り口」（誰が・何が・どこで）を定義する
-- METRICS      : 集計する「値」（SUM / COUNT / AVG）を定義する

use schema MART;

CREATE OR REPLACE SEMANTIC VIEW MART.SEM_PURCHASE_EVENTS
  TABLES (
    -- エイリアス AS 物理テーブル名  PRIMARY KEY (主キー列)
    fact     AS MART.FACT_PURCHASE_EVENTS PRIMARY KEY (event_id, sku),
    users    AS MART.DIM_USERS            PRIMARY KEY (user_id),
    products AS MART.DIM_PRODUCTS         PRIMARY KEY (sku)
  )
  RELATIONSHIPS (
    -- fact の user_id → DIM_USERS の user_id（多対1）
    fact (user_id) REFERENCES users,
    -- fact の sku → DIM_PRODUCTS の sku（多対1）
    fact (sku)     REFERENCES products
  )
  DIMENSIONS (
    -- DIMENSION 名 AS テーブルエイリアス.物理列名
    fact.event_time_dim       AS fact.event_time,
    fact.category_dim         AS fact.category,
    users.user_name_dim       AS users.user_name,
    users.prefecture_dim      AS users.prefecture,
    products.product_name_dim AS products.product_name,
    products.category_dim     AS products.category
  )
  METRICS (
    -- METRIC 名 AS 集計式
    -- Cortex Analyst がこれらの名前で集計を理解する
    fact.total_sales   AS SUM(fact.line_amount),   -- 売上合計
    fact.total_qty     AS SUM(fact.qty),            -- 購入数量合計
    fact.order_count   AS COUNT(DISTINCT fact.event_id), -- 注文件数
    fact.avg_price     AS AVG(fact.price)           -- 平均単価
  )
;

-- Check: Semantic View の定義を確認する
DESCRIBE SEMANTIC VIEW MART.SEM_PURCHASE_EVENTS;
SHOW SEMANTIC VIEWS IN SCHEMA MART;


-- ============================================================
-- 2. Cortex Analyst（REST API 呼び出し）
-- ============================================================
-- Cortex Analyst は SQL 関数ではなく REST API で呼び出す。
-- 以下の Python コードをローカル PC または Snowsight の Python Worksheet で実行する。
--
-- 必要なライブラリ:
--   pip install snowflake-connector-python requests
--
-- ----------------------------------------------------------------
-- Python コード（SQL ファイルにはコメントとして記載）
-- ----------------------------------------------------------------
--
-- import snowflake.connector
-- import requests
-- import json
--
-- ACCOUNT   = "your-account.snowflakecomputing.com"
-- USER      = "your_user"
-- PASSWORD  = "your_password"
--
-- # セッショントークンを取得
-- conn = snowflake.connector.connect(
--     account=ACCOUNT, user=USER, password=PASSWORD,
--     database="LEARN_DB", schema="MART", warehouse="LEARN_WH"
-- )
-- token = conn.rest.token
--
-- # Cortex Analyst API を呼び出す
-- url = f"https://{ACCOUNT}/api/v2/cortex/analyst/message"
-- headers = {
--     "Authorization": f'Snowflake Token="{token}"',
--     "Content-Type":  "application/json",
-- }
-- payload = {
--     "messages": [
--         {
--             "role": "user",
--             "content": [{"type": "text", "text": "都道府県別の売上合計を教えてください"}]
--         }
--     ],
--     "semantic_view": "LEARN_DB.MART.SEM_PURCHASE_EVENTS"
-- }
--
-- response = requests.post(url, headers=headers, json=payload)
-- result   = response.json()
--
-- for block in result["message"]["content"]:
--     if block["type"] == "text":
--         print("AI の解釈:", block["text"])
--     elif block["type"] == "sql":
--         print("生成された SQL:\n", block["statement"])
-- ----------------------------------------------------------------
--
-- Snowsight UI で試す場合:
--   左メニュー → AI & ML → Cortex Analyst
--   → Semantic View で LEARN_DB.MART.SEM_PURCHASE_EVENTS を選択
--   → 自然言語で質問する


-- ============================================================
-- 3. Cortex Search
-- ============================================================
-- テキストを全文検索（BM25）とベクトル検索（Embedding）で
-- ハイブリッドにインデックス化するサービスを作成する。
-- 作成後は TARGET_LAG のスケジュールで自動更新される。
--
-- ON         : 検索対象の列（テキスト型）
-- ATTRIBUTES : フィルタリングに使える列（@eq などで絞り込み可能）
-- TARGET_LAG : ベーステーブルの更新からの最大遅延時間

use schema STAGING;

CREATE OR REPLACE CORTEX SEARCH SERVICE STAGING.REVIEW_SEARCH
  ON         review_text          -- review_text を全文+ベクトルでインデックス化
  ATTRIBUTES user_id              -- user_id でフィルタリングできるようにする
  WAREHOUSE  = LEARN_WH
  TARGET_LAG = '1 day'           -- 1日以内に最新データを反映
AS
  SELECT review_id, user_id, review_text
  FROM STAGING.REVIEWS;

-- Check: サービスの状態を確認
SHOW CORTEX SEARCH SERVICES IN SCHEMA STAGING;
DESCRIBE CORTEX SEARCH SERVICE STAGING.REVIEW_SEARCH;


-- ============================================================
-- 4. Cortex Search でクエリを発行する
-- ============================================================
-- SEARCH_PREVIEW 関数でハイブリッド検索を実行する。
-- 引数: (サービス名, JSON 文字列)
-- JSON のキー: query（検索語）, columns（返す列）, limit（件数）, filter（絞り込み）

-- Run this first: 「comfortable shoes」に意味的に近いレビューを検索
SELECT
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'STAGING.REVIEW_SEARCH',
    '{
      "query":   "comfortable shoes",
      "columns": ["review_id", "user_id", "review_text"],
      "limit":   3
    }'
  ) AS raw_results;

-- Check: 結果を LATERAL FLATTEN で行に展開して読みやすくする
SELECT
  r.value:review_id::STRING   AS review_id,
  r.value:user_id::STRING     AS user_id,
  r.value:review_text::STRING AS review_text
FROM (
  SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'STAGING.REVIEW_SEARCH',
    '{"query": "comfortable shoes", "columns": ["review_id","user_id","review_text"], "limit": 3}'
  ) AS raw
),
LATERAL FLATTEN(INPUT => raw:results) r;

-- フィルター付き検索: user_id = 'u002' のレビューのみ対象
SELECT
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'STAGING.REVIEW_SEARCH',
    '{
      "query":   "delivery problem",
      "columns": ["review_id", "review_text"],
      "filter":  {"@eq": {"user_id": "u002"}},
      "limit":   5
    }'
  ) AS filtered_results;

-- Try this:
-- 1. SEM_PURCHASE_EVENTS に新しいメトリクス（例: MAX(line_amount) AS max_line_amount）を追加し
--    Cortex Analyst に「最も高額な明細を教えてください」と質問してみる
-- 2. REVIEW_SEARCH の検索クエリを "coffee aroma" に変えて、
--    意味的に近いレビュー（r002）がヒットすることを確認してみる
