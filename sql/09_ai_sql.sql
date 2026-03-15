-- What you learn:
-- - AI_COMPLETE で要約する
-- - AI_CLASSIFY で分類する
-- - AI_EXTRACT で必要情報を抜き出す
--
-- Prerequisite:
-- grant database role SNOWFLAKE.CORTEX_USER to role <your_role>;
--
-- 【CORTEX_USER 権限の取得手順】
-- AI 関数（AI_COMPLETE / AI_CLASSIFY / AI_EXTRACT）を使うには
-- SNOWFLAKE.CORTEX_USER データベースロールが必要です。
--
-- 1. 自分のロールを確認する:
--    SELECT current_role();
--
-- 2. ACCOUNTADMIN や SYSADMIN で以下を実行する:
--    -- 例: 自分のロールが SYSADMIN の場合
--    GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE SYSADMIN;
--
--    -- 例: カスタムロール MY_ROLE に付与する場合
--    GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE MY_ROLE;
--
-- 3. ロールを切り替えて再ログイン（またはセッションをリフレッシュ）すると
--    権限が有効になります。Snowsight の場合は一度ログアウト→ログインし直すと確実。

use warehouse LEARN_WH;
use database LEARN_DB;
use schema STAGING;

create or replace table STAGING.REVIEWS (
  review_id string,
  user_id string,
  review_text string
);

truncate table STAGING.REVIEWS;

insert into STAGING.REVIEWS values
  (
    'r001',
    'u001',
    '配送がとても速く、品質も申し分ありませんでした。シューズはトレイルランニングに最適で快適に使えました。'
  ),
  (
    'r002',
    'u002',
    'コーヒーの香りは素晴らしかったですが、パッケージが少し破損した状態で届きました。'
  ),
  (
    'r003',
    'u003',
    '商品自体は概ね問題ありませんでしたが、セットアップの説明書がわかりにくく困りました。'
  );

-- 1. AI_COMPLETE: 1文要約
-- 💰 コスト注意: AI 関数はトークン消費に応じた課金が発生します
-- 目安: ~$0.008/1,000トークン、1,000行適用で概算 $5〜$20
-- 推奨: まず LIMIT 10 で動作確認してから全件適用してください
select
  review_id,
  review_text,
  AI_COMPLETE(
    'claude-sonnet-4-6',
    '以下のカスタマーレビューを日本語で1文に要約してください: ' || review_text
  ) as summary_ja
from STAGING.REVIEWS;

-- 2. AI_CLASSIFY: 感情分類
-- 💰 コスト注意: AI 関数はトークン消費に応じた課金が発生します
-- 目安: ~$0.008/1,000トークン、1,000行適用で概算 $5〜$20
-- 推奨: まず LIMIT 10 で動作確認してから全件適用してください
select
  review_id,
  AI_CLASSIFY(
    review_text,
    [
      object_construct('label', 'positive', 'description', '全体的に好意的なレビュー'),
      object_construct('label', 'neutral',  'description', '賛否混在、またはバランスの取れたレビュー'),
      object_construct('label', 'negative', 'description', '概ね不満のあるレビュー')
    ],
    object_construct('task_description', 'カスタマーレビューの感情を分類してください')
  ) as sentiment_result
from STAGING.REVIEWS;

-- 3. AI_EXTRACT: 何について書かれているかを抽出
-- 💰 コスト注意: AI 関数はトークン消費に応じた課金が発生します
-- 目安: ~$0.008/1,000トークン、1,000行適用で概算 $5〜$20
-- 推奨: まず LIMIT 10 で動作確認してから全件適用してください
select
  review_id,
  AI_EXTRACT(
    review_text,
    object_construct(
      'product_quality', 'レビューは商品の品質についてどのように述べていますか？',
      'delivery',        'レビューは配送・発送についてどのように述べていますか？',
      'issue',           'どのような問題や不満が述べられていますか？'
    )
  ) as extracted_points
from STAGING.REVIEWS;

-- 4. structured data と合わせる例
-- 💰 コスト注意: AI 関数はトークン消費に応じた課金が発生します
-- 目安: ~$0.008/1,000トークン、1,000行適用で概算 $5〜$20
-- 推奨: まず LIMIT 10 で動作確認してから全件適用してください
select
  e.event_id,
  e.user_id,
  e.event_time,
  r.review_id,
  AI_COMPLETE(
    'claude-sonnet-4-6',
    '以下のレビューから読み取れる顧客の意図を日本語で1行にまとめてください: ' || r.review_text
  ) as customer_intent
from STAGING.STG_EVENTS e
join STAGING.REVIEWS r
  on e.user_id = r.user_id
order by e.event_time;

-- Check.
select * from STAGING.REVIEWS order by review_id;

-- Try this:
-- AI_CLASSIFY のラベルを sentiment ではなく product / delivery / usability に変えてみてください。

-- ============================================================
-- AI 関数のコスト確認
-- ============================================================

-- AI サービスの利用コストを日別に確認
-- ※ ACCOUNT_USAGE ビューはリアルタイムではなく最大3時間遅延があります
SELECT
    USAGE_DATE,
    SERVICE_TYPE,
    CREDITS_USED,
    CREDITS_USED * 3 AS ESTIMATED_COST_USD  -- $3/クレジットの概算
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE SERVICE_TYPE = 'AI_SERVICES'
ORDER BY USAGE_DATE DESC
LIMIT 30;
