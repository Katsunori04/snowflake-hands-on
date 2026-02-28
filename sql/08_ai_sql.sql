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
    'Fast delivery and good quality. The shoes were comfortable for trail running.'
  ),
  (
    'r002',
    'u002',
    'Coffee aroma was excellent, but the package arrived slightly damaged.'
  ),
  (
    'r003',
    'u003',
    'The product was okay overall, but setup instructions were confusing.'
  );

-- 1. AI_COMPLETE: 1文要約
-- 💰 コスト注意: AI 関数はトークン消費に応じた課金が発生します
-- 目安: ~$0.008/1,000トークン、1,000行適用で概算 $5〜$20
-- 推奨: まず LIMIT 10 で動作確認してから全件適用してください
select
  review_id,
  review_text,
  AI_COMPLETE(
    'claude-3-5-sonnet',
    'Summarize this customer review in one short sentence in Japanese: ' || review_text
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
      object_construct('label', 'positive', 'description', 'Overall favorable review'),
      object_construct('label', 'neutral', 'description', 'Mixed or balanced review'),
      object_construct('label', 'negative', 'description', 'Mostly dissatisfied review')
    ],
    object_construct('task_description', 'Classify customer review sentiment')
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
      'product_quality', 'What does the review say about quality?',
      'delivery', 'What does the review say about shipping or delivery?',
      'issue', 'What problem or complaint is mentioned?'
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
    'claude-3-5-sonnet',
    'Write one Japanese bullet summarizing the likely customer intent behind this review: ' || r.review_text
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
