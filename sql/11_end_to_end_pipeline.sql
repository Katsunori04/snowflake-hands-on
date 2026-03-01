-- What you learn:
-- - この教材の全体像を 1 本の流れとして確認する
-- - どこで raw / staging / mart / AI を使うか整理する

-- 【全体パイプライン図】
--
--   datasets/events_sample.json
--           │
--           │ Snowsight でアップロード（03章）
--           ▼
--     @RAW.EVENT_STAGE
--           │
--           │ COPY INTO / Snowpipe（03章）
--           ▼
--     RAW.RAW_EVENTS_PIPE
--           │
--           │ Stream → Task → MERGE（04章）
--           ▼
--     MART.FACT_PURCHASE_EVENTS
--           │
--           ├──→ DIM_USERS / DIM_PRODUCTS / DIM_DATE（06章）
--           └──→ AI_COMPLETE / AI_CLASSIFY（08章）

-- 1. raw にあるデータの確認（← 03章 COPY INTO で取り込んだデータ）
select
  raw:event_id::string as event_id,
  raw:user_id::string as user_id,
  raw:event_type::string as event_type
from LEARN_DB.RAW.RAW_EVENTS_PIPE
order by event_id;

-- 2. mart に変換された結果の確認（← 04章 Task の MERGE で生成したデータ）
select
  event_id,
  user_id,
  sku,
  category,
  qty,
  line_amount
from LEARN_DB.MART.FACT_PURCHASE_EVENTS
order by event_id, sku;

-- 3. star schema での集計（← 06章 DIM_PRODUCTS と JOIN）
select
  d.category,
  sum(f.line_amount) as sales_amount
from LEARN_DB.MART.FACT_PURCHASE_EVENTS f
join LEARN_DB.MART.DIM_PRODUCTS d
  on f.sku = d.sku
group by d.category
order by sales_amount desc;

-- 4. AI で review を要約（← 08章 STAGING.REVIEWS のデータ）
select
  review_id,
  AI_COMPLETE(
    'claude-3-5-sonnet',
    'Summarize this review in plain Japanese: ' || review_text
  ) as summary_ja
from LEARN_DB.STAGING.REVIEWS
order by review_id;

-- Final checkpoint:
-- - raw は元データを保持（03章）
-- - stream は差分を提供（04章）
-- - task は変換を定期実行（04章）
-- - mart は分析しやすい形（06章）
-- - AI はテキスト列の後段処理（08章）
