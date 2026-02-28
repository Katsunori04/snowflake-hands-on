-- What you learn:
-- - stream で差分を取る
-- - task で定期実行する
-- - Snowflake 内でバッチ的な処理を作る

-- 【この章の位置づけ】
-- 03章で RAW_EVENTS_PIPE（ファイルから取り込んだ生データ）が準備できた。
-- この章では「新しく追加されたデータだけ」を効率よく FACT テーブルに反映する
-- Stream + Task の仕組みを作る。
--
-- フロー:
--   @EVENT_STAGE (Stage)
--       ↓ COPY INTO / Snowpipe（03章）
--   RAW_EVENTS_PIPE（生データテーブル）
--       ↓ Stream が差分を記録（この章）
--   RAW_EVENTS_STREAM
--       ↓ Task が定期的に MERGE を実行（この章）
--   FACT_PURCHASE_EVENTS（分析用 FACT テーブル）

-- ============================================================
-- 【Stream の時系列フロー】
-- ============================================================
-- 1. データ INSERT
--    → Stream にオフセット付き変更行が積まれる
--      （各行に metadata$action='INSERT', metadata$isupdate=false が付く）
--
-- 2. Task が起動（スケジュールまたは手動）
--    → Stream から WHERE metadata$action='INSERT' で新規行を取得
--    → MERGE ステートメントを実行
--
-- 3. MERGE で FACT テーブルに挿入/更新
--    → WHEN MATCHED    : 既存行を UPDATE
--    → WHEN NOT MATCHED: 新規行を INSERT
--
-- 4. MERGE 成功 → Stream のオフセットが進む
--    → 次回 Stream を参照すると、処理済み行は見えなくなる
--    → 新しい変更行のみが Stream に現れる
--
-- ※ MERGE 実行後の Stream には DELETE メタ行が記録されるが、
--    metadata$action='INSERT' フィルタで除外することで二重処理を防ぐ
-- ============================================================

use warehouse LEARN_WH;
use database LEARN_DB;
use schema RAW;

-- Stream を作成: RAW_EVENTS_PIPE への変更（INSERT / DELETE）を追跡するオブジェクト。
-- Stream を作成した時点以降の変更だけが記録される（過去の変更は含まれない）。
-- この Stream を SELECT / MERGE するたびに「前回読んだ後の差分」が返る。
create or replace stream RAW.RAW_EVENTS_STREAM
  on table RAW.RAW_EVENTS_PIPE;

use schema MART;

-- FACT テーブルに product_name / category を持つ理由:
-- スタースキーマでは通常これらは DIM_PRODUCTS に置くが、
-- 「購入時点の商品情報」を記録するため FACT 側にも非正規化して持たせている。
-- 商品マスタが後から変わっても、購入当時の名前・カテゴリが保持される。
create or replace table MART.FACT_PURCHASE_EVENTS (
  event_id     string,
  user_id      string,
  event_time   timestamp_ntz,
  sku          string,
  product_name string,
  category     string,
  qty          number,
  price        number(10,2),
  line_amount  number(12,2),
  src_filename string,
  inserted_at  timestamp_ntz default current_timestamp()
);

-- ============================================================
-- 手動 MERGE（動作確認用）
-- ============================================================
-- Stream の差分を FACT に反映する MERGE 文。
-- まずは手動で実行して Stream のオフセットが進む仕組みを確認する。
-- ※ この MERGE が成功した時点で Stream のオフセットが進む。
--   失敗（エラーやウェアハウス停止）した場合はオフセットは進まず、
--   次回の実行時に同じ行が再度 Stream に現れる（べき等性）。

-- 【metadata$action の説明】
-- Stream が変更を記録する際に付与する操作種別カラム。取りうる値:
--   'INSERT' → 新規挿入された行
--   'DELETE' → 削除された行
--   'UPDATE' は DELETE（元の値）+ INSERT（新しい値）の 2 行として記録される
-- ここでは新しく追加されたイベントのみを FACT に取り込みたいので
-- where s.metadata$action = 'INSERT' でフィルタしている。
--
-- 【MERGE の ON 条件が event_id + sku の複合キーである理由】
-- 1 イベントには複数の商品（SKU）が含まれる可能性がある（items 配列）。
-- 例: event_id="e001" に A001（Trail Shoes）と B001（Coffee Beans）の 2 行
-- event_id だけでは一意に特定できないため、sku を加えて複合キーとしている。
merge into MART.FACT_PURCHASE_EVENTS tgt    -- ← 書き込み先（FACT テーブル）
using (
  select
    s.raw:event_id::string as event_id,
    s.raw:user_id::string as user_id,
    to_timestamp_ntz(s.raw:event_time::string) as event_time,
    item.value:sku::string as sku,
    item.value:product_name::string as product_name,
    item.value:category::string as category,
    item.value:qty::number as qty,
    item.value:price::number(10,2) as price,
    item.value:qty::number * item.value:price::number(10,2) as line_amount,
    s.src_filename
  from RAW.RAW_EVENTS_STREAM s,             -- ← Stream から「未処理の新規行のみ」を取得
  lateral flatten(input => s.raw:items) item  -- ← items 配列を 1行 1商品に展開
  where s.metadata$action = 'INSERT'        -- ← DELETE 行（MERGE 由来のメタ行）を除外
) src
on tgt.event_id = src.event_id              -- ← 重複チェックのキー（event_id + sku）
and tgt.sku = src.sku                       -- ← 1イベント内の複数商品を個別に管理
when matched then update set                -- ← 既存行があれば上書き（べき等性の確保）
  tgt.user_id      = src.user_id,
  tgt.event_time   = src.event_time,
  tgt.product_name = src.product_name,
  tgt.category     = src.category,
  tgt.qty          = src.qty,
  tgt.price        = src.price,
  tgt.line_amount  = src.line_amount,
  tgt.src_filename = src.src_filename
when not matched then insert (              -- ← 新しい行ならそのまま INSERT
  event_id,
  user_id,
  event_time,
  sku,
  product_name,
  category,
  qty,
  price,
  line_amount,
  src_filename
) values (
  src.event_id,
  src.user_id,
  src.event_time,
  src.sku,
  src.product_name,
  src.category,
  src.qty,
  src.price,
  src.line_amount,
  src.src_filename
);

-- MERGE 後の確認
-- カテゴリ別売上（5行返ること）
select
  category,
  sum(line_amount) as sales_amount,
  count(*) as item_count
from MART.FACT_PURCHASE_EVENTS
group by category
order by sales_amount desc;

-- 月別イベント数（3行返ること: 2025-12 / 2026-01 / 2026-02）
select
  date_trunc('month', event_time) as month,
  count(*) as event_count
from MART.FACT_PURCHASE_EVENTS
group by 1
order by 1;

-- ============================================================
-- 定期実行用 Task
-- ============================================================
-- Task は作成直後 SUSPENDED 状態。alter task ... resume で開始する。
-- Stream を使った MERGE なので、差分がない場合は MERGE が何も処理しない（高速）。
--
-- 【CRON 式の読み方】
-- schedule = 'USING CRON 分 時 日 月 曜日 タイムゾーン'
--
-- 例: 'USING CRON 0/5 * * * * Asia/Tokyo'
--   0/5  → 0分始まりで5分ごと（0, 5, 10, 15, ... 分）
--   *    → 毎時
--   *    → 毎日
--   *    → 毎月
--   *    → 毎曜日
--   Asia/Tokyo → 日本時間（JST = UTC+9）
--
-- よく使うパターン:
--   'USING CRON 0 1 * * * Asia/Tokyo'     → 毎日 01:00 JST
--   'USING CRON 0 * * * * Asia/Tokyo'     → 毎時 0 分（毎時正時）
--   'USING CRON 0 9 * * 1 Asia/Tokyo'     → 毎週月曜 09:00 JST
create or replace task STAGING.LOAD_FACT_PURCHASE_EVENTS
  warehouse = LEARN_WH
  schedule  = 'USING CRON 0/5 * * * * Asia/Tokyo'   -- 5 分ごとに実行
as
-- ↓ 手動 MERGE と全く同じ文をここに記述する
-- （Task の AS 節に SQL を直書きする「04章スタイル」。05章では SP に切り出す）
merge into MART.FACT_PURCHASE_EVENTS tgt
using (
  select
    s.raw:event_id::string as event_id,
    s.raw:user_id::string as user_id,
    to_timestamp_ntz(s.raw:event_time::string) as event_time,
    item.value:sku::string as sku,
    item.value:product_name::string as product_name,
    item.value:category::string as category,
    item.value:qty::number as qty,
    item.value:price::number(10,2) as price,
    item.value:qty::number * item.value:price::number(10,2) as line_amount,
    s.src_filename
  from RAW.RAW_EVENTS_STREAM s,
  lateral flatten(input => s.raw:items) item
  where s.metadata$action = 'INSERT'
) src
on tgt.event_id = src.event_id
and tgt.sku = src.sku
when matched then update set
  tgt.user_id      = src.user_id,
  tgt.event_time   = src.event_time,
  tgt.product_name = src.product_name,
  tgt.category     = src.category,
  tgt.qty          = src.qty,
  tgt.price        = src.price,
  tgt.line_amount  = src.line_amount,
  tgt.src_filename = src.src_filename
when not matched then insert (
  event_id,
  user_id,
  event_time,
  sku,
  product_name,
  category,
  qty,
  price,
  line_amount,
  src_filename
) values (
  src.event_id,
  src.user_id,
  src.event_time,
  src.sku,
  src.product_name,
  src.category,
  src.qty,
  src.price,
  src.line_amount,
  src.src_filename
);

-- Task の開始と確認
-- ※ 作成直後は SUSPENDED。resume しないとスケジュールが動かない。
alter task STAGING.LOAD_FACT_PURCHASE_EVENTS resume;
show tasks like 'LOAD_FACT_PURCHASE_EVENTS' in schema STAGING;

-- 必要に応じて停止
-- alter task STAGING.LOAD_FACT_PURCHASE_EVENTS suspend;

-- Check.
-- Stream の未処理差分を確認（MERGE 後は空になっているはず）
select * from RAW.RAW_EVENTS_STREAM;
select * from MART.FACT_PURCHASE_EVENTS order by event_time, event_id, sku;

-- Task の実行ログを確認（5分後に再確認）
select *
from table(information_schema.task_history())
order by scheduled_time desc
limit 20;

-- Try this:
-- schedule を毎日 01:00 実行に変えるならどう書くか考えてみてください。
-- 答え例: schedule = 'USING CRON 0 1 * * * Asia/Tokyo'
