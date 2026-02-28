-- What you learn:
-- - internal stage を作る
-- - COPY INTO と Snowpipe を使い分ける
-- - stage -> pipe -> raw table の流れを作る

-- 【02章との違い】
-- 02章では RAW.RAW_EVENTS に SQL の INSERT で直接データを入れた（練習用）。
-- 03章ではファイルから取り込む本線の仕組みを作る。
--   RAW_EVENTS      → 02章で SQL INSERT した練習用テーブル
--   RAW_EVENTS_PIPE → ファイルから COPY INTO / Snowpipe で取り込む本線用テーブル
-- 04章以降は RAW_EVENTS_PIPE を参照するので、この章を必ず実行しておくこと。

use warehouse LEARN_WH;
use database LEARN_DB;
use schema RAW;

create or replace file format RAW.JSON_FF
  type = json
  strip_outer_array = false;

create or replace stage RAW.EVENT_STAGE
  file_format = RAW.JSON_FF;

create or replace table RAW.RAW_EVENTS_PIPE (
  raw variant,
  src_filename string,
  loaded_at timestamp_ntz default current_timestamp()
);

-- 【Snowsight でファイルをアップロードする手順】
-- datasets/events_sample.json を @RAW.EVENT_STAGE にアップロードしてください。
--
-- 画面操作手順:
--   1. 左メニューの [Data] をクリック
--   2. [Databases] > [LEARN_DB] > [RAW] > [Stages] を展開
--   3. [EVENT_STAGE] をクリック
--   4. 右上の [+ Files] ボタンをクリック
--   5. datasets/events_sample.json を選択してアップロード
--
-- アップロード確認 SQL（ファイルが見えれば成功）:
list @RAW.EVENT_STAGE;

-- その後、まずは手動ロードを試します。
--
-- COPY INTO オプションの補足:
--   on_error = 'CONTINUE'  → エラー行をスキップして処理を続行（今回の設定）
--   on_error = 'ABORT_STATEMENT' → エラーが出たら即中止（デフォルト）
--   on_error = 'SKIP_FILE'      → エラーがあったファイル全体をスキップ
copy into RAW.RAW_EVENTS_PIPE(raw, src_filename)
from (
  select
    $1,
    metadata$filename
  from @RAW.EVENT_STAGE
)
file_format = (format_name = RAW.JSON_FF)
on_error = 'CONTINUE';

-- COPY INTO 実行後の検証（件数とファイル名を確認）
select count(*) as row_count from RAW.RAW_EVENTS_PIPE;

select
  src_filename,
  raw:event_id::string as event_id,
  raw:user_id::string as user_id,
  loaded_at
from RAW.RAW_EVENTS_PIPE
order by loaded_at;

-- 自動取り込み用 pipe
-- auto_ingest = false の意味:
--   false → 手動で alter pipe ... refresh を実行してデータを取り込む（このハンズオンの設定）
--   true  → S3/GCS などのイベント通知と連携して、ファイルが置かれると自動で取り込む
--            ※ true にするにはクラウドストレージ側でイベント通知設定が必要
create or replace pipe RAW.EVENTS_PIPE
  auto_ingest = false
as
copy into RAW.RAW_EVENTS_PIPE(raw, src_filename)
from (
  select
    $1,
    metadata$filename
  from @RAW.EVENT_STAGE
)
file_format = (format_name = RAW.JSON_FF)
on_error = 'CONTINUE';

-- Check.
show stages like 'EVENT_STAGE' in schema RAW;
show pipes like 'EVENTS_PIPE' in schema RAW;
select system$pipe_status('RAW.EVENTS_PIPE');

-- Try this:
-- RAW.RAW_EVENTS ではなく RAW.RAW_EVENTS_PIPE に入った JSON から event_type を確認してください。
