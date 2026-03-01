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

-- ============================================================
-- オプション: GENERATOR を使って大量テストデータを生成する
-- ============================================================
-- 03章のファイル取り込み（200件）完了後に実行すると合計 1000件程度になる。
--
-- ■ 目的
--   GROUP BY 集計や売上推移の分析を試す際に、より現実的なデータ量で体験するため。
--   本番では Snowpipe で継続的にデータが蓄積されるが、ハンズオンでは
--   GENERATOR でまとめて疑似データを投入して同様の体験ができる。
--
-- ■ GENERATOR とは
--   TABLE(GENERATOR(ROWCOUNT => N)) は N 行のダミー行を生成する仮想テーブル。
--   seq4() で 0 始まりの連番、UNIFORM(min, max, RANDOM()) で乱数を生成できる。
--
-- ■ この SQL で生成するデータの設計
--   - イベント数 : 800件（ファイル取り込みの 200件と合わせて ~1000件）
--   - ユーザー   : u001〜u030 からランダム
--   - 期間       : 2025-12-01〜2026-02-28（3ヶ月）
--   - 商品       : SKU0001〜SKU0100 からランダム（100種）
--   - デバイス   : iOS / Android / PC のいずれか
--   - 数量       : 1〜3 のランダム
--   - 価格       : 100〜9999 のランダム
-- ============================================================
INSERT INTO RAW.RAW_EVENTS_PIPE (raw, src_filename, loaded_at)
SELECT
  PARSE_JSON(
    '{"event_id":"gen_' || seq4()::STRING || '",'
    || '"user_id":"u' || LPAD(UNIFORM(1, 30, RANDOM())::STRING, 3, '0') || '",'
    || '"event_type":"purchase",'
    || '"event_time":"' || DATEADD(second, UNIFORM(0, 7776000, RANDOM()),
         '2025-12-01'::TIMESTAMP_NTZ)::STRING || 'Z",'
    || '"device":{"os":"' || CASE MOD(seq4(),3)
         WHEN 0 THEN 'iOS' WHEN 1 THEN 'Android' ELSE 'PC' END || '","app_version":"2.0.0"},'
    || '"items":[{"sku":"SKU' || LPAD(UNIFORM(1,100,RANDOM())::STRING,4,'0') || '", "product_name":"Product_' || UNIFORM(1,50,RANDOM())::STRING || '", "category":"Category_' || UNIFORM(1,10,RANDOM())::STRING || '", "qty":' || UNIFORM(1,3,RANDOM())::STRING || ', "price":' || UNIFORM(100,9999,RANDOM())::STRING || '}]}'
  ),
  'generated',
  CURRENT_TIMESTAMP()
FROM TABLE(GENERATOR(ROWCOUNT => 800));

-- 生成後の件数確認
SELECT
  src_filename,
  COUNT(*) AS row_count
FROM RAW.RAW_EVENTS_PIPE
GROUP BY src_filename
ORDER BY src_filename;
