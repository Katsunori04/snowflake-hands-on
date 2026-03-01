-- What you learn:
-- - ストアドプロシージャで SQL ロジックを再利用する
-- - Dynamic Table で Stream + Task を宣言的に置き換える
-- - Task の AFTER 句で依存 Task を作り DAG を組む
-- - Snowflake Alerts で異常を検知・通知する

-- 【04章との違い】
-- 04章: Task の AS 節に MERGE 文を直書き
--         Task → MERGE（直書き） → FACT
-- 05章: MERGE をプロシージャ化し、Task から CALL する
--         Task → CALL SP → MERGE（SP 内） → FACT
--
-- メリット: SP に切り出すと「CALL SP_NAME()」1行でテスト・再利用できる。
--           Task を修正しなくても SP の内容を変えるだけでロジックを更新できる。

use warehouse LEARN_WH;
use database LEARN_DB;

-- ============================================================
-- 1. ストアドプロシージャ
-- ============================================================
-- 04章の MERGE ロジックをプロシージャとしてカプセル化する。
-- これにより:
--   - Task の AS 節に長い MERGE 文を書く代わりに CALL SP_NAME() と書ける
--   - CALL MART.SP_MERGE_PURCHASE_EVENTS() でいつでも手動テストできる
--   - 複数の Task や他の SP からも同じロジックを再利用できる

use schema MART;

-- ストアドプロシージャ: 04章の「Task に直書きしていた MERGE 文」を関数化したもの
-- RETURNS STRING: 実行結果として文字列を返す
-- LANGUAGE SQL: Snowflake Scripting（SQL ベース）で記述
-- $$ ... $$ の中がプロシージャ本体
CREATE OR REPLACE PROCEDURE MART.SP_MERGE_PURCHASE_EVENTS()
  RETURNS STRING
  LANGUAGE SQL
AS $$
BEGIN
  -- 04章の手動 MERGE と同一の処理。内容は変えず「名前をつけて呼び出せる」ようにした。
  MERGE INTO MART.FACT_PURCHASE_EVENTS tgt
  USING (
    SELECT
      s.raw:event_id::STRING    AS event_id,
      s.raw:user_id::STRING     AS user_id,
      TO_TIMESTAMP_NTZ(s.raw:event_time::STRING) AS event_time,
      item.value:sku::STRING    AS sku,
      item.value:product_name::STRING AS product_name,
      item.value:category::STRING     AS category,
      item.value:qty::NUMBER          AS qty,
      item.value:price::NUMBER(10,2)  AS price,
      item.value:qty::NUMBER * item.value:price::NUMBER(10,2) AS line_amount,
      s.src_filename
    FROM RAW.RAW_EVENTS_STREAM s,
    LATERAL FLATTEN(INPUT => s.raw:items) item
    WHERE s.metadata$action = 'INSERT'
  ) src
  ON tgt.event_id = src.event_id
  AND tgt.sku     = src.sku
  WHEN MATCHED THEN UPDATE SET
    tgt.user_id      = src.user_id,
    tgt.event_time   = src.event_time,
    tgt.product_name = src.product_name,
    tgt.category     = src.category,
    tgt.qty          = src.qty,
    tgt.price        = src.price,
    tgt.line_amount  = src.line_amount,
    tgt.src_filename = src.src_filename
  WHEN NOT MATCHED THEN INSERT (
    event_id, user_id, event_time, sku,
    product_name, category, qty, price, line_amount, src_filename
  ) VALUES (
    src.event_id, src.user_id, src.event_time, src.sku,
    src.product_name, src.category, src.qty, src.price,
    src.line_amount, src.src_filename
  );
  RETURN '完了';
END;
$$;

-- プロシージャを手動実行して動作確認
-- ← 04章の MERGE 手動実行と同じ結果になるはず
CALL MART.SP_MERGE_PURCHASE_EVENTS();

-- Check: FACT テーブルにデータが入っているか確認
SELECT
  category,
  SUM(line_amount) AS sales_amount,
  COUNT(*) AS item_count
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY category
ORDER BY sales_amount DESC;

-- プロシージャの一覧を確認
SHOW PROCEDURES IN SCHEMA MART;


-- ============================================================
-- 2. Dynamic Table
-- ============================================================
-- 「どのようなデータであるべきか」を SELECT 文で宣言するだけで
-- 自動更新されるテーブルを作る。Stream も Task も自分では作らなくてよい。
--
-- Stream + Task のアプローチと比べた場合:
-- | 項目       | Stream + Task               | Dynamic Table        |
-- |------------|------------------------------|----------------------|
-- | 記述スタイル | 手続き的（MERGE/INSERT 文）   | 宣言的（SELECT 文）  |
-- | 更新の仕組み | Task が定期起動して MERGE     | Snowflake が管理     |
-- | 向いている  | 複雑なロジック・外部連携       | 変換がシンプルな場合 |

use schema STAGING;

-- Dynamic Table: 「このSELECT結果を常に最新に保つ」という宣言。Task 不要。
-- LAG = '1 minute': 最大 1 分の遅延を許容してリフレッシュする
--   → リアルタイム性が不要ならコストを抑えられる（即時なら LAG = '0'）
-- WAREHOUSE: リフレッシュ処理に使うウェアハウス
CREATE OR REPLACE DYNAMIC TABLE STAGING.DYN_STG_EVENTS
  LAG       = '1 minute'
  WAREHOUSE = LEARN_WH
AS
  SELECT
    raw:event_id::STRING    AS event_id,
    raw:user_id::STRING     AS user_id,
    raw:event_type::STRING  AS event_type,
    TO_TIMESTAMP_NTZ(raw:event_time::STRING) AS event_time,
    raw:device::STRING      AS device,
    src_filename,
    loaded_at
  FROM RAW.RAW_EVENTS_PIPE;

-- Check: Dynamic Table の状態と中身を確認
SHOW DYNAMIC TABLES IN SCHEMA STAGING;

SELECT * FROM STAGING.DYN_STG_EVENTS ORDER BY event_time LIMIT 20;

-- 更新履歴を確認（数分待ってから実行するとリフレッシュ記録が見える）
SELECT *
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME => 'STAGING.DYN_STG_EVENTS'
))
ORDER BY refresh_start_time DESC
LIMIT 10;


-- ============================================================
-- 3. Task の依存関係（AFTER 句で DAG を組む）
-- ============================================================
-- ルート Task が PIPE を refresh し、
-- 完了したら子 Task がプロシージャを CALL する。
--
-- DAG の構造:
--   TASK_LOAD_PIPE（5分ごとに起動: ルート Task）
--         │ 完了後に自動起動
--         ▼
--   TASK_MERGE_FACT（SP_MERGE_PURCHASE_EVENTS を実行: 子 Task）
--
-- 【resume の順序が重要】
-- ルート Task を先に resume すると、次のスケジュール実行タイミングで子を起動しようとする。
-- そのとき子が suspended のままだとルートが完了しても子は実行されない。
-- → 子を先に resume してからルートを resume することで初回から確実に全ステップが動く。

use schema RAW;

-- ルート Task: Pipe を refresh してファイルを取り込む（スケジュールあり）
CREATE OR REPLACE TASK RAW.TASK_LOAD_PIPE
  WAREHOUSE = LEARN_WH
  SCHEDULE  = 'USING CRON 0/5 * * * * Asia/Tokyo'   -- 5 分ごとに実行
AS
  ALTER PIPE RAW.EVENTS_PIPE REFRESH;

-- 子 Task: ルート Task 完了後にプロシージャを実行（スケジュールなし）
-- AFTER 句で依存する Task を指定する。スケジュールは AFTER が定義すれば不要。
CREATE OR REPLACE TASK RAW.TASK_MERGE_FACT
  WAREHOUSE = LEARN_WH
  AFTER     RAW.TASK_LOAD_PIPE                  -- ← ルートの完了を待って起動
AS
  CALL MART.SP_MERGE_PURCHASE_EVENTS();

-- ① 子 Task を先に resume（重要! これを先にしないとルート完了時に子が動かない）
ALTER TASK RAW.TASK_MERGE_FACT RESUME;

-- ② ルート Task を resume（これで DAG 全体が動き始める）
ALTER TASK RAW.TASK_LOAD_PIPE  RESUME;

-- Check: Task の状態と依存関係を確認
SHOW TASKS IN SCHEMA RAW;

-- Task の実行ログを確認（スケジュール後に実行）
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY scheduled_time DESC
LIMIT 20;

-- 必要に応じて停止（ルートを suspend すると子も起動しなくなる）
-- ALTER TASK RAW.TASK_LOAD_PIPE  SUSPEND;
-- ALTER TASK RAW.TASK_MERGE_FACT SUSPEND;


-- ============================================================
-- 4. Snowflake Alerts
-- ============================================================
-- FACT_PURCHASE_EVENTS が空になったときに通知するアラートを作る。
-- SCHEDULE で確認頻度、IF で発火条件、THEN で実行するアクションを定義する。
--
-- 注意: SYSTEM$SEND_EMAIL を実際に使うには
--   NOTIFICATION INTEGRATION の設定が別途必要。
--   動作確認だけなら THEN 節を SELECT 'triggered' に置き換えてテストできる。

use schema MART;

CREATE OR REPLACE ALERT MART.ALERT_EMPTY_FACT
  WAREHOUSE = LEARN_WH
  SCHEDULE  = '5 MINUTE'                  -- 5 分ごとに条件を評価
  IF (EXISTS (
    -- 件数が 0 の場合に発火
    SELECT 1 FROM MART.FACT_PURCHASE_EVENTS HAVING COUNT(*) = 0
  ))
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'your-email@example.com',
      'ALERT: FACT_PURCHASE_EVENTS が空です',
      'MART.FACT_PURCHASE_EVENTS の件数が 0 件になりました。パイプラインを確認してください。'
    );

-- Alert を有効化
ALTER ALERT MART.ALERT_EMPTY_FACT RESUME;

-- Check: Alert の状態を確認
SHOW ALERTS IN SCHEMA MART;

-- Try this:
-- 1. SP_MERGE_PURCHASE_EVENTS にパラメータ（target_schema STRING）を追加してみる
-- 2. Dynamic Table の LAG を '5 minutes' に変えて更新頻度を観察してみる
