# 第5章: 処理の再利用と宣言的更新（ストアドプロシージャ・Dynamic Table）

> この章で実行するファイル: `sql/05_stored_proc_dynamic_table.sql`

## この章で学ぶこと

- ストアドプロシージャで SQL ロジックを再利用する方法
- Dynamic Table で Stream + Task を宣言的に置き換える方法
- Task の AFTER 句で複数処理を依存関係付きで連鎖させる方法
- Snowflake Alerts で異常を検知・通知する方法

## 前提条件

- 第4章（`sql/04_streams_tasks.sql`）が完了していること
- `MART.FACT_PURCHASE_EVENTS` が作成済みであること

---

## 概念解説

### 手続き的アプローチ vs 宣言的アプローチ

第4章では「Stream を読んで → MERGE する → Task でスケジュール実行する」という **手続き的** なパイプラインを作りました。この章では同じ結果を 2 つのアプローチで実現します。

```
【手続き的（Stored Procedure + Task）】
Task が「いつ・何をするか」を明示的に記述する

TASK_LOAD_PIPE ──→ TASK_MERGE_FACT
  │                   │
  │ PIPE を refresh   │ SP_MERGE_PURCHASE_EVENTS() を CALL
  ▼                   ▼
RAW_EVENTS_PIPE   FACT_PURCHASE_EVENTS

【宣言的（Dynamic Table）】
「どのようなデータであるべきか」を定義し、更新はシステムに任せる

RAW_EVENTS_PIPE
  │
  │ LAG = '1 minute' で自動更新
  ▼
DYN_STG_EVENTS（Dynamic Table）
```

---

### ストアドプロシージャとは

SQL ロジックをまとめて **名前を付けて再利用** できるオブジェクトです。

- `CREATE PROCEDURE` で定義し、`CALL` で実行する
- 変数・条件分岐・ループが使える（Snowflake Scripting）
- Task の AS 節に長い MERGE 文を書く代わりに `CALL procedure_name()` と書ける

```sql
-- 定義
CREATE OR REPLACE PROCEDURE MART.SP_MERGE_PURCHASE_EVENTS()
  RETURNS STRING
  LANGUAGE SQL
AS $$
  MERGE INTO MART.FACT_PURCHASE_EVENTS ...;
  RETURN '完了';
$$;

-- 実行
CALL MART.SP_MERGE_PURCHASE_EVENTS();
```

---

### Dynamic Table とは

`SELECT` 文の結果を常に最新の状態に保ち続けるテーブルです。

| 比較項目 | Stream + Task | Dynamic Table |
|---|---|---|
| 更新方法 | Task が定期実行 | Snowflake が自動管理 |
| 記述スタイル | 手続き的（MERGE 文） | 宣言的（SELECT 文） |
| 適したケース | 複雑な条件・外部連携 | シンプルな変換ロジック |
| 依存関係 | 手動で Task を連鎖 | 自動で依存を解決 |

```sql
CREATE OR REPLACE DYNAMIC TABLE STAGING.DYN_STG_EVENTS
  LAG = '1 minute'       -- 最大 1 分の遅延を許容
  WAREHOUSE = LEARN_WH
AS
  SELECT event_data:event_id::STRING AS event_id, ...
  FROM RAW.RAW_EVENTS_PIPE;
```

`LAG = '1 minute'` は「最大 1 分前のデータまで許容する」という新鮮さの設定です。

---

### Task の依存関係（AFTER 句）

複数の Task を DAG（有向非巡回グラフ）として連鎖させるには `AFTER` 句を使います。

```
TASK_LOAD_PIPE（ルート Task：CRON スケジュール）
      │
      │ 完了したら自動的に起動
      ▼
TASK_MERGE_FACT（子 Task：スケジュールなし）
```

```sql
-- ルート Task（スケジュールあり）
CREATE OR REPLACE TASK RAW.TASK_LOAD_PIPE
  WAREHOUSE = LEARN_WH
  SCHEDULE  = 'USING CRON 0/5 * * * * Asia/Tokyo'
AS
  ALTER PIPE RAW.EVENTS_PIPE REFRESH;

-- 子 Task（AFTER で依存を定義）
CREATE OR REPLACE TASK RAW.TASK_MERGE_FACT
  WAREHOUSE = LEARN_WH
  AFTER     RAW.TASK_LOAD_PIPE          -- ← ここで依存を定義
AS
  CALL MART.SP_MERGE_PURCHASE_EVENTS();
```

> **注意**: Task DAG を動かすには、**全ての Task を resume** する必要があります。ルート Task を resume すると子 Task も自動起動を許可されます。

---

### Snowflake Alerts

指定した条件が真になったとき、指定した処理（通知・SQL 実行など）を自動実行するオブジェクトです。

```sql
CREATE OR REPLACE ALERT MART.ALERT_EMPTY_FACT
  WAREHOUSE = LEARN_WH
  SCHEDULE  = '5 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM MART.FACT_PURCHASE_EVENTS HAVING COUNT(*) = 0
  ))
  THEN CALL SYSTEM$SEND_EMAIL(...);
```

---

## ハンズオン手順

### Step 1: ストアドプロシージャを作成する

第4章の MERGE ロジックをプロシージャ化します。

```sql
CREATE OR REPLACE PROCEDURE MART.SP_MERGE_PURCHASE_EVENTS()
  RETURNS STRING
  LANGUAGE SQL
AS $$
  MERGE INTO MART.FACT_PURCHASE_EVENTS tgt
  USING (
    SELECT
      s.raw:event_id::STRING AS event_id,
      s.raw:user_id::STRING AS user_id,
      TO_TIMESTAMP_NTZ(s.raw:event_time::STRING) AS event_time,
      item.value:sku::STRING AS sku,
      item.value:product_name::STRING AS product_name,
      item.value:category::STRING AS category,
      item.value:qty::NUMBER AS qty,
      item.value:price::NUMBER(10,2) AS price,
      item.value:qty::NUMBER * item.value:price::NUMBER(10,2) AS line_amount,
      s.src_filename
    FROM RAW.RAW_EVENTS_STREAM s,
    LATERAL FLATTEN(INPUT => s.raw:items) item
    WHERE s.metadata$action = 'INSERT'
  ) src
  ON tgt.event_id = src.event_id AND tgt.sku = src.sku
  WHEN MATCHED THEN UPDATE SET
    tgt.qty = src.qty, tgt.price = src.price, tgt.line_amount = src.line_amount
  WHEN NOT MATCHED THEN INSERT (
    event_id, user_id, event_time, sku, product_name, category,
    qty, price, line_amount, src_filename
  ) VALUES (
    src.event_id, src.user_id, src.event_time, src.sku, src.product_name, src.category,
    src.qty, src.price, src.line_amount, src.src_filename
  );
  RETURN '完了';
$$;
```

動作確認:

```sql
CALL MART.SP_MERGE_PURCHASE_EVENTS();
SELECT * FROM MART.FACT_PURCHASE_EVENTS ORDER BY event_time, event_id, sku;
```

---

### Step 2: Dynamic Table を作成する

`RAW_EVENTS_PIPE` のデータを常に展開し続ける Dynamic Table を作成します。

```sql
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
```

更新履歴の確認:

```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME => 'STAGING.DYN_STG_EVENTS'
))
ORDER BY refresh_start_time DESC
LIMIT 10;
```

---

### Step 3: Task DAG を組む（AFTER 句）

ストアドプロシージャを Task から呼び出し、依存関係を設定します。

```sql
-- ルート Task: Pipe を refresh する
CREATE OR REPLACE TASK RAW.TASK_LOAD_PIPE
  WAREHOUSE = LEARN_WH
  SCHEDULE  = 'USING CRON 0/5 * * * * Asia/Tokyo'
AS
  ALTER PIPE RAW.EVENTS_PIPE REFRESH;

-- 子 Task: Procedure を CALL する（ルート完了後に自動実行）
CREATE OR REPLACE TASK RAW.TASK_MERGE_FACT
  WAREHOUSE = LEARN_WH
  AFTER     RAW.TASK_LOAD_PIPE
AS
  CALL MART.SP_MERGE_PURCHASE_EVENTS();

-- 両方を resume する（子 Task を先に resume する）
ALTER TASK RAW.TASK_MERGE_FACT RESUME;
ALTER TASK RAW.TASK_LOAD_PIPE  RESUME;
```

実行ログの確認:

```sql
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY scheduled_time DESC
LIMIT 20;
```

---

### Step 4: Alert を作成する

`FACT_PURCHASE_EVENTS` が空になったときに通知するアラートを作成します。

```sql
CREATE OR REPLACE ALERT MART.ALERT_EMPTY_FACT
  WAREHOUSE = LEARN_WH
  SCHEDULE  = '5 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM MART.FACT_PURCHASE_EVENTS HAVING COUNT(*) = 0
  ))
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'your-email@example.com',
      'ALERT: FACT_PURCHASE_EVENTS が空です',
      'MART.FACT_PURCHASE_EVENTS の件数が 0 件になりました。パイプラインを確認してください。'
    );

ALTER ALERT MART.ALERT_EMPTY_FACT RESUME;
```

> **注意**: `SYSTEM$SEND_EMAIL` を使うには、事前に Snowflake の通知統合（NOTIFICATION INTEGRATION）の設定が必要です。動作確認だけなら、THEN 節を `SELECT 'alert triggered'` に変えてテストできます。

---

## 確認クエリ

```sql
-- プロシージャの一覧
SHOW PROCEDURES IN SCHEMA MART;

-- Dynamic Table の状態確認
SHOW DYNAMIC TABLES IN SCHEMA STAGING;

-- Task の状態確認
SHOW TASKS IN SCHEMA RAW;

-- Alert の状態確認
SHOW ALERTS IN SCHEMA MART;
```

---

## Try This

1. **MERGE procedure にパラメータを追加してみる**

   `SP_MERGE_PURCHASE_EVENTS` のシグネチャを変更し、対象スキーマ名（`'MART'`）をパラメータとして受け取れるようにしてみましょう。

   ヒント: `RETURNS STRING LANGUAGE SQL` の前に `(target_schema STRING)` を追加し、プロシージャ本体内で動的スキーマ名を使います。

2. **Dynamic Table の LAG を変えてみる**

   `LAG = '1 minute'` を `LAG = '5 minutes'` に変更して、更新頻度がどう変わるか `DYNAMIC_TABLE_REFRESH_HISTORY` で確認してみましょう。

---

## まとめ

| 概念 | ポイント |
|---|---|
| Stored Procedure | SQL ロジックを名前付きでカプセル化。`CALL` で再利用 |
| Dynamic Table | `SELECT` を定義するだけで、更新はシステムが管理（宣言的） |
| Task AFTER 句 | 複数 Task を DAG として連鎖。ルート Task のスケジュールで全体が動く |
| Alerts | 条件が真になったとき自動的に処理を実行（監視・通知） |
| TASK_HISTORY | Task の実行ログ・エラーを確認する関数 |

次の章では、`FACT_PURCHASE_EVENTS` から DIM テーブルを作成してスタースキーマを完成させます。

## 参考リンク

- [ストアドプロシージャの概要](https://docs.snowflake.com/en/developer-guide/stored-procedure/stored-procedures-overview)
- [CREATE PROCEDURE](https://docs.snowflake.com/en/sql-reference/sql/create-procedure)
- [Dynamic Table の概要](https://docs.snowflake.com/en/user-guide/dynamic-tables-intro)
- [CREATE DYNAMIC TABLE](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table)
- [Task の依存関係（DAG）](https://docs.snowflake.com/en/user-guide/tasks-graphs)
- [Snowflake Alerts の概要](https://docs.snowflake.com/en/user-guide/alerts)
- [CREATE ALERT](https://docs.snowflake.com/en/sql-reference/sql/create-alert)
