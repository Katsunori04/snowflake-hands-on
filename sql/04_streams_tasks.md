# 第4章: 増分バッチ（Streams & Tasks）

> この章で実行するファイル: `sql/04_streams_tasks.sql`

## この章で学ぶこと

- Stream で RAW テーブルの差分（新着データ）を追跡する
- MERGE で差分を FACT テーブルに反映する
- Task で定期実行のスケジュールを設定する
- Snowflake 内でバッチ処理を自動化する

## 前提条件

- 第0章（`sql/00_setup.sql`）が完了していること
- 第3章（`sql/03_snowpipe.sql`）が完了していること
- `RAW.RAW_EVENTS_PIPE` にデータが存在すること

---

## 概念解説

### Stream の仕組み（差分追跡）

Snowflake の **Stream** は、テーブルへの変更（INSERT / DELETE / UPDATE）を追跡するオブジェクトです。Stream を読み込むと「前回読んだ後に追加・変更されたデータ」だけを取得できます。

```
RAW.RAW_EVENTS_PIPE（テーブル）
        │
        │ 変更を記録
        ▼
RAW.RAW_EVENTS_STREAM（Stream）
        │
        │ SELECT（未処理の差分だけを返す）
        ▼
   MERGE で FACT に反映
```

**重要**: Stream は読み込まれると、そのデータが「処理済み」としてクリアされます（次回は新しい差分だけが返る）。

---

### `metadata$action` の 3 パターン

Stream が変更を記録する際、各行に `metadata$action` という操作種別カラムが付与されます。

| `metadata$action` | 意味 |
|---|---|
| `'INSERT'` | 新規挿入された行 |
| `'DELETE'` | 削除された行 |
| UPDATE | `'DELETE'`（元の値）+ `'INSERT'`（新しい値）の 2 行として記録される |

今回は「新しく追加されたイベントのみ FACT に取り込む」ため、`where s.metadata$action = 'INSERT'` でフィルタしています。

---

### MERGE ON 条件が `event_id + sku` の複合キーである理由

1 つのイベント（1 つの `event_id`）に複数の商品（SKU）が含まれる可能性があります。

```
event_id="e001"
  items = [
    {"sku": "A001", ...},   ← 1 行目
    {"sku": "B005", ...}    ← 2 行目
  ]
```

`event_id` だけでは「どの商品の行か」を一意に特定できないため、`sku` を加えた複合キーとしています。

```sql
on tgt.event_id = src.event_id
and tgt.sku = src.sku       -- ← sku も加えて複合キー
```

---

### CRON 式の読み方

Task のスケジュールは CRON 式で指定します。

```
'USING CRON 分 時 日 月 曜日 タイムゾーン'
```

| フィールド | 指定例 | 意味 |
|---|---|---|
| 分 | `0` | 0 分 |
| 分 | `0/5` | 0 分始まりで 5 分ごと（0, 5, 10, ...） |
| 時 | `*` | 毎時 |
| 時 | `1` | 1 時 |
| 日 | `*` | 毎日 |
| 月 | `*` | 毎月 |
| 曜日 | `*` | 毎曜日 |
| 曜日 | `1` | 月曜日 |

**よく使うパターン**:

| CRON 式 | 実行タイミング |
|---|---|
| `0/5 * * * * Asia/Tokyo` | 毎 5 分 |
| `0 1 * * * Asia/Tokyo` | 毎日 01:00 JST |
| `0 * * * * Asia/Tokyo` | 毎時 0 分（毎時正時） |
| `0 9 * * 1 Asia/Tokyo` | 毎週月曜 09:00 JST |

---

## ハンズオン手順

### Step 1: Stream を作成する

```sql
create or replace stream RAW.RAW_EVENTS_STREAM
  on table RAW.RAW_EVENTS_PIPE;
```

---

### Step 2: FACT テーブルを作成する

```sql
create or replace table MART.FACT_PURCHASE_EVENTS (
  event_id string,
  user_id string,
  event_time timestamp_ntz,
  sku string,
  product_name string,  -- 購入時点の商品名を記録（非正規化）
  category string,      -- 購入時点のカテゴリを記録（非正規化）
  qty number,
  price number(10,2),
  line_amount number(12,2),
  src_filename string,
  inserted_at timestamp_ntz default current_timestamp()
);
```

**設計メモ**: `product_name` と `category` を FACT に持たせているのは「購入時点の商品情報」を記録するためです。商品マスタが後から変わっても、購入当時の名前・カテゴリが保持されます（→ 詳細は01章の補足を参照）。

---

### Step 3: 手動で増分 MERGE を実行する

Stream の差分を FACT に MERGE します。まずは手動で実行して動作を確認します。

```sql
merge into MART.FACT_PURCHASE_EVENTS tgt
using (
  -- Stream から INSERT 行のみを取得し、LATERAL FLATTEN で配列を展開
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
  where s.metadata$action = 'INSERT'   -- 新規挿入のみ
) src
on tgt.event_id = src.event_id         -- 複合キーで一致判定
and tgt.sku = src.sku
when matched then update set           -- 既存行があれば更新
  tgt.qty = src.qty,
  tgt.price = src.price,
  ...
when not matched then insert (...)     -- 新規行なら挿入
values (...);
```

MERGE 後の確認:

```sql
select * from MART.FACT_PURCHASE_EVENTS order by event_time, event_id, sku;
```

---

### Step 4: Task を作成して定期実行を設定する

```sql
create or replace task STAGING.LOAD_FACT_PURCHASE_EVENTS
  warehouse = LEARN_WH
  schedule = 'USING CRON 0/5 * * * * Asia/Tokyo'  -- 5 分ごと
as
-- ここに Step 3 と同じ MERGE 文を記述（sql/04_streams_tasks.sql を参照）
;
```

Task の開始・停止:

```sql
-- Task を開始（デフォルトは SUSPENDED 状態）
alter task STAGING.LOAD_FACT_PURCHASE_EVENTS resume;

-- Task の状態を確認
show tasks like 'LOAD_FACT_PURCHASE_EVENTS' in schema STAGING;

-- 必要に応じて停止
alter task STAGING.LOAD_FACT_PURCHASE_EVENTS suspend;
```

> **注意**: Task は作成直後は `SUSPENDED` 状態です。`alter task ... resume` を実行しないとスケジュールが開始されません。

---

## 確認クエリ

```sql
-- Stream の未処理差分を確認（MERGE 後は空になる）
select * from RAW.RAW_EVENTS_STREAM;

-- FACT テーブルの内容確認
select * from MART.FACT_PURCHASE_EVENTS order by event_time, event_id, sku;
```

---

## Try This

**Task のスケジュールを毎日 01:00 に変えるにはどう書くか考えてみてください。**

<details>
<summary>答え例</summary>

```sql
create or replace task STAGING.LOAD_FACT_PURCHASE_EVENTS
  warehouse = LEARN_WH
  schedule = 'USING CRON 0 1 * * * Asia/Tokyo'   -- 毎日 01:00 JST
as
-- ... MERGE 文は同じ
;

alter task STAGING.LOAD_FACT_PURCHASE_EVENTS resume;
```

`0 1 * * * Asia/Tokyo` の読み方:
- `0` → 0 分
- `1` → 1 時
- `* * *` → 毎日・毎月・毎曜日
- `Asia/Tokyo` → 日本時間（JST = UTC+9）

</details>

---

## まとめ

| 概念 | ポイント |
|---|---|
| Stream | テーブルへの変更（差分）を追跡するオブジェクト |
| `metadata$action` | Stream が付与する操作種別（INSERT / DELETE） |
| MERGE | 差分を既存テーブルに「upsert（挿入 or 更新）」する |
| 複合キー | `event_id + sku` で 1 明細行を一意に特定 |
| Task | SQL をスケジュール実行するオブジェクト |
| CRON 式 | `分 時 日 月 曜日 タイムゾーン` の書式 |

次の章では、`FACT_PURCHASE_EVENTS` から DIM テーブルを作成してスタースキーマを完成させます。

## 参考リンク

- [Stream の概要](https://docs.snowflake.com/en/user-guide/streams-intro)
- [MERGE 文](https://docs.snowflake.com/en/sql-reference/sql/merge)
- [Task の概要](https://docs.snowflake.com/en/user-guide/tasks-intro)
- [TASK_HISTORY 関数](https://docs.snowflake.com/en/sql-reference/functions/task_history)
