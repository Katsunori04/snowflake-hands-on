# 第3章: ファイル取り込み（Snowpipe）

> この章で実行するファイル: `sql/03_snowpipe.sql`

## この章で学ぶこと

- Internal Stage の仕組みを理解する
- COPY INTO で手動ファイルロードを実行する
- Snowpipe（PIPE オブジェクト）を作成する
- COPY INTO と Snowpipe の使い分けを理解する

## 前提条件

- 第0章（`sql/00_setup.sql`）が完了していること
- `datasets/events_sample.json` が手元にあること

---

## 概念解説

### Stage とは

Snowflake の **Stage** は、データファイルを一時的に置いておく「置き場所」です。ここにファイルをアップロードしてから `COPY INTO` でテーブルに取り込みます。

```
ファイル（PC上）
     │
     │ アップロード（Snowsight の UI 操作）
     ▼
 @RAW.EVENT_STAGE   ← Internal Stage（Snowflake 内のストレージ）
     │
     │ COPY INTO（SQL実行）
     ▼
 RAW.RAW_EVENTS_PIPE（テーブル）
```

**Internal Stage** は Snowflake が管理するストレージに置かれ、追加設定なしで使えます（External Stage は S3/GCS/Azure Blob などを指定する必要があります）。

---

### COPY INTO vs Snowpipe の使い分け

| | COPY INTO | Snowpipe |
|---|---|---|
| **実行方法** | SQL を手動で実行 | PIPE オブジェクトに定義して呼び出す |
| **向いているケース** | バッチ・一括ロード | 継続的・小バッチの自動取り込み |
| **自動化** | Task や外部スケジューラと組み合わせる | `auto_ingest=true` でクラウドイベントと連携可能 |

---

### 02章との違い

| | 02章（`RAW_EVENTS`） | 03章（`RAW_EVENTS_PIPE`） |
|---|---|---|
| **用途** | SQL の INSERT 練習用 | ファイル取り込みの本線 |
| **データソース** | SQL の `parse_json()` で直接 INSERT | ファイル（JSON）から COPY INTO |
| **04章以降の参照先** | 使わない | **こちらを使う** |

> 04章以降は `RAW.RAW_EVENTS_PIPE` を参照します。この章を必ず実行しておいてください。

---

## ハンズオン手順

### Step 1: FILE FORMAT / STAGE / テーブルを作成する

```sql
-- JSON 取り込み用のファイルフォーマットを定義
create or replace file format RAW.JSON_FF
  type = json
  strip_outer_array = false;

-- Internal Stage を作成（ファイルの置き場所）
create or replace stage RAW.EVENT_STAGE
  file_format = RAW.JSON_FF;

-- 取り込み先テーブル（src_filename でどのファイルから来たか追跡できる）
create or replace table RAW.RAW_EVENTS_PIPE (
  raw variant,
  src_filename string,
  loaded_at timestamp_ntz default current_timestamp()
);
```

---

### Step 2: Snowsight でファイルをアップロードする

`datasets/events_sample.json` を `@RAW.EVENT_STAGE` にアップロードします。

**画面操作手順（5 ステップ）**:

1. Snowsight 左メニューの **[Data]** をクリック
2. **[Databases]** → **[LEARN_DB]** → **[RAW]** → **[Stages]** を展開
3. **[EVENT_STAGE]** をクリック
4. 右上の **[+ Files]** ボタンをクリック
5. `datasets/events_sample.json` を選択してアップロード

アップロード確認 SQL（ファイルが見えれば成功）:

```sql
list @RAW.EVENT_STAGE;
```

---

### Step 3: COPY INTO でデータを取り込む（手動ロード）

```sql
copy into RAW.RAW_EVENTS_PIPE(raw, src_filename)
from (
  select
    $1,                   -- ファイルの各 JSON オブジェクト
    metadata$filename     -- アップロードしたファイル名を記録
  from @RAW.EVENT_STAGE
)
file_format = (format_name = RAW.JSON_FF)
on_error = 'CONTINUE';
```

**`on_error` オプションの比較**:

| オプション | 動作 |
|---|---|
| `'CONTINUE'` | エラー行をスキップして処理を続行（今回の設定） |
| `'ABORT_STATEMENT'` | エラーが出たら即中止（デフォルト） |
| `'SKIP_FILE'` | エラーがあったファイル全体をスキップ |

取り込み後の検証:

```sql
-- 件数確認
select count(*) as row_count from RAW.RAW_EVENTS_PIPE;

-- ファイル名と中身の確認
select
  src_filename,
  raw:event_id::string as event_id,
  raw:user_id::string as user_id,
  loaded_at
from RAW.RAW_EVENTS_PIPE
order by loaded_at;
```

---

### Step 4: PIPE を作成する（自動取り込みの準備）

```sql
create or replace pipe RAW.EVENTS_PIPE
  auto_ingest = false
as
copy into RAW.RAW_EVENTS_PIPE(raw, src_filename)
from (
  select $1, metadata$filename from @RAW.EVENT_STAGE
)
file_format = (format_name = RAW.JSON_FF)
on_error = 'CONTINUE';
```

**`auto_ingest` の違い**:

| 設定 | 動作 |
|---|---|
| `false`（今回） | 手動で `alter pipe ... refresh` を実行してデータを取り込む |
| `true` | S3/GCS などのイベント通知と連携し、ファイルが置かれると自動で取り込む。クラウドストレージ側でイベント通知設定が別途必要 |

Airflow（12章）などの外部オーケストレーターから `alter pipe RAW.EVENTS_PIPE refresh;` を定期実行するのが典型的なパターンです。

---

## 確認クエリ

```sql
-- Stage が作成されているか確認
show stages like 'EVENT_STAGE' in schema RAW;

-- Pipe が作成されているか確認
show pipes like 'EVENTS_PIPE' in schema RAW;

-- Pipe のステータス確認
select system$pipe_status('RAW.EVENTS_PIPE');
```

---

## Try This

**`RAW.RAW_EVENTS_PIPE` に入った JSON から `event_type` を確認してください。**

<details>
<summary>答え例</summary>

```sql
select
  raw:event_id::string as event_id,
  raw:event_type::string as event_type,
  src_filename
from RAW.RAW_EVENTS_PIPE
order by event_id;
```

02章の `RAW.RAW_EVENTS`（SQL INSERT で入れたデータ）ではなく、`RAW.RAW_EVENTS_PIPE`（ファイルから取り込んだデータ）を参照していることを確認してください。

</details>

---

### Step 5（オプション）: GENERATOR で大量テストデータを追加する

ファイル取り込みの 200件だけでは集計行が少ないと感じる場合に、`GENERATOR` を使って一気に 800件の疑似データを追加できます。実行後は合計 ~1000件になります。

```sql
-- GENERATOR(ROWCOUNT => 800): 800行のダミー行を生成する仮想テーブル
-- seq4()     : 0始まりの連番（行番号として使う）
-- UNIFORM()  : 指定範囲の乱数整数を生成
-- DATEADD()  : 基準日から秒数オフセットを加算して日時を生成

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
    || '"items":[{"sku":"...", "product_name":"...", "category":"...",
         "qty":' || UNIFORM(1,3,RANDOM())::STRING || ', "price":...}]}'
  ),
  'generated',
  CURRENT_TIMESTAMP()
FROM TABLE(GENERATOR(ROWCOUNT => 800));
```

> **注意**: GENERATOR で生成した行は `src_filename = 'generated'` として記録されます。ファイル由来の行（`src_filename` にファイルパスが入る）と区別できます。

生成後の確認:

```sql
SELECT src_filename, COUNT(*) AS row_count
FROM RAW.RAW_EVENTS_PIPE
GROUP BY src_filename
ORDER BY src_filename;
```

---

## まとめ

| 概念 | ポイント |
|---|---|
| Internal Stage | Snowflake 内の一時ファイル置き場。追加設定不要 |
| FILE FORMAT | ファイルの形式（JSON・CSV など）を定義するオブジェクト |
| `COPY INTO` | Stage のファイルをテーブルに手動でロードする |
| PIPE | `COPY INTO` を定義して繰り返し呼び出せるオブジェクト |
| `auto_ingest` | `true` にするとクラウドイベントと連携して自動取り込み |

## よくあるエラーと対処法

| 症状 | 原因 | 対処法 |
|---|---|---|
| Snowsight の Snowpipe UI が読み込み中のままになる | UI 側の表示遅延や一時的な不調 | UI を待ち続けず、`SELECT SYSTEM$PIPE_STATUS('RAW.EVENTS_PIPE');` で取り込み状態を確認する |
| `COPY INTO` 実行後も行が増えない | Stage に対象ファイルがない、または既に同じファイルを取り込み済み | `LIST @RAW.EVENT_STAGE;` でファイル有無を確認し、再取り込み検証なら `FORCE = TRUE` を使うか別名ファイルで試す |

次の章では、このテーブル（`RAW_EVENTS_PIPE`）を Stream と Task で差分処理する方法を学びます。

## 参考リンク

- [Internal Stage の概要](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-create-stage)
- [COPY INTO（テーブルへ）](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table)
- [Snowpipe の概要](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro)
