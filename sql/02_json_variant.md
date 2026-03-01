# 第2章: JSON と VARIANT 型

> この章で実行するファイル: `sql/02_json_variant.sql`

## この章で学ぶこと

- VARIANT 型に JSON を格納する方法を理解する
- JSON path 構文（`raw:key::type`）で値を取り出す
- `LATERAL FLATTEN` で配列を行に展開する
- STAGING テーブルに整形済みデータを作成する

## 前提条件

- 第0章（`sql/00_setup.sql`）が完了していること
- `LEARN_DB.RAW` スキーマが存在すること

---

## 概念解説

### VARIANT 型とは

Snowflake の **VARIANT 型**は、JSON・XML・Avro などの半構造化データをそのまま格納できる特別な型です。スキーマを事前に決めなくても柔軟にデータを受け入れられます。

```sql
-- VARIANT 型の列を持つテーブル
create or replace table RAW.RAW_EVENTS (
  raw variant,                              -- JSON をそのまま格納
  loaded_at timestamp_ntz default current_timestamp()
);
```

**典型的な使い方**: アプリケーションのログやイベントデータを RAW 層に VARIANT で取り込み、STAGING 層で型変換・整形する。

---

### JSON path 構文の読み方

VARIANT 型から値を取り出すには、コロン（`:`）記法を使います。

```
raw:event_id::string
 │    │        └─ :: は型変換演算子（::string でキャスト）
 │    └─────────── JSON のキー名
 └──────────────── VARIANT 型の列名
```

| 構文 | 意味 |
|---|---|
| `raw:event_id` | JSON の `"event_id"` フィールド（VARIANT のまま） |
| `raw:event_id::string` | `string` 型にキャスト |
| `raw:device.os::string` | ネストしたオブジェクトにはドット（`.`）でアクセス |
| `raw:device.app_version::string` | さらに深いネストも同様 |

---

### LATERAL FLATTEN の展開イメージ

JSON の `items` 配列のように、**1 行に複数の要素が含まれる配列**を行に展開するのが `LATERAL FLATTEN` です。

**展開前（1 イベントに 2 商品の items 配列）**:

```
event_id="e001"
  items = [
    {"sku": "A001", "price": 12000},   ← 要素[0]
    {"sku": "B005", "price":   900}    ← 要素[1]
  ]
```

**LATERAL FLATTEN 後（1 商品 = 1 行 に展開）**:

```
event_id │ sku  │ price
─────────┼──────┼──────
e001     │ A001 │ 12000
e001     │ B005 │   900
e002     │ B005 │   900
```

**構文の読み方**:

```sql
lateral flatten(input => raw:items) item
               └──────────────────   └─── 展開後の各要素を参照するエイリアス
                展開対象の配列フィールド
```

`item.value` が展開された 1 要素（オブジェクト）を指し、`item.value:price::number(10,2)` のように各フィールドにアクセスします。

```
item.value:price::number(10,2)
     │      │      └─── 数値型キャスト（精度10桁、小数2桁）
     │      └────────── 要素内の "price" フィールド
     └───────────────── 展開された1要素（オブジェクト）
```

### FLATTEN が返す主な列

`LATERAL FLATTEN` は、展開した要素そのものだけでなく、何番目の要素か・元のどこにあったかも列として返します。

| 列名 | 説明 | `items` 配列を展開したときの例 |
|---|---|---|
| `SEQ` | 入力行ごとの連番 | `1`, `2` |
| `KEY` | オブジェクトのキー名。配列要素では通常 `NULL` | `NULL` |
| `PATH` | 元データ上のパス | `[0]`, `[1]` |
| `INDEX` | 配列内の位置 | `0`, `1` |
| `VALUE` | 展開された要素そのもの（VARIANT 型） | `{"sku":"A001","price":12000,...}` |
| `THIS` | 展開対象だった元の配列 / オブジェクト | `[{"sku":"A001",...},{"sku":"B005",...}]` |

確認用クエリ:

```sql
select
  raw:event_id::string as event_id,
  item.seq,
  item.key,
  item.path,
  item.index,
  item.value,
  item.this
from RAW.RAW_EVENTS,
lateral flatten(input => raw:items) item
order by event_id, item.index;
```

### `value:price` と書ける理由

`item.value` は VARIANT 型なので、通常の `raw:event_id` と同じ JSON パス構文で中のキーを参照できます。

| 構文 | 意味 |
|---|---|
| `item.value` | 展開された 1 要素全体 |
| `item.value:price` | その要素の `price` キーを取り出す |
| `item.value:price::number(10,2)` | 取り出した値を数値にキャストする |
| `item.value:product_name::string` | 同じ要素から商品名を取り出す |

つまり `item.value:price` は「`VALUE` 列に入っている JSON オブジェクトから `price` を読む」という意味です。

---

## ハンズオン手順

### Step 1: JSON を VARIANT 型で INSERT する

```sql
insert into RAW.RAW_EVENTS(raw)
select parse_json('{
  "event_id": "e001",
  "user_id": "u001",
  "event_type": "purchase",
  "event_time": "2026-02-28T10:00:00Z",
  "device": { "os": "iOS", "app_version": "1.2.0" },
  "items": [
    {"sku": "A001", "product_name": "Trail Shoes", "qty": 1, "price": 12000},
    {"sku": "B005", "product_name": "Coffee Beans", "qty": 2, "price": 900}
  ]
}');
```

`parse_json()` 関数が文字列を VARIANT 型に変換します。

---

### Step 2: JSON path で値を取り出す

```sql
select
  raw:event_id::string as event_id,
  raw:user_id::string as user_id,
  raw:event_type::string as event_type,
  raw:device.os::string as device_os,           -- ネストアクセス
  raw:device.app_version::string as app_version,
  raw:review_text::string as review_text,
  to_timestamp_ntz(raw:event_time::string) as event_time
from RAW.RAW_EVENTS
order by event_id;
```

---

### Step 3: LATERAL FLATTEN で items 配列を展開する

```sql
select
  raw:event_id::string as event_id,
  item.value:sku::string as sku,
  item.value:product_name::string as product_name,
  item.value:category::string as category,
  item.value:qty::number as qty,
  item.value:price::number(10,2) as price,
  item.value:qty::number * item.value:price::number(10,2) as line_amount
from RAW.RAW_EVENTS,
lateral flatten(input => raw:items) item   -- items 配列を行に展開
order by event_id, sku;
```

`from` 句に `,` でつなぐと暗黙的な CROSS JOIN になります。これが `LATERAL FLATTEN` の書き方です。

---

### Step 4: STAGING テーブルに整形済みデータを作成する

CTAS（CREATE TABLE AS SELECT）でクエリ結果をテーブルとして保存します。

```sql
-- イベントヘッダーの STAGING テーブル
create or replace table STAGING.STG_EVENTS as
select
  raw:event_id::string as event_id,
  raw:user_id::string as user_id,
  raw:event_type::string as event_type,
  raw:device.os::string as device_os,
  raw:device.app_version::string as app_version,
  raw:review_text::string as review_text,
  to_timestamp_ntz(raw:event_time::string) as event_time,
  loaded_at
from RAW.RAW_EVENTS;

-- 購入明細の STAGING テーブル（配列展開済み）
create or replace table STAGING.STG_EVENT_ITEMS as
select
  raw:event_id::string as event_id,
  item.value:sku::string as sku,
  item.value:product_name::string as product_name,
  item.value:qty::number as qty,
  item.value:price::number(10,2) as price,
  item.value:qty::number * item.value:price::number(10,2) as line_amount
from RAW.RAW_EVENTS,
lateral flatten(input => raw:items) item;
```

---

## 確認クエリ

```sql
select * from STAGING.STG_EVENTS order by event_id;
select * from STAGING.STG_EVENT_ITEMS order by event_id, sku;
```

`STG_EVENTS` には 2 行、`STG_EVENT_ITEMS` には 3 行（e001 に 2 商品、e002 に 1 商品）が表示されれば成功です。

---

## 注意: 02章のデータは練習用

> この章の `RAW.RAW_EVENTS` に INSERT したデータは **SQL の練習用**です。
>
> **03章以降で本線として使うのは `RAW.RAW_EVENTS_PIPE`**（ファイルから取り込んだデータ）です。
> 04章以降は `RAW_EVENTS_PIPE` を参照します。

---

## Try This

**`event_type = 'purchase'` の `items` だけを展開し、`product_name` ごとの合計金額を集計してください。**

<details>
<summary>答え例</summary>

```sql
select
  item.value:product_name::string as product_name,
  sum(item.value:qty::number * item.value:price::number(10,2)) as total_amount
from RAW.RAW_EVENTS
, lateral flatten(input => raw:items) item
where raw:event_type::string = 'purchase'
group by item.value:product_name::string
order by total_amount desc;
```

ポイント:

- `where raw:event_type::string = 'purchase'` で購入イベントだけに絞る
- `item.value:...` で展開済みの各商品要素を読む
- `sum(qty * price)` で明細金額を集計する

</details>

---

## まとめ

| 概念 | ポイント |
|---|---|
| VARIANT 型 | JSON をスキーマ定義なしで格納できる型 |
| `raw:key::type` | JSON path でフィールドを取り出す構文 |
| `raw:obj.key` | ネストしたオブジェクトへのアクセス |
| `LATERAL FLATTEN` | 配列を 1 要素 = 1 行に展開する |
| CTAS | `CREATE TABLE AS SELECT` でクエリ結果をテーブル化 |

## よくあるエラーと対処法

| エラー | 原因 | 対処法 |
|---|---|---|
| `parse_json(...)` が `NULL` を返す | JSON 文字列の引用符や改行のエスケープが崩れている | まず文字列全体をそのまま見直し、必要なら `parse_json($${ ... }$$)` のようにドルクォートで囲んでエスケープ漏れを避ける |
| `SQL compilation error: error line ... invalid identifier 'ITEM.VALUE:...'` | `LATERAL FLATTEN` のエイリアス名と参照先が一致していない | `lateral flatten(input => raw:items) item` と書いたら、列参照も `item.value:price` のように同じエイリアス名でそろえる |

次の章では、ファイルから自動的にデータを取り込む Snowpipe を学びます。

## 参考リンク

- [VARIANT 型の概要](https://docs.snowflake.com/ja/user-guide/semistructured-intro)
- [半構造化データのクエリ](https://docs.snowflake.com/ja/user-guide/querying-semistructured)
- [FLATTEN 関数](https://docs.snowflake.com/ja/sql-reference/functions/flatten)

## 学習チェックリスト

- [ ] VARIANT 型にデータを挿入できた
- [ ] コロン記法（`::`）と `GET()` でネストされた値を取り出せた
- [ ] `FLATTEN()` で配列を展開できた
- [ ] JSON データのフィルタリングと集計ができた
