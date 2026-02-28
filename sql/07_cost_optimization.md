# 第7章: コスト最適化の基本

> この章で実行するファイル: `sql/07_cost_optimization.sql`

## この章で学ぶこと

- Snowflake Warehouse のコスト構造を理解する
- auto_suspend / auto_resume の設定がコストに与える影響を理解する
- RAW JSON の直接参照と STAGING テーブルのコスト差を体感する
- クエリ履歴からパフォーマンスを確認する

## 前提条件

- 第0章（`sql/00_setup.sql`）が完了していること
- 第2章（`sql/02_json_variant.sql`）が完了していること（`RAW.RAW_EVENTS` のデータが必要）

---

## 概念解説

### Warehouse のコスト構造

Snowflake のコストは主に **Warehouse の起動時間**で決まります。

```
クレジット消費 = ウェアハウスサイズ × 起動時間（時間単位）

例: XSMALL で 1 時間起動 = 1 クレジット消費
    XSMALL で 30 秒起動   = 1 クレジット消費（最低 60 秒課金）
```

**ポイント**: Warehouse が起動している間はクエリを実行していなくてもクレジットを消費します。`auto_suspend` を短く設定することでアイドル時のコストを削減できます。

---

### auto_suspend が短いほどコストが下がる仕組み

```
auto_suspend = 60（秒）の場合:

クエリ実行 → 60 秒後に自動停止 → 次のクエリで自動再起動
   ↑                                       ↑
   課金開始                              課金開始（最低 60 秒分）
```

| auto_suspend 設定 | アイドル課金 | 次のクエリの起動速度 |
|---|---|---|
| 60 秒 | 最小限 | 少し待つ（数秒） |
| 300 秒 | 5 分分のアイドル課金 | 早い（ウォームスタート） |
| 3600 秒 | 1 時間分のアイドル課金 | 常に早い |

学習目的では `auto_suspend = 60` で十分です。

---

### RAW JSON の直接参照 vs STAGING テーブルのコスト差

```
RAW JSON を毎回展開するクエリ（コスト高め）:
  RAW.RAW_EVENTS（VARIANT）→ LATERAL FLATTEN → 毎回 JSON を解析

整形済みテーブルを読むクエリ（コスト低め）:
  STAGING.STG_EVENT_ITEMS（構造化済み）→ 必要な列だけスキャン
```

Snowflake はカラムナー形式（列単位）でデータを格納しています。VARIANT 型は列単位のプルーニング（不要列のスキップ）が効きにくく、毎回 JSON を解析するオーバーヘッドもあります。

---

## ハンズオン手順

### Step 1: Warehouse の現在の設定を確認する

```sql
show warehouses like 'LEARN_WH';
```

`auto_suspend`、`auto_resume`、`size` カラムを確認します。

---

### Step 2: auto_suspend を設定する

```sql
alter warehouse LEARN_WH set
  auto_suspend = 60,
  auto_resume = true;
```

---

### Step 3: RAW JSON を毎回展開するクエリを実行する

```sql
-- コスト高め: LATERAL FLATTEN で毎回 JSON を解析
select
  raw:event_id::string as event_id,
  item.value:sku::string as sku,
  item.value:qty::number as qty,
  item.value:price::number as price
from RAW.RAW_EVENTS,
lateral flatten(input => raw:items) item;
```

---

### Step 4: 整形済みテーブルを読むクエリを実行する

```sql
-- コスト低め: STAGING に展開済みなのでスキャン量が少ない
select
  event_id,
  sku,
  qty,
  price
from STAGING.STG_EVENT_ITEMS;
```

---

### Step 5: クエリ履歴でパフォーマンスを比較する

```sql
select
  query_id,
  query_text,
  warehouse_name,
  total_elapsed_time,   -- 実行時間（ミリ秒）
  bytes_scanned         -- スキャンしたデータ量（バイト）
from table(information_schema.query_history_by_warehouse(
  warehouse_name => 'LEARN_WH',
  end_time_range_start => dateadd('hour', -1, current_timestamp()),
  result_limit => 20
))
order by start_time desc;
```

`bytes_scanned` を比較すると、STAGING テーブルから読む方が少ないことが確認できます（サンプルデータが少ないため差は小さいですが、本番では大きな差になります）。

---

## 確認クエリ

```sql
-- auto_suspend の設定を確認
show parameters like 'AUTO_SUSPEND' in warehouse LEARN_WH;
```

---

## コストを抑えるための 5 つのチェックポイント

1. **ウェアハウスはまず XSMALL から始める** → 性能が足りなければ後でサイズアップ
2. **`auto_suspend` は短く設定する** → アイドル課金を防ぐ（学習用は 60 秒が目安）
3. **RAW JSON の直接参照を常用しない** → STAGING / MART に展開済みのテーブルを使う
4. **よく使う列は STAGING / MART に整形しておく** → 毎回 JSON 展開の繰り返しを避ける
5. **Task の実行頻度を細かくしすぎない** → Warehouse の起動回数が増えるとコストが積み上がる

---

## Try This

**5 分おきの Task を 1 時間おきに変えると何が良くて何が悪いか考えてみてください。**

<details>
<summary>解説</summary>

**1 時間おきに変えるメリット（コスト面）**:
- Warehouse の起動回数が 12 分の 1 になり、最低課金（60 秒）の積み上がりが減る
- アイドル期間が増え、全体の課金時間が短くなる

**1 時間おきに変えるデメリット（鮮度面）**:
- 新しいデータが FACT テーブルに反映されるまでの遅延が最大 1 時間になる
- リアルタイム性が求められるユースケース（在庫管理・不正検知など）には適さない

**トレードオフの考え方**: ビジネス要件として「どのくらいのデータ鮮度が必要か」を確認してから頻度を決めます。分析レポートの更新が 1 日 1 回で十分なら、Task も 1 日 1 回で OK です。

</details>

---

## まとめ

| コスト要因 | 対策 |
|---|---|
| Warehouse のサイズ | XSMALL から始めて必要に応じてスケールアップ |
| アイドル時間 | `auto_suspend` を短く設定（60 秒目安） |
| クエリのスキャン量 | RAW JSON の直接参照を避け STAGING テーブルを使う |
| Task の頻度 | ビジネス要件の鮮度に合わせた頻度に設定 |

次の章では、Snowflake Cortex の AI 関数でテキストデータを分析します。

## 参考リンク

- [Virtual Warehouse の概要](https://docs.snowflake.com/en/user-guide/warehouses-overview)
- [コスト管理の概要](https://docs.snowflake.com/en/user-guide/cost-understanding-compute)
- [QUERY_HISTORY ビュー](https://docs.snowflake.com/en/sql-reference/account-usage/query_history)
