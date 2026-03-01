# 第1章: データモデリングの基本

> この章で実行するファイル: `sql/01_modeling_basics.sql`

## この章で学ぶこと

- 正規化・非正規化・スタースキーマの違いを理解する
- Snowflake で RAW / STAGING / MART にどう配置するかを掴む
- FACT テーブルと DIM テーブルの関係を理解する

## 前提条件

- 第0章（`sql/00_setup.sql`）が完了していること
- `LEARN_DB`、`RAW`、`STAGING`、`MART` のスキーマが存在すること

---

## 概念解説

### 正規化 vs スタースキーマ

データベース設計には「正規化」と「スタースキーマ」という 2 つの考え方があります。それぞれの特徴と使いどころを理解しましょう。

| | 正規化 | スタースキーマ |
|---|---|---|
| **設計思想** | 重複を排除してデータの整合性を保つ | FACT（計測値）と DIM（属性）に分ける |
| **更新のしやすさ** | ◎ 1 箇所変更するだけで済む | △ DIM と FACT の両方を管理する必要がある |
| **集計のしやすさ** | △ 毎回 JOIN が必要 | ◎ DIM を JOIN するだけで属性が取れる |
| **BI ツールとの相性** | △ | ◎ |
| **主な用途** | トランザクション DB（アプリ層） | DWH・分析基盤（MART 層） |

---

### スタースキーマの構造

スタースキーマは中央の **FACT テーブル**（数値・計測値）を、周囲の **DIM テーブル**（属性・マスタ）が囲む構造です。

```
         DIM_USERS
        (user_id, name, prefecture)
              │
              │ JOIN
              ▼
DIM_PRODUCTS ── FACT_ORDER_LINES ─── DIM_DATE
(sku, name,      (order_id,           (date_key,
 category)        order_date,          year, month,
                  user_id,             day)
                  sku,
                  qty, price,
                  line_amount)
```

**FACT テーブルの役割**: 計測値（金額、数量）と外部キー（user_id, sku）を持つ
**DIM テーブルの役割**: 属性情報（名前、カテゴリ、地域）を持つ

---

## ハンズオン手順

### Step 1: 正規化テーブル 4 本を作成・データ投入する

STAGING 層に正規化されたテーブルを作成します。

```sql
-- ユーザーテーブル
create or replace table STAGING.USERS_NORM (
  user_id string, user_name string, prefecture string
);

-- 商品テーブル
create or replace table STAGING.PRODUCTS_NORM (
  sku string, product_name string, category string
);

-- 注文テーブル
create or replace table STAGING.ORDERS_NORM (
  order_id string, user_id string, order_date date
);

-- 注文明細テーブル
create or replace table STAGING.ORDER_ITEMS_NORM (
  order_id string, sku string, qty number, price number(10,2)
);
```

---

### Step 2: 4 テーブル JOIN で正規化の欠点を体感する

正規化データでは、注文明細に商品名とユーザー名を表示するだけで 4 テーブルの JOIN が必要です。

```sql
select
  o.order_id,
  o.order_date,
  u.user_name,      -- USERS_NORM から
  p.product_name,   -- PRODUCTS_NORM から
  p.category,
  i.qty,
  i.price,
  i.qty * i.price as line_amount
from STAGING.ORDERS_NORM o
join STAGING.USERS_NORM u on o.user_id = u.user_id
join STAGING.ORDER_ITEMS_NORM i on o.order_id = i.order_id
join STAGING.PRODUCTS_NORM p on i.sku = p.sku
order by o.order_id, p.sku;
```

**注目ポイント**: 毎回この 4 テーブル JOIN を書くのは非効率。スタースキーマで事前に整形することで、分析クエリを簡潔にできます。

---

### Step 3: MART 層にスタースキーマを作成する

```sql
-- DIM テーブル（属性）
create or replace table MART.DIM_USERS (
  user_id string, user_name string, prefecture string
);

create or replace table MART.DIM_PRODUCTS (
  sku string, product_name string, category string
);

-- FACT テーブル（計測値 + 外部キー）
create or replace table MART.FACT_ORDER_LINES (
  order_id string,
  order_date date,
  user_id string,   -- DIM_USERS への外部キー
  sku string,       -- DIM_PRODUCTS への外部キー
  qty number,
  price number(10,2),
  line_amount number(12,2)
);
```

---

### Step 4: FACT + DIM で集計する

スタースキーマを使うと集計クエリがシンプルになります。

```sql
select
  d.category,
  sum(f.line_amount) as sales_amount
from MART.FACT_ORDER_LINES f
join MART.DIM_PRODUCTS d on f.sku = d.sku
group by d.category
order by sales_amount desc;
```

---

## 確認クエリ

```sql
select * from MART.FACT_ORDER_LINES order by order_id, sku;
select * from MART.DIM_USERS order by user_id;
select * from MART.DIM_PRODUCTS order by sku;
```

---

## 補足: FACT に product_name を「持たせる」設計と「持たせない」設計

この章の `FACT_ORDER_LINES` では product_name を FACT に持たせず DIM_PRODUCTS に分離しています。

一方、**04章の `FACT_PURCHASE_EVENTS`** では product_name を FACT 側に非正規化して持たせています。

| 設計 | メリット | デメリット |
|---|---|---|
| DIM に分離（この章） | 商品名が変わっても DIM_PRODUCTS を 1 箇所変更するだけで済む | JOIN が必要 |
| FACT に非正規化（04章） | 「購入時点の商品名」を記録できる。JOIN 不要 | 商品名変更後は過去データと現在 DIM が乖離する |

**ポイント**: イベントデータ（注文・購入履歴）では「その時点の情報」を記録したいことが多く、非正規化が有効なケースがあります。

---

## Try This

**prefecture 別の売上を出してください。**

<details>
<summary>答え例</summary>

```sql
select
  u.prefecture,
  sum(f.line_amount) as sales_amount
from MART.FACT_ORDER_LINES f
join MART.DIM_USERS u on f.user_id = u.user_id
group by u.prefecture
order by sales_amount desc;
```

`DIM_USERS` に `prefecture` カラムがあるため、FACT と JOIN するだけで地域別集計ができます。

</details>

---

## まとめ

| 概念 | ポイント |
|---|---|
| 正規化 | 重複排除・更新しやすいが、集計に JOIN が多くなる |
| スタースキーマ | FACT（計測値）+ DIM（属性）の分離。分析・BI に最適 |
| STAGING 層 | 正規化された整形データを置く |
| MART 層 | スタースキーマで集計しやすい形を置く |

次の章では、JSON データ（VARIANT 型）の扱い方を学びます。

## 参考リンク

- [テーブルの種類（Permanent / Transient / Temporary）](https://docs.snowflake.com/ja/user-guide/tables-temp-transient)
- [データ型の一覧](https://docs.snowflake.com/ja/sql-reference/intro-summary-data-types)

## 学習チェックリスト

- [ ] ノーマライズとデノーマライズの違いを説明できる
- [ ] ディメンションテーブルとファクトテーブルの役割を区別できる
- [ ] Snowflake でテーブルを作成してデータを挿入できた
- [ ] スタースキーマとスノーフレークスキーマの違いを理解した
