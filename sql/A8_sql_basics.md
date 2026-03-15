# 付録A8: SQL の基本（SELECT〜ウィンドウ関数・DDL）

> **この付録の位置づけ**: 本編（00〜13章）を始める前に構文を確認したいとき、または途中で詰まったときに参照するリファレンスです。
> 本編で使っている **FACT_PURCHASE_EVENTS / DIM_USERS / DIM_PRODUCTS / DIM_DATE** を題材にするため、知っているデータで構文を学べます。

---

## 前提

- **06_star_schema.sql を完了済み**であること
  （`MART.FACT_PURCHASE_EVENTS` / `DIM_PRODUCTS` / `DIM_USERS` / `DIM_DATE` が存在する状態）

### FACT_PURCHASE_EVENTS の列一覧（参考）

| 列名 | 型 | 説明 |
|---|---|---|
| event_id | STRING | イベントID |
| user_id | STRING | ユーザーID |
| event_time | TIMESTAMP_NTZ | 購入日時 |
| sku | STRING | 商品コード |
| product_name | STRING | 商品名 |
| category | STRING | カテゴリ |
| qty | NUMBER | 数量 |
| price | NUMBER(10,2) | 単価 |
| line_amount | NUMBER(12,2) | 購入金額（qty × price） |
| src_filename | STRING | 取り込み元ファイル名 |

---

## Section 1: SELECT / FROM

### 押さえるポイント

- `SELECT *` は全列取得。本番コードでは使わず、列を明示するのが基本
- `AS` で列に別名を付けられる（`AS` は省略可だが明示推奨）
- SELECT 句には数値演算・文字列結合などの**式**を書ける
- `TYPEOF(列)` でデータ型を確認できる（デバッグ時に便利）

```sql
-- 計算列の例
SELECT qty * price AS calc_amount, qty * price * 1.1 AS tax_included
FROM MART.FACT_PURCHASE_EVENTS;

-- 文字列結合: || 演算子または CONCAT()
SELECT user_id || ' - ' || product_name AS summary FROM MART.FACT_PURCHASE_EVENTS;
```

> **参考**: Snowflake 公式ドキュメント — [SELECT](https://docs.snowflake.com/ja/sql-reference/sql/select)

---

## Section 2: WHERE

### 押さえるポイント

| 演算子 | 意味 | 例 |
|---|---|---|
| `=` / `<>` | 等値 / 不等値 | `category = 'Sports'` |
| `>` / `<` / `>=` / `<=` | 比較 | `line_amount >= 5000` |
| `IN (...)` | 複数値のいずれか | `category IN ('A', 'B')` |
| `LIKE '%x%'` | パターンマッチ | `product_name LIKE '%Phone%'` |
| `BETWEEN a AND b` | 範囲（両端含む） | `line_amount BETWEEN 1000 AND 5000` |
| `IS NULL` / `IS NOT NULL` | NULLチェック | `src_filename IS NULL` |
| `AND` / `OR` / `NOT` | 論理演算 | `a AND b` |

> **注意**: `= NULL` は**常に FALSE**。NULL の判定は必ず `IS NULL` を使う（詳細は Section 9）。

> **参考**: Snowflake 公式ドキュメント — [SELECT](https://docs.snowflake.com/ja/sql-reference/sql/select)（WHERE 仕様を含む）

---

## Section 3: ORDER BY / LIMIT / DISTINCT

### 押さえるポイント

- `ORDER BY 列 DESC` で降順（デフォルトは ASC 昇順）
- `ORDER BY 列1, 列2` — 第1キーが同じ行に対して第2キーで並び替え
- `LIMIT N` は `ORDER BY` と組み合わせて「上位N件」を取得するパターンが多い
- `SELECT DISTINCT 列` でユニーク値のみ返す。`COUNT(DISTINCT 列)` でユニーク件数

> **参考**: Snowflake 公式ドキュメント — [SELECT](https://docs.snowflake.com/ja/sql-reference/sql/select)（ORDER BY / LIMIT / DISTINCT 仕様を含む）

---

## Section 4: GROUP BY + 集計関数 / HAVING

### 集計関数の一覧

| 関数 | 意味 | NULL の扱い |
|---|---|---|
| `COUNT(*)` | 全行数 | NULLを含む |
| `COUNT(列)` | 非NULLの件数 | NULLを除く |
| `SUM(列)` | 合計 | NULLを無視 |
| `AVG(列)` | 平均 | NULLを除いた平均 |
| `MAX(列)` / `MIN(列)` | 最大 / 最小 | NULLを無視 |

### WHERE vs HAVING — 評価タイミングの違い

```
実行順序:  FROM → WHERE → GROUP BY → 集計 → HAVING → SELECT → ORDER BY
```

| | 役割 | 集計関数を書ける？ |
|---|---|---|
| `WHERE` | 集計**前**の行フィルタ | ✗ |
| `HAVING` | 集計**後**のフィルタ | ✓ |

```sql
-- WHERE: 集計前（行を除外）
-- HAVING: 集計後（グループを除外）
SELECT category, COUNT(*), SUM(line_amount)
FROM MART.FACT_PURCHASE_EVENTS
WHERE line_amount >= 1000          -- ← 行レベルで絞る（WHERE）
GROUP BY category
HAVING COUNT(*) >= 3               -- ← グループレベルで絞る（HAVING）
```

> **参考**: Snowflake 公式ドキュメント — [集計関数](https://docs.snowflake.com/ja/sql-reference/functions-aggregation)

---

## Section 5: DDL

### DDL / DML / DCL の分類

| 分類 | 意味 | 代表的な命令 |
|---|---|---|
| **DDL** (Data Definition Language) | **構造**を操作 | CREATE / ALTER / DROP / TRUNCATE |
| **DML** (Data Manipulation Language) | **データ**を操作 | SELECT / INSERT / UPDATE / DELETE / MERGE |
| **DCL** (Data Control Language) | **権限**を操作 | GRANT / REVOKE |

### 重要な DDL の概念

**CREATE OR REPLACE**
Snowflake では冪等性（何度実行しても同じ結果）のために `CREATE OR REPLACE` を多用します。既存オブジェクトを上書き再作成するため、エラーにならずスクリプトを再実行できます。
> **注意（Time Travel）**: `CREATE OR REPLACE` でドロップされたテーブルは Time Travel 内で保持され、保持期間中はストレージコストに影響します。頻繁に実行する場合は `DROP TABLE` を明示して不要なデータを消すか、保持期間（`DATA_RETENTION_TIME_IN_DAYS`）を短縮することを検討してください。

**CTAS（CREATE TABLE AS SELECT）**
```sql
CREATE OR REPLACE TABLE new_table AS SELECT ... FROM existing_table;
```
集計・変換結果をテーブルに「固める」最もシンプルな方法。本編06章で DIM_USERS / DIM_PRODUCTS を作る際に使われているパターンです。

**TRUNCATE vs DROP の違い**

| 操作 | テーブル定義 | データ | 復元 |
|---|---|---|---|
| `TRUNCATE TABLE` | 残る | 削除 | Time Travel で復元可 |
| `DROP TABLE` | 削除 | 削除 | Time Travel で復元可（保持期間内） |

テストや実験のリセットには `TRUNCATE`、テーブル自体を撤廃するときは `DROP` を使います。

**VIEW**
データを持たない「SELECT の名前付き保存」。ストレージコストはゼロ。クエリを毎回書く手間を省き、アクセス制御の境界にもなります（本編07章で詳しく扱います）。

### 本編との接続マッピング

| DDL | 本編での使われ方 |
|---|---|
| `CREATE DATABASE / SCHEMA` | 00章（環境セットアップ） |
| `CREATE TABLE`（列定義） | 01章（テーブル設計）/ 06章（DIM/FACT） |
| `CREATE TABLE AS SELECT` | 06章（DIM_USERS / DIM_PRODUCTS を SELECT DISTINCT で作成） |
| `ALTER TABLE` | 08章（コスト最適化）/ A5（クラスタリング設定） |
| `CREATE VIEW` | 07章（V_SALES_DETAIL / V_CATEGORY_MONTHLY_SALES） |
| `TRUNCATE TABLE` | 実験・リセット時に使用 |

> **参考**: Snowflake 公式ドキュメント — [CREATE TABLE](https://docs.snowflake.com/ja/sql-reference/sql/create-table) / [CREATE VIEW](https://docs.snowflake.com/ja/sql-reference/sql/create-view)

---

## Section 6: JOIN

### テーブルエイリアスとは

`FROM テーブル名 別名` の形式でテーブルに短い別名を付けられます。`AS` は省略可能です。

```sql
FROM MART.FACT_PURCHASE_EVENTS f      -- f がエイリアス（AS f でも同義）
INNER JOIN MART.DIM_USERS u           -- u がエイリアス
  ON f.user_id = u.user_id
```

#### エイリアスが必要なケース

| 状況 | 必要性 | 理由 |
|---|---|---|
| **同名カラムが複数テーブルに存在** | **必須** | `user_id` が FACT と DIM 両方にある場合、`ON user_id = user_id` と書くと曖昧エラーになる |
| **同一テーブルを複数回 JOIN（自己結合）** | **必須** | テーブルを区別するための唯一の手段 |
| **単一テーブルのみ参照** | 不要 | エラーにならないが、書いた方が統一感がある |

#### `f.line_amount` の `f.` は省略できる？

`line_amount` は `FACT_PURCHASE_EVENTS` にしか存在しない列なので、技術的には省略可能です。
ただし **JOIN が多いクエリでは「どのテーブルの列か」を明示する方が可読性が上がります**。

```sql
-- 省略可能だが…
SELECT line_amount FROM MART.FACT_PURCHASE_EVENTS f INNER JOIN MART.DIM_USERS u ON ...

-- テーブルを明示した方が読みやすい
SELECT f.line_amount FROM MART.FACT_PURCHASE_EVENTS f INNER JOIN MART.DIM_USERS u ON ...
```

本編の SQL でも JOIN クエリでは必ずプレフィックスを付けるスタイルを統一しています。

### 4種類の JOIN

```
テーブル A:   1, 2, 3
テーブル B:   2, 3, 4

INNER JOIN:          2, 3         ← 両方に存在する行のみ
LEFT JOIN:     1,    2, 3         ← A の全行（B にない 1 は B 側が NULL）
RIGHT JOIN:          2, 3, 4      ← B の全行（B にない 4 は A 側が NULL）
FULL OUTER JOIN: 1,  2, 3, 4     ← 両方の全行（マッチしない側は NULL）
```

### JOIN の選び方

| 状況 | 使うJOIN |
|---|---|
| DIM にないキーは除外したい | `INNER JOIN` |
| FACT の全行を残しつつ DIM の属性を付けたい | `LEFT JOIN` |
| どちらか一方にしかないレコードを見つけたい | `FULL OUTER JOIN` + `WHERE ... IS NULL` |

> **実務のヒント**: `RIGHT JOIN` は `LEFT JOIN` で書き換えられます（テーブルの順序を入れ替えるだけ）。可読性のため `LEFT JOIN` に統一するチームが多いです。

### 本編06章との接続

本編06章のスタースキーマ集計は以下のパターンです。FACT を起点に DIM を JOIN する構造を理解しておくと、後の章でも読みやすくなります。

```sql
SELECT u.prefecture, d.category, SUM(f.line_amount) AS total_sales
FROM MART.FACT_PURCHASE_EVENTS f
INNER JOIN MART.DIM_USERS    u ON f.user_id = u.user_id
INNER JOIN MART.DIM_PRODUCTS d ON f.sku     = d.sku
GROUP BY u.prefecture, d.category;
```

> **参考**: Snowflake 公式ドキュメント — [JOIN](https://docs.snowflake.com/ja/sql-reference/constructs/join)

---

## Section 7: サブクエリと CTE

### サブクエリとは

クエリの中に別のクエリを埋め込んだもの。書ける場所は3か所です。

| 場所 | 名前 | 用途 |
|---|---|---|
| `FROM (SELECT ...)` | インラインビュー | 集計結果をさらに絞り込む |
| `WHERE 列 IN (SELECT ...)` | サブクエリ IN | 別テーブルの値で絞り込む |
| `SELECT (SELECT ...)` | スカラーサブクエリ | 1値を各行に付ける |

### CTE（Common Table Expression）とは

`WITH 名前 AS (SELECT ...)` で名前を付けたサブクエリ。サブクエリと機能は同じですが、以下の点が優れています。

- **可読性**: 処理を上から順に読める
- **多段参照**: 後続の CTE から名前で参照できる
- **デバッグ**: 途中の CTE だけを実行して確認できる

```sql
-- サブクエリ版（ネストが深い）
SELECT * FROM (SELECT category, SUM(line_amount) AS total FROM FACT GROUP BY category) WHERE total > 50000;

-- CTE版（フラットで読みやすい）
WITH cat_total AS (
  SELECT category, SUM(line_amount) AS total FROM FACT GROUP BY category
)
SELECT * FROM cat_total WHERE total > 50000;
```

### 多段変換パターン（CTEで raw → staging → mart を体験）

```sql
WITH with_tier AS (
  -- staging 相当: 変換・フラグ付与
  SELECT *, CASE WHEN line_amount >= 10000 THEN 'high' ELSE 'low' END AS tier
  FROM MART.FACT_PURCHASE_EVENTS
),
tier_summary AS (
  -- mart 相当: 集計
  SELECT tier, COUNT(*) AS cnt, SUM(line_amount) AS total FROM with_tier GROUP BY tier
)
SELECT * FROM tier_summary;
```

このパターンは、本編の RAW → STAGING → MART という3層構造の考え方と同じです。

### 本編との接続

| 本編 | サブクエリ/CTEとの関連 |
|---|---|
| 04章 MERGE | `MERGE ... USING (SELECT ...)` のインラインビュー |
| 10章 Semantic View | CTEに近いビジネスロジックの記述構造 |

> **参考**: Snowflake 公式ドキュメント — [WITH（CTE）](https://docs.snowflake.com/ja/sql-reference/constructs/with)

---

## Section 8: ウィンドウ関数

### GROUP BY との本質的な違い

| | 行数 | 使い方 |
|---|---|---|
| `GROUP BY` | **減る**（グループ数分になる） | 集計結果だけ欲しいとき |
| ウィンドウ関数 | **変わらない**（元の行数のまま） | 元の行を保ちながら集計値・順位を付けたいとき |

```sql
-- GROUP BY: 行が減る
SELECT category, SUM(line_amount) FROM FACT GROUP BY category;  -- カテゴリ数行

-- ウィンドウ関数: 行が減らない
SELECT event_id, category, SUM(line_amount) OVER (PARTITION BY category) AS cat_total FROM FACT;
```

### OVER() の3層構造

```sql
集計関数() OVER (
  PARTITION BY 列    -- ← どのグループで計算するか（省略=全体）
  ORDER BY 列        -- ← 順序（ランキング・累計で必要）
  ROWS BETWEEN ...   -- ← フレーム（累計の範囲指定）
)
```

### 主要なウィンドウ関数

| 関数 | 用途 | ORDER BY 必要？ |
|---|---|---|
| `SUM() OVER()` | 合計・累計 | 累計のときのみ |
| `AVG() OVER()` | 移動平均 | 任意 |
| `ROW_NUMBER()` | 連番（重複なし） | ✓ 必須 |
| `RANK()` | 順位（同順位で番号が飛ぶ） | ✓ 必須 |
| `DENSE_RANK()` | 順位（同順位で番号が飛ばない） | ✓ 必須 |
| `LAG(列, N)` | N行前の値 | ✓ 必須 |
| `LEAD(列, N)` | N行後の値 | ✓ 必須 |

### RANK vs DENSE_RANK の例

```
line_amount: 10000, 10000, 5000

RANK:        1,     1,     3    ← 同順位の後は「飛ぶ」（2位がない）
DENSE_RANK:  1,     1,     2    ← 同順位の後は「続く」
```

### ROWS BETWEEN の意味

```sql
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
-- UNBOUNDED PRECEDING: パーティションの先頭から
-- CURRENT ROW: 現在の行まで
-- → 累計計算に使う最頻出パターン
```

### WHERE句で直接使えない制約と回避策

ウィンドウ関数はSELECT句の評価後に確定するため、WHERE句には書けません。CTEでラップすることで解決できます。

```sql
-- NG: ウィンドウ関数を WHERE に直接書けない
SELECT * FROM FACT WHERE ROW_NUMBER() OVER (...) = 1;  -- エラー

-- OK: CTE でラップしてから WHERE で絞る
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY line_amount DESC) AS rn
  FROM MART.FACT_PURCHASE_EVENTS
)
SELECT * FROM ranked WHERE rn = 1;
```

> **参考**: Snowflake 公式ドキュメント — [分析（ウィンドウ）関数](https://docs.snowflake.com/ja/sql-reference/functions-analytic)

---

## Section 9: CASE WHEN と NULL処理

### CASE式は「値を返す式」

CASE式はどこでも使えます（SELECT / GROUP BY / WHERE / ORDER BY）。特に GROUP BY と組み合わせると強力です。

```sql
-- GROUP BY に CASE を使う（金額帯ごとに集計）
SELECT
  CASE WHEN line_amount >= 10000 THEN '高額' ELSE '通常' END AS tier,
  COUNT(*), SUM(line_amount)
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY 1;  -- GROUP BY の列番号参照（1 = SELECT の1列目）
```

### 0/1 フラグ × SUM パターン

条件件数を集計したいとき、CASE で 0/1 を作り SUM で集計する手法は頻出です。

```sql
SUM(CASE WHEN 条件 THEN 1 ELSE 0 END) AS 件数
```

### Snowflake固有の省略形

| 関数 | 意味 | 等価なSQL |
|---|---|---|
| `IFF(条件, 真, 偽)` | 2択の CASE WHEN | `CASE WHEN 条件 THEN 真 ELSE 偽 END` |
| `NVL(a, b)` | NULL なら b を返す | `COALESCE(a, b)` |
| `NULLIF(a, b)` | a = b なら NULL | — |

### NULLの特殊性

NULL は「値が存在しない」ことを表す特殊な状態です。

```sql
-- NULL との比較は常に NULL（FALSE ではなく「不明」）
NULL = NULL    → NULL（FALSE ではない）
NULL <> NULL   → NULL
NULL = 0       → NULL

-- そのため = で比較しても行が返らない
WHERE col = NULL    -- ← 常に0件
WHERE col IS NULL   -- ← 正しい
```

**COUNT(\*) と COUNT(列) の差 = NULL の行数**
```sql
SELECT COUNT(*) - COUNT(src_filename) AS null_count FROM MART.FACT_PURCHASE_EVENTS;
```

### NULL 処理関数の使い分け

| 関数 | 使いどころ |
|---|---|
| `COALESCE(a, b, c, ...)` | 最初の非NULL値を返す。LEFT JOINでNULLになる列のデフォルト値設定に使う |
| `NVL(a, b)` | 2引数版 COALESCE。書き方が短くなる |
| `NULLIF(a, b)` | ゼロ除算防止（`line_amount / NULLIF(qty, 0)`）でよく使う |
| `IFF(col IS NULL, '不明', col)` | 条件付きで異なるデフォルト値を返したいとき |

> **注意（COALESCE の型変換）**: 異なるデータ型の引数を混在させると、Snowflake が暗黙的に `NUMBER(18,5)` などに変換する場合があります。型を統一するか、明示的に `CAST` することで予期しない変換を防げます。

### 本編06章との接続

本編06章の DIM_USERS 作成では CASE WHEN でダミーデータを設定しています。

```sql
-- 06_star_schema.sql より
SELECT user_id,
  CASE WHEN user_id = 'u001' THEN 'Aki' WHEN user_id = 'u002' THEN 'Mina' ELSE 'Unknown' END AS user_name
FROM MART.FACT_PURCHASE_EVENTS;
```

これはマスタテーブルが存在しない場合の学習用の簡略化です。本番では別テーブルから JOIN で取得します。

> **参考**: Snowflake 公式ドキュメント — [CASE](https://docs.snowflake.com/ja/sql-reference/functions/case) / [COALESCE](https://docs.snowflake.com/ja/sql-reference/functions/coalesce)

---

## Section 10: Snowflake オブジェクト階層とネームスペース

### Snowflake のオブジェクト階層

```
Account（アカウント）
└── Warehouse（計算リソース ※課金対象・DB/Schemaとは独立）
└── Database（データベース）
    └── Schema（スキーマ）
        └── Table / View / Stage / Pipe / Task / Stream / Sequence ...
```

> **Warehouse の別扱い**: Warehouse は SQL の実行エンジン（計算リソース）です。Database / Schema とは独立した概念で、`USE WAREHOUSE` で切り替えます。クエリを実行するたびに Warehouse のクレジットが消費されるため、費用に直結します。

### USE コマンドとコンテキスト

`USE` でアクティブなコンテキストを設定すると、以降の SQL でオブジェクト名を省略できます。

```sql
USE WAREHOUSE LEARN_WH;   -- 計算リソースを指定
USE DATABASE HANDS_ON_DB; -- DB を指定
USE SCHEMA MART;          -- Schema を指定

-- 現在のコンテキストを確認
SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE();
```

### 完全修飾名（Fully Qualified Name）

`database.schema.table` の3段階の形式で書くと、`USE` の設定に関わらずどこからでも参照できます。

```sql
-- 完全修飾: どの環境でも確実に動く
SELECT COUNT(*) FROM HANDS_ON_DB.MART.FACT_PURCHASE_EVENTS;

-- DB を USE 済みなら省略可
SELECT COUNT(*) FROM MART.FACT_PURCHASE_EVENTS;

-- DB + Schema を USE 済みなら両方省略可
SELECT COUNT(*) FROM FACT_PURCHASE_EVENTS;
```

本編の SQL が `HANDS_ON_DB.MART.FACT_PURCHASE_EVENTS` と書いている理由は、スクリプトを `USE` なしで実行しても動くようにするためです。

### SHOW コマンド — オブジェクト一覧の確認

```sql
SHOW WAREHOUSES;                         -- アカウント内の WH 一覧
SHOW DATABASES;                          -- DB 一覧
SHOW SCHEMAS IN DATABASE HANDS_ON_DB;    -- DB 内の Schema 一覧
SHOW TABLES IN SCHEMA HANDS_ON_DB.MART;  -- Schema 内のテーブル一覧
```

### INFORMATION_SCHEMA — メタデータの参照

各 Database に自動で用意されるシステムビュー群です。テーブル・列・権限などのメタデータを SQL で確認できます。

```sql
-- MART スキーマのテーブル一覧・行数・サイズを確認
SELECT table_name, table_type, row_count, bytes
FROM HANDS_ON_DB.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'MART'
ORDER BY table_name;
```

### 本編との接続

- **00章** で実際に CREATE したオブジェクト（WH・DB・Schema）が、ここで確認できる構造になっています
- **SHOW** コマンドの出力 ≒ `INFORMATION_SCHEMA` のビューを GUI で見たもの（Snowsight のオブジェクトエクスプローラーと同じ情報）

> **参考**: Snowflake 公式ドキュメント — [アクセス制御の概要](https://docs.snowflake.com/ja/user-guide/security-access-control-overview)（SHOW / INFORMATION_SCHEMA 関連も含む）

---

## Section 11: ユーザー・ロール・権限の基本

### RBAC（ロールベースアクセス制御）の考え方

```
ユーザー（User）
    ↓ GRANT ROLE
ロール（Role）        ← 権限の「まとめ」
    ↓ GRANT 権限
オブジェクト（Table / Schema / Database / Warehouse ...）
```

ユーザーに直接権限を付けず、ロールに権限をまとめてからユーザーに付与します。ユーザーが増えてもロールを付与するだけで済むため、管理コストを大幅に削減できます。

### Snowflake システムロール早見表

| ロール | 主な権限 | 使いどころ |
|---|---|---|
| `ACCOUNTADMIN` | 全権限（課金・ユーザー管理を含む） | 初期設定のみ。普段は使わない |
| `SYSADMIN` | WH・DB・Schema・Table の作成/操作 | 本編で使うメインロール |
| `SECURITYADMIN` | ユーザー・ロールの作成・GRANT 操作 | ロール管理専用 |
| `USERADMIN` | ユーザー・ロールの作成のみ | ユーザー作成専用 |
| `PUBLIC` | 全ユーザーが自動で持つ最低限の権限 | 何もしなくても全員が持つ |

> **推奨パターン**: カスタムロールは必ず `SYSADMIN` 配下に置く（`GRANT ROLE カスタムロール TO ROLE SYSADMIN`）。そうしないと SYSADMIN でも管理できない孤立ロールになります。

### 最小権限の原則 — USAGE と SELECT を分けて付与

テーブルを「読む」ためには3層の権限が必要です。

```sql
-- ① Database を「見る」権限
GRANT USAGE ON DATABASE HANDS_ON_DB TO ROLE ANALYST_ROLE;

-- ② Schema を「見る」権限
GRANT USAGE ON SCHEMA HANDS_ON_DB.MART TO ROLE ANALYST_ROLE;

-- ③ データを「読む」権限
GRANT SELECT ON ALL TABLES IN SCHEMA HANDS_ON_DB.MART TO ROLE ANALYST_ROLE;
```

USAGE（見る）と SELECT（読む）を分けることで「スキーマは見えるがデータは読めない」という細かな制御が可能になります。

### FUTURE GRANTS — 将来のテーブルにも自動付与

```sql
GRANT SELECT ON FUTURE TABLES IN SCHEMA HANDS_ON_DB.MART TO ROLE ANALYST_ROLE;
```

このコマンドを一度実行しておくと、MART スキーマに後から追加されたテーブルにも自動で SELECT 権限が付与されます。テーブルを追加するたびに GRANT を再実行する手間が省けます。

### 権限の確認と取り消し

```sql
-- 権限の確認
SHOW GRANTS TO ROLE ANALYST_ROLE;                               -- ロールが持つ権限
SHOW GRANTS ON TABLE HANDS_ON_DB.MART.FACT_PURCHASE_EVENTS;    -- テーブルへの権限

-- 権限の取り消し
REVOKE SELECT ON ALL TABLES IN SCHEMA HANDS_ON_DB.MART FROM ROLE ANALYST_ROLE;
```

### 本編との接続

- **00章**: 本編では `SYSADMIN` で全操作しています。実務では用途ごとにロールを分けるのが標準です
- **A3章**: Dynamic Masking・Row Access Policy など、行/列レベルの高度なアクセス制御は A3 章を参照してください

> **参考**: Snowflake 公式ドキュメント — [アクセス制御の概要](https://docs.snowflake.com/ja/user-guide/security-access-control-overview)

---

## セクション間の関係図

```
Section 1 SELECT/FROM
    ↓ 行を絞る
Section 2 WHERE
    ↓ 並べる・絞る
Section 3 ORDER BY / LIMIT / DISTINCT
    ↓ 集計する
Section 4 GROUP BY / HAVING
    ↓ オブジェクトを作る
Section 5 DDL
    ↓ テーブルを結合する
Section 6 JOIN
    ↓ 複雑なクエリを整理する
Section 7 サブクエリ / CTE
    ↓ 行を保ちながら集計値を付ける
Section 8 ウィンドウ関数
    ↓ 条件分岐・NULL を扱う
Section 9 CASE WHEN / NULL処理
    ↓ オブジェクト構造を理解する
Section 10 オブジェクト階層・ネームスペース
    ↓ アクセス制御を理解する
Section 11 ユーザー・ロール・権限
```

---

## 本編との接続マップ

| 本編 | この付録との関連 |
|---|---|
| 00章 環境準備 | DDL（Section 5）: CREATE DATABASE / SCHEMA / WAREHOUSE<br>オブジェクト階層（Section 10）: 作成したオブジェクトの構造確認 |
| 01章 データモデリング | DDL（Section 5）: CREATE TABLE・列定義 |
| 02章 JSON / VARIANT | SELECT（Section 1）/ WHERE（Section 2）の基本 |
| 04章 Streams + Tasks | JOIN（Section 6）/ サブクエリ（Section 7）: MERGE の USING 句 |
| 05章 Dynamic Table | CTE（Section 7）: 多段変換の考え方 |
| 06章 スタースキーマ | DDL CTAS（Section 5）/ JOIN（Section 6）/ CASE WHEN（Section 9） |
| 07章 View | DDL CREATE VIEW（Section 5） |
| 08章 コスト最適化 | DDL ALTER TABLE（Section 5） |
| 09章 AI 関数 | SELECT / GROUP BY の応用 |
| 10章 Semantic View | CTE に近い構造（Section 7） |
| A3章 RBAC | 権限の基本（Section 11）の発展形: Dynamic Masking / Row Access Policy |

---

## 練習問題: SQL 20本ノック

> **この付録の使い方**
> Section 1〜9 の構文を体で覚えるための演習問題です。
> 前提: **06_star_schema.sql が完了済み**（`MART.FACT_PURCHASE_EVENTS` / `DIM_USERS` / `DIM_PRODUCTS` が存在する状態）。
> 解答は `<details>` をクリックして確認してください。Section 5（DDL）/ Section 10-11（オブジェクト階層・RBAC）はクエリ演習向きでないため対象外です。

---

### Q1〜Q4: Section 1-2（SELECT / WHERE）

---

### Q1. 税込み計算列を追加せよ

**難易度**: ★☆☆

**問題**: `MART.FACT_PURCHASE_EVENTS` から `event_id`・`product_name`・`line_amount`・税込み金額（`line_amount * 1.1`）を取得せよ。税込み金額の列名は `tax_included` とすること。

**ヒント**: `SELECT`, `AS`, 算術演算（`*`）

<details>
<summary>解答を見る</summary>

```sql
SELECT
    event_id,
    product_name,
    line_amount,
    line_amount * 1.1 AS tax_included
FROM MART.FACT_PURCHASE_EVENTS;
```

</details>

---

### Q2. Electronics カテゴリを金額降順で取得せよ

**難易度**: ★☆☆

**問題**: `MART.FACT_PURCHASE_EVENTS` から `category = 'Electronics'` の行を `line_amount` の降順で取得せよ。取得列は `event_id`・`product_name`・`line_amount`。

**ヒント**: `WHERE`, `ORDER BY ... DESC`

<details>
<summary>解答を見る</summary>

```sql
SELECT
    event_id,
    product_name,
    line_amount
FROM MART.FACT_PURCHASE_EVENTS
WHERE category = 'Electronics'
ORDER BY line_amount DESC;
```

</details>

---

### Q3. BETWEEN で中価格帯を絞れ

**難易度**: ★☆☆

**問題**: `MART.FACT_PURCHASE_EVENTS` から `line_amount` が 3000 以上 10000 以下の行を取得せよ。取得列は `event_id`・`category`・`product_name`・`line_amount`。

**ヒント**: `WHERE ... BETWEEN ... AND ...`

<details>
<summary>解答を見る</summary>

```sql
SELECT
    event_id,
    category,
    product_name,
    line_amount
FROM MART.FACT_PURCHASE_EVENTS
WHERE line_amount BETWEEN 3000 AND 10000;
```

</details>

---

### Q4. LIKE で「Phone」を含む商品を探せ

**難易度**: ★☆☆

**問題**: `MART.FACT_PURCHASE_EVENTS` から `product_name` に「Phone」を含む行が何件あるかを取得せよ。結果列名は `phone_count`。

**ヒント**: `WHERE ... LIKE '%...%'`, `COUNT(*)`

<details>
<summary>解答を見る</summary>

```sql
SELECT COUNT(*) AS phone_count
FROM MART.FACT_PURCHASE_EVENTS
WHERE product_name LIKE '%Phone%';
```

</details>

---

### Q5〜Q6: Section 3（ORDER BY / LIMIT / DISTINCT）

---

### Q5. 購入金額トップ5を取得せよ

**難易度**: ★☆☆

**問題**: `MART.FACT_PURCHASE_EVENTS` から `line_amount` が大きい順に上位5件を取得せよ。取得列は `event_id`・`user_id`・`product_name`・`line_amount`。

**ヒント**: `ORDER BY ... DESC`, `LIMIT`

<details>
<summary>解答を見る</summary>

```sql
SELECT
    event_id,
    user_id,
    product_name,
    line_amount
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY line_amount DESC
LIMIT 5;
```

</details>

---

### Q6. 購入カテゴリのユニーク一覧を取得せよ

**難易度**: ★☆☆

**問題**: `MART.FACT_PURCHASE_EVENTS` に存在するカテゴリの一覧を重複なしで取得し、アルファベット昇順で並べよ。

**ヒント**: `SELECT DISTINCT`, `ORDER BY`

<details>
<summary>解答を見る</summary>

```sql
SELECT DISTINCT category
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY category;
```

</details>

---

### Q7〜Q9: Section 4（GROUP BY / HAVING）

---

### Q7. カテゴリ別・購入件数と合計金額

**難易度**: ★☆☆

**問題**: `MART.FACT_PURCHASE_EVENTS` をカテゴリ別に集計し、購入件数（`purchase_count`）と合計金額（`total_amount`）を取得せよ。合計金額の降順で並べること。

**ヒント**: `GROUP BY`, `COUNT(*)`, `SUM()`

<details>
<summary>解答を見る</summary>

```sql
SELECT
    category,
    COUNT(*)        AS purchase_count,
    SUM(line_amount) AS total_amount
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY category
ORDER BY total_amount DESC;
```

</details>

---

### Q8. 3回以上購入したユーザーを絞れ

**難易度**: ★★☆

**問題**: `MART.FACT_PURCHASE_EVENTS` からユーザーごとの購入回数を集計し、3回以上購入しているユーザーのみを取得せよ。取得列は `user_id`・`purchase_count`。

**ヒント**: `GROUP BY`, `HAVING COUNT(*) >= 3`

<details>
<summary>解答を見る</summary>

```sql
SELECT
    user_id,
    COUNT(*) AS purchase_count
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY user_id
HAVING COUNT(*) >= 3
ORDER BY purchase_count DESC;
```

</details>

---

### Q9. 平均購入額5000円超のカテゴリのみ

**難易度**: ★★☆

**問題**: `MART.FACT_PURCHASE_EVENTS` をカテゴリ別に集計し、平均購入金額が5000円を超えるカテゴリのみを取得せよ。取得列は `category`・`avg_amount`（小数第2位まで）・`purchase_count`。

**ヒント**: `HAVING AVG() > 5000`, `ROUND()`

<details>
<summary>解答を見る</summary>

```sql
SELECT
    category,
    ROUND(AVG(line_amount), 2) AS avg_amount,
    COUNT(*)                   AS purchase_count
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY category
HAVING AVG(line_amount) > 5000
ORDER BY avg_amount DESC;
```

</details>

---

### Q10〜Q12: Section 6（JOIN）

---

### Q10. 都道府県別の合計購入金額

**難易度**: ★★☆

**問題**: `MART.FACT_PURCHASE_EVENTS` と `MART.DIM_USERS` を `user_id` で INNER JOIN し、都道府県（`prefecture`）別の合計購入金額を取得せよ。取得列は `prefecture`・`total_amount`。合計金額の降順で並べること。

**ヒント**: `INNER JOIN ... ON`, `GROUP BY`, `SUM()`

<details>
<summary>解答を見る</summary>

```sql
SELECT
    u.prefecture,
    SUM(f.line_amount) AS total_amount
FROM MART.FACT_PURCHASE_EVENTS f
INNER JOIN MART.DIM_USERS u ON f.user_id = u.user_id
GROUP BY u.prefecture
ORDER BY total_amount DESC;
```

</details>

---

### Q11. カテゴリ×商品名の販売集計

**難易度**: ★★☆

**問題**: `MART.FACT_PURCHASE_EVENTS` と `MART.DIM_PRODUCTS` を `sku` で INNER JOIN し、カテゴリ（`d.category`）と商品名（`d.product_name`）ごとに販売件数（`sales_count`）と合計金額（`total_amount`）を集計せよ。カテゴリ昇順・合計金額降順で並べること。

**ヒント**: `INNER JOIN`（複数テーブル）, `GROUP BY 複数列`

<details>
<summary>解答を見る</summary>

```sql
SELECT
    d.category,
    d.product_name,
    COUNT(*)            AS sales_count,
    SUM(f.line_amount)  AS total_amount
FROM MART.FACT_PURCHASE_EVENTS f
INNER JOIN MART.DIM_PRODUCTS d ON f.sku = d.sku
GROUP BY d.category, d.product_name
ORDER BY d.category, total_amount DESC;
```

</details>

---

### Q12. 購入履歴のないユーザーを探せ

**難易度**: ★★★

**問題**: `MART.DIM_USERS` の全ユーザーのうち、`MART.FACT_PURCHASE_EVENTS` に1件も購入記録がないユーザーの `user_id` と `user_name` を取得せよ。

**ヒント**: `LEFT JOIN`, `WHERE ... IS NULL`（FACT 側のキーが NULL = 購入なし）

<details>
<summary>解答を見る</summary>

```sql
SELECT
    u.user_id,
    u.user_name
FROM MART.DIM_USERS u
LEFT JOIN MART.FACT_PURCHASE_EVENTS f ON u.user_id = f.user_id
WHERE f.user_id IS NULL;
```

</details>

---

### Q13〜Q15: Section 7（サブクエリ / CTE）

---

### Q13. 合計金額が最大のカテゴリを CTE で求めよ

**難易度**: ★★☆

**問題**: CTE を使って、カテゴリ別合計金額を計算し、その中で合計金額が最大のカテゴリ名と金額を取得せよ。

**ヒント**: `WITH ... AS (...)`, `WHERE total = (SELECT MAX(total) FROM ...)`

<details>
<summary>解答を見る</summary>

```sql
WITH cat_total AS (
    SELECT
        category,
        SUM(line_amount) AS total
    FROM MART.FACT_PURCHASE_EVENTS
    GROUP BY category
)
SELECT category, total
FROM cat_total
WHERE total = (SELECT MAX(total) FROM cat_total);
```

</details>

---

### Q14. 全体平均を超える購入レコードを取得せよ

**難易度**: ★★☆

**問題**: `MART.FACT_PURCHASE_EVENTS` から、`line_amount` が全体平均を超えるレコードを取得せよ。取得列は `event_id`・`product_name`・`line_amount`・全体平均（`overall_avg`、小数第2位まで）。

**ヒント**: スカラーサブクエリ `(SELECT AVG(...) FROM ...)` を SELECT 句と WHERE 句の両方で使う

<details>
<summary>解答を見る</summary>

```sql
SELECT
    event_id,
    product_name,
    line_amount,
    ROUND((SELECT AVG(line_amount) FROM MART.FACT_PURCHASE_EVENTS), 2) AS overall_avg
FROM MART.FACT_PURCHASE_EVENTS
WHERE line_amount > (SELECT AVG(line_amount) FROM MART.FACT_PURCHASE_EVENTS)
ORDER BY line_amount DESC;
```

</details>

---

### Q15. 多段 CTE で上位3ユーザーを抽出せよ

**難易度**: ★★★

**問題**: 多段 CTE を使って、以下の処理を順番に実行し、合計購入金額が上位3位以内のユーザーの `user_id`・`user_name`・`total_amount`・`rank` を取得せよ。
1. ユーザー別合計金額を集計
2. DIM_USERS と JOIN してユーザー名を付与
3. 合計金額の降順でランクを付けて上位3位を抽出

**ヒント**: 多段 CTE（`WITH a AS (...), b AS (...)`）, `RANK() OVER`

<details>
<summary>解答を見る</summary>

```sql
WITH user_total AS (
    -- Step1: ユーザー別合計金額
    SELECT
        user_id,
        SUM(line_amount) AS total_amount
    FROM MART.FACT_PURCHASE_EVENTS
    GROUP BY user_id
),
user_named AS (
    -- Step2: ユーザー名を付与
    SELECT
        t.user_id,
        u.user_name,
        t.total_amount
    FROM user_total t
    INNER JOIN MART.DIM_USERS u ON t.user_id = u.user_id
),
user_ranked AS (
    -- Step3: ランク付け
    SELECT
        user_id,
        user_name,
        total_amount,
        RANK() OVER (ORDER BY total_amount DESC) AS rank
    FROM user_named
)
SELECT user_id, user_name, total_amount, rank
FROM user_ranked
WHERE rank <= 3;
```

</details>

---

### Q16〜Q18: Section 8（ウィンドウ関数）

---

### Q16. カテゴリ内ランキング1位のレコードを取得せよ

**難易度**: ★★★

**問題**: `MART.FACT_PURCHASE_EVENTS` で、カテゴリ内で `line_amount` が最大のレコードを各カテゴリから1件ずつ取得せよ。取得列は `category`・`product_name`・`line_amount`・`rank`。

**ヒント**: `RANK() OVER (PARTITION BY category ORDER BY line_amount DESC)`, CTE でラップして `WHERE rank = 1`

<details>
<summary>解答を見る</summary>

```sql
WITH ranked AS (
    SELECT
        category,
        product_name,
        line_amount,
        RANK() OVER (PARTITION BY category ORDER BY line_amount DESC) AS rank
    FROM MART.FACT_PURCHASE_EVENTS
)
SELECT category, product_name, line_amount, rank
FROM ranked
WHERE rank = 1
ORDER BY category;
```

</details>

---

### Q17. 購入金額の累計を計算せよ

**難易度**: ★★☆

**問題**: `MART.FACT_PURCHASE_EVENTS` を `event_time` 昇順で並べ、各行に購入金額の累計（`running_total`）を付けて取得せよ。取得列は `event_id`・`event_time`・`line_amount`・`running_total`。

**ヒント**: `SUM() OVER (ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`

<details>
<summary>解答を見る</summary>

```sql
SELECT
    event_id,
    event_time,
    line_amount,
    SUM(line_amount) OVER (
        ORDER BY event_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY event_time;
```

</details>

---

### Q18. 直前購入金額を LAG で取得せよ

**難易度**: ★★★

**問題**: `MART.FACT_PURCHASE_EVENTS` をユーザーごと・`event_time` 昇順で並べ、各行に「同一ユーザーの直前購入金額」（`prev_amount`）と「今回との差額」（`diff`）を付けて取得せよ。直前購入がない場合は 0 として扱うこと。取得列は `user_id`・`event_time`・`line_amount`・`prev_amount`・`diff`。

**ヒント**: `LAG(line_amount, 1, 0) OVER (PARTITION BY user_id ORDER BY event_time)`

<details>
<summary>解答を見る</summary>

```sql
SELECT
    user_id,
    event_time,
    line_amount,
    LAG(line_amount, 1, 0) OVER (
        PARTITION BY user_id
        ORDER BY event_time
    ) AS prev_amount,
    line_amount - LAG(line_amount, 1, 0) OVER (
        PARTITION BY user_id
        ORDER BY event_time
    ) AS diff
FROM MART.FACT_PURCHASE_EVENTS
ORDER BY user_id, event_time;
```

</details>

---

### Q19〜Q20: Section 9（CASE WHEN / NULL処理）

---

### Q19. 購入金額を3段階に分類して集計せよ

**難易度**: ★★☆

**問題**: `MART.FACT_PURCHASE_EVENTS` の `line_amount` を以下の3段階に分類し、各ランクの件数と合計金額を集計せよ。

| 条件 | ランク |
|---|---|
| 10000 以上 | `高額` |
| 5000 以上 10000 未満 | `中額` |
| 5000 未満 | `少額` |

取得列は `price_rank`・`purchase_count`・`total_amount`。ランクの順（高額→中額→少額）で並べること。

**ヒント**: `CASE WHEN ... THEN ... WHEN ... THEN ... ELSE ... END`, `GROUP BY`

<details>
<summary>解答を見る</summary>

```sql
SELECT
    CASE
        WHEN line_amount >= 10000 THEN '高額'
        WHEN line_amount >= 5000  THEN '中額'
        ELSE '少額'
    END                  AS price_rank,
    COUNT(*)             AS purchase_count,
    SUM(line_amount)     AS total_amount
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY price_rank
ORDER BY
    CASE price_rank
        WHEN '高額' THEN 1
        WHEN '中額' THEN 2
        ELSE 3
    END;
```

</details>

---

### Q20. NULL を '不明' に変換し NULL 件数も集計せよ

**難易度**: ★★☆

**問題**: `MART.FACT_PURCHASE_EVENTS` から以下の2つを取得せよ。
1. `src_filename` の NULL を `'不明'` に変換した列（`src_filename_display`）と `line_amount` を全件取得
2. 別クエリで `src_filename` が NULL の行数（`null_count`）と非 NULL の行数（`not_null_count`）を取得

**ヒント**: `COALESCE(列, '不明')`, `COUNT(*) - COUNT(列)` で NULL 件数を計算

<details>
<summary>解答を見る</summary>

```sql
-- 1. NULL を '不明' に変換して全件取得
SELECT
    COALESCE(src_filename, '不明') AS src_filename_display,
    line_amount
FROM MART.FACT_PURCHASE_EVENTS;

-- 2. NULL 件数 / 非 NULL 件数を集計
SELECT
    COUNT(*) - COUNT(src_filename) AS null_count,
    COUNT(src_filename)            AS not_null_count
FROM MART.FACT_PURCHASE_EVENTS;
```

</details>
