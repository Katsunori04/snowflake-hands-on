# 第12章: dbt 入門

> 参照ファイル: `dbt/` ディレクトリ全体

## この章で学ぶこと

- dbt（data build tool）が何をするツールなのかを理解する
- Snowflake Task との違いを理解する
- `ref()` と `source()` によるモデル依存関係の定義方法を理解する
- `dbt run` / `dbt test` / `dbt docs generate` の基本的な使い方を知る

## 前提条件

- 第3章（`sql/03_snowpipe.sql`）が完了していること（`RAW.RAW_EVENTS_PIPE` にデータが必要）
- dbt がインストールされていること（`pip install dbt-snowflake` または `uv add dbt-snowflake`）

---

## 概念解説

### dbt とは

**dbt（data build tool）** は、データウェアハウス内の変換処理（Transform）を管理するツールです。「SQL テンプレート + テスト + ドキュメント」を組み合わせて、データパイプラインの品質と再現性を高めます。

```
[dbt が担当するのは「T」の部分]

Extract → Load → Transform（← dbt）
           ↑
      Snowpipe や
      COPY INTO が担当
```

### dbt と Snowflake Task の違い

| | Snowflake Task | dbt |
|---|---|---|
| **役割** | Snowflake 内でのスケジュール実行 | 変換ロジックの定義・テスト・ドキュメント化 |
| **スケジュール** | Task が自身でスケジュールを持つ | dbt 単体にはスケジュール機能がない（Airflow などと組み合わせる） |
| **テスト** | 自前で確認クエリを書く | `schema.yml` に定義するだけで自動テスト |
| **依存関係** | 手動で管理 | `ref()` / `source()` で自動的に DAG を構築 |
| **ドキュメント** | 別途 Wiki 等で管理 | `dbt docs generate` でデータカタログを自動生成 |

---

## セットアップ

### Step 1: profiles.yml をコピーする

```bash
cp dbt/profiles.example.yml ~/.dbt/profiles.yml
```

dbt は `~/.dbt/profiles.yml` を自動的に読み込みます。

### Step 2: 各項目を書き換える

`~/.dbt/profiles.yml` を開いて `<...>` の部分を実際の値に置き換えます。

```yaml
snowflake_learn:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "<your_account_locator>"   # 例: abc12345.us-east-1
      user: "<your_username>"             # Snowflake のログイン名
      password: "<your_password>"         # 直書きより環境変数を推奨
      role: "<your_role>"                 # 例: SYSADMIN
      database: LEARN_DB
      warehouse: LEARN_WH
      schema: MART                        # デフォルトのスキーマ
      threads: 4
      client_session_keep_alive: false
```

> **セキュリティメモ**: パスワードは直書きせず、環境変数を利用することを推奨します。
> ```yaml
> password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
> ```

---

## モデルの依存関係（DAG）

このプロジェクトのモデル構成と依存関係を確認します。

```
RAW.RAW_EVENTS_PIPE（source）
         │
         ├──→ stg_events        （イベントヘッダーの整形）
         └──→ stg_event_items   （購入明細の整形・FLATTEN）
                   │
                   ├──→ fct_purchase_events  （購入ファクト）
                   ├──→ dim_users            （ユーザー DIM）
                   └──→ dim_products         （商品 DIM）
```

### `source()` と `ref()` の使い分け

| 関数 | 役割 | 使用場所 |
|---|---|---|
| `source('RAW', 'RAW_EVENTS_PIPE')` | dbt 管理外のテーブル（RAW 層）を参照 | 最初のモデル（stg_events など） |
| `ref('stg_event_items')` | dbt が管理するモデルを参照 | 下流のモデル（fct_*、dim_* など） |

**`source()` の例**（`dbt/models/stg_events.sql`）:

```sql
select
  raw:event_id::string as event_id,
  raw:user_id::string as user_id,
  raw:event_type::string as event_type,
  ...
from {{ source('RAW', 'RAW_EVENTS_PIPE') }}
```

**`ref()` の例**（`dbt/models/fct_purchase_events.sql`）:

```sql
select
  event_id, user_id, event_time, sku,
  product_name, category, qty, price, line_amount, src_filename
from {{ ref('stg_event_items') }}
```

dbt がビルド時に依存関係を解析し、正しい順序で実行してくれます。

### dim_users モデル（`dbt/models/dim_users.sql`）

```sql
select distinct
  user_id,
  'Unknown' as user_name,
  'Unknown' as prefecture
from {{ ref('stg_events') }}
```

> **注意（デモ用の簡略化）**: `'Unknown'` はデモ用の定数です。
> 本番環境では、`source()` マクロを使って実際のユーザーデータソースから取得します。
>
> ```sql
> -- 本番構成の例
> SELECT user_id, user_name, email
> FROM {{ source('raw', 'users') }}
> ```

### fct_purchase_events モデル（`dbt/models/fct_purchase_events.sql`）

```sql
select
  event_id,
  user_id,
  event_time,
  sku,
  product_name,
  category,
  qty,
  price,
  line_amount,
  src_filename
from {{ ref('stg_event_items') }}
```

> **設計メモ（非正規化の意図）**: `product_name` と `category` をファクトテーブルに持たせているのは、
> **購入時点の商品情報を保持する**ためです。
> 後から `DIM_PRODUCTS` の商品名・カテゴリが変更されても、
> 購入当時の記録が失われない設計になっています（Slowly Changing Dimension Type 1 への対策）。

---

## テストの定義

`dbt/models/schema.yml` に定義するだけで、自動テストが実行されます。

```yaml
models:
  - name: stg_events
    columns:
      - name: event_id
        tests:
          - not_null    # NULL が含まれていないかチェック
          - unique      # 重複がないかチェック
      - name: user_id
        tests:
          - not_null
```

手動で確認クエリを書かなくても、`dbt test` でこれらが自動的に検証されます。

---

## 3.5. 接続確認: dbt debug

dbt プロジェクトが正しく設定されているか確認するには `dbt debug` を実行します。

```bash
dbt debug
```

**成功時の出力例:**
```
All checks passed!
```

**よくあるエラーと対処法:**

| エラー | 原因 | 対処法 |
|--------|------|--------|
| `Could not connect to Snowflake` | 認証情報の誤り・ネットワーク問題 | `profiles.yml` の account / user / password を確認 |
| `Database 'XXX' does not exist` | データベース名の誤り | `profiles.yml` の database を `LEARN_DB` に修正 |
| `Insufficient privileges` | スキーマ/ウェアハウスの権限不足 | Snowflake で `GRANT USAGE ON SCHEMA` を実行 |

---

## 実行コマンド

```bash
# dbt プロジェクトのディレクトリに移動
cd dbt/

# 全モデルをビルド（依存順に自動で実行）
dbt run

# テストを実行（schema.yml で定義したテスト）
dbt test

# ドキュメント（データカタログ）を生成してブラウザで表示
dbt docs generate
dbt docs serve
```

`dbt docs serve` を実行するとブラウザが開き、モデルの DAG 図・カラム定義・テスト結果を視覚的に確認できます。

---

## 手動 SQL との比較

| 機能 | 手動 SQL（01〜11章） | dbt |
|---|---|---|
| **再現性** | SQL ファイルを順に実行 | `dbt run` 1 コマンドで全モデルを順に実行 |
| **依存関係管理** | 手動で実行順を管理 | `ref()` で自動解析・正しい順で実行 |
| **テスト** | 確認クエリを手動で書く | `schema.yml` に定義して `dbt test` で自動実行 |
| **ドキュメント** | 別途 Wiki 等で管理 | `dbt docs` でデータカタログを自動生成 |
| **変更管理** | SQL ファイルを手動で更新 | git + dbt で変更履歴を管理しやすい |

---

## まとめ

| 概念 | ポイント |
|---|---|
| dbt model | 1 つの SQL ファイルが 1 つのテーブル or ビューになる |
| `source()` | dbt 管理外のテーブル（RAW 層）を参照する |
| `ref()` | dbt が管理するモデルを参照。依存関係を自動解析 |
| `dbt run` | 全モデルをビルド |
| `dbt test` | `schema.yml` のテストを自動実行 |
| `dbt docs` | データカタログを自動生成 |

次の章では、Airflow を使って Snowflake のパイプラインをオーケストレーションする方法を学びます。

## 参考リンク

- [dbt ドキュメント](https://docs.getdbt.com/)
- [dbt Snowflake アダプタ](https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup)
- [Snowflakeでのdbtプロジェクト](https://docs.snowflake.com/ja/user-guide/data-engineering/dbt-projects-on-snowflake)

## 学習チェックリスト

- [ ] dbt の役割（SQL 変換層のバージョン管理・テスト）を説明できる
- [ ] `dbt run` / `dbt test` の基本コマンドを理解した
- [ ] ref() 関数でモデル間の依存関係を定義できる
- [ ] Snowflake と dbt の接続設定（profiles.yml）を理解した
