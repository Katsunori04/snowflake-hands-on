# Snowflake Hands-On for Beginners

`SELECT` は書けるが、Snowflake のパイプライン構築、バッチ処理、AI 関数、基本的なデータモデリングはこれから、という学習者向けの教材です。

この教材の方針:

- 先に動くコードを貼る
- その後で短く意味を確認する
- `RAW -> STAGING -> MART` の流れで統一する
- `正規化`、`非正規化`、`スタースキーマ` を軽く入れる
- `dbt` と `Airflow` は最小サンプルで役割を掴む

## 使い方

1. `sql/00_setup.sql` を実行する
2. `sql/01_modeling_basics.sql` でテーブル設計の考え方を確認する
3. `sql/02_json_variant.sql` で JSON を扱う
4. `datasets/events_sample.json` を stage にアップロードする
5. `sql/03_snowpipe.sql` で取り込みを作る
6. `sql/04_streams_tasks.sql` で差分バッチを作る
7. `sql/05_star_schema.sql` で分析用テーブルを作る
8. `sql/06_cost_optimization.sql` で運用の基本を確認する
9. `sql/07_ai_sql.sql` で AI SQL を試す
10. `sql/08_end_to_end_pipeline.sql` で全体像を復習する

`dbt` と `Airflow` は最後に次の順で読みます。

- `dbt/profiles.example.yml`
- `dbt/dbt_project.yml`
- `dbt/models/schema.yml`
- `dbt/models/*.sql`
- `airflow/snowflake_event_pipeline.py`

## 学習ゴール

この教材を終えると、次を説明できる状態を目指します。

- JSON を `VARIANT` で扱う
- `Snowpipe` でファイル取り込みを自動化する
- `Streams + Tasks` で増分バッチを組む
- `fact` と `dimension` を分ける理由を理解する
- Snowflake の基本的なコスト最適化を説明する
- `AI_COMPLETE`、`AI_CLASSIFY`、`AI_EXTRACT` を SQL から使う
- `dbt` と `Airflow` の役割差を説明する

## ディレクトリ構成

```text
snowflake-hands-on/
  README.md
  datasets/
    events_sample.json
  sql/
    00_setup.sql
    01_modeling_basics.sql
    02_json_variant.sql
    03_snowpipe.sql
    04_streams_tasks.sql
    05_star_schema.sql
    06_cost_optimization.sql
    07_ai_sql.sql
    08_end_to_end_pipeline.sql
  dbt/
    profiles.example.yml
    dbt_project.yml
    models/
      schema.yml
      stg_events.sql
      stg_event_items.sql
      dim_users.sql
      dim_products.sql
      fct_purchase_events.sql
  airflow/
    snowflake_event_pipeline.py
```

## 前提

- Snowflake の worksheet が使える
- `CREATE DATABASE`, `CREATE WAREHOUSE`, `CREATE STAGE`, `CREATE PIPE`, `CREATE TASK` ができる権限がある
- AI 関数を試す場合は `SNOWFLAKE.CORTEX_USER` が必要
- Airflow と dbt はこの教材では「最小構成の読み物 + コピペ用サンプル」

## 題材

題材は EC サイトのイベントログです。イベント JSON は以下のような形です。

- イベント単位:
  `event_id`, `user_id`, `event_type`, `event_time`, `device`, `review_text`
- 明細単位:
  `items[*].sku`, `items[*].qty`, `items[*].price`

この 1 つの題材で、次の 3 つをつなげます。

- アプリに近い生データの保持
- 分析しやすい形への変換
- AI 関数によるテキスト処理

## 章ごとの見方

各 SQL ファイルは同じ読み方にしています。

- `What you learn`: この章の目的
- `Run this first`: 最初にそのまま実行する SQL
- `Check`: 結果確認用の SQL
- `Try this`: 1 つだけ自分で変える練習

## 設計の考え方

この教材では、設計を次の 3 層で整理します。

- `RAW`: 元データをなるべくそのまま置く
- `STAGING`: 列型や粒度を整える
- `MART`: 集計や BI で使いやすい形にする

また、設計用語は次のレベルで押さえれば十分です。

- `正規化`: 重複を減らし更新しやすくする
- `非正規化`: 読みやすさや集計しやすさのために列を持たせる
- `スタースキーマ`: `fact` と `dimension` に分ける

## 学習スケジュール例

### 6日プラン

1. Day 1: `00_setup.sql`, `01_modeling_basics.sql`, `02_json_variant.sql`
2. Day 2: `03_snowpipe.sql`
3. Day 3: `04_streams_tasks.sql`
4. Day 4: `05_star_schema.sql`, `06_cost_optimization.sql`
5. Day 5: `07_ai_sql.sql`
6. Day 6: `dbt`, `airflow`, `08_end_to_end_pipeline.sql`

### 10日プラン

1. Day 1: 環境準備
2. Day 2: 設計の基本
3. Day 3: JSON
4. Day 4: Snowpipe
5. Day 5: Streams
6. Day 6: Tasks
7. Day 7: Star schema
8. Day 8: Cost optimization
9. Day 9: AI SQL
10. Day 10: dbt, Airflow, 総復習

## 補足

- `Snowpipe` は SQL だけで完結しないため、`datasets/events_sample.json` を Snowsight から stage にアップロードする手順を 1 回だけ挟みます
- `AI_COMPLETE` の出力はモデルにより多少変わります
- `AI_CLASSIFY` と `AI_EXTRACT` はコストが発生するため、小さなサンプルで試してください
