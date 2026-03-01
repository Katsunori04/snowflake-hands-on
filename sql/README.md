# sql/ フォルダ

このフォルダには、ハンズオン教材の SQL ファイルと教材テキストが章ごとに入っています。

## 使い方

各章は以下の順で進めてください。

1. **教材テキスト（.md）を読む** — 章の目的・コードの意味を把握する
2. **SQL ファイル（.sql）を Snowsight で実行する** — コードを動かして確認する

> 第12章（dbt）と第13章（Airflow）は SQL ファイルがなく、各専用フォルダにサンプルコードが入っています。

---

## 章一覧

| 章 | テーマ | 教材テキスト | SQL ファイル |
|---|---|---|---|
| 第0章 | 環境準備 | [00_setup.md](./00_setup.md) | [00_setup.sql](./00_setup.sql) |
| 第1章 | データモデリングの基本 | [01_modeling_basics.md](./01_modeling_basics.md) | [01_modeling_basics.sql](./01_modeling_basics.sql) |
| 第2章 | JSON と VARIANT | [02_json_variant.md](./02_json_variant.md) | [02_json_variant.sql](./02_json_variant.sql) |
| 第3章 | ファイル取り込み（Snowpipe） | [03_snowpipe.md](./03_snowpipe.md) | [03_snowpipe.sql](./03_snowpipe.sql) |
| 第4章 | 増分バッチ（Streams & Tasks） | [04_streams_tasks.md](./04_streams_tasks.md) | [04_streams_tasks.sql](./04_streams_tasks.sql) |
| 第5章 | 処理の再利用と宣言的更新（ストアドプロシージャ・Dynamic Table） | [05_stored_proc_dynamic_table.md](./05_stored_proc_dynamic_table.md) | [05_stored_proc_dynamic_table.sql](./05_stored_proc_dynamic_table.sql) |
| 第6章 | スタースキーマの構築 | [06_star_schema.md](./06_star_schema.md) | [06_star_schema.sql](./06_star_schema.sql) |
| 第7章 | 分析の入口を作る（View / Secure View） | [07_views.md](./07_views.md) | [07_views.sql](./07_views.sql) |
| 第8章 | コスト最適化の基本 | [08_cost_optimization.md](./08_cost_optimization.md) | [08_cost_optimization.sql](./08_cost_optimization.sql) |
| 第9章 | AI 関数（Snowflake Cortex） | [09_ai_sql.md](./09_ai_sql.md) | [09_ai_sql.sql](./09_ai_sql.sql) |
| 第10章 | セマンティックビュー・Cortex Analyst・Cortex Search | [10_semantic_view_cortex.md](./10_semantic_view_cortex.md) | [10_semantic_view_cortex.sql](./10_semantic_view_cortex.sql) |
| 第11章 | 全体パイプラインの復習 | [11_end_to_end_pipeline.md](./11_end_to_end_pipeline.md) | [11_end_to_end_pipeline.sql](./11_end_to_end_pipeline.sql) |
| 第12章 | dbt 入門 | [12_dbt.md](./12_dbt.md) | [dbt/ フォルダ](../dbt/) |
| 第13章 | Airflow 入門 | [13_airflow.md](./13_airflow.md) | [airflow/ フォルダ](../airflow/) |

---

## SQL ファイルの読み方

各 SQL ファイルは同じ構成になっています。

- `What you learn` — この章の目的
- `Run this first` — 最初にそのまま実行するブロック
- `Check` — 結果確認用のクエリ
- `Try this` — 1 つだけ自分で変えてみる練習
