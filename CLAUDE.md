# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## プロジェクト概要

Snowflake 初学者向けハンズオン教材。「SELECT は書けるがパイプライン構築はこれから」という
ターゲット向けに、生データ（RAW）→ 変換（STAGING）→ 分析（MART）の流れを実際に動かして学ぶ。

## リポジトリ構成

```
sql/          # 本編 00〜13章 + 付録 A1〜A7（各章に .sql と .md がペア）
datasets/     # サンプルデータ（events_sample.json: EC サイトイベント 200件 NDJSON形式）
dbt/          # dbt プロジェクトサンプル（models/ 以下の SQL は変更しない）
airflow/      # Airflow DAG サンプル（snowflake_event_pipeline.py）
```

## SQL ファイルの統一フォーマット

各 `.sql` ファイルは以下のパターンで記述されている:

```
-- What you learn: [学習目的]
-- Run this first. [最初に実行する SQL ブロック]
-- Check: [結果確認クエリ]
-- Try this: [練習問題]
```

SQL コードそのものは変更せず、コメント・説明の追加・日本語化のみで可読性を向上させる方針。

## データフロー

```
events_sample.json（ローカル）
    ↓ Snowsight GUI でアップロード
@RAW.EVENT_STAGE
    ↓ Snowpipe（03章）
RAW_EVENTS_PIPE（生データ VARIANT）
    ↓ Stream + Task（04章）
FACT_PURCHASE_EVENTS（購入ファクト）
    ↓ JOIN
DIM_USERS / DIM_PRODUCTS / DIM_DATE（06章）
    ↓ View / Semantic View
分析・AI・Cortex（07〜10章）
```

重要: `RAW_EVENTS`（02章 INSERT 練習用）と `RAW_EVENTS_PIPE`（03章以降の本線）は別テーブル。
04章以降は `RAW_EVENTS_PIPE` を参照する。

## 開発コマンド

### dbt（12章）

```bash
# Snowflake 接続設定（初回のみ）
cp dbt/profiles.example.yml ~/.dbt/profiles.yml
# profiles.yml を編集して account / user / password / role を入力

# dbt 操作（uv を使う場合）
uv add dbt-snowflake
uv run dbt debug       # 接続確認
uv run dbt run         # モデル実行
uv run dbt test        # データテスト
uv run dbt docs generate && uv run dbt docs serve  # ドキュメント確認
```

dbt のセットアップには 03章（RAW.RAW_EVENTS_PIPE）の完了が必要。

### Airflow（13章）

DAG ファイル: `airflow/snowflake_event_pipeline.py`

```bash
# 構文確認のみ
python airflow/snowflake_event_pipeline.py
```

実際の実行は Airflow UI（http://localhost:8080）から行う。
Airflow の接続設定: Admin > Connections > snowflake_default にアカウント情報を入力。
セットアップには 03章・04章の完了が必要。

## ドキュメントリンク方針

- Snowflake 公式ドキュメントのリンクは `/ja/` を優先する（例: `docs.snowflake.com/ja/...`）
- docs.getdbt.com / airflow.apache.org / code.visualstudio.com は日本語版なしのためそのまま

## 章番号マッピング

| 章 | ファイル | テーマ |
|---|---|---|
| 00 | 00_setup | 環境準備（WH・DB・Schema） |
| 01 | 01_modeling_basics | データモデリング基本 |
| 02 | 02_json_variant | JSON と VARIANT・LATERAL FLATTEN |
| 03 | 03_snowpipe | ファイル取り込み・Snowpipe |
| 04 | 04_streams_tasks | Streams + Tasks 増分バッチ |
| 05 | 05_stored_proc_dynamic_table | ストアドプロシージャ・Dynamic Table |
| 06 | 06_star_schema | スタースキーマ構築 |
| 07 | 07_views | View / Secure View |
| 08 | 08_cost_optimization | コスト最適化 |
| 09 | 09_ai_sql | AI 関数（Cortex 基本） |
| 10 | 10_semantic_view_cortex | Semantic View・Cortex Analyst・Cortex Search |
| 11 | 11_end_to_end_pipeline | 全体パイプライン復習 |
| 12 | 12_dbt.md | dbt 入門（SQL ファイルなし、dbt/ 参照） |
| 13 | 13_airflow.md | Airflow 入門（SQL ファイルなし、airflow/ 参照） |

付録: A1（アーキテクチャ）/ A2（Time Travel）/ A3（RBAC）/ A4（Data Sharing）/ A5（クラスタリング）/ A6（Snowpark）/ A7（VS Code）
