# 第12章: Airflow 入門

> 参照ファイル: `airflow/snowflake_event_pipeline.py`

## この章で学ぶこと

- Airflow が何をするツールなのかを理解する
- Snowflake Task との使い分けを理解する
- Snowflake 接続設定（Connection）の方法を理解する
- DAG の構造とタスク依存関係の書き方を理解する

## 前提条件

- 第3章（`sql/03_snowpipe.sql`）が完了していること（`RAW.EVENTS_PIPE` が必要）
- 第4章（`sql/04_streams_tasks.sql`）が完了していること（`RAW.RAW_EVENTS_STREAM`、`MART.FACT_PURCHASE_EVENTS` が必要）
- Apache Airflow がインストール・起動していること

---

## 概念解説

### Airflow とは

**Apache Airflow** は、データパイプラインのワークフローをコードで定義・スケジュール・監視するオーケストレーションツールです。Python コードで DAG（有向非巡回グラフ）を定義します。

```
DAG（Directed Acyclic Graph）= タスクの依存関係グラフ

Task A → Task B → Task C
  ↑                  ↑
先に実行        A と B が終わったら実行
```

### Snowflake Task との使い分け

| | Snowflake Task | Airflow |
|---|---|---|
| **主な用途** | Snowflake 内の SQL 処理のスケジュール実行 | 複数システムをまたぐワークフロー管理 |
| **設定場所** | Snowflake（SQL で定義） | Airflow（Python で定義） |
| **外部連携** | 限定的（Snowflake 内完結） | S3・Slack・BigQuery・API など豊富 |
| **モニタリング** | Snowsight のタスク履歴 | Airflow UI の DAG ビュー |
| **エラー通知** | 設定が必要 | メール・Slack への通知が容易 |

**使い分けの目安**:
- **Snowflake Task**: Snowflake 内で完結する変換処理（シンプルな MERGE など）
- **Airflow**: 外部ファイルの取得・他サービスへのデータ送信・複数 DB をまたぐ処理

---

## 接続設定（Airflow → Snowflake）

Airflow の UI から Snowflake への接続を設定します。

**設定手順（5 ステップ）**:

1. Airflow UI にログインする（デフォルト: `http://localhost:8080`）
2. 上部メニューの **[Admin]** → **[Connections]** を開く
3. **[+]** ボタンで新しい接続を追加する
4. 以下の項目を入力する:

| 項目 | 値 |
|---|---|
| Connection Id | `snowflake_default` |
| Connection Type | `Snowflake` |
| Schema | `MART`（デフォルトスキーマ） |
| Login | Snowflake のユーザー名 |
| Password | Snowflake のパスワード |
| Extra（JSON） | 下記参照 |

**Extra の JSON**:
```json
{
  "account": "<your_account_locator>",
  "warehouse": "LEARN_WH",
  "database": "LEARN_DB",
  "role": "<your_role>"
}
```

5. **[Save]** をクリックして保存する

---

## DAG の解説

`airflow/snowflake_event_pipeline.py` の構造を確認します。

### DAG の全体構成

```python
with DAG(
    dag_id="snowflake_event_pipeline",
    start_date=datetime(2026, 2, 28),
    schedule="@hourly",    # 1 時間ごとに実行
    catchup=False,         # 過去分のバックフィルをしない
) as dag:
    ...
```

### 3 タスクの役割

```
run_raw_load_sql
  │ alter pipe RAW.EVENTS_PIPE refresh;
  │ → Stage のファイルを PIPE で取り込む
  │
  ▼
run_transform_sql
  │ MERGE into MART.FACT_PURCHASE_EVENTS ...
  │ → Stream の差分を FACT に MERGE
  │
  ▼
run_quality_check
    select count(*) from MART.FACT_PURCHASE_EVENTS;
    → FACT テーブルの件数を確認（品質チェック）
```

### タスク依存関係の書き方

```python
run_raw_load_sql >> run_transform_sql >> run_quality_check
```

`>>` 演算子でタスクの依存関係（実行順）を定義します。これにより:
- `run_raw_load_sql` が成功したら `run_transform_sql` を実行
- `run_transform_sql` が成功したら `run_quality_check` を実行

---

## 各タスクのコード抜粋

### Task 1: PIPE の更新

```python
run_raw_load_sql = SQLExecuteQueryOperator(
    task_id="run_raw_load_sql",
    conn_id="snowflake_default",   # 接続設定で定義した ID
    sql="""
    use warehouse LEARN_WH;
    use database LEARN_DB;
    use schema RAW;
    alter pipe RAW.EVENTS_PIPE refresh;  -- Stage のファイルを取り込む
    """,
)
```

### Task 2: MERGE（変換処理）

```python
run_transform_sql = SQLExecuteQueryOperator(
    task_id="run_transform_sql",
    conn_id="snowflake_default",
    sql="""
    -- Stream の差分を FACT テーブルへ MERGE
    merge into MART.FACT_PURCHASE_EVENTS tgt
    using (
      select ...
      from RAW.RAW_EVENTS_STREAM s,
      lateral flatten(input => s.raw:items) item
      where s.metadata$action = 'INSERT'
    ) src
    on tgt.event_id = src.event_id and tgt.sku = src.sku
    when matched then update set ...
    when not matched then insert (...) values (...);
    """,
)
```

### Task 3: 品質チェック

```python
run_quality_check = SQLExecuteQueryOperator(
    task_id="run_quality_check",
    conn_id="snowflake_default",
    sql="""
    select count(*) as row_count
    from LEARN_DB.MART.FACT_PURCHASE_EVENTS;
    """,
)
```

---

## Airflow の主要な概念

| 概念 | 説明 |
|---|---|
| DAG | タスクの依存関係グラフ。Python ファイルで定義 |
| Task | DAG の中の 1 つの処理単位 |
| Operator | Task の種類。`SQLExecuteQueryOperator` は SQL を実行する |
| Connection | 外部システムへの接続設定（Airflow UI で管理） |
| `>>` | タスクの依存関係を定義する演算子 |

---

## 参考: dbt + Airflow の組み合わせ

Airflow は dbt とも組み合わせて使えます。

```python
# Airflow で dbt run を呼ぶパターン（参考）
from airflow.operators.bash import BashOperator

run_dbt = BashOperator(
    task_id="run_dbt",
    bash_command="cd /path/to/dbt && dbt run",
)

# データ取り込み → dbt 変換 → 品質チェック の順に実行
run_raw_load_sql >> run_dbt >> run_quality_check
```

この組み合わせにより:
- **Airflow**: スケジュール管理・外部連携・エラー通知
- **dbt**: 変換ロジックの定義・テスト・ドキュメント

という役割分担ができます。

---

## まとめ

| 概念 | ポイント |
|---|---|
| Airflow DAG | Python コードでタスクの依存関係を定義 |
| Connection | 外部システム（Snowflake）への接続設定 |
| `SQLExecuteQueryOperator` | SQL を実行する Airflow の Operator |
| `>>` 演算子 | タスクの実行順（依存関係）を定義 |
| dbt との組み合わせ | Airflow でスケジュール、dbt で変換ロジックを管理 |

これで全章のハンズオンが完了です。お疲れさまでした！

## 参考リンク

- [Apache Airflow ドキュメント](https://airflow.apache.org/docs/)
- [Snowflake Provider for Airflow](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/index.html)
- [Snowflake + Airflow 連携ガイド](https://docs.snowflake.com/en/user-guide/ecosystem-airflow)

## 学習チェックリスト

- [ ] Airflow の DAG・Task・Operator の概念を説明できる
- [ ] SnowflakeOperator を使った SQL 実行タスクを理解した
- [ ] DAG の依存関係（`>>` 演算子）を設定できる
- [ ] Airflow と Snowflake Task の使い分けを判断できる
