# airflow/ フォルダ

このフォルダには、Apache Airflow の DAG サンプルが入っています。

先に **[sql/10_airflow.md](../sql/10_airflow.md)** を読んでから、このフォルダのファイルを参照してください。

---

## ファイル一覧

| ファイル | 役割 |
|---|---|
| [snowflake_event_pipeline.py](./snowflake_event_pipeline.py) | Snowflake のパイプラインを自動実行する DAG サンプル |

---

## DAG 処理フロー

この DAG は以下の 3 タスクを順番に実行します。

```text
refresh_pipe
    │
    │  SYSTEM$PIPE_FORCE_RESUME でパイプを再開し、
    │  PIPE_STATUS で取り込み完了を確認する
    ▼
merge_to_fact
    │
    │  RAW_EVENTS_STREAM の差分を FACT_PURCHASE_EVENTS に MERGE する
    ▼
check_row_count
       FACT_PURCHASE_EVENTS の件数を確認する
```

---

## 接続設定

Airflow の Connections に `snowflake_default` という名前で Snowflake の接続情報を登録する必要があります。

設定手順の詳細は [sql/10_airflow.md](../sql/10_airflow.md) を参照してください。
