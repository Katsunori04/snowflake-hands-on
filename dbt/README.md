# dbt/ フォルダ

このフォルダには、dbt（data build tool）のサンプルプロジェクトが入っています。

先に **[sql/11_dbt.md](../sql/11_dbt.md)** を読んでから、このフォルダのファイルを参照してください。

---

## ファイル一覧

| ファイル | 役割 |
|---|---|
| [profiles.example.yml](./profiles.example.yml) | Snowflake 接続設定のサンプル。`~/.dbt/profiles.yml` にコピーして使う |
| [dbt_project.yml](./dbt_project.yml) | dbt プロジェクトの基本設定（プロジェクト名・モデルのスキーマ設定など） |
| [models/schema.yml](./models/schema.yml) | モデルのテスト・ドキュメント定義（列の説明・not_null テストなど） |
| [models/stg_events.sql](./models/stg_events.sql) | RAW_EVENTS_PIPE からイベント単位に整形する Staging モデル |
| [models/stg_event_items.sql](./models/stg_event_items.sql) | RAW_EVENTS_PIPE から商品明細単位に展開する Staging モデル |
| [models/dim_users.sql](./models/dim_users.sql) | ユーザーのディメンションテーブルを生成するモデル |
| [models/dim_products.sql](./models/dim_products.sql) | 商品のディメンションテーブルを生成するモデル |
| [models/fct_purchase_events.sql](./models/fct_purchase_events.sql) | 購入イベントのファクトテーブルを生成するモデル |

---

## セットアップ手順

```bash
# 1. 接続設定ファイルをコピーする
cp dbt/profiles.example.yml ~/.dbt/profiles.yml

# 2. profiles.yml を開いて Snowflake の認証情報を入力する
#    （account / user / password / database / warehouse）

# 3. dbt の動作確認
dbt debug

# 4. モデルを実行する
dbt run
```

詳細は [sql/11_dbt.md](../sql/11_dbt.md) を参照してください。
