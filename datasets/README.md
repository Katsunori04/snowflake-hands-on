# datasets/ フォルダ

このフォルダには、ハンズオンで使用するサンプルデータが入っています。

---

## ファイル一覧

| ファイル | 内容 |
|---|---|
| [events_sample.json](./events_sample.json) | EC サイトのイベントログ（JSON 形式、複数イベントを改行区切りで格納） |

---

## JSON の構造

1 イベントは以下の構造を持ちます。

```json
{
  "event_id": "evt_001",
  "user_id": "u_001",
  "event_type": "purchase",
  "event_time": "2024-01-15T10:30:00Z",
  "device": "mobile",
  "review_text": "とても使いやすかったです",
  "items": [
    { "sku": "SKU_A", "qty": 2, "price": 1500 },
    { "sku": "SKU_B", "qty": 1, "price": 3000 }
  ]
}
```

| フィールド | 型 | 説明 |
|---|---|---|
| `event_id` | STRING | イベントの一意識別子 |
| `user_id` | STRING | ユーザーの識別子 |
| `event_type` | STRING | イベント種別（`purchase` / `view` など） |
| `event_time` | TIMESTAMP | イベント発生日時（UTC） |
| `device` | STRING | 使用デバイス（`mobile` / `desktop` など） |
| `review_text` | STRING | レビューテキスト（7章の AI 関数で使用） |
| `items[]` | ARRAY | 購入明細。`sku`・`qty`・`price` を持つ |

---

## このファイルの使い方

このファイルは **[sql/03_snowpipe.md](../sql/03_snowpipe.md)** の手順に従って Snowflake の Stage にアップロードします。

アップロード後、以下のコマンドで確認できます。

```sql
list @RAW.EVENT_STAGE;
```
