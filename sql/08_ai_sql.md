# 第8章: AI 関数（Snowflake Cortex）

> この章で実行するファイル: `sql/08_ai_sql.sql`

## この章で学ぶこと

- Snowflake Cortex の AI 関数（`AI_COMPLETE`、`AI_CLASSIFY`、`AI_EXTRACT`）を使う
- SQL の中でテキストの要約・分類・情報抽出を行う
- 構造化データとテキストデータを組み合わせた分析を行う

## 前提条件

- 第0章（`sql/00_setup.sql`）が完了していること
- 第2章（`sql/02_json_variant.sql`）が完了していること（`STAGING.STG_EVENTS` が必要）
- **CORTEX_USER 権限**が付与されていること（下記を参照）

---

## CORTEX_USER 権限の取得手順

AI 関数を使うには `SNOWFLAKE.CORTEX_USER` データベースロールが必要です。

**Step 1: 自分のロールを確認する**

```sql
SELECT current_role();
```

**Step 2: ACCOUNTADMIN か SYSADMIN で権限を付与する**

```sql
-- 自分のロールが SYSADMIN の場合
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE SYSADMIN;

-- カスタムロール（例: MY_ROLE）に付与する場合
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE MY_ROLE;
```

**Step 3: 権限を有効化する**

ロールを切り替えるか、一度ログアウトして再ログインすると権限が有効になります。Snowsight の場合は一度ログアウト→ログインし直すと確実です。

---

## 概念解説

### Snowflake Cortex とは

**Snowflake Cortex** は、Snowflake が提供する AI / ML 機能のプラットフォームです。追加のインフラ設定なしで、SQL の中から AI 機能を呼び出せます。

### AI 関数の役割の違い

| 関数 | 役割 | 典型的なユースケース |
|---|---|---|
| `AI_COMPLETE` | LLM を使った自由なテキスト生成・変換 | 要約、翻訳、説明生成 |
| `AI_CLASSIFY` | 定義したラベルへの分類 | 感情分析、カテゴリ分類 |
| `AI_EXTRACT` | テキストから指定した情報を抽出 | エンティティ抽出、キーワード抽出 |

---

## ハンズオン手順

### Step 1: レビューデータを準備する

```sql
create or replace table STAGING.REVIEWS (
  review_id string,
  user_id string,
  review_text string
);

insert into STAGING.REVIEWS values
  ('r001', 'u001', 'Fast delivery and good quality. The shoes were comfortable for trail running.'),
  ('r002', 'u002', 'Coffee aroma was excellent, but the package arrived slightly damaged.'),
  ('r003', 'u003', 'The product was okay overall, but setup instructions were confusing.');
```

---

### Step 2: AI_COMPLETE で 1 文要約する

```sql
select
  review_id,
  review_text,
  AI_COMPLETE(
    'claude-3-5-sonnet',                                     -- 使用するモデル
    'Summarize this customer review in one short sentence in Japanese: ' || review_text
  ) as summary_ja
from STAGING.REVIEWS;
```

**期待される出力例**:

| review_id | summary_ja |
|---|---|
| r001 | 配送が速く品質も良く、トレイルランニングに最適なシューズでした。 |
| r002 | コーヒーの香りは素晴らしかったが、パッケージが破損して届きました。 |
| r003 | 全体的に普通の商品だが、セットアップ説明が分かりにくかった。 |

---

### Step 3: AI_CLASSIFY で感情分類する

```sql
select
  review_id,
  AI_CLASSIFY(
    review_text,
    [
      object_construct('label', 'positive', 'description', 'Overall favorable review'),
      object_construct('label', 'neutral',  'description', 'Mixed or balanced review'),
      object_construct('label', 'negative', 'description', 'Mostly dissatisfied review')
    ],
    object_construct('task_description', 'Classify customer review sentiment')
  ) as sentiment_result
from STAGING.REVIEWS;
```

`sentiment_result` は JSON 形式で `{"label": "positive", "score": 0.95}` のような結果が返ります。

---

### Step 4: AI_EXTRACT で必要情報を抽出する

```sql
select
  review_id,
  AI_EXTRACT(
    review_text,
    object_construct(
      'product_quality', 'What does the review say about quality?',
      'delivery',        'What does the review say about shipping or delivery?',
      'issue',           'What problem or complaint is mentioned?'
    )
  ) as extracted_points
from STAGING.REVIEWS;
```

各キーに対して AI がテキストから該当する記述を抽出します。

---

### Step 5: 構造化データと組み合わせる

イベントデータ（`STG_EVENTS`）とレビューデータを JOIN して AI で分析します。

```sql
select
  e.event_id,
  e.user_id,
  e.event_time,
  r.review_id,
  AI_COMPLETE(
    'claude-3-5-sonnet',
    'Write one Japanese bullet summarizing the likely customer intent behind this review: ' || r.review_text
  ) as customer_intent
from STAGING.STG_EVENTS e
join STAGING.REVIEWS r on e.user_id = r.user_id
order by e.event_time;
```

---

## 確認クエリ

```sql
select * from STAGING.REVIEWS order by review_id;
```

---

## 注意事項

- **AI 関数にはコストがかかります**: 大量データに適用すると Claude API の利用コストが発生します。最初は小サンプルで試してから本番に適用してください
- **出力はモデルによって変わります**: `AI_COMPLETE` の結果はモデルのバージョンや同じプロンプトでも揺れることがあります。重要な判断には人間のレビューを組み合わせてください

---

## Try This

**`AI_CLASSIFY` のラベルを感情（sentiment）ではなく、問い合わせカテゴリに変えてみてください。**

<details>
<summary>答え例</summary>

```sql
select
  review_id,
  AI_CLASSIFY(
    review_text,
    [
      object_construct('label', 'product',  'description', 'About product quality or features'),
      object_construct('label', 'delivery', 'description', 'About shipping, delivery, or packaging'),
      object_construct('label', 'usability','description', 'About ease of use or instructions')
    ],
    object_construct('task_description', 'Classify customer review by topic')
  ) as topic_result
from STAGING.REVIEWS;
```

ラベルの定義を変えるだけで、同じデータを異なる軸で分類できます。

</details>

---

## まとめ

| 関数 | 引数 | 出力 |
|---|---|---|
| `AI_COMPLETE(model, prompt)` | モデル名・プロンプト文字列 | 生成テキスト |
| `AI_CLASSIFY(text, labels, options)` | テキスト・ラベル配列・オプション | ラベルとスコアの JSON |
| `AI_EXTRACT(text, schema)` | テキスト・抽出スキーマ | 抽出された情報の JSON |

## よくあるエラーと対処法

| エラー / 症状 | 原因 | 対処法 |
|---|---|---|
| `SQL access control error: Insufficient privileges to operate on database role 'CORTEX_USER'` | 実行ロールに `SNOWFLAKE.CORTEX_USER` が付与されていない | 管理権限のあるロールで `GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE <your_role>;` を実行する |
| 権限を付与したのに AI 関数がまだ失敗する | セッションが古いロール状態を保持している | `USE ROLE <your_role>;` で切り替え直すか、Snowsight / セッションを再接続してから再実行する |

次の章では、これまでの全章の内容を 1 つのパイプラインとして確認します。

## 参考リンク

- [Cortex LLM 関数の概要](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions)
- [AI_COMPLETE（COMPLETE 関数）](https://docs.snowflake.com/en/sql-reference/functions/complete-snowflake-cortex)
- [AI_CLASSIFY（CLASSIFY_TEXT 関数）](https://docs.snowflake.com/en/sql-reference/functions/classify-text-snowflake-cortex)
- [AI_EXTRACT（EXTRACT_ANSWER 関数）](https://docs.snowflake.com/en/sql-reference/functions/extract-answer-snowflake-cortex)
- [CORTEX_USER ロールの付与](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions#required-privileges)

## 学習チェックリスト

- [ ] `SNOWFLAKE.CORTEX.COMPLETE()` でテキスト生成ができた
- [ ] `SENTIMENT()` でレビューのセンチメント分析ができた
- [ ] `TRANSLATE()` でテキスト翻訳ができた
- [ ] Cortex 関数がウェアハウス上で実行される仕組みを理解した
