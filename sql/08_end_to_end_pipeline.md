# 第8章: 全体パイプラインの復習

> この章で実行するファイル: `sql/08_end_to_end_pipeline.sql`

## この章で学ぶこと

- 01〜07章で構築したパイプライン全体を俯瞰する
- 各層（RAW / STAGING / MART / AI）の役割を整理する
- 確認クエリを実行して全体がつながっていることを確認する

## 前提条件

以下の章がすべて完了していること:

| 章 | ファイル | 作成するもの |
|---|---|---|
| 第0章 | `00_setup.sql` | LEARN_WH、LEARN_DB、3 スキーマ |
| 第2章 | `02_json_variant.sql` | RAW_EVENTS、STG_EVENTS |
| 第3章 | `03_snowpipe.sql` | EVENT_STAGE、RAW_EVENTS_PIPE、EVENTS_PIPE |
| 第4章 | `04_streams_tasks.sql` | FACT_PURCHASE_EVENTS |
| 第5章 | `05_star_schema.sql` | DIM_USERS、DIM_PRODUCTS、DIM_DATE |
| 第7章 | `07_ai_sql.sql` | STAGING.REVIEWS |

---

## 全体パイプライン図

```
datasets/events_sample.json
        │
        │ Snowsight でアップロード（03章）
        ▼
  @RAW.EVENT_STAGE
        │
        │ COPY INTO / Snowpipe（03章）
        ▼
  RAW.RAW_EVENTS_PIPE      RAW.RAW_EVENTS（02章・練習用）
        │
        │ Stream → Task → MERGE（04章）
        ▼
  MART.FACT_PURCHASE_EVENTS
        │
        ├──→ DIM_USERS / DIM_PRODUCTS / DIM_DATE（05章）
        │
        └──→ STAGING.REVIEWS → AI_COMPLETE / AI_CLASSIFY（07章）
```

---

## 確認クエリ

### Check 1: RAW 層のデータ（← 03章 COPY INTO で取り込んだデータ）

```sql
select
  raw:event_id::string as event_id,
  raw:user_id::string as user_id,
  raw:event_type::string as event_type
from LEARN_DB.RAW.RAW_EVENTS_PIPE
order by event_id;
```

ファイルから取り込んだイベントデータが表示されれば OK です。

---

### Check 2: MART 層の変換結果（← 04章 Task の MERGE で生成したデータ）

```sql
select
  event_id,
  user_id,
  sku,
  category,
  qty,
  line_amount
from LEARN_DB.MART.FACT_PURCHASE_EVENTS
order by event_id, sku;
```

RAW の JSON が展開・変換されて構造化データになっていることを確認します。

---

### Check 3: スタースキーマでの集計（← 05章 DIM_PRODUCTS と JOIN）

```sql
select
  d.category,
  sum(f.line_amount) as sales_amount
from LEARN_DB.MART.FACT_PURCHASE_EVENTS f
join LEARN_DB.MART.DIM_PRODUCTS d on f.sku = d.sku
group by d.category
order by sales_amount desc;
```

カテゴリ別の売上がスタースキーマの JOIN で集計できることを確認します。

---

### Check 4: AI でレビューを要約（← 07章 STAGING.REVIEWS のデータ）

```sql
select
  review_id,
  AI_COMPLETE(
    'claude-3-5-sonnet',
    'Summarize this review in plain Japanese: ' || review_text
  ) as summary_ja
from LEARN_DB.STAGING.REVIEWS
order by review_id;
```

SQL の中で AI が日本語要約を生成することを確認します。

---

## Final Checkpoint: 各層の役割まとめ

| 層 | 代表テーブル | 役割 | 作成した章 |
|---|---|---|---|
| RAW | `RAW_EVENTS_PIPE` | 元データを保持。JSON の VARIANT 型で格納 | 03章 |
| RAW | `RAW_EVENTS_STREAM` | 差分（新着データ）を提供 | 04章 |
| STAGING | `STG_EVENTS`, `STG_EVENT_ITEMS` | JSON を展開・整形した構造化テーブル | 02章 |
| STAGING | `REVIEWS` | AI 関数で処理するテキストデータ | 07章 |
| MART | `FACT_PURCHASE_EVENTS` | 購入イベントのファクト（Task が定期変換） | 04章 |
| MART | `DIM_*` | 属性情報のディメンション | 05章 |

---

## パイプラインを延長するヒント

この基本パイプラインをベースに、以下のような拡張が考えられます:

- **dbt を使う**: SQL テンプレート・テスト・ドキュメント化を自動化（→ 09章）
- **Airflow を使う**: 外部システムとの連携・より複雑なワークフロー管理（→ 10章）
- **External Stage を使う**: S3/GCS のファイルを自動取り込み（auto_ingest=true）
- **AI 関数を拡張する**: `AI_CLASSIFY` で商品カテゴリの自動分類などに活用

---

## まとめ

このハンズオンで構築した内容:

1. **環境準備**（00章）: Warehouse / Database / Schema の 3 層構造
2. **データモデリング**（01章）: 正規化・スタースキーマの概念
3. **JSON 処理**（02章）: VARIANT 型・LATERAL FLATTEN
4. **ファイル取り込み**（03章）: Stage・COPY INTO・Snowpipe
5. **差分バッチ**（04章）: Stream・MERGE・Task
6. **スタースキーマ**（05章）: DIM テーブルの作成と集計
7. **コスト最適化**（06章）: Warehouse 設定・STAGING テーブルの活用
8. **AI 関数**（07章）: Cortex の AI_COMPLETE・AI_CLASSIFY・AI_EXTRACT

これらを組み合わせることで、JSON のイベントデータからスタースキーマの分析基盤まで、Snowflake 内で完結するパイプラインを構築できます。
