# 付録A5: パフォーマンス詳細（キャッシュ・クラスタリング・Query Profile）

> **SnowPro Core 対策** — Domain 4: Performance Concepts（15%）

---

## この章で学ぶこと

- Snowflake の 3 層キャッシュの仕組みと使い分け
- マイクロパーティションと Pruning の深掘り
- Clustering Key でデータの物理配置を最適化する方法
- Query Profile の読み方（ボトルネック特定）

---

## 概念解説

### 1. Snowflake の 3 層キャッシュ

| キャッシュ | 場所 | 有効期間 | ヒット条件 |
|---|---|---|---|
| **Result Cache** | Cloud Services 層 | **24 時間** | 全く同じ SQL・同じデータ |
| **Local Disk Cache** | Warehouse の SSD | Warehouse 起動中 | 同じテーブルデータを再スキャン |
| **Remote Disk Cache** | S3/GCS（実ストレージ） | 常時 | 実データ（常に存在） |

```
クエリ実行フロー:
  1. Result Cache を確認（同じ SQL が24時間以内に実行されたか？）
     → ヒット: 即座に結果を返す（Warehouse は起動不要）

  2. Local Disk Cache を確認（Warehouse が起きていて同じデータを持っているか？）
     → ヒット: SSD から読み込み（リモートより高速）

  3. Remote Disk Cache（S3/GCS）から読み込む
     → ネットワーク経由でストレージを読む（最も時間がかかる）
```

**Result Cache のヒット条件（全て満たす必要あり）**:
- SQL 文字列が完全に一致（スペース 1 文字の違いでもミス）
- 参照テーブルのデータが変更されていない
- `CURRENT_TIMESTAMP()` など非決定的関数を含まない
- `USE_CACHED_RESULT = TRUE`（デフォルト）

---

### 2. マイクロパーティションと Pruning（復習）

> → 概念の詳細は **付録A1** 参照。

付録A1 で学んだ内容を、パフォーマンス視点で深掘りします。

**Pruning が効く状況**:
```sql
-- ○ Pruning が効く: event_time は時系列でデータに整列しやすい
WHERE event_time >= '2026-01-01' AND event_time < '2026-02-01'

-- ○ Pruning が効く: 特定カテゴリのみを検索
WHERE category = 'Electronics'

-- × Pruning が効きにくい: ランダムに分散した user_id での検索
WHERE user_id = 'user_abc'  -- user_id はパーティション境界と無関係に散在
```

**`average_overlaps` の読み方**:
- `0`: 理想的な状態（パーティション間で値の範囲が全く重複しない）
- 高い値: パーティション間でデータが混在している（Pruning 効率が低い）

---

### 3. Clustering Key

Clustering Key を設定すると、Snowflake がバックグラウンドで自動的にデータを物理的に再整列させます（Automatic Clustering）。

**Clustering Key を設定すべきテーブルの条件**:
- 行数が数億以上の大規模テーブル
- 特定の列（例: 日付・カテゴリ）での WHERE フィルタが多い
- 現状の `average_overlaps` が高い

**設定しなくても良いケース**:
- 小〜中規模テーブル（Pruning の恩恵が少ない）
- ほぼ全件スキャンするクエリが多い
- 毎回異なる列で検索する場合

**コスト注意**: Automatic Clustering はバックグラウンドでクレジットを消費します。

#### Automatic Clustering のコスト要因

| 要因 | 影響 |
|------|------|
| テーブルサイズ | 大きいほどリクラスタリングコストが高い |
| データ変更頻度 | INSERT / UPDATE が多いほどコストが高い |
| Clustering Key 列数 | 列数が多いほど計算コストが高い |

**有効化の判断基準**: テーブルが 1TB 以上かつ `average_overlaps > 1` の場合に有効。

---

### 4. Query Profile の読み方

Snowsight の「クエリ詳細」画面から Query Profile にアクセスできます。

```
Query Profile の主要指標:
  Bytes Scanned    : スキャンしたデータ量（少ないほど良い）
  Partitions Total : パーティション総数
  Partitions Scanned: 実際にスキャンしたパーティション数
     → Partitions Scanned / Partitions Total が低いほど Pruning が効いている

  ノードタイプ:
    TableScan       : テーブルスキャン（Pruning 情報を確認）
    Aggregate       : 集計処理
    Join            : 結合処理（大きければボトルネック候補）
    Sort            : ソート処理
    Filter          : フィルタ処理
```

**ボトルネック特定のヒント**:
- `TableScan` で `Partitions Scanned` が `Partitions Total` に近い → Clustering Key を検討
- `Join` ノードが重い → テーブルの結合順序・結合キーを見直す
- `Spillage` が発生している → Warehouse サイズアップを検討

---

## ハンズオン

**A5_performance_clustering.sql** を開き、上から順に実行してください。

### Step 1: Result Cache を体感する

```sql
-- 1回目（Warehouse でデータをスキャン）
SELECT category, SUM(line_amount) AS sales
FROM MART.FACT_PURCHASE_EVENTS
GROUP BY category;

-- ↑ 同じ SQL をもう一度実行 → Snowsight で "Bytes scanned: 0" を確認
```

### Step 2: Result Cache をバイパスして強制スキャン

```sql
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
-- 同じ SQL を実行 → 今度は実データをスキャン
ALTER SESSION SET USE_CACHED_RESULT = TRUE;
```

### Step 3: Clustering Key を設定する

```sql
-- event_time と category でクラスタリング
CREATE OR REPLACE TABLE MART.FACT_PURCHASE_EVENTS_CLUSTERED
  CLUSTER BY (event_time::DATE, category)
AS SELECT * FROM MART.FACT_PURCHASE_EVENTS;

-- クラスタリング後の状態を確認
SELECT SYSTEM$CLUSTERING_INFORMATION(
  'MART.FACT_PURCHASE_EVENTS_CLUSTERED',
  '(event_time::DATE, category)'
);
```

### Step 4: クラスタリングコストを確認する

```sql
-- 過去30日間のテーブル別クラスタリングコスト
SELECT
  TABLE_CATALOG,
  TABLE_SCHEMA,
  TABLE_NAME,
  SUM(CREDITS_USED)                              AS total_credits,
  SUM(NUM_BYTES_RECLUSTERED) / POWER(1024, 3)   AS gb_reclustered
FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
ORDER BY total_credits DESC;
```

---

## 試験対策ポイント

- **Result Cache**: Cloud Services 層・24 時間有効・Warehouse 不要・同一 SQL のみ
- **Local Disk Cache**: Warehouse の SSD・Warehouse 再起動でクリア
- **Clustering Key**: 大規模テーブル・特定列フィルタが多い場合に有効
- **Automatic Clustering**: バックグラウンドで自動維持・クレジット消費あり
- **average_overlaps**: 0 が理想・高いほど Pruning 効率が低い
- **Query Profile**: `Partitions Scanned / Partitions Total` でPruning効率を確認
- **Spillage**: Warehouse の SSD に収まらず、ストレージに溢れた状態（パフォーマンス低下）
