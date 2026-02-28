# 付録A1: Snowflake アーキテクチャ詳細

> **SnowPro Core 対策** — Domain 1: Snowflake Data Cloud Features & Architecture（25%）

---

## この章で学ぶこと

- 3 層アーキテクチャ（Storage / Compute / Cloud Services）の役割と責務
- Snowflake エディションの違いと選択基準
- マイクロパーティションの仕組みと Pruning による高速化
- Virtual Warehouse のサイズ・Multi-cluster の動作確認

---

## 概念解説

### 1. 3 層アーキテクチャ

Snowflake は「共有ディスク（Shared Disk）」と「シェアードナッシング（Shared Nothing）」の長所を組み合わせた独自アーキテクチャを持ちます。

```
┌───────────────────────────────────────────────────┐
│  Cloud Services Layer（クラウドサービス層）          │
│  ・クエリ最適化（パーサー・プランナー・オプティマイザー）  │
│  ・メタデータ管理（テーブル・スキーマ・統計情報）        │
│  ・認証・アクセス制御                               │
│  ・トランザクション管理                              │
└───────────────────────────────────────────────────┘
          ↕ クエリ計画・メタデータ参照
┌───────────────────────────────────────────────────┐
│  Compute Layer（仮想ウェアハウス層）                 │
│  ・Virtual Warehouse（MPP クラスタ）                │
│  ・クエリを並列で処理し、結果を返す                   │
│  ・独立してスケール可能（XS / S / M / L / XL …）    │
│  ・ローカルディスクキャッシュ（SSD）を保有              │
└───────────────────────────────────────────────────┘
          ↕ データ読み書き
┌───────────────────────────────────────────────────┐
│  Storage Layer（ストレージ層）                      │
│  ・S3（AWS）/ GCS（GCP）/ Azure Blob に保存         │
│  ・マイクロパーティション形式（列ストア・圧縮済み）       │
│  ・Compute と独立して課金・スケール                  │
└───────────────────────────────────────────────────┘
```

**ポイント**: Storage と Compute が分離しているため
- 複数の Warehouse が同じデータを同時に読める（読み取り競合なし）
- Warehouse を停止してもデータは消えない
- 必要なときだけ Warehouse を立ち上げてコストを抑えられる

---

### 2. エディション比較

| 機能 | Standard | Enterprise | Business Critical | VPS |
|---|---|---|---|---|
| Time Travel 最大期間 | **1 日** | **90 日** | 90 日 | 90 日 |
| Multi-cluster Warehouse | ✗ | ✓ | ✓ | ✓ |
| Dynamic Data Masking | ✗ | ✓ | ✓ | ✓ |
| Row Access Policy | ✗ | ✓ | ✓ | ✓ |
| データ暗号化（Tri-Secret Secure） | ✗ | ✗ | ✓ | ✓ |
| HIPAA / PCI DSS 準拠 | ✗ | ✗ | ✓ | ✓ |
| 専用メタデータストア | ✗ | ✗ | ✗ | ✓ |

> **試験頻出**: Time Travel が最大 **90 日**になるのは **Enterprise 以上**。Standard は **1 日**が上限。

---

### 3. マイクロパーティションと Pruning

#### マイクロパーティションとは

Snowflake はデータを **16〜512 MB** の「マイクロパーティション」という単位に自動分割して保存します。

```
テーブル全体
├── partition_0001.parquet（列A: 1-100, 列B: 2024-01-01〜2024-01-10）
├── partition_0002.parquet（列A: 101-200, 列B: 2024-01-11〜2024-01-20）
├── partition_0003.parquet（列A: 201-300, 列B: 2024-01-21〜2024-01-31）
└── ...
```

各パーティションには「どの列にどの値の範囲が入っているか」というメタデータが Cloud Services 層に保存されています。

#### Pruning（プルーニング）の仕組み

```sql
-- WHERE 条件: event_time >= '2024-01-21'
-- → Cloud Services がメタデータを確認
--   partition_0001（〜01-10）→ スキップ ✗
--   partition_0002（〜01-20）→ スキップ ✗
--   partition_0003（01-21〜）→ スキャン ✓
-- → 実際に読むのは partition_0003 だけ！
```

**Pruning の効果を上げる条件**:
- WHERE 句の列でデータが時系列や範囲で整列している
- Clustering Key を設定することで物理的な整列を強制できる（付録A5 参照）

---

### 4. Virtual Warehouse のサイズと Multi-cluster

| サイズ | 目安のノード数 | クレジット消費/時間 |
|---|---|---|
| XS | 1 | 1 |
| S | 2 | 2 |
| M | 4 | 4 |
| L | 8 | 8 |
| XL | 16 | 16 |
| 2XL | 32 | 32 |

**Multi-cluster Warehouse**（Enterprise 以上）:
- 同時接続ユーザーが多い場合に自動でクラスタを追加（スケールアウト）
- 最小クラスタ数と最大クラスタ数を設定
- 高負荷が解消されると自動でクラスタを削減（スケールイン）

---

## ハンズオン

**A1_architecture.sql** を開き、上から順に実行してください。

### Step 1: マイクロパーティション情報を確認する

```sql
SELECT SYSTEM$CLUSTERING_INFORMATION('MART.FACT_PURCHASE_EVENTS');
```

返ってくる JSON の `average_overlaps`（重複度）が低いほど Pruning が効きやすい状態です。

### Step 2: テーブルのストレージ情報を確認する

```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.TABLE_STORAGE_METRICS(
  DATABASE_NAME => 'LEARN_DB'
));
```

### Step 3: Warehouse のメタデータを確認する

```sql
SHOW WAREHOUSES;
```

`SIZE`、`STATE`（STARTED / SUSPENDED）、`RUNNING` / `QUEUED` のクエリ数を確認できます。

### Step 4: Cloud Services 層のコストを確認する

```sql
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'LEARN_WH'
ORDER BY START_TIME DESC
LIMIT 10;
```

---

## 試験対策ポイント

- **Storage と Compute の分離**: 複数 Warehouse が同じデータを同時参照可能
- **Cloud Services 層**: クエリ最適化・メタデータ・認証を担当（Warehouse を使わない）
- **マイクロパーティション**: 自動分割・列ストア・Pruning でスキャン量を削減
- **Time Travel 90 日**: Enterprise 以上が必要
- **Multi-cluster Warehouse**: Enterprise 以上で同時接続急増に自動対応
- **Tri-Secret Secure**: Business Critical 以上（顧客管理の暗号鍵を使う）
