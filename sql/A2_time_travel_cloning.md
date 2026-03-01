# 付録A2: Time Travel / Fail-safe / Zero-Copy Cloning

> **SnowPro Core 対策** — Domain 1（25%）・Domain 6: Data Protection & Recovery（10%）

---

## この章で学ぶこと

- Time Travel で過去のデータを SQL から参照・復元する方法
- Fail-safe（フェイルセーフ）と Time Travel の違い
- Zero-Copy Cloning でストレージコストゼロの複製を作る方法

---

## 概念解説

### 1. Time Travel

Time Travel は、テーブル・スキーマ・データベースの「過去の状態」を SQL で参照できる機能です。

```
現在
│  ← Time Travel 期間（Standard: 最大1日 / Enterprise以上: 最大90日）
│
過去データをSQLで参照・復元可能
│
│  ← Fail-safe 期間（さらに7日間 ※Snowflakeが内部保持）
│
それ以前のデータは完全削除（復元不可）
```

#### 3 つの指定方法

| 方法 | 構文例 | 用途 |
|---|---|---|
| OFFSET | `AT (OFFSET => -600)` | 「N 秒前」を相対指定 |
| TIMESTAMP | `AT (TIMESTAMP => '2026-01-01 10:00:00'::TIMESTAMP_NTZ)` | 特定時刻を絶対指定 |
| STATEMENT | `BEFORE (STATEMENT => '<query_id>')` | 特定クエリ実行直前の状態 |

> **注意**: `AT` はその時刻を含む状態、`BEFORE` はその直前の状態を返します。

---

### 2. Fail-safe

Fail-safe は Time Travel 期間が終了した後、さらに **7 日間**（Temporary / Transient テーブルは 0 日）Snowflake 内部でデータを保持する仕組みです。

**Fail-safe の特徴**:
- ユーザーが直接 SQL で操作することはできない
- Snowflake サポートへの依頼経由でのみ復元可能
- 目的: 壊滅的な障害発生時の最後の砦

| テーブル種別 | Time Travel 上限 | Fail-safe |
|---|---|---|
| Permanent（通常） | Standard: 1日 / Enterprise: 90日 | 7日 |
| Transient | 0〜1日 | 0日（なし） |
| Temporary | 0〜1日 | 0日（なし） |

> **試験頻出**: Transient / Temporary テーブルには **Fail-safe がない**。コスト削減のために使うが、データ保護は弱くなる。

---

### 3. Zero-Copy Cloning

Zero-Copy Cloning は、データの実体をコピーせずメタデータだけを複製する機能です。

```
元テーブル              クローンテーブル
┌──────────┐            ┌──────────┐
│ データA   │ ← 共有 → │ データA   │  ※同じストレージを参照
│ データB   │ ← 共有 → │ データB   │
└──────────┘            └──────────┘
     ↑                       ↑
  変更後は独立コピーが発生（Copy-on-Write）
```

**特徴**:
- クローン直後のストレージコストは **ほぼ 0**
- クローン後に片方でデータを変更すると、変更分だけ新しいストレージが発生
- テーブル / スキーマ / データベース 単位でクローン可能
- Time Travel の過去時点をクローンすることも可能
- クローン直後のストレージ追加コスト = 0（メタデータのみコピー）
- ただし Clone 後に変更が発生した時点から、その変更分が課金される（「Clone = 永久無料」ではない）

**主な用途**:
- 本番 DB を開発環境にゼロコストで複製
- 破壊的な変換前のバックアップ
- テスト用データセットの準備

---

## ハンズオン

**A2_time_travel_cloning.sql** を開き、上から順に実行してください。

### Step 1: OFFSET で過去データを参照する

```sql
-- 10分前（600秒前）の FACT_PURCHASE_EVENTS を参照
SELECT * FROM MART.FACT_PURCHASE_EVENTS
  AT (OFFSET => -60 * 10)
LIMIT 5;
```

### Step 2: TIMESTAMP で特定時刻のデータを参照する

```sql
SELECT * FROM MART.FACT_PURCHASE_EVENTS
  AT (TIMESTAMP => '2026-02-28 00:00:00'::TIMESTAMP_NTZ)
LIMIT 5;
```

### Step 3: テーブルを誤削除して UNDROP で復元する

```sql
-- テーブルを削除
DROP TABLE MART.FACT_PURCHASE_EVENTS_BACKUP;

-- Time Travel 期間内なら UNDROP で復元できる
UNDROP TABLE MART.FACT_PURCHASE_EVENTS_BACKUP;
```

### Step 4: Zero-Copy Cloning でテーブルを複製する

```sql
-- テーブルをゼロコピー複製
CREATE TABLE MART.FACT_PURCHASE_EVENTS_CLONE
  CLONE MART.FACT_PURCHASE_EVENTS;

-- データベース丸ごとクローン（開発環境の複製に有効）
CREATE DATABASE LEARN_DB_CLONE
  CLONE LEARN_DB;
```

### Step 5: Time Travel 期間を確認・変更する

```sql
-- 現在の Time Travel 設定を確認
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS'
  IN TABLE MART.FACT_PURCHASE_EVENTS;

-- Time Travel 期間を7日に変更（Enterprise以上）
ALTER TABLE MART.FACT_PURCHASE_EVENTS
  SET DATA_RETENTION_TIME_IN_DAYS = 7;
```

> **ストレージコスト試算（参考）**
> - 1 TB テーブル、毎日 10% 変更、90日保持 → 最大 9 TB 相当の履歴が発生
> - 推奨: ステージング環境 = 1日 / 本番 = 7〜14日（コストと保護のバランス）

---

## 試験対策ポイント

- **Time Travel 上限**: Standard = **1 日** / Enterprise 以上 = **最大 90 日**
- **Fail-safe**: 7 日間・ユーザー操作不可・Transient/Temporary は **0 日**
- **UNDROP**: DROP したテーブル/スキーマ/DBを Time Travel 期間内に復元
- **Zero-Copy Cloning**: ストレージコスト 0 で即時複製・Copy-on-Write
- **過去時点クローン**: `CREATE TABLE ... CLONE ... AT (OFFSET => -3600)` のように指定可能
- **Transient テーブルの使い所**: 中間テーブル（ETL の一時領域）でコスト削減
