# 付録A4: Secure Data Sharing

> **SnowPro Core 対策** — Domain 1（25%）・Domain 6: Data Protection & Recovery（10%）

---

## この章で学ぶこと

- Secure Data Sharing の仕組みと Provider / Consumer モデル
- Share オブジェクトへのオブジェクト追加方法
- Secure View を使って個人情報を除外して共有する方法
- Snowflake Data Marketplace の概要

---

## 概念解説

### 1. Secure Data Sharing とは

Secure Data Sharing は、データを**物理的にコピーせず**、別の Snowflake アカウントにリアルタイムでデータを共有する機能です。

```
Provider アカウント          Consumer アカウント
┌─────────────────┐          ┌─────────────────┐
│ LEARN_DB        │  Share   │ Shared DB       │
│  └─ MART        │ ──────→  │  └─ MART        │
│     └─ TABLE_A  │（メタデータ）  └─ TABLE_A  │
│                 │          │                 │
│ ストレージ       │ ←───── 実データを参照 ─────┘
└─────────────────┘
```

**ポイント**:
- データは **Provider のストレージに留まる**（ゼロコピー）
- Consumer はリアルタイムでデータを参照できる
- 課金は **Consumer 側の Warehouse** が負担（Consumer の Compute を使う）
- **同じクラウドリージョン内のアカウント間**が基本（クロスリージョンは別機能）

---

### 2. 共有できるオブジェクト

| オブジェクト | 共有可否 | 備考 |
|---|---|---|
| テーブル | ✓ | 通常テーブル |
| 外部テーブル | ✓ | S3 等の外部データ |
| **Secure View** | ✓ | 通常ビューは共有不可・Secure View のみ |
| **Secure UDF** | ✓ | 通常 UDF は共有不可 |
| Dynamic Table | ✓ | |
| ステージ | ✗ | 共有不可 |
| タスク・Stream | ✗ | 共有不可 |

> **試験頻出**: 通常の VIEW は共有できない。共有するには **SECURE VIEW** を使う。
> Secure View は内部の SQL が Consumer から見えない（実装を隠蔽できる）。

---

### Secure View の詳細

#### なぜ Secure View が必要か

通常の VIEW は Consumer が `GET_DDL()` で内部 SQL を参照できる。
Secure View は DDL が隠蔽されるため、実装ロジック（フィルタ条件・計算式など）を守れる。

```sql
-- 通常の VIEW（DDL が見える）
CREATE OR REPLACE VIEW orders_view AS
SELECT * FROM orders WHERE region = 'JP';

-- Secure VIEW（DDL が隠蔽される）
CREATE OR REPLACE SECURE VIEW orders_secure_view AS
SELECT * FROM orders WHERE region = 'JP';
```

#### 性能コスト

Secure View は Snowflake の INLINE 最適化（プッシュダウン）が無効になる。
→ 不要な場所で使わず、「データ共有（Marketplace / Direct Share）」が必要な場合のみ使用する。

#### 使い分けガイド

| 状況 | 推奨 |
|------|------|
| データ共有（Marketplace・Direct Share）| Secure View 必須 |
| 社内の権限制御のみ | 通常 VIEW + Column-Level Security |
| パフォーマンス重視 | 通常 VIEW |

---

### 3. Provider / Consumer モデル

```
Provider 側の操作:
  1. SHARE オブジェクトを作成
  2. 共有したいオブジェクトを SHARE に追加（GRANT）
  3. Consumer アカウントを SHARE に追加

Consumer 側の操作:
  1. SHARE からデータベースを作成（CREATE DATABASE FROM SHARE）
  2. 通常の SELECT でデータを参照
```

---

### 4. Snowflake Data Marketplace

Snowflake が運営するデータ流通プラットフォームです。

- **無料データ**: 公的機関のデータ（気象、国勢調査等）
- **有料データ**: 金融データ、マーケティングデータ等
- **プロバイダー**: データ会社が自社データを販売・無料提供
- 取得したデータは自分のアカウントに Live Share として接続される（コピー不要）

---

## ハンズオン

**A4_data_sharing.sql** を開き、上から順に実行してください。

> **注意**: SHARE オブジェクトの作成と Consumer アカウントの追加には **ACCOUNTADMIN** 権限が必要です。
> 同一アカウント内での動作確認（Secure View の作成まで）は SYSADMIN で可能です。

### Step 1: SHARE オブジェクトを作成する

```sql
USE ROLE ACCOUNTADMIN;
CREATE SHARE LEARN_DB_SHARE;
SHOW SHARES;
```

### Step 2: 共有するオブジェクトを追加する

```sql
GRANT USAGE ON DATABASE LEARN_DB TO SHARE LEARN_DB_SHARE;
GRANT USAGE ON SCHEMA LEARN_DB.MART TO SHARE LEARN_DB_SHARE;
GRANT SELECT ON TABLE LEARN_DB.MART.FACT_PURCHASE_EVENTS TO SHARE LEARN_DB_SHARE;
SHOW GRANTS TO SHARE LEARN_DB_SHARE;
```

### Step 3: Secure View を作成して共有する

個人情報（user_id）を除外した Secure View を作成します。

```sql
CREATE OR REPLACE SECURE VIEW MART.SECURE_FACT_PURCHASE AS
SELECT event_id, event_time, sku, category, qty, line_amount
FROM MART.FACT_PURCHASE_EVENTS;
-- ↑ user_id を除外してプライバシーを保護

GRANT SELECT ON VIEW MART.SECURE_FACT_PURCHASE TO SHARE LEARN_DB_SHARE;
```

### Step 4: Consumer 側の操作（概念確認）

実際に別アカウントがある場合の操作例です。

```sql
-- Provider 側: Consumer アカウントを追加
-- ALTER SHARE LEARN_DB_SHARE ADD ACCOUNTS = <consumer_account_identifier>;

-- Consumer 側（別アカウントで実行）:
-- CREATE DATABASE SHARED_LEARN_DB FROM SHARE <provider_account>.LEARN_DB_SHARE;
-- SELECT * FROM SHARED_LEARN_DB.MART.SECURE_FACT_PURCHASE;
```

---

## 試験対策ポイント

- **データはコピーされない**: Provider のストレージを Consumer がリアルタイム参照
- **課金は Consumer 側**: Consumer の Warehouse クレジットを消費
- **Secure View が必要**: 通常の VIEW は共有不可
- **ACCOUNTADMIN が必要**: SHARE 作成・Consumer 追加操作
- **同一クラウドリージョンが基本**: クロスリージョン共有は Data Clean Room 等の別機能
- **Snowflake Data Marketplace**: Live Share として接続（データコピー不要）
- **Reader Account**: Consumer が Snowflake アカウントを持っていない場合でも、Provider が Reader Account を作成して共有可能
