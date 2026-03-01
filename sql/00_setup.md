# 第0章: 環境準備

> この章で実行するファイル: `sql/00_setup.sql`

## この章で学ぶこと

- 学習用の Warehouse / Database / Schema を一度で用意する
- RAW / STAGING / MART の役割と 3 層アーキテクチャを理解する
- Warehouse の主要オプション（auto_suspend・auto_resume・initially_suspended）の意味を理解する

## 前提条件

- Snowflake アカウントを持っていること
- Snowsight（Web UI）にログインできること
- SYSADMIN または ACCOUNTADMIN ロールで操作できること

---

## 概念解説

### 3 層アーキテクチャ

Snowflake のデータ基盤では、データをその加工度合いに応じて 3 つのスキーマ（層）に分けて管理します。

```
┌──────────────────────────────────────────────────┐
│                   LEARN_DB                       │
│                                                  │
│  ┌─────────┐   ┌──────────┐   ┌────────────┐    │
│  │   RAW   │ → │ STAGING  │ → │    MART    │    │
│  └─────────┘   └──────────┘   └────────────┘    │
│  元データを      型変換・整形     集計・BI向け      │
│  そのまま保持    を行う層         最終形            │
└──────────────────────────────────────────────────┘
```

| 層 | 役割 | 特徴 |
|---|---|---|
| **RAW** | 元データを加工せず保持 | JSON の VARIANT 型で格納することが多い |
| **STAGING** | 型変換・粒度調整などの整形 | 正規化・FLATTEN 済みの構造化データ |
| **MART** | 集計・BI 向けに最適化 | スタースキーマ・集計済みテーブル |

この設計の利点は、**元データを RAW に保持しておくことで、整形ロジックを後から変更・再実行できる**ことです。

---

### Warehouse とは

Snowflake の「Warehouse」は SQL を実行するコンピュータリソース（クラスター）です。サイズに応じてクレジット消費量が変わります。

```
XSMALL < SMALL < MEDIUM < LARGE < X-LARGE < ...
 1 クレジット/時  2  4  8  16 ...
```

学習用途には **XSMALL** で十分です。

#### 主要オプションの意味

| オプション | 設定値 | 意味 |
|---|---|---|
| `warehouse_size` | `'XSMALL'` | 最小サイズ。学習・小規模なら十分 |
| `auto_suspend` | `60`（秒） | クエリが来なくなってから 60 秒後に自動停止。アイドル時のコストを削減 |
| `auto_resume` | `true` | クエリが来ると自動で再起動。手動操作が不要 |
| `initially_suspended` | `true` | 作成直後は停止状態。すぐに課金が始まらない |

---

## ハンズオン手順

### Step 1: Warehouse を作成する

```sql
create or replace warehouse LEARN_WH
  warehouse_size = 'XSMALL'
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true;
```

`initially_suspended = true` にしておくと、作成直後はウェアハウスが停止状態のため、クエリを実行するまで課金されません。

---

### Step 2: Database を作成する

```sql
create or replace database LEARN_DB;
```

このハンズオン専用のデータベースを作成します。

---

### Step 3: 3 層のスキーマを作成する

```sql
create or replace schema LEARN_DB.RAW;
create or replace schema LEARN_DB.STAGING;
create or replace schema LEARN_DB.MART;
```

---

### Step 4: 使用するコンテキストを設定する

```sql
use warehouse LEARN_WH;
use database LEARN_DB;
use schema RAW;
```

`use` 文で現在のセッションのデフォルトを設定すると、以降のクエリでデータベース名やスキーマ名を省略できます。

---

## 確認クエリ

```sql
-- Warehouse が作成されているか確認
show warehouses like 'LEARN_WH';

-- スキーマが 3 つ作成されているか確認
show schemas in database LEARN_DB;
```

`show schemas` の結果に `RAW`、`STAGING`、`MART` の 3 行が表示されれば成功です。

---

## Try This

**auto_suspend を 300 秒に変えた場合、何が変わるか説明してみてください。**

<details>
<summary>解説</summary>

`auto_suspend = 300` にすると、クエリが来なくなってから **5 分間** ウェアハウスが起動したままになります。

- **メリット**: 短時間に複数のクエリを実行する場合、ウェアハウスの起動・停止の往復コストがかからない（ウォームアップ待ちがなくなる）
- **デメリット**: アイドル状態でも 5 分間クレジットを消費し続ける

学習用途では短い方が無駄な課金を防げます。本番環境では、バッチ処理が連続して走るなら長め、インタラクティブな用途なら短めに設定するのが基本です。

</details>

---

## まとめ

| 作成したリソース | 用途 |
|---|---|
| `LEARN_WH` | SQL 実行エンジン（XSMALL、60 秒で自動停止） |
| `LEARN_DB` | このハンズオン専用のデータベース |
| `LEARN_DB.RAW` | 元データ保持層 |
| `LEARN_DB.STAGING` | 整形層 |
| `LEARN_DB.MART` | 集計・分析層 |

## よくあるエラーと対処法

| エラー | 原因 | 対処法 |
|---|---|---|
| `Object 'LEARN_DB' already exists` | 既に同名の DB / Schema / Warehouse がある | 学習環境を作り直すなら `CREATE OR REPLACE` を使うか、不要なオブジェクトを `DROP` してから再実行する |
| `Insufficient privileges` | `CREATE WAREHOUSE` や `CREATE DATABASE` を実行できるロールではない | 先頭で `USE ROLE SYSADMIN;` を実行し、必要なら管理者に権限付与を依頼する |

次の章では、この 3 層アーキテクチャを使ってデータモデリングの基本を学びます。

## 学習チェックリスト

- [ ] Snowflake トライアルアカウントを作成できた
- [ ] Web UI（Snowsight）にログインできた
- [ ] ウェアハウス・データベース・スキーマを作成できた
- [ ] 基本的な SQL（SELECT）をワークシートで実行できた
- [ ] ロールを切り替えてアクセス制御を確認できた
