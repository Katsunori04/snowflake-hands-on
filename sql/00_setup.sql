-- What you learn:
-- - 学習用の warehouse / database / schema を一度で用意する
-- - RAW / STAGING / MART の役割を固定する

-- Run this first.

-- XSMALL: 最小サイズのウェアハウス。学習用途には十分で料金も最低限。
--   サイズは XSMALL → SMALL → MEDIUM → LARGE → ... と大きくなるにつれ処理速度が上がるが料金も上がる。
-- auto_suspend = 60: クエリが来なくなってから60秒後に自動停止。アイドル時のコストを削減する。
-- auto_resume = true: クエリが来ると自動で再起動するので手動操作が不要。
-- initially_suspended = true: 作成直後は停止状態で作成。すぐに課金が始まらない。
create or replace warehouse LEARN_WH
  warehouse_size = 'XSMALL'
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true;

-- この教材専用のデータベースを作成する。
create or replace database LEARN_DB;

-- 3層アーキテクチャでスキーマを分ける。
-- RAW     : 元データをなるべく加工せず保持する層
-- STAGING : 型変換・粒度調整など「整形」を行う層
-- MART    : 集計・BI 向けに最適化した最終層
create or replace schema LEARN_DB.RAW;
create or replace schema LEARN_DB.STAGING;
create or replace schema LEARN_DB.MART;

use warehouse LEARN_WH;
use database LEARN_DB;
use schema RAW;

-- Check.
show warehouses like 'LEARN_WH';
show schemas in database LEARN_DB;

-- Try this:
-- auto_suspend の秒数を 300 に変えた場合に何が変わるか説明してみてください。
