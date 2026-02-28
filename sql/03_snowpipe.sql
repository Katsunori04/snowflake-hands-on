-- What you learn:
-- - internal stage を作る
-- - COPY INTO と Snowpipe を使い分ける
-- - stage -> pipe -> raw table の流れを作る

use warehouse LEARN_WH;
use database LEARN_DB;
use schema RAW;

create or replace file format RAW.JSON_FF
  type = json
  strip_outer_array = false;

create or replace stage RAW.EVENT_STAGE
  file_format = RAW.JSON_FF;

create or replace table RAW.RAW_EVENTS_PIPE (
  raw variant,
  src_filename string,
  loaded_at timestamp_ntz default current_timestamp()
);

-- Snowsight の stage upload で datasets/events_sample.json を @RAW.EVENT_STAGE にアップロードしてください。
-- その後、まずは手動ロードを試します。

copy into RAW.RAW_EVENTS_PIPE(raw, src_filename)
from (
  select
    $1,
    metadata$filename
  from @RAW.EVENT_STAGE
)
file_format = (format_name = RAW.JSON_FF)
on_error = 'CONTINUE';

select
  src_filename,
  raw:event_id::string as event_id,
  raw:user_id::string as user_id,
  loaded_at
from RAW.RAW_EVENTS_PIPE
order by loaded_at;

-- 自動取り込み用 pipe
create or replace pipe RAW.EVENTS_PIPE
  auto_ingest = false
as
copy into RAW.RAW_EVENTS_PIPE(raw, src_filename)
from (
  select
    $1,
    metadata$filename
  from @RAW.EVENT_STAGE
)
file_format = (format_name = RAW.JSON_FF)
on_error = 'CONTINUE';

-- Check.
show stages like 'EVENT_STAGE' in schema RAW;
show pipes like 'EVENTS_PIPE' in schema RAW;
select system$pipe_status('RAW.EVENTS_PIPE');

-- Try this:
-- RAW.RAW_EVENTS ではなく RAW.RAW_EVENTS_PIPE に入った JSON から event_type を確認してください。
