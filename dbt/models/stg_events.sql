select
  raw:event_id::string as event_id,
  raw:user_id::string as user_id,
  raw:event_type::string as event_type,
  raw:device.os::string as device_os,
  raw:device.app_version::string as app_version,
  raw:review_text::string as review_text,
  to_timestamp_ntz(raw:event_time::string) as event_time,
  src_filename,
  loaded_at
from {{ source('RAW', 'RAW_EVENTS_PIPE') }}
