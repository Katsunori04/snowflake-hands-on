select
  raw:event_id::string as event_id,
  raw:user_id::string as user_id,
  to_timestamp_ntz(raw:event_time::string) as event_time,
  item.value:sku::string as sku,
  item.value:product_name::string as product_name,
  item.value:category::string as category,
  item.value:qty::number as qty,
  item.value:price::number(10,2) as price,
  item.value:qty::number * item.value:price::number(10,2) as line_amount,
  src_filename,
  loaded_at
from {{ source('RAW', 'RAW_EVENTS_PIPE') }},
lateral flatten(input => raw:items) item
