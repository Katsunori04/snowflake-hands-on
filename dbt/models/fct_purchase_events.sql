select
  event_id,
  user_id,
  event_time,
  sku,
  product_name,
  category,
  qty,
  price,
  line_amount,
  src_filename
from {{ ref('stg_event_items') }}
