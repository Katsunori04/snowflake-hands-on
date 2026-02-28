select distinct
  sku,
  product_name,
  category
from {{ ref('stg_event_items') }}
