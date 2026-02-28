select distinct
  user_id,
  'Unknown' as user_name,
  'Unknown' as prefecture
from {{ ref('stg_events') }}
