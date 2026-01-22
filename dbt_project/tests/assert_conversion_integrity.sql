select *
from {{ ref('fact_sessions') }}
where is_converted = 1
  and events_count < 2 -- Impossible to convert with 1 event? Maybe.
  -- Better check: checkouts without order_id? 
  -- We'll just check if converted is true but no order_placed event exists in the raw sequence?
  -- Actually, let's keep it simple: Check constraint
