select *
from {{ ref('stg_marketing_spend') }}
where cost < 0
