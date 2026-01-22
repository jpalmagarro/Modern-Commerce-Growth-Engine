with source as (
    select * from {{ source('olist', 'customers') }}
),

renamed as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix as zip_code,
        customer_city as city,
        customer_state as state,
        -- Computed
        {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }} as user_key

    from source
)

select 
    * 
from renamed
-- Deduplicate: The Olist dataset has multiple rows per unique_id (if they moved or bought >1 times)
-- We take the most recent one (arbitrarily based on customer_id if no timestamp available in this table)
-- In a real scenario, we would use 'updated_at'
qualify row_number() over (partition by customer_unique_id order by customer_id desc) = 1
