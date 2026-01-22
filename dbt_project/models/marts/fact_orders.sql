with orders as (
    select * from {{ ref('stg_olist_orders') }}
    where status = 'delivered' -- Filter early for efficiency
),

customers as (
    select * from {{ ref('stg_olist_customers') }}
),

sessions as (
    select * from {{ ref('fact_sessions') }}
),

-- Join Orders to Customers to get Unique ID
orders_enriched as (
    select 
        o.order_id,
        o.customer_id,
        c.customer_unique_id,
        o.purchase_at
    from orders o
    join customers c on o.customer_id = c.customer_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.customer_unique_id,
        s.session_id,
        
        -- Dimensions
        coalesce(s.traffic_source, 'Direct/Unknown') as traffic_source,
        coalesce(s.device, 'Unknown') as device,
        
        -- Measures
        o.purchase_at,
        1 as is_order
        
    from orders_enriched o
    left join sessions s 
        on o.customer_unique_id = s.user_id
        and o.purchase_at between s.session_start_at and s.session_end_at
    -- Deduplicate: If an order matches multiple sessions (rare overlaps), take the one starting latest
    -- This enforces 1:1 relationship for the fact table
    qualify row_number() over (partition by o.order_id order by s.session_start_at desc) = 1
)

select * from final
