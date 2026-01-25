with customers as (
    select * from {{ ref('stg_olist_customers') }}
),

orders as (
    select * from {{ ref('stg_olist_orders') }}
),

customer_orders as (
    select
        c.customer_unique_id,
        count(o.order_id) as total_orders,
        min(o.purchase_at) as first_order_at,
        max(o.purchase_at) as last_order_at,
        -- Monetary (Assuming we would join with items/payments here, keeping simple for now)
        -- We don't have price in stg_orders, so we count frequency/recency mainly
        date_diff('day', max(o.purchase_at), current_date) as recency_days
        
    from customers c
    join orders o on c.customer_id = o.customer_id
    where o.status = 'delivered'
    group by 1
),

final as (
    select
        c.*,
        co.total_orders,
        co.first_order_at,
        co.last_order_at,
        co.recency_days,
        
        -- RFM Segments (Simplified)
        case
            when recency_days <= 90 and total_orders >= 3 then 'VIP'
            when recency_days <= 180 then 'Active'
            else 'Churned'
        end as customer_segment
        
    from customers c
    left join customer_orders co on c.customer_unique_id = co.customer_unique_id
),

ab_groups as (
    select * from {{ ref('stg_ab_test_groups') }}
),

final_with_ab as (
    select
        f.*,
        coalesce(ab.ab_group, 'control') as ab_group -- Default to control if missing (though we fixed generation)
    from final f
    left join ab_groups ab on f.customer_unique_id = ab.customer_unique_id
)

select * from final_with_ab
