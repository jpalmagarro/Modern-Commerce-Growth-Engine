with events as (
    select * from {{ ref('stg_web_events') }}
),

-- 1. Identify "New Session" flags based on 30-min inactivity window
time_lags as (
    select
        *,
        -- Previous event timestamp for the same user
        lag(event_at) over (partition by customer_unique_id order by event_at) as previous_event_at,
        
        -- Time difference in minutes
        date_diff('minute', 
            lag(event_at) over (partition by customer_unique_id order by event_at), 
            event_at
        ) as time_diff_minutes
    from events
),

session_flags as (
    select
        *,
        case
            when previous_event_at is null then 1 -- First event ever
            when time_diff_minutes > 30 then 1    -- New session after inactivity
            else 0
        end as is_new_session
    from time_lags
),

-- 2. Generate Unique Session IDs
session_ids as (
    select
        *,
        -- Cumulative sum of flags generates a sequential ID per user
        sum(is_new_session) over (partition by customer_unique_id order by event_at) as user_session_index
    from session_flags
),

-- 3. Create a globally unique session_id
final_ids as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['customer_unique_id', 'user_session_index']) }} as session_id
    from session_ids
),

-- 4. Aggregate to Session Level
aggregated as (
    select
        session_id,
        customer_unique_id as user_id,
        
        min(event_at) as session_start_at,
        max(event_at) as session_end_at,
        
        -- Event Counts
        count(*) as events_count,
        sum(case when event_type = 'page_view' then 1 else 0 end) as page_views,
        sum(case when event_type = 'add_to_cart' then 1 else 0 end) as add_to_carts,
        sum(case when event_type = 'checkout_start' then 1 else 0 end) as checkouts,
        sum(case when event_type = 'order_placed' then 1 else 0 end) as orders,
        
        -- Flags
        max(case when event_type = 'add_to_cart' then 1 else 0 end) as has_cart,
        max(case when event_type = 'checkout_start' then 1 else 0 end) as has_checkout,
        max(case when event_type = 'order_placed' then 1 else 0 end) as has_purchase,
        
        -- Attribution (First Touch per session)
        -- We take the source of the first event in the session
        first_value(traffic_source) over (partition by session_id order by event_at) as session_source,
        first_value(device) over (partition by session_id order by event_at) as device
        
    from final_ids
    group by 1, 2, traffic_source, device, event_at -- Need careful grouping or use distinct with window functions
    -- Actually, simple group by is better if we extract first_value logic differently or rely on 'min' logic? 
    -- Let's stick to standard aggregation. Source/Device usually constant per session in this synthetic data.
    -- But 'first_value' requires window. Let's start simpler for aggregation.
),

-- Refined Aggregation (to handle source/device correctly without exploding rows)
-- We'll pick the source/device of the *start* of the session
session_attributes as (
    select 
        session_id,
        traffic_source, 
        device
    from (
        select 
            session_id, 
            traffic_source, 
            device,
            row_number() over (partition by session_id order by event_at asc) as rn
        from final_ids
    ) where rn = 1
),

final_aggregation as (
    select
        f.session_id,
        f.customer_unique_id as user_id,
        min(f.event_at) as session_start_at,
        max(f.event_at) as session_end_at,
        date_diff('second', min(f.event_at), max(f.event_at)) as duration_seconds,
        
        count(*) as events_count,
        sum(case when f.event_type = 'page_view' then 1 else 0 end) as page_views,
        sum(case when f.event_type = 'add_to_cart' then 1 else 0 end) as add_to_carts,
        sum(case when f.event_type = 'checkout_start' then 1 else 0 end) as checkouts,
        sum(case when f.event_type = 'order_placed' then 1 else 0 end) as orders,
        
        max(case when f.event_type = 'order_placed' then 1 else 0 end) as is_converted
        
    from final_ids f
    group by 1, 2
)

select
    a.*,
    attr.traffic_source,
    attr.device
from final_aggregation a
left join session_attributes attr on a.session_id = attr.session_id
