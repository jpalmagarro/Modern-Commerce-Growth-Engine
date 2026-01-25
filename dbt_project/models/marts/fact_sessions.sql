with sessions as (
    select * from {{ ref('int_sessions_aggregated') }}
)

select 
    session_id,
    user_id,
    session_start_at,
    session_end_at,
    duration_seconds,
    duration_seconds,
    events_count,
    page_views,
    add_to_carts,
    checkouts,
    traffic_source,
    device,
    is_converted

from sessions
