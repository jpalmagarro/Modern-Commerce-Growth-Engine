with sessions as (
    select 
        cast(session_start_at as date) as date,
        traffic_source,
        count(*) as sessions,
        sum(is_converted) as conversions
        -- We would sum revenue here if we joined with items
    from {{ ref('fact_sessions') }}
    group by 1, 2
),

spend as (
    select * from {{ ref('stg_marketing_spend') }}
),

joined as (
    select
        coalesce(s.date, sp.spend_date) as date,
        coalesce(s.traffic_source, sp.channel) as channel,
        
        coalesce(sp.cost, 0) as cost,
        coalesce(sp.reported_sessions, 0) as reported_sessions,
        coalesce(s.sessions, 0) as attribution_sessions,
        coalesce(s.conversions, 0) as attribution_conversions,
        
        -- Calculated Metrics
        case when coalesce(s.conversions,0) > 0 then 
             coalesce(sp.cost, 0) / s.conversions 
             else 0 
        end as calculated_cac -- Cost / Conversions
        
    from sessions s
    full outer join spend sp 
        on s.date = sp.spend_date 
        and s.traffic_source = sp.channel
)

select * from joined
