with source as (
    select * from {{ source('olist', 'web_events') }}
),

renamed as (
    select
        event_id,
        user_id as customer_unique_id, -- Align naming with Olist
        session_id,
        event_type,
        page_url,
        source as traffic_source,
        device,
        cast(timestamp as timestamp) as event_at

    from source
)

select * from renamed
