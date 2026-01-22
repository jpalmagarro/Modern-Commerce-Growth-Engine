with source as (
    select * from {{ source('olist', 'marketing_spend') }}
),

renamed as (
    select
        cast(date as date) as spend_date,
        source as channel,
        cast(cost as decimal(10,2)) as cost,
        cast(sessions as int) as reported_sessions

    from source
)

select * from renamed
