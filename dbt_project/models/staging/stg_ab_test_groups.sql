with source as (
    select * from {{ source('olist', 'ab_test_groups') }}
),

renamed as (
    select
        customer_unique_id,
        ab_group
    from source
)

select * from renamed
