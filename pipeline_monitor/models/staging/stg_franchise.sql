with source as (
    select * from {{ source('raw_pos', 'franchise') }}
),

renamed as (
    select
        franchise_id,
        first_name || ' ' || last_name  as franchise_owner_name,
        city                             as franchise_city,
        country                          as franchise_country,
        e_mail                           as franchise_email
    from source
)

select * from renamed