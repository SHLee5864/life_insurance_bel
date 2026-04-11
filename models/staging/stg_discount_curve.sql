{{ config(materialized='view') }}


with base_version as (
    select version_id
    from {{ source('life_insurance_raw', 'assumption_version') }}
    where assumption_type = 'discount'
      and is_base_version = true
), 

src as (

    select
        tenor_month,
        zero_rate_annual,
        version_id
    from {{ source('life_insurance_raw', 'discount_curve') }}
    where version_id = (select version_id from base_version)

),

final as (

    select
        tenor_month,
        zero_rate_annual,
        version_id
    from src
)

select * from final
