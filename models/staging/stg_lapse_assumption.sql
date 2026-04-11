{{ config(materialized='view') }}

with base_version as (
    select version_id
    from {{ source('life_insurance_raw', 'assumption_version') }}
    where assumption_type = 'lapse'
      and is_base_version = true
),

 src as (

    select
        duration_year,
        lapse_rate_annual,
        version_id
    from {{ source('life_insurance_raw', 'lapse_assumption') }}
    where version_id = (select version_id from base_version)

),

final as (

    select
        duration_year,
        lapse_rate_annual,
        version_id
    from src
)

select * from final
