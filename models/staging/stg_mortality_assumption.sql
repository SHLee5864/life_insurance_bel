{{ config(materialized='view') }}

with base_version as (
    select version_id
    from {{ source('life_insurance_raw', 'assumption_version') }}
    where assumption_type = 'mortality'
      and is_base_version = true
),

 src as (

    select
        age,
        sex,
        qx_annual,
        source,
        year,
        version_id
    from {{ source('life_insurance_raw', 'mortality_assumption') }}
    where version_id = (select version_id from base_version)

),

final as (

    select
        age,
        sex,
        qx_annual,
        source,
        year,
        version_id
    from src
)

select * from final
