{{ config(materialized='view') }}

with src as (

    select
        assumption_type,
        version_id,
        version_name,
        is_base_version
    from {{ source('life_insurance_raw', 'assumption_version') }}

),

final as (

    select
        assumption_type,
        version_id,
        version_name,
        is_base_version
    from src
)

select * from final
