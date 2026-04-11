{{ config(materialized='view') }}

with base_version as (
    select version_id
    from {{ source('life_insurance_raw', 'assumption_version') }}
    where assumption_type = 'expense'
      and is_base_version = true
),

src as (

    select
        expense_type,
        expense_basis,
        expense_rate,
        version_id,
        comment
    from {{ source('life_insurance_raw', 'expense_assumption') }}
    where version_id = (select version_id from base_version)
),

final as (

    select
        expense_type,
        expense_basis,
        expense_rate,
        version_id,
        comment
    from src
)

select * from final
