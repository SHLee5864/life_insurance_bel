{{ config(materialized='table') }}

select
    cohort_id,
    sex,
    scenario_id,
    projection_ok,
    version_id,
    mort_version_id,
    inforce_ok,
    cashflow_ok,
    discount_ok,
    bel_reconciled,
    sensitivity_ok,
    overall_validation_status
from {{ ref('val_validation_summary') }}