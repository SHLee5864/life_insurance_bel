{{ config(materialized='view') }}

with cashflows as (
    select * from {{ ref('int_cashflow_lines') }}
),

frame as (
    select cohort_id, sex, scenario_id, projection_month, discount_shift_bps
    from {{ ref('int_projection_frame') }}
),

curve as (
    select tenor_month, zero_rate_annual, version_id
    from {{ ref('stg_discount_curve') }}
),

cashflows_with_frame as (
    select
        c.*,
        f.discount_shift_bps
    from cashflows c
    inner join frame f
        on c.cohort_id = f.cohort_id
        and c.sex = f.sex
        and c.scenario_id = f.scenario_id
        and c.projection_month = f.projection_month
),

-- discount month: month_start → t-1, month_end → t
discount_months as (
    select
        *,
        case
            when cashflow_timing = 'month_start' then projection_month - 1
            when cashflow_timing = 'month_end' then projection_month
        end as discount_month
    from cashflows_with_frame
),

-- 보간을 위한 lower/upper tenor 매핑
interpolated as (
    select
        dm.*,

        -- lower/upper tenor
        case
            when dm.discount_month <= 0 then 0
            else cast(floor((dm.discount_month - 1) / 12.0) * 12 as int)
        end as lower_tenor,

        case
            when dm.discount_month <= 0 then 12
            else cast(ceil(dm.discount_month / 12.0) * 12 as int)
        end as upper_tenor

    from discount_months dm
),
-- curve versions cross join
with_curve_version as (
    select
        i.*,
        cv.version_id
    from interpolated i
    cross join (select distinct version_id from curve) cv
),

-- lower/upper rate JOIN
with_rates as (
    select
        wcv.*,
        coalesce(cl.zero_rate_annual, 0.0) as lower_rate,
        cu.zero_rate_annual as upper_rate
    from with_curve_version wcv
    left join curve cl 
        on wcv.lower_tenor = cl.tenor_month 
        and wcv.version_id = cl.version_id
    left join curve cu 
        on wcv.upper_tenor = cu.tenor_month 
        and wcv.version_id = cu.version_id
),

-- 선형 보간 + stress shift + DF 계산
final as (
    select
        cohort_id,
        sex,
        scenario_id,
        projection_month,
        cashflow_type,
        cashflow_amount,
        cashflow_timing,
        discount_month,
        version_id,

        -- 보간된 zero rate
        case
            when discount_month <= 0 then 0.0
            when lower_tenor = upper_tenor then upper_rate
            else lower_rate + (upper_rate - lower_rate)
                 * (cast(discount_month as double) - lower_tenor)
                 / (upper_tenor - lower_tenor)
        end + discount_shift_bps / 10000.0 as interpolated_rate,

        -- discount factor
        case
            when discount_month <= 0 then 1.0
            else 1.0 / pow(
                1.0 + (
                    case
                        when lower_tenor = upper_tenor then upper_rate
                        else lower_rate + (upper_rate - lower_rate)
                            * (cast(discount_month as double) - lower_tenor)
                            / (upper_tenor - lower_tenor)
                    end + discount_shift_bps / 10000.0
                ),
                cast(discount_month as double) / 12.0
            )
        end as discount_factor

    from with_rates
)

select
    cohort_id,
    sex,
    scenario_id,
    projection_month,
    cashflow_type,
    cashflow_amount,
    cashflow_timing,
    version_id,
    discount_factor,
    cashflow_amount * discount_factor as discounted_cashflow
from final