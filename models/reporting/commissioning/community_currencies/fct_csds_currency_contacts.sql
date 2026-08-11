with fiscal_years as (
    select
        fiscal_year_start
        , gdp_deflator
        , case when fiscal_year_start = min(fiscal_year_start) over ()
            then '1900-01-01'::date
            else date_from_parts(fiscal_year_start, 4, 1)
        end as fy_range_start
        , case when fiscal_year_start = max(fiscal_year_start) over ()
            then '2099-12-31'::date
            else date_from_parts(fiscal_year_start + 1, 3, 31)
        end as fy_range_end
    from {{ ref('uk_cost_indices') }}
)

, price_base_deflator as (
    select max(gdp_deflator) as base_gdp_deflator
    from {{ ref('uk_cost_indices') }}
    where fiscal_year_start = 2026
)

, price_attempts as (
    select
        c.*
        , exact_price.price_gbp as exact_price_gbp
        , age_default_price.price_gbp as age_default_price_gbp
    from {{ ref('int_csds_contact_currency') }} as c
    left join {{ ref('nhse_currency_prices_2627') }} as exact_price
        on c.currency_code = exact_price.currency_code
    left join {{ ref('nhse_currency_prices_2627') }} as age_default_price
        on iff(c.age_category = 'CYP', 'CCO99Z', 'CAO99Z') = age_default_price.currency_code
)

, priced as (
    select
        *
        , coalesce(exact_price_gbp, age_default_price_gbp) as unit_price_2627_gbp
        , case
            when exact_price_gbp is not null then 'exact'
            when age_default_price_gbp is not null then 'age_default'
        end as price_source
    from price_attempts
)

select
    p.unique_service_request_identifier
    , p.unique_care_contact_identifier
    , p.sk_patient_id
    , p.person_id
    , p.org_id_prov
    , p.care_contact_date
    , fy.fiscal_year_start
    , p.attendance_status
    , p.is_costed_attendance
    , p.age_category
    , p.currency_code
    , p.currency_source
    , p.unit_price_2627_gbp
    , p.price_source
    , coalesce(mff.mff_factor, 1.0) as mff_factor
    , iff(
        p.is_costed_attendance
        , p.unit_price_2627_gbp * coalesce(mff.mff_factor, 1.0) * fy.gdp_deflator / pb.base_gdp_deflator
        , 0
    ) as proxy_cost
from priced as p
cross join price_base_deflator as pb
left join fiscal_years as fy
    on p.care_contact_date between fy.fy_range_start and fy.fy_range_end
left join {{ ref('nhse_provider_mff_2627') }} as mff
    on p.org_id_prov = mff.provider_code
