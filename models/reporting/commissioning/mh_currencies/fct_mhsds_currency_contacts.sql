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

, activity as (
    select
        c.*
        , substring(c.currency_code, 4, 2) as currency_family
        , right(c.currency_code, 1) as currency_setting_code
    from {{ ref('int_mhsds_contact_currency') }} as c
)

, price_attempts as (
    select
        a.*
        , exact_price.price_gbp as exact_price_gbp
        , population_price.price_gbp as population_price_gbp
        , unclassified_setting_price.price_gbp as unclassified_setting_price_gbp
        , unclassified_family_price.price_gbp as unclassified_family_price_gbp
    from activity as a
    left join {{ ref('nhse_currency_prices_2627') }} as exact_price
        on a.currency_code = exact_price.currency_code
    left join {{ ref('nhse_currency_prices_2627') }} as population_price
        on a.currency_group = population_price.population_group
        and a.currency_family = population_price.currency_family
        and population_price.setting_code = 'Z'
    -- 99-family codes (MAZ/MCS) fall back to MBU97 crisis prices; MAZ99Z and
    -- MCS99Z are priced, so these attempts only fire for the 96/97 families
    left join {{ ref('nhse_currency_prices_2627') }} as unclassified_setting_price
        on unclassified_setting_price.population_group = 'MBU'
        and unclassified_setting_price.currency_family = iff(a.currency_family = '99', '97', a.currency_family)
        and unclassified_setting_price.setting_code = a.currency_setting_code
    left join {{ ref('nhse_currency_prices_2627') }} as unclassified_family_price
        on unclassified_family_price.population_group = 'MBU'
        and unclassified_family_price.currency_family = iff(a.currency_family = '99', '97', a.currency_family)
        and unclassified_family_price.setting_code = 'Z'
)

, priced as (
    select
        *
        , coalesce(
            exact_price_gbp
            , population_price_gbp
            , unclassified_setting_price_gbp
            , unclassified_family_price_gbp
        ) as unit_price_2627_gbp
        , case
            when exact_price_gbp is not null then 'exact'
            when population_price_gbp is not null then 'population_family'
            when unclassified_setting_price_gbp is not null then 'unclassified_setting'
            when unclassified_family_price_gbp is not null then 'unclassified_family'
        end as price_source
    from price_attempts
)

select
    p.uniq_care_cont_id
    , p.uniq_serv_req_id
    , p.sk_patient_id
    , p.person_id
    , p.org_id_prov
    , p.care_cont_date
    , fy.fiscal_year_start
    , p.attend_status
    , p.attend_status in ('5', '6') or p.attend_status is null as is_costed_attendance
    , p.currency_group
    , p.currency_code
    , p.unit_price_2627_gbp
    , p.price_source
    , coalesce(mff.mff_factor, 1.0) as mff_factor
    , iff(
        p.attend_status in ('5', '6') or p.attend_status is null
        , p.unit_price_2627_gbp * coalesce(mff.mff_factor, 1.0) * fy.gdp_deflator / pb.base_gdp_deflator
        , 0
    ) as proxy_cost
    , p.is_cyp
    , p.is_crisis_referral
    , p.winning_tier
from priced as p
cross join price_base_deflator as pb
left join fiscal_years as fy
    on p.care_cont_date between fy.fy_range_start and fy.fy_range_end
left join {{ ref('nhse_provider_mff_2627') }} as mff
    on p.org_id_prov = mff.provider_code
