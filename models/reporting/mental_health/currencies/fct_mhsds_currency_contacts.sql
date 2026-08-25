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

, priced as (
    select
        c.*
        , price.unit_price_2627_gbp
        , price.price_source
    from {{ ref('int_mhsds_contact_currency') }} as c
    left join {{ ref('int_nhse_currency_price_resolution') }} as price
        on c.currency_code = price.currency_code
)

select
    p.uniq_care_cont_id
    , p.uniq_serv_req_id
    , p.sk_patient_id
    , p.person_id
    , p.org_id_prov
    , p.dm_icb_commissioner
    , p.commissioner_icb_code
    , p.care_cont_date
    , fy.fiscal_year_start
    , p.attend_status
    -- lpad guards against zero-padded codes ('05'/'06') appearing in a
    -- future feed; nulls costed per national guidance
    , lpad(p.attend_status, 2, '0') in ('05', '06') or p.attend_status is null as is_costed_attendance
    , p.currency_group
    , p.currency_code
    , p.unit_price_2627_gbp
    , p.price_source
    , coalesce(mff.mff_factor, 1.0) as mff_factor
    , iff(
        lpad(p.attend_status, 2, '0') in ('05', '06') or p.attend_status is null
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
