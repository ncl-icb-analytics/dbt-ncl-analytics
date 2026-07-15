{{
    config(
        materialized='table',
        tags=['monthly', 'sus', 'outpatient', 'consolidated']
    )
}}

/*
    Published outpatient consolidated dataset. The Date Range source has already
    combined the legacy Current, Rec and PostRec streams upstream.
*/

with organisation_by_code as (
    select
        organisation_code,
        sk_organisation_id,
        sk_organisation_id_current_care_org
    from {{ ref('stg_dictionary_dbo_organisation') }}
    qualify row_number() over (
        partition by organisation_code
        order by coalesce(end_date, '9999-12-31'::date) desc, last_updated desc
    ) = 1
),

organisation_by_id as (
    select
        sk_organisation_id,
        organisation_code
    from {{ ref('stg_dictionary_dbo_organisation') }}
    qualify row_number() over (
        partition by sk_organisation_id
        order by coalesce(end_date, '9999-12-31'::date) desc, last_updated desc
    ) = 1
),

consolidated as (
    select
        f.*,
        coalesce(current_org.organisation_code, f.organisation_code_code_of_provider)
            as zorganisation_code_code_of_provider_merged
    from {{ ref('fct_sus_op_monthly') }} as f
    left join organisation_by_code as provider
        on left(f.organisation_code_code_of_provider, 3) = provider.organisation_code
    left join organisation_by_id as current_org
        on coalesce(
            provider.sk_organisation_id_current_care_org,
            provider.sk_organisation_id
        ) = current_org.sk_organisation_id
    where f.zcommissioning_access in (0, 1)
      and coalesce(f.administrative_category, '00') not in ('02', '2')
)

select *
from consolidated
