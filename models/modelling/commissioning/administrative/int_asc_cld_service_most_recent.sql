{{ config(materialized="table") }}

with latest_submission_periods as (
    select distinct
        la_code,
        reporting_period_start_date,
        reporting_period_end_date
    from {{ ref("stg_asc_cld_adult_social_care_cld") }}
    qualify row_number() over (
        partition by la_code
        order by reporting_period_end_date desc, reporting_period_start_date desc
    ) = 1
),

borough_names as(
    select organisation_code, organisation_name 
    from {{ ref("stg_dictionary_dbo_organisation") }} 
    where organisation_primary_role = 'RO141'
    and organisation_code is not null
),

latest_period_cld as (
    select borough.organisation_name as borough_name, asc_cld.*
    from {{ ref("stg_asc_cld_adult_social_care_cld") }} as asc_cld
    inner join latest_submission_periods as latest
        on asc_cld.la_code = latest.la_code
        and asc_cld.reporting_period_start_date = latest.reporting_period_start_date
        and asc_cld.reporting_period_end_date = latest.reporting_period_end_date
    left join borough_names as borough
        on asc_cld.la_code = borough.organisation_code
    where asc_cld.sk_patient_id is not null 
    and latest.reporting_period_end_date between dateadd(month, -24, current_date()) and current_date() -- reporting period is relatively up to date at time of review [this is more for records with errors in LA code so they aren't too old] TO DO: consider requirement for valid borough id
),

-- use_within_scope as ( -- not adding a date filter as reporting period is up to date at time of review
--     select * from latest_period_cld
--     where event_start_date between dateadd(month, -24, current_date()) and current_date()
-- )

recommended_dedupe as ( -- https://www.ardengemcsu.nhs.uk/media/3748/cld-dashboard-methodology-document-for-users.pdf
    select
        sk_patient_id,
        la_code,
        borough_name,
        reporting_period_start_date,
        reporting_period_end_date,
        event_start_date,
        client_type,
        primary_support_reason,
        client_funding_status,
        service_type,
        service_component,
        delivery_mechanism
    from latest_period_cld
    where event_type = 'Service'
    qualify row_number() over (
        partition by
            la_code,
            sk_patient_id,
            event_start_date,
            service_type,
            service_component,
            delivery_mechanism
        order by dm_received_date desc, dm_row_id desc
    ) = 1
),

cleaned_support_reasons as (
    select
        sk_patient_id,
        la_code,
        borough_name,
        service_type,
        {{ clean_asc_cld_primary_support_reason('primary_support_reason') }}
            as primary_support_reason_cleaned
        , primary_support_reason
    from recommended_dedupe
),

categorised_support_reasons as (
    select
        sk_patient_id,
        la_code,
        borough_name,
        service_type,
        primary_support_reason_cleaned,
        {{ asc_cld_primary_support_reason_category('primary_support_reason_cleaned') }}
            as primary_support_reason_category
        , primary_support_reason
    from cleaned_support_reasons
)

select
    sk_patient_id,
    array_agg(distinct borough_name) as borough_name,
    array_agg(distinct service_type) as service_type,
    array_agg(distinct primary_support_reason_category) as primary_support_reason_category,
    boolor_agg(
        primary_support_reason_cleaned = 'Physical support: Personal care support'
    ) as has_physical_support_personal_care,
    boolor_agg(
        primary_support_reason_cleaned = 'Physical support: Access and mobility only'
    ) as has_physical_support_access_mobility,
    boolor_agg(
        primary_support_reason_cleaned = 'Learning disability support'
    ) as has_learning_disability_support,
    boolor_agg(
        primary_support_reason_cleaned = 'Mental health support'
    ) as has_mental_health_support,
    boolor_agg(
        primary_support_reason_cleaned = 'Unknown'
    ) as has_unknown_primary_support_reason,
    boolor_agg(
        primary_support_reason_cleaned = 'Support with memory and cognition'
    ) as has_memory_cognition_support,
    boolor_agg(
        primary_support_reason_cleaned = 'Social support: Support to unpaid carer'
    ) as has_social_support_unpaid_carer,
    boolor_agg(
        primary_support_reason_cleaned = 'Social support: Support for social isolation or other'
    ) as has_social_support_social_isolation,
    boolor_agg(
        primary_support_reason_cleaned = 'Sensory support: Support for visual impairment'
    ) as has_sensory_support_visual_impairment,
    boolor_agg(
        primary_support_reason_cleaned = 'Sensory support: Support for hearing impairment'
    ) as has_sensory_support_hearing_impairment,
    boolor_agg(
        primary_support_reason_cleaned = 'Social support: Substance misuse support'
    ) as has_social_support_substance_misuse,
    boolor_agg(
        primary_support_reason_cleaned = 'Sensory support: Support for dual impairment'
    ) as has_sensory_support_dual_impairment,
    boolor_agg(
        primary_support_reason_cleaned = 'Social support: Asylum seeker support'
    ) as has_social_support_asylum_seeker
from categorised_support_reasons
group by sk_patient_id
