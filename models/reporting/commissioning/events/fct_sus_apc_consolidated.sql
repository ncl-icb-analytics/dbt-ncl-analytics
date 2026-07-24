{{ config(materialized="table") }}

/*
Published consolidated APC fact table.

Grain: One row per spell per month (filtered)
Purpose: Restrict to commissioning-access records, exclude private activity,
         and resolve provider to current-care organisation

For comprehensive unfiltered data and debugging, use fct_sus_apc_monthly instead.
*/

with fct_base as (
    select * from {{ ref('fct_sus_apc_monthly') }}
),

/* Exclude private patient activity and non-commissioning access */
commissioning_filtered as (
    select
        fct_base.*,
        1 as zcommissioning_access  /* All rows here are commissioning access */
    from fct_base
    where administrative_category_classification != 'PRIVATE'
        and sensitive_spell_flag = 0
),

/* Resolve provider to current-care organisation via merger mapping */
provider_resolved as (
    select
        commissioning_filtered.*,
        coalesce(org_merge.current_organisation_code, commissioning_filtered.commissioning_service_agreement_provider) 
            as zorganisation_code_code_of_provider_merged
    from commissioning_filtered
    left join {{ ref('stg_dictionary_organisation_mergers') }} as org_merge
        on commissioning_filtered.commissioning_service_agreement_provider = org_merge.predecessor_organisation_code
        and commissioning_filtered.spell_admission_date >= org_merge.merger_effective_date
),

/* Final published fact - subset key columns */
final_consolidated as (
    select
        sk_spell_id,
        spell_id,
        sk_patient_id,
        zcommissioning_access,
        zorganisation_code_code_of_provider_merged,
        spell_admission_date,
        spell_discharge_date,
        spell_admission_year,
        spell_admission_month,
        spell_admission_quarter,
        financial_year,
        length_of_stay_days,
        total_bed_days,
        episode_count,
        zcommissioner_code,
        zbusinessrule,
        zcontracttype,
        administrative_category,
        administrative_category_classification,
        admission_source_code,
        discharge_destination,
        spell_hrg_code,
        spell_tariff_value,
        patient_date_of_birth,
        patient_age_at_admission,
        patient_gender_code,
        patient_gender_description,
        patient_ethnic_category_code,
        patient_ethnic_category_description,
        provider_name,
        provider_type,
        care_home_flag,
        discharged_to_care_home_flag,
        sensitive_spell_flag,
        place_of_delivery_derived,
        dominant_episode_specialty,
        dominant_episode_treatment_function,
        source_extract_type,
        zdatatype,
        fact_load_timestamp
    from provider_resolved
)

select * from final_consolidated
