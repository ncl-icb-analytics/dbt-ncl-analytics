{{ config(materialized="table") }}

/*
Monthly SUS APC fact table with full transformations applied.

Grain: One row per spell per month
Purpose: Core fact table for commissioning reporting with commissioner attribution, 
         business rules, and SLA classification

Includes all spells (both commissioning and private activity) for comprehensive audit trail.
Private spells can be filtered at consumption layer.
*/

with int_spells as (
    select * from {{ ref('int_sus_apc_spell_monthly') }}
),

/* Enrich with provider and site details */
provider_enriched as (
    select
        int_spells.*,
        coalesce(provider_dict.organisation_name, int_spells.commissioning_service_agreement_provider) as provider_name,
        provider_dict.organisation_code as provider_code,
        provider_dict.organisation_type as provider_type
    from int_spells
    left join {{ ref('stg_dictionary_dbo_provider') }} as provider_dict
        on int_spells.commissioning_service_agreement_provider = provider_dict.organisation_code
),

/* Enrich with patient demographics */
demographics_enriched as (
    select
        provider_enriched.*,
        pat_demo.patient_date_of_birth,
        pat_demo.patient_age_at_admission,
        pat_demo.patient_gender_code,
        pat_demo.patient_gender_description,
        pat_demo.patient_ethnic_category_code,
        pat_demo.patient_ethnic_category_description
    from provider_enriched
    left join {{ ref('stg_dictionary_dbo_patient_demographics') }} as pat_demo
        on provider_enriched.sk_patient_id = pat_demo.sk_patient_id
),

/* Add calendar date dimensions */
date_enhanced as (
    select
        demographics_enriched.*,
        extract(year from demographics_enriched.spell_admission_date) as spell_admission_year,
        extract(month from demographics_enriched.spell_admission_date) as spell_admission_month,
        extract(quarter from demographics_enriched.spell_admission_date) as spell_admission_quarter,
        extract(year from demographics_enriched.spell_discharge_date) as spell_discharge_year,
        extract(month from demographics_enriched.spell_discharge_date) as spell_discharge_month,
        dbt_date.fiscal_year(demographics_enriched.spell_admission_date, 'GB') as financial_year
    from demographics_enriched
),

/* Apply additional data quality flags */
quality_flagged as (
    select
        date_enhanced.*,
        case
            when date_enhanced.spell_admission_date > current_timestamp() then 1
            else 0
        end as future_date_flag,
        case
            when date_enhanced.patient_age_at_admission < 0 or date_enhanced.patient_age_at_admission > 120 then 1
            else 0
        end as invalid_age_flag,
        case
            when date_enhanced.zcommissioner_code = 'UNKNOWN' then 1
            else 0
        end as missing_commissioner_flag,
        case
            when date_enhanced.spell_hrg_code is null then 1
            else 0
        end as missing_hrg_flag
    from date_enhanced
),

/* Final fact table */
final_fact as (
    select
        {{ dbt_utils.generate_surrogate_key(['spell_id', 'financial_year']) }} as sk_spell_id,
        spell_id,
        sk_patient_id,
        local_patient_identifier,
        spell_admission_date,
        spell_discharge_date,
        spell_admission_year,
        spell_admission_month,
        spell_admission_quarter,
        spell_discharge_year,
        spell_discharge_month,
        financial_year,
        length_of_stay_days,
        total_bed_days,
        episode_count,
        zcommissioner_code,
        commissioner_lookup_method,
        commissioner_lookup_priority,
        administrative_category,
        administrative_category_classification,
        admission_method,
        admission_method_classification,
        admission_source_code,
        discharge_destination,
        spell_hrg_code,
        commissioning_service_agreement_provider,
        provider_name,
        provider_code,
        provider_type,
        patient_date_of_birth,
        patient_age_at_admission,
        patient_gender_code,
        patient_gender_description,
        patient_ethnic_category_code,
        patient_ethnic_category_description,
        sensitive_spell_flag,
        place_of_delivery_derived,
        care_home_flag,
        discharged_to_care_home_flag,
        spell_tariff_value,
        dominant_episode_specialty,
        dominant_episode_treatment_function,
        source_extract_type,
        zdatatype,
        zbusinessrule,
        zcontracttype,
        future_date_flag,
        invalid_age_flag,
        missing_commissioner_flag,
        missing_hrg_flag,
        model_execution_timestamp,
        current_timestamp() as fact_load_timestamp
    from quality_flagged
)

select * from final_fact
