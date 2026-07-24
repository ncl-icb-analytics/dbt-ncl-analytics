{{ config(materialized="table") }}

/*
Intermediate-layer spell aggregation for APC spells.

Grain: One row per spell (aggregated from episode-level source data)
Purpose: Apply spell-level transformations including commissioner attribution, 
         business rules, post-processing, and SLA classification

Feeds: fct_sus_apc_monthly and fct_sus_apc_consolidated
*/

with stg_spells as (
    select 
        primarykey_id as spell_id,
        sk_patient_id,
        spell_admission_date,
        spell_discharge_date,
        spell_gp_practice_code,
        spell_commissioning_service_agreement_provider,
        patient_lsoa_code,
        provider_postcode,
        administrative_category,
        admission_method,
        admission_source_code,
        discharge_destination,
        sensitive_flag,
        spell_hrg_code,
        source_extract_type,
        local_patient_identifier
    from {{ ref('stg_sus_apc_spell_episodes') }}
    where row_number() over (partition by primarykey_id order by last_updated_date desc) = 1
),

/* Apply commissioner assignment macro */
commissioner_assigned as (
    {{ assign_sus_apc_commissioner(
        spell_table_ref=ref('stg_sus_apc_spell_episodes'),
        commissioner_practice_ref=source('manual_sources', 'SUS_COMMISSIONER_PRACTICE'),
        commissioner_lsoa_ref=source('manual_sources', 'SUS_COMMISSIONER_LSOA'),
        commissioner_provider_ref=source('manual_sources', 'SUS_COMMISSIONER_PROVIDER'),
        commissioner_provider_postcode_ref=source('manual_sources', 'SUS_COMMISSIONER_PROVIDER_POSTCODE')
    ) }}
),

/* Enrich with commissioner details */
commissioner_enriched as (
    select
        commissioner_assigned.spell_id,
        commissioner_assigned.sk_patient_id,
        commissioner_assigned.spell_admission_date,
        commissioner_assigned.commissioner_code as zcommissioner_code,
        commissioner_assigned.commissioner_lookup_method,
        commissioner_assigned.commissioner_lookup_priority,
        stg_spells.*
    from commissioner_assigned
    inner join stg_spells on commissioner_assigned.spell_id = stg_spells.spell_id
),

/* Apply post-processing transformations */
postprocessed_spells as (
    {{ sus_apc_postprocessing(
        spell_data=ref('commissioner_enriched'),
        tariff_ref_table=ref('stg_sus_apc_tariff'),
        care_home_mapping_ref=ref('stg_lookup_care_home_admission_source'),
        admission_source_mapping_ref=ref('stg_dictionary_admission_source')
    ) }}
),

/* Calculate clinical metrics and episode grouping */
spell_clinical_metrics as (
    select
        postprocessed_spells.*,
        count(distinct episodes_id) over (partition by spell_id) as episode_count,
        sum(episode_bed_days) over (partition by spell_id) as total_bed_days,
        max(case when dominant_episode_flag = '1' then care_professional_main_specialty end) 
            over (partition by spell_id) as dominant_episode_specialty,
        max(case when dominant_episode_flag = '1' then care_professional_treatment_function end) 
            over (partition by spell_id) as dominant_episode_treatment_function,
        datediff(day, spell_admission_date, spell_discharge_date) as length_of_stay_days
    from postprocessed_spells
),

/* Apply business rules via lateral join to seed data */
business_rules_application as (
    select
        spell_clinical_metrics.*,
        listagg(distinct rules.rule_name, ' | ') within group (order by rules.rule_id) as zbusinessrule,
        max(rules.contract_type) as zcontracttype
    from spell_clinical_metrics
    left join {{ ref('sus_apc_business_rules_sla') }} as rules on 1=1
        and rules.active = 1
        and (rules.admission_type = 'ALL' or rules.admission_type = spell_clinical_metrics.administrative_category_classification)
        and (rules.hrg_category = 'ALL' or rules.hrg_category = spell_clinical_metrics.spell_hrg_code)
        and (
            rules.rule_type = 'LENGTH_OF_STAY'
            and spell_clinical_metrics.length_of_stay_days >= rules.los_threshold_min
            and spell_clinical_metrics.length_of_stay_days <= rules.los_threshold_max
        )
        and spell_clinical_metrics.spell_admission_date >= rules.effective_from
        and spell_clinical_metrics.spell_admission_date < dateadd(day, 1, rules.effective_to)
    group by spell_clinical_metrics.*
),

/* Final aggregation to spell grain */
final_spell_monthly as (
    select
        spell_id,
        sk_patient_id,
        local_patient_identifier,
        spell_admission_date,
        spell_discharge_date,
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
        sensitive_spell_flag,
        place_of_delivery_derived,
        care_home_flag,
        discharged_to_care_home_flag,
        spell_tariff_value,
        dominant_episode_specialty,
        dominant_episode_treatment_function,
        commissioning_service_agreement_provider,
        source_extract_type,
        'SPELL_UNIFIED' as zdatatype,
        zbusinessrule,
        zcontracttype,
        current_timestamp() as model_execution_timestamp
    from business_rules_application
    where row_number() over (partition by spell_id order by spell_admission_date) = 1
)

select * from final_spell_monthly
