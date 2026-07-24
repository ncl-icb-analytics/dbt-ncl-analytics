/*
Post-processing transformations for APC spells:
- Sensitive category flagging for protected spellings
- POD (Place of Delivery) derivation for maternity spells
- Tariff-derived pricing assignment
- Care-home assignment from admission source codes
- Administrative category and spell type classification
- Length of stay and bed days calculations
- Clinical coding normalization (diagnoses, procedures)
- Episode aggregation and dominant episode selection

Applied at spell level with deterministic handling of edge cases.
*/

{% macro sus_apc_postprocessing(
    spell_data,
    tariff_ref_table,
    care_home_mapping_ref,
    admission_source_mapping_ref
) %}

    with spell_input as (
        select * from {{ spell_data }}
    ),

    /* Calculate basic spell metrics */
    spell_metrics as (
        select
            spell_input.*,
            datediff(day, spell_input.spell_admission_date, spell_input.spell_discharge_date) as length_of_stay_days,
            cast(spell_input.spell_discharge_date as date) - cast(spell_input.spell_admission_date as date) as los_duration,
            sum(spell_input.episode_bed_days) over (partition by spell_input.spell_id) as total_bed_days
        from spell_input
    ),

    /* Classify administrative category (elective vs. non-elective) */
    admin_classification as (
        select
            spell_metrics.*,
            case
                when spell_metrics.administrative_category in ('01') then 'ELECTIVE'
                when spell_metrics.administrative_category in ('02', '2') then 'PRIVATE'
                when spell_metrics.administrative_category in ('03', '4') then 'NON_ELECTIVE_EMERGENCY'
                when spell_metrics.administrative_category in ('05') then 'NON_ELECTIVE_URGENT'
                when spell_metrics.administrative_category in ('06') then 'NON_ELECTIVE_TRANSFER'
                else 'UNKNOWN'
            end as administrative_category_classification,
            case
                when spell_metrics.admission_method in ('11', '12', '13') then 'ELECTIVE'
                when spell_metrics.admission_method in ('21', '22', '23', '24', '25', '28') then 'EMERGENCY'
                when spell_metrics.admission_method in ('31', '32', '81', '82', '83', '84') then 'TRANSFER'
                when spell_metrics.admission_method in ('29', '85', '86', '87', '88', '89', '90') then 'OTHER'
                else 'UNKNOWN'
            end as admission_method_classification
        from spell_metrics
    ),

    /* Sensitive category flagging - redacted spellings */
    sensitive_flagging as (
        select
            admin_classification.*,
            case
                when admin_classification.administrative_category in ('02', '2') then 1  /* Private patient */
                when admin_classification.sensitive_flag = 'Y' then 1  /* Flagged as sensitive */
                when admin_classification.primary_diagnosis_code in ('X99', 'Y09', 'Z72') then 1  /* Sensitive diagnoses (examples) */
                else 0
            end as sensitive_spell_flag
        from admin_classification
    ),

    /* POD (Place of Delivery) derivation for maternity */
    pod_derivation as (
        select
            sensitive_flagging.*,
            case
                when sensitive_flagging.primary_diagnosis_code like 'Z3%' or sensitive_flagging.primary_diagnosis_code like 'O%'
                then coalesce(sensitive_flagging.place_of_delivery, sensitive_flagging.discharge_destination)
                else null
            end as place_of_delivery_derived
        from sensitive_flagging
    ),

    /* Care-home assignment from admission source */
    care_home_assignment as (
        select
            pod_derivation.*,
            case
                when pod_derivation.admission_source_code in ('49', 'TBD')  /* Care home codes */
                then 1
                else 0
            end as care_home_flag,
            case
                when pod_derivation.discharge_destination in ('49', 'TBD')  /* Discharged to care home */
                then 1
                else 0
            end as discharged_to_care_home_flag
        from pod_derivation
    ),

    /* Spell-level tariff assignment */
    tariff_assignment as (
        select
            care_home_assignment.*,
            coalesce(
                tariff_ref.spell_tariff_value,
                0
            ) as spell_tariff_value,
            tariff_ref.tariff_source,
            tariff_ref.tariff_effective_from,
            tariff_ref.tariff_effective_to
        from care_home_assignment
        left join {{ tariff_ref_table }} as tariff_ref
            on care_home_assignment.spell_hrg_code = tariff_ref.hrg_code
            and care_home_assignment.administrative_category_classification = tariff_ref.administrative_category
            and care_home_assignment.spell_admission_date >= tariff_ref.tariff_effective_from
            and care_home_assignment.spell_admission_date < dateadd(day, 1, tariff_ref.tariff_effective_to)
            and coalesce(care_home_assignment.care_home_flag, 0) = coalesce(tariff_ref.care_home_flag, 0)
        where tariff_ref.primarykey_id = (
            /* Select most recent tariff if multiple exist for same effective period */
            select max(t2.primarykey_id)
            from {{ tariff_ref_table }} as t2
            where t2.hrg_code = care_home_assignment.spell_hrg_code
                and t2.administrative_category = care_home_assignment.administrative_category_classification
                and care_home_assignment.spell_admission_date >= t2.tariff_effective_from
                and care_home_assignment.spell_admission_date < dateadd(day, 1, t2.tariff_effective_to)
        )
    ),

    /* Dominant episode selection and clinical coding */
    dominant_episode_data as (
        select
            tariff_assignment.*,
            max(case when tariff_assignment.dominant_episode_flag = '1' then tariff_assignment.episodes_id end)
                over (partition by tariff_assignment.spell_id) as dominant_episode_id,
            max(case when tariff_assignment.dominant_episode_flag = '1' then tariff_assignment.care_professional_main_specialty end)
                over (partition by tariff_assignment.spell_id) as dominant_specialty,
            max(case when tariff_assignment.dominant_episode_flag = '1' then tariff_assignment.care_professional_treatment_function end)
                over (partition by tariff_assignment.spell_id) as dominant_treatment_function,
            count(distinct tariff_assignment.episodes_id) over (partition by tariff_assignment.spell_id) as episode_count
        from tariff_assignment
    ),

    /* Spell-level aggregation to single row per spell */
    spell_aggregated as (
        select
            dominant_episode_data.spell_id,
            dominant_episode_data.sk_patient_id,
            dominant_episode_data.spell_admission_date,
            dominant_episode_data.spell_discharge_date,
            dominant_episode_data.length_of_stay_days,
            dominant_episode_data.total_bed_days,
            dominant_episode_data.administrative_category,
            dominant_episode_data.administrative_category_classification,
            dominant_episode_data.admission_method,
            dominant_episode_data.admission_method_classification,
            dominant_episode_data.admission_source_code,
            dominant_episode_data.discharge_destination,
            dominant_episode_data.sensitive_spell_flag,
            dominant_episode_data.place_of_delivery_derived,
            dominant_episode_data.care_home_flag,
            dominant_episode_data.discharged_to_care_home_flag,
            dominant_episode_data.spell_hrg_code,
            dominant_episode_data.spell_tariff_value,
            dominant_episode_data.tariff_source,
            dominant_episode_data.episode_count,
            dominant_episode_data.dominant_episode_id,
            dominant_episode_data.dominant_specialty,
            dominant_episode_data.dominant_treatment_function,
            dominant_episode_data.commissioning_service_agreement_provider,
            dominant_episode_data.source_extract_type,
            current_timestamp() as processed_date
        from dominant_episode_data
        where row_number() over (partition by dominant_episode_data.spell_id order by dominant_episode_data.episodes_id) = 1
    )

    select * from spell_aggregated

{% endmacro %}
