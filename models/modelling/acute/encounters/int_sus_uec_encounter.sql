
/*
Emergency care encounters from SUS

Clinical Purpose:
- Establishing demand for emergency care services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/
with
ethnicity_codes as (
    select distinct bk_ethnicity_code, ethnicity_desc
    from {{ref('stg_dictionary_dbo_ethnicity')}}
    where ethnicity_code_type = 'Current' or bk_ethnicity_code = '99'
    ),
gender_codes as (
    select distinct gender_code, gender
    from {{ref('stg_dictionary_dbo_gender')}}
    ),
commissioner_codes as (
    select commissioner_code, commissioner_name
    from {{ ref('stg_dictionary_dbo_commissioner') }}
    qualify row_number() over (
        partition by commissioner_code
        order by coalesce(end_date, '9999-12-31'::date) desc, start_date desc
    ) = 1
    ),
treatment_function_codes as (
    select bk_specialty_code, treatment_function_description
    from {{ ref('stg_dictionary_dbo_specialties') }}
    where is_treatment_function = true
    qualify row_number() over (
        partition by bk_specialty_code
        order by date_updated desc nulls last, date_created desc nulls last
    ) = 1
    ),
snomed_codes as (
    select snomed_code, preferred_term
    from {{ ref('stg_dictionary_snomed_concept') }}
    ),
diagnosis_dictionary as (
    select snomed_code, snomed_uk_preferred_term
    from {{ ref('stg_dictionary_ecds_diagnosis') }}
    qualify row_number() over (
        partition by snomed_code
        order by dv_is_active desc nulls last, valid_to desc nulls first, valid_from desc nulls last
    ) = 1
    ),
treatment_dictionary as (
    select snomed_code, snomed_uk_preferred_term
    from {{ ref('stg_dictionary_ecds_treatment') }}
    qualify row_number() over (
        partition by snomed_code
        order by dv_is_active desc nulls last, valid_to desc nulls first, valid_from desc nulls last
    ) = 1
    ),
investigation_dictionary as (
    select snomed_code, snomed_uk_preferred_term
    from {{ ref('stg_dictionary_ecds_investigation') }}
    qualify row_number() over (
        partition by snomed_code
        order by dv_is_active desc nulls last, valid_to desc nulls first, valid_from desc nulls last
    ) = 1
    ),
comorbidity_dictionary as (
    select snomed_code, snomed_uk_preferred_term
    from {{ ref('stg_dictionary_ecds_comorbidity') }}
    qualify row_number() over (
        partition by snomed_code
        order by dv_is_active desc nulls last, valid_to desc nulls first, valid_from desc nulls last
    ) = 1
    ),
diagnosis_codes_wide as (
    select
        primarykey_id
        , max(case when snomed_id = 1 then code end) as primary_diagnosis_code_snomed
        , max(case when snomed_id = 2 then code end) as secondary_diagnosis_1_code_snomed
        , max(case when snomed_id = 3 then code end) as secondary_diagnosis_2_code_snomed
        , max(case when snomed_id = 4 then code end) as secondary_diagnosis_3_code_snomed
        , max(case when snomed_id = 5 then code end) as secondary_diagnosis_4_code_snomed
        , max(case when snomed_id = 6 then code end) as secondary_diagnosis_5_code_snomed
        , max(case when snomed_id = 7 then code end) as secondary_diagnosis_6_code_snomed
        , max(case when snomed_id = 8 then code end) as secondary_diagnosis_7_code_snomed
        , max(case when snomed_id = 9 then code end) as secondary_diagnosis_8_code_snomed
        , max(case when snomed_id = 10 then code end) as secondary_diagnosis_9_code_snomed
        , max(case when snomed_id = 11 then code end) as secondary_diagnosis_10_code_snomed
        , max(case when snomed_id = 12 then code end) as secondary_diagnosis_11_code_snomed
        , max(case when snomed_id = 13 then code end) as secondary_diagnosis_12_code_snomed
        , max(case when snomed_id = 2 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as secondary_diagnosis_1_desc_snomed
        , max(case when snomed_id = 3 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as secondary_diagnosis_2_desc_snomed
        , max(case when snomed_id = 4 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as secondary_diagnosis_3_desc_snomed
        , max(case when snomed_id = 5 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as secondary_diagnosis_4_desc_snomed
        , max(case when snomed_id = 6 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as secondary_diagnosis_5_desc_snomed
        , max(case when snomed_id = 7 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as secondary_diagnosis_6_desc_snomed
        , max(case when snomed_id = 8 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as secondary_diagnosis_7_desc_snomed
        , max(case when snomed_id = 9 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as secondary_diagnosis_8_desc_snomed
        , max(case when snomed_id = 10 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as secondary_diagnosis_9_desc_snomed
        , max(case when snomed_id = 11 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as secondary_diagnosis_10_desc_snomed
        , max(case when snomed_id = 12 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as secondary_diagnosis_11_desc_snomed
        , max(case when snomed_id = 13 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as secondary_diagnosis_12_desc_snomed
    from {{ ref('stg_sus_ecds_clinical_diagnoses_snomed') }} as c
    left join diagnosis_dictionary as d on c.code = d.snomed_code
    left join snomed_codes as s on c.code = s.snomed_code
    where snomed_id between 1 and 13
    group by primarykey_id
    ),
investigation_codes_wide as (
    select
        primarykey_id
        , max(case when snomed_id = 1 then code end) as clinical_investigation_1_code_snomed
        , max(case when snomed_id = 2 then code end) as clinical_investigation_2_code_snomed
        , max(case when snomed_id = 3 then code end) as clinical_investigation_3_code_snomed
        , max(case when snomed_id = 4 then code end) as clinical_investigation_4_code_snomed
        , max(case when snomed_id = 5 then code end) as clinical_investigation_5_code_snomed
        , max(case when snomed_id = 6 then code end) as clinical_investigation_6_code_snomed
        , max(case when snomed_id = 7 then code end) as clinical_investigation_7_code_snomed
        , max(case when snomed_id = 8 then code end) as clinical_investigation_8_code_snomed
        , max(case when snomed_id = 9 then code end) as clinical_investigation_9_code_snomed
        , max(case when snomed_id = 10 then code end) as clinical_investigation_10_code_snomed
        , max(case when snomed_id = 11 then code end) as clinical_investigation_11_code_snomed
        , max(case when snomed_id = 12 then code end) as clinical_investigation_12_code_snomed
        , max(case when snomed_id = 2 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as clinical_investigation_2_desc_snomed
        , max(case when snomed_id = 3 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as clinical_investigation_3_desc_snomed
        , max(case when snomed_id = 4 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as clinical_investigation_4_desc_snomed
        , max(case when snomed_id = 5 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as clinical_investigation_5_desc_snomed
        , max(case when snomed_id = 6 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as clinical_investigation_6_desc_snomed
        , max(case when snomed_id = 7 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as clinical_investigation_7_desc_snomed
        , max(case when snomed_id = 8 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as clinical_investigation_8_desc_snomed
        , max(case when snomed_id = 9 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as clinical_investigation_9_desc_snomed
        , max(case when snomed_id = 10 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as clinical_investigation_10_desc_snomed
        , max(case when snomed_id = 11 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as clinical_investigation_11_desc_snomed
        , max(case when snomed_id = 12 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as clinical_investigation_12_desc_snomed
    from {{ ref('stg_sus_ecds_clinical_investigations_snomed') }} as c
    left join investigation_dictionary as d on c.code = d.snomed_code
    left join snomed_codes as s on c.code = s.snomed_code
    where snomed_id between 1 and 12
    group by primarykey_id
    ),
treatment_codes_wide as (
    select
        primarykey_id
        , max(case when snomed_id = 1 then code end) as treatment_1_code_snomed
        , max(case when snomed_id = 2 then code end) as treatment_2_code_snomed
        , max(case when snomed_id = 3 then code end) as treatment_3_code_snomed
        , max(case when snomed_id = 4 then code end) as treatment_4_code_snomed
        , max(case when snomed_id = 5 then code end) as treatment_5_code_snomed
        , max(case when snomed_id = 6 then code end) as treatment_6_code_snomed
        , max(case when snomed_id = 7 then code end) as treatment_7_code_snomed
        , max(case when snomed_id = 8 then code end) as treatment_8_code_snomed
        , max(case when snomed_id = 9 then code end) as treatment_9_code_snomed
        , max(case when snomed_id = 10 then code end) as treatment_10_code_snomed
        , max(case when snomed_id = 11 then code end) as treatment_11_code_snomed
        , max(case when snomed_id = 12 then code end) as treatment_12_code_snomed
        , max(case when snomed_id = 2 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as treatment_2_desc_snomed
        , max(case when snomed_id = 3 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as treatment_3_desc_snomed
        , max(case when snomed_id = 4 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as treatment_4_desc_snomed
        , max(case when snomed_id = 5 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as treatment_5_desc_snomed
        , max(case when snomed_id = 6 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as treatment_6_desc_snomed
        , max(case when snomed_id = 7 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as treatment_7_desc_snomed
        , max(case when snomed_id = 8 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as treatment_8_desc_snomed
        , max(case when snomed_id = 9 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as treatment_9_desc_snomed
        , max(case when snomed_id = 10 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as treatment_10_desc_snomed
        , max(case when snomed_id = 11 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as treatment_11_desc_snomed
        , max(case when snomed_id = 12 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as treatment_12_desc_snomed
    from {{ ref('stg_sus_ecds_clinical_treatments_snomed') }} as c
    left join treatment_dictionary as d on c.code = d.snomed_code
    left join snomed_codes as s on c.code = s.snomed_code
    where snomed_id between 1 and 12
    group by primarykey_id
    ),
comorbidity_codes_wide as (
    select
        primarykey_id
        , max(case when comorbidities_id = 1 then code end) as comorbidity_1_code_snomed
        , max(case when comorbidities_id = 2 then code end) as comorbidity_2_code_snomed
        , max(case when comorbidities_id = 3 then code end) as comorbidity_3_code_snomed
        , max(case when comorbidities_id = 4 then code end) as comorbidity_4_code_snomed
        , max(case when comorbidities_id = 5 then code end) as comorbidity_5_code_snomed
        , max(case when comorbidities_id = 6 then code end) as comorbidity_6_code_snomed
        , max(case when comorbidities_id = 7 then code end) as comorbidity_7_code_snomed
        , max(case when comorbidities_id = 8 then code end) as comorbidity_8_code_snomed
        , max(case when comorbidities_id = 9 then code end) as comorbidity_9_code_snomed
        , max(case when comorbidities_id = 10 then code end) as comorbidity_10_code_snomed
        , max(case when comorbidities_id = 1 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as comorbidity_1_desc_snomed
        , max(case when comorbidities_id = 2 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as comorbidity_2_desc_snomed
        , max(case when comorbidities_id = 3 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as comorbidity_3_desc_snomed
        , max(case when comorbidities_id = 4 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as comorbidity_4_desc_snomed
        , max(case when comorbidities_id = 5 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as comorbidity_5_desc_snomed
        , max(case when comorbidities_id = 6 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as comorbidity_6_desc_snomed
        , max(case when comorbidities_id = 7 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as comorbidity_7_desc_snomed
        , max(case when comorbidities_id = 8 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as comorbidity_8_desc_snomed
        , max(case when comorbidities_id = 9 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as comorbidity_9_desc_snomed
        , max(case when comorbidities_id = 10 then coalesce(d.snomed_uk_preferred_term, s.preferred_term) end) as comorbidity_10_desc_snomed
    from {{ ref('stg_sus_ecds_clinical_comorbidities') }} as c
    left join comorbidity_dictionary as d on c.code = d.snomed_code
    left join snomed_codes as s on c.code = s.snomed_code
    where comorbidities_id between 1 and 10
    group by primarykey_id
    ),
first_expected_treatment as (
    select primarykey_id, expected_treatment_at
    from {{ ref('stg_sus_ecds_attendance_expected_treatment_times') }}
    qualify row_number() over (
        partition by primarykey_id
        order by expected_treatment_times_id, rownumber_id
    ) = 1
    ),
first_referred_to_service as (
    select
        r.primarykey_id
        , r.referred_to_service_code
        , d.snomed_uk_preferred_term as referred_to_service_desc
        , r.assessment_date
        , r.assessment_time
    from {{ ref('stg_sus_ecds_attendance_referred_to') }} as r
    left join {{ ref('stg_dictionary_ecds_referredtoservice') }} as d
        on r.referred_to_service_code = d.snomed_code
    qualify row_number() over (
        partition by r.primarykey_id
        order by r.referred_to_id, r.rownumber_id
    ) = 1
    ),
first_injury_alcohol_drug_involvement as (
    select
        i.primarykey_id
        , i.code
        , d.snomed_uk_preferred_term as description
    from {{ ref('stg_sus_ecds_clinical_injury_alcohol_drug_involvements') }} as i
    left join {{ ref('stg_dictionary_ecds_injurydrugalcohol') }} as d
        on i.code = d.snomed_code
    qualify row_number() over (
        partition by i.primarykey_id
        order by i.alcohol_drug_involvements_id, i.rownumber_id
    ) = 1
    ),
first_mental_health_legal_status as (
    select primarykey_id, legal_status_code
    from {{ ref('stg_sus_ecds_patient_mental_health_act_legal_status') }}
    qualify row_number() over (
        partition by primarykey_id
        order by mental_health_act_legal_status_id, rownumber_id
    ) = 1
    )

select 
    /* Information needed to derive standard encounter information */
    core.primarykey_id as visit_occurrence_id
    , core.sk_patient_id
    , 'SUS_ECDS' as source
    , core.local_patient_identifier
    , core.system_transaction_cds_unique_identifier as cds_unique_identifier
    , core.commissioning_service_agreement_provider_reference_number as provider_reference_number

    /* Location */
    ,  {{ clean_organisation_id('attendance_location_hes_provider_3') }} as organisation_id
    , dict_org.organisation_name as organisation_name
    , core.attendance_location_site as site_id
    , dict_site.organisation_name as site_name
    , case
        when core.attendance_location_department_type = '01' then 'AE-T1'
        when core.attendance_location_department_type = '02' then 'AE-Other'
        when core.attendance_location_department_type = '03' then 'UCC'
        when core.attendance_location_department_type = '04' then 'WiC'
        when core.attendance_location_department_type = '05' then 'SDEC'
        else 'Others' end as pod
    , core.attendance_location_department_type as department_type
    , core.attendance_location_activity_type as urgent_care_setting_type

    /* Time & date */
    , core.attendance_arrival_date as start_date
    , core.attendance_arrival_time as start_time
    , {{ fin_year_from_date('core.attendance_arrival_date') }} as financial_year
    , {{ fin_month_from_date('core.attendance_arrival_date') }} as financial_month
    , core.attendance_departure_date as end_date
    , core.attendance_departure_time as end_time
    , core.attendance_departure_time_since_arrival as duration
    , core.attendance_initial_assessment_date as initial_assessment_date
    , core.attendance_initial_assessment_time as initial_assessment_time
    , core.attendance_initial_assessment_time_since_arrival as initial_assessment_time_since_arrival
    , core.attendance_seen_for_treatment_date as seen_for_treatment_date
    , core.attendance_seen_for_treatment_time as seen_for_treatment_time
    , core.attendance_seen_for_treatment_time_since_arrival as seen_for_treatment_time_since_arrival
    , core.attendance_conclusion_date as conclusion_date
    , core.attendance_conclusion_time as conclusion_time
    , core.attendance_conclusion_time_since_arrival as conclusion_time_since_arrival
    , core.attendance_decision_to_admit_date as decided_to_admit_date
    , core.attendance_decision_to_admit_time as decided_to_admit_time
    , core.attendance_decision_to_admit_time_since_arrival as decided_to_admit_time_since_arrival
    , core.attendance_clinically_ready_to_proceed_timestamp as clinically_ready_to_proceed_at
    , core.attendance_clinically_ready_to_proceed_time_since_arrival
        as clinically_ready_to_proceed_time_since_arrival
    , expected_treatment.expected_treatment_at as expected_treatment_time

    /* Clinical information */
    -- complaint information
    , core.clinical_chief_complaint_code as chief_complaint_code
    , dict_complaint.snomed_uk_preferred_term as chief_complaint_desc
    , dict_complaint.ecds_group1 as chief_complaint_ecds_group1
    , core.clinical_chief_complaint_is_injury_related as is_injury_related
    , core.clinical_acuity_code as acuity

    -- injury information is kept with the complaint fields for maintainability
    , core.clinical_injury_intent_code as injury_intent_code
    , injury_intent.snomed_uk_preferred_term as injury_intent_desc
    , core.clinical_injury_mechanism_code as injury_mechanism_code
    , injury_mechanism.snomed_uk_preferred_term as injury_mechanism_desc
    , core.clinical_injury_place_type as place_of_injury_code
    , place_of_injury.snomed_uk_preferred_term as place_of_injury_desc
    , core.clinical_injury_date as injury_date
    , core.clinical_injury_time as injury_time
    , core.clinical_disease_notification_code as disease_notification_code
    , disease_notification.preferred_term as disease_notification_desc
    , injury_alcohol_drug.code as injury_alcohol_drug_involvement_code
    , injury_alcohol_drug.description as injury_alcohol_drug_involvement_desc

    -- diagnosis information
    , diagnosis.primary_diagnosis_code_snomed
    , diag_dict.snomed_uk_preferred_term as primary_diagnosis_desc_snomed
    , diag_dict.icd10_mapping as primary_diagnosis_code_icd10
    , diag_dict.icd10_description as primary_diagnosis_desc_icd10
    , diag_dict.ecds_group1 as primary_diagnosis_desc_ecds_group1
    , diagnosis.secondary_diagnosis_1_code_snomed
    , diagnosis.secondary_diagnosis_1_desc_snomed
    , diagnosis.secondary_diagnosis_2_code_snomed
    , diagnosis.secondary_diagnosis_2_desc_snomed
    , diagnosis.secondary_diagnosis_3_code_snomed
    , diagnosis.secondary_diagnosis_3_desc_snomed
    , diagnosis.secondary_diagnosis_4_code_snomed
    , diagnosis.secondary_diagnosis_4_desc_snomed
    , diagnosis.secondary_diagnosis_5_code_snomed
    , diagnosis.secondary_diagnosis_5_desc_snomed
    , diagnosis.secondary_diagnosis_6_code_snomed
    , diagnosis.secondary_diagnosis_6_desc_snomed
    , diagnosis.secondary_diagnosis_7_code_snomed
    , diagnosis.secondary_diagnosis_7_desc_snomed
    , diagnosis.secondary_diagnosis_8_code_snomed
    , diagnosis.secondary_diagnosis_8_desc_snomed
    , diagnosis.secondary_diagnosis_9_code_snomed
    , diagnosis.secondary_diagnosis_9_desc_snomed
    , diagnosis.secondary_diagnosis_10_code_snomed
    , diagnosis.secondary_diagnosis_10_desc_snomed
    , diagnosis.secondary_diagnosis_11_code_snomed
    , diagnosis.secondary_diagnosis_11_desc_snomed
    , diagnosis.secondary_diagnosis_12_code_snomed
    , diagnosis.secondary_diagnosis_12_desc_snomed
    
    -- treatment 
    , treatments.treatment_1_code_snomed as primary_treatment
    , treat_dict.snomed_uk_preferred_term as primary_treatment_desc_snomed
    , treat_dict.ecds_group1 as primary_treatment_desc_ecds_group1
    , treatments.treatment_1_code_snomed
    , treatments.treatment_2_code_snomed
    , treatments.treatment_2_desc_snomed
    , treatments.treatment_3_code_snomed
    , treatments.treatment_3_desc_snomed
    , treatments.treatment_4_code_snomed
    , treatments.treatment_4_desc_snomed
    , treatments.treatment_5_code_snomed
    , treatments.treatment_5_desc_snomed
    , treatments.treatment_6_code_snomed
    , treatments.treatment_6_desc_snomed
    , treatments.treatment_7_code_snomed
    , treatments.treatment_7_desc_snomed
    , treatments.treatment_8_code_snomed
    , treatments.treatment_8_desc_snomed
    , treatments.treatment_9_code_snomed
    , treatments.treatment_9_desc_snomed
    , treatments.treatment_10_code_snomed
    , treatments.treatment_10_desc_snomed
    , treatments.treatment_11_code_snomed
    , treatments.treatment_11_desc_snomed
    , treatments.treatment_12_code_snomed
    , treatments.treatment_12_desc_snomed
    
    -- investigation 
    , investigations.clinical_investigation_1_code_snomed as primary_investigation
    , inv_dict.snomed_uk_preferred_term as primary_investigation_desc_snomed
    , inv_dict.ecds_group1 as primary_investigation_desc_ecds_group1
    , investigations.clinical_investigation_1_code_snomed
    , investigations.clinical_investigation_2_code_snomed
    , investigations.clinical_investigation_2_desc_snomed
    , investigations.clinical_investigation_3_code_snomed
    , investigations.clinical_investigation_3_desc_snomed
    , investigations.clinical_investigation_4_code_snomed
    , investigations.clinical_investigation_4_desc_snomed
    , investigations.clinical_investigation_5_code_snomed
    , investigations.clinical_investigation_5_desc_snomed
    , investigations.clinical_investigation_6_code_snomed
    , investigations.clinical_investigation_6_desc_snomed
    , investigations.clinical_investigation_7_code_snomed
    , investigations.clinical_investigation_7_desc_snomed
    , investigations.clinical_investigation_8_code_snomed
    , investigations.clinical_investigation_8_desc_snomed
    , investigations.clinical_investigation_9_code_snomed
    , investigations.clinical_investigation_9_desc_snomed
    , investigations.clinical_investigation_10_code_snomed
    , investigations.clinical_investigation_10_desc_snomed
    , investigations.clinical_investigation_11_code_snomed
    , investigations.clinical_investigation_11_desc_snomed
    , investigations.clinical_investigation_12_code_snomed
    , investigations.clinical_investigation_12_desc_snomed

    -- comorbidities are pivoted by the source sequence identifier
    , comorbidities.comorbidity_1_code_snomed
    , comorbidities.comorbidity_1_desc_snomed
    , comorbidities.comorbidity_2_code_snomed
    , comorbidities.comorbidity_2_desc_snomed
    , comorbidities.comorbidity_3_code_snomed
    , comorbidities.comorbidity_3_desc_snomed
    , comorbidities.comorbidity_4_code_snomed
    , comorbidities.comorbidity_4_desc_snomed
    , comorbidities.comorbidity_5_code_snomed
    , comorbidities.comorbidity_5_desc_snomed
    , comorbidities.comorbidity_6_code_snomed
    , comorbidities.comorbidity_6_desc_snomed
    , comorbidities.comorbidity_7_code_snomed
    , comorbidities.comorbidity_7_desc_snomed
    , comorbidities.comorbidity_8_code_snomed
    , comorbidities.comorbidity_8_desc_snomed
    , comorbidities.comorbidity_9_code_snomed
    , comorbidities.comorbidity_9_desc_snomed
    , comorbidities.comorbidity_10_code_snomed
    , comorbidities.comorbidity_10_desc_snomed
    
    /* Arrival information */
    -- arrival mode and desc
    , core.attendance_arrival_arrival_mode_code as arrival_mode_code
    , dict_arrival.snomed_uk_preferred_term as arrival_mode_desc
    , core.attendance_arrival_attendance_category as attendance_category_code
    , case core.attendance_arrival_attendance_category
        when '1' then 'Unplanned first attendance for a new or deteriorating condition'
        when '2' then 'Unplanned follow-up within 7 days at this emergency care service'
        when '3' then 'Unplanned follow-up within 7 days at another emergency care service'
        when '4' then 'Planned follow-up within 7 days at this emergency care service'
        when 'X' then 'Not applicable - patient dead on arrival'
      end as attendance_category_desc
    , core.attendance_arrival_planned as is_arrival_planned
    , core.attendance_arrival_ambulance_incident_number as ambulance_incident_number
    , core.attendance_arrival_conveying_ambulance_trust as conveying_ambulance_trust_code
    , ambulance_trust.organisation_name as conveying_ambulance_trust_name
    , core.attendance_arrival_ambulance_care_contact_identifier
        as ambulance_care_contact_identifier
    , core.attendance_arrival_attendance_source_code as attendance_source_code
    , attendance_source.snomed_uk_preferred_term as attendance_source_desc
    , core.attendance_arrival_attendance_source_organisation
        as attendance_source_organisation_site_identifier
    , coalesce(
        attendance_source_site.organisation_name,
        attendance_source_provider.organisation_name
      ) as attendance_source_organisation_site_name

    /* Discharge information */
    , core.attendance_discharge_destination_code as discharge_destination_code
    , dict_dist.snomed_uk_preferred_term as discharge_destination_desc
    , core.attendance_discharge_status_code as discharge_status_code
    , discharge_status.snomed_uk_preferred_term as discharge_status_desc
    , core.attendance_discharge_follow_up_code as discharge_follow_up_code
    , discharge_follow_up.snomed_uk_preferred_term as discharge_follow_up_desc
    , core.attendance_discharge_information_given_code as discharge_information_given_code
    , discharge_information.preferred_term as discharge_information_given_desc
    , core.attendance_decision_to_admit_treatment_function_code
        as decided_to_admit_treatment_function_code
    , treatment_function.treatment_function_description
        as decided_to_admit_treatment_function_desc
    , core.attendance_decision_to_admit_receiving_site as receiving_site_id
    , coalesce(
        receiving_site.organisation_name,
        receiving_site_provider.organisation_name
      ) as receiving_site_name

    /* Clinician information */
    , '180' as main_specialty_code
    , 'Emergency Medicine' as main_specialty_name

    /* Commissioning information */
    , core.commissioning_grouping_health_resource_group as hrg_code
    , dict_hrg.hrg_description as core_hrg_desc
    , dict_hrg.hrg_chapter_key as core_hrg_chapter
    , dict_hrg.hrg_chapter as core_hrg_chapter_desc
    , core.commissioning_national_pricing_final_price as cost
    , core.commissioning_national_pricing_costing_period as applicable_costing_period
    , core.commissioning_national_pricing_excluded as is_national_tariff_excluded
    , core.commissioning_national_pricing_tariff as national_tariff
    , core.commissioning_national_pricing_final_price as national_tariff_final_price
    , core.commissioning_national_pricing_market_forces_factor as mff_factor
    , core.commissioning_national_pricing_market_forces_adjustment as mff_adjustment
    , core.patient_residence_ccg_from_patient_postcode as residence_commissioner_code_at_event
    , residence_commissioner.commissioner_name as residence_commissioner_name_at_event
    , core.commissioning_service_agreement_commissioner as registrant_commissioner_code_at_event
    , registrant_commissioner.commissioner_name as registrant_commissioner_name_at_event

    /* patient information at time of event */
    , core.patient_age_at_arrival as age_at_event
    , core.patient_patient_type as patient_type_code
    , case
        when core.patient_patient_type = 'ADU' then 'Adult'
        when core.patient_patient_type = 'CHI' then 'Child'
        else core.patient_patient_type
      end as patient_type_desc
    , core.patient_stated_gender as gender_at_event
    , gen.gender as gender_desc_at_event
    , core.patient_ethnic_category as ethnicity_at_event
    , eth.ethnicity_desc as ethnicity_desc_at_event
    , core.patient_usual_address_postcode_district as postcode_district_at_event
    , core.patient_usual_address_lsoa_11 as lsoa_11_at_event
    , core.patient_usual_address_local_authority_district as lad_at_event
    , core.patient_usual_address_index_of_multiple_deprivation_decile as imd_at_event
    , core.patient_gp_registration_general_practice as reg_practice_at_event
    , core.patient_gp_registration_general_practitioner as general_practitioner_code
    , general_practitioner.gp_name as general_practitioner_name
    , 'AE_ATTENDANCE' as visit_occurrence_type

    /* Referral to treatment information */
    , core.referral_patient_pathway_identifier_value_pseudo as patient_pathway_identifier
    , core.referral_period_status as referral_to_treatment_status_code
    , rtt_status.rtt_period_status_description as referral_to_treatment_status_desc
    , core.referral_period_start_date as referral_to_treatment_period_start_date
    , core.referral_period_end_date as referral_to_treatment_period_end_date
    , core.referral_waiting_time_measurement_type as waiting_time_measurement_type_code
    , referred_to.referred_to_service_code
    , referred_to.referred_to_service_desc
    , referred_to.assessment_date as referred_to_service_assessment_date
    , referred_to.assessment_time as referred_to_service_assessment_time

    /* Mental health information */
    , mental_health_legal_status.legal_status_code as mental_health_legal_status_code

from {{ ref('stg_sus_ecds_emergency_care')}} as core

/* Diagnosis code for infering reason */ -- ADD DEDUP LOGIC?
left join diagnosis_codes_wide as diagnosis
    on core.primarykey_id = diagnosis.primarykey_id

left join {{ref('stg_dictionary_ecds_diagnosis')}} as diag_dict
    on diagnosis.primary_diagnosis_code_snomed = diag_dict.snomed_code

/* First investigation code for infering reason */ 
left join investigation_codes_wide as investigations
    on core.primarykey_id = investigations.primarykey_id

left join
    {{ ref('stg_dictionary_ecds_investigation') }} as inv_dict
    on investigations.clinical_investigation_1_code_snomed = inv_dict.snomed_code

/* First treatment for infering reason  */ 
left join treatment_codes_wide as treatments
    on core.primarykey_id = treatments.primarykey_id

left join
    {{ ref('stg_dictionary_ecds_treatment') }} as treat_dict
    on treatments.treatment_1_code_snomed = treat_dict.snomed_code

left join comorbidity_codes_wide as comorbidities
    on core.primarykey_id = comorbidities.primarykey_id

left join first_expected_treatment as expected_treatment
    on core.primarykey_id = expected_treatment.primarykey_id

left join first_referred_to_service as referred_to
    on core.primarykey_id = referred_to.primarykey_id

left join first_injury_alcohol_drug_involvement as injury_alcohol_drug
    on core.primarykey_id = injury_alcohol_drug.primarykey_id

left join first_mental_health_legal_status as mental_health_legal_status
    on core.primarykey_id = mental_health_legal_status.primarykey_id

/* context dictionaries  */
left join 
    {{ref('stg_dictionary_ecds_arrivalmode')}} as dict_arrival
    on core.attendance_arrival_arrival_mode_code = dict_arrival.snomed_code
left join 
    {{ref('stg_dictionary_ecds_dischargedestination')}} as dict_dist
    on core.attendance_discharge_destination_code = dict_dist.snomed_code

left join 
    {{ref('stg_dictionary_ecds_chiefcomplaint')}} as dict_complaint
    on core.clinical_chief_complaint_code = dict_complaint.snomed_code

left join {{ ref('stg_dictionary_ecds_injuryintent') }} as injury_intent
    on core.clinical_injury_intent_code = injury_intent.snomed_code

left join {{ ref('stg_dictionary_ecds_injurymechanism') }} as injury_mechanism
    on core.clinical_injury_mechanism_code = injury_mechanism.snomed_code

left join {{ ref('stg_dictionary_ecds_placeofinjury') }} as place_of_injury
    on core.clinical_injury_place_type = place_of_injury.snomed_code

left join {{ ref('stg_dictionary_ecds_attendancesource') }} as attendance_source
    on core.attendance_arrival_attendance_source_code = attendance_source.snomed_code

left join {{ ref('stg_dictionary_ecds_dischargestatus') }} as discharge_status
    on core.attendance_discharge_status_code = discharge_status.snomed_code

left join {{ ref('stg_dictionary_ecds_dischargefollowup') }} as discharge_follow_up
    on core.attendance_discharge_follow_up_code = discharge_follow_up.snomed_code

left join {{ ref('stg_dictionary_snomed_concept') }} as disease_notification
    on core.clinical_disease_notification_code = disease_notification.snomed_code

left join {{ ref('stg_dictionary_snomed_concept') }} as discharge_information
    on core.attendance_discharge_information_given_code = discharge_information.snomed_code

/* Demographic dictionaries */
left join ethnicity_codes as eth
    on core.patient_ethnic_category = eth.bk_ethnicity_code
    
left join gender_codes as gen
    on core.patient_stated_gender = gen.gender_code

-- NHS provider and site reference models are deduplicated to protect encounter grain.
left join {{ ref('organisation_nhs_site') }} as dict_site on
    core.attendance_location_site = dict_site.organisation_code

left join {{ ref('organisation_nhs_provider') }} as ambulance_trust on
    core.attendance_arrival_conveying_ambulance_trust = ambulance_trust.organisation_code

left join {{ ref('organisation_nhs_site') }} as attendance_source_site on
    core.attendance_arrival_attendance_source_organisation = attendance_source_site.organisation_code

-- A very small number of source values are provider rather than site codes.
left join {{ ref('organisation_nhs_provider') }} as attendance_source_provider on
    core.attendance_arrival_attendance_source_organisation
    = attendance_source_provider.organisation_code

left join {{ ref('organisation_nhs_site') }} as receiving_site on
    core.attendance_decision_to_admit_receiving_site = receiving_site.organisation_code

left join {{ ref('organisation_nhs_provider') }} as receiving_site_provider on
    core.attendance_decision_to_admit_receiving_site
    = receiving_site_provider.organisation_code

-- provider name
left join {{ ref('organisation_nhs_provider') }} as dict_org on
    core.attendance_location_hes_provider_3 = dict_org.organisation_code

left join
    {{ ref('stg_dictionary_dbo_hrg') }} as dict_hrg
    on core.commissioning_grouping_health_resource_group = dict_hrg.hrg_code

left join commissioner_codes as residence_commissioner
    on core.patient_residence_ccg_from_patient_postcode = residence_commissioner.commissioner_code

left join commissioner_codes as registrant_commissioner
    on core.commissioning_service_agreement_commissioner = registrant_commissioner.commissioner_code

left join treatment_function_codes as treatment_function
    on core.attendance_decision_to_admit_treatment_function_code = treatment_function.bk_specialty_code

left join {{ ref('stg_dictionary_dbo_gp') }} as general_practitioner
    on core.patient_gp_registration_general_practitioner = general_practitioner.gp_code

left join {{ ref('stg_dictionary_dbo_rttperiodstatus') }} as rtt_status
    on core.referral_period_status = rtt_status.rtt_period_status_code
