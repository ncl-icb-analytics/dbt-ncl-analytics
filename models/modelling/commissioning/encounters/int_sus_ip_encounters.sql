/*
Inpatient encounters from SUS

Clinical Purpose:
- Establishing use of inpatient services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/

select 
    /* Information needed to derive standard encounter information */
    core.primarykey_id as encounter_id
    , core.sk_patient_id
    , core.spell_care_location_site_code_of_treatment as site_id
    , core.spell_admission_date as start_date
    , dict_adm_method.admission_method_name as admission_method
    , dict_adm_method.admission_method_group as admission_method_group
    , dict_patient_class.patient_classification_name as admission_patient_classification
    , core.spell_discharge_date as end_date
    , core.spell_discharge_length_of_hospital_stay as duration
    , datediff(day, core.spell_admission_date, coalesce(core.spell_discharge_date, current_date)) as duration_to_date
    , core.spell_commissioning_grouping_core_hrg as acuity_proxy
    , core.spell_clinical_coding_grouper_derived_primary_diagnosis  || ', ' || core.spell_clinical_coding_grouper_derived_secondary_diagnosis  as flat_diagnosis_codes
    , core.spell_clinical_coding_grouper_derived_dominant_procedure as primary_treatment
    , 'SUS_APC' as source
    , case 
        when core.spell_admission_method in ('11', '12', '13') -- dict_adm_method.admission_method_group = 'Elective'
            and core.spell_admission_patient_classification = '2' -- dict_patient_class.patient_classification_name = 'Day case admission'
            then 'DC' -- Day Case
        when core.spell_admission_method in ('11', '12', '13') -- dict_adm_method.admission_method_group = 'Elective'
            and core.spell_admission_patient_classification = '1' -- dict_patient_class.patient_classification_name = 'Ordinary admission'
            then 'EL' -- Elective
        when core.spell_admission_method in ('11', '12', '13') --  dict_adm_method.admission_method_group = 'Elective'
            and core.spell_admission_patient_classification in ('3', '4') -- dict_patient_class.patient_classification_name in ('Regular day admission', 'Regular night admission')
            then 'RA' -- Regular Attender (day & night)
        when core.spell_admission_method in ('21', '22', '23', '24', '25', '28','2A','2B','2C','2D') -- dict_adm_method.admission_method_group = 'Non-elective - emergency'
            and datediff(day, core.spell_admission_date, coalesce(core.spell_discharge_date, current_date)) = 0 
            then 'NEL-ZLOS'
        when core.spell_admission_method in ('21', '22', '23', '24', '25', '28','2A','2B','2C','2D') -- dict_adm_method.admission_method_group = 'Non-elective - emergency'
            and datediff(day, core.spell_admission_date, coalesce(core.spell_discharge_date, current_date)) >= 1 
            then 'NEL-LOS+1'
        when core.spell_admission_method in ('31', '32','82', '83') -- dict_adm_method.admission_method_group in ('Non-elective - Maternity') or dict_adm_method.admission_method_name in ('The birth of a baby', 'Baby born outside the Provider')
            then 'NELNE'
        when core.spell_admission_method = '81' -- dict_adm_method.admission_method_name = 'Transfer'
            then 'TRANSFERS'
        else 'OTHER' end as pod
    , iff(core.spell_admission_admission_sub_type = 'NON', core.spell_admission_admission_type, core.spell_admission_admission_sub_type) as type
    , core.spell_commissioning_tariff_calculation_final_price as cost

from {{ ref('stg_sus_apc_spell')}} as core

left join {{ ref('stg_dictionary_ip_admissionmethods')}} as dict_adm_method
    ON core.spell_admission_method = dict_adm_method.bk_admission_method_code

left join {{ ref('stg_dictionary_dbo_patientclassification')}} as dict_patient_class
    ON core.spell_admission_patient_classification = dict_patient_class.bk_patient_classification_code
