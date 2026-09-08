select
    source_table
    , clinical_record_type
    , count(*) as record_count
    , count(distinct source_record_id) as unique_items
    , sum(accepted_source_record_count) as represented_source_rows
    , count_if(sk_patient_id is null) as missing_patient_key
    , count_if(clinical_at is null) as missing_clinical_time
    , count_if(clinical_code is not null and clinical_description is null) as unlabelled_codes
    , count_if(clinical_value is not null and clinical_code is null) as values_without_codes
    , count_if(is_source_derived_date_inconsistent) as derived_date_disagreements
    , count_if(is_care_activity_linked = false) as missing_activity_parents
    , count_if(is_care_activity_person_consistent = false) as activity_person_disagreements
    , count_if(is_care_contact_person_consistent = false) as contact_person_disagreements
    , count_if(is_clinical_date_after_reporting_period) as dates_after_reporting_period
    , count_if(is_assessment_before_reporting_period) as assessments_before_reporting_period
    , count_if(is_assessment_code_in_activity_component) as assessment_codes_in_components
from {{ ref('fct_csds_clinical_record') }}
group by source_table, clinical_record_type;

select clinical_record_type, assessment_response_status, count(*) as record_count
from {{ ref('fct_csds_clinical_record') }}
where assessment_response_status is not null
group by clinical_record_type, assessment_response_status;

select coding_scheme_kind, clinical_label_status, count(*) as record_count
from {{ ref('fct_csds_clinical_record') }}
group by coding_scheme_kind, clinical_label_status;

select unit_of_measurement_match_type, count(*) as observation_count,
    count_if(unit_of_measurement_code is null) as missing_unit
from {{ ref('fct_csds_clinical_record') }}
where clinical_record_type = 'observation'
group by unit_of_measurement_match_type;
