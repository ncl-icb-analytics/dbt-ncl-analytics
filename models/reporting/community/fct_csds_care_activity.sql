select
    {{ dbt_utils.generate_surrogate_key(['a.unique_submission_id', 'a.unique_care_activity_identifier']) }} as source_record_id
    , 'CSDS' as source_dataset
    , a.cyp202_unique_id as source_row_id
    , a.unique_care_activity_identifier
    , a.care_activity_identifier
    , a.unique_care_contact_identifier
    , a.care_contact_identifier
    , c.unique_service_request_identifier
    , case when c.cyp201_unique_id is not null then
        {{ dbt_utils.generate_surrogate_key(['c.unique_service_request_identifier', 'c.unique_care_contact_identifier']) }}
    end as contact_source_record_id
    , c.cyp201_unique_id as contact_source_row_id
    , c.unique_service_request_identifier as referral_source_record_id
    , a.person_id
    , b.sk_patient_id
    , c.care_contact_date::date as care_contact_date
    , c.care_contact_time::time as care_contact_time
    , case
        when c.care_contact_date is null then null
        when c.care_contact_time is null then c.care_contact_date::date::timestamp_ntz
        else timestamp_ntz_from_parts(c.care_contact_date::date, c.care_contact_time::time)
    end as care_contact_at
    , case
        when c.care_contact_date is null then null
        when c.care_contact_time is null then 'date'
        else 'timestamp'
    end as care_contact_time_precision
    , c.ic_age_at_care_contact_date as age_at_contact
    , c.activity_location_type_code
    , loc.description as activity_location_type_name
    , a.community_care_activity_type as activity_type_code
    , activity_type.description as activity_type_name
    , a.clinical_contact_duration_of_care_activity as clinical_contact_duration_minutes
    , a.care_professional_local_identifier
    , a.unique_care_professional_id_local
    , a.procedure_scheme_in_use_community_care
    , procedure_scheme.description as procedure_scheme_name
    , a.coded_procedure_clinical_terminology
    , a.finding_scheme_in_use_community_care
    , finding_scheme.description as finding_scheme_name
    , a.coded_finding_coded_clinical_entry
    , a.observation_scheme_in_use_community_care
    , observation_scheme.description as observation_scheme_name
    , a.coded_observation_clinical_terminology
    , a.observation_value
    , a.ucum_unit_of_measurement
    , c.cyp201_unique_id is not null as is_contact_linked
    , case when c.cyp201_unique_id is null or a.person_id is null or c.person_id is null then null
        else a.person_id = c.person_id end as is_contact_person_consistent
    , a.organisation_code_provider as provider_organisation_code
    , a.unique_submission_id
    , a.reporting_period_start_date::date as reporting_period_start_date
    , a.reporting_period_end_date::date as reporting_period_end_date
    , a.effective_from as source_file_received_at
    , a.csds_version
    , a.file_type
from {{ ref('stg_csds_care_activity_history') }} as a
left join {{ ref('stg_csds_care_contact_history') }} as c
    on a.unique_submission_id = c.unique_submission_id
    and a.unique_care_contact_identifier = c.unique_care_contact_identifier
left join {{ ref('stg_csds_bridging') }} as b on a.person_id = b.person_id
left join {{ ref('activity_location_type') }} as loc on trim(c.activity_location_type_code) = loc.code
left join {{ ref('csds_activity_code_lookup') }} as activity_type
    on activity_type.code_set_name = 'activity_type' and upper(trim(a.community_care_activity_type)) = activity_type.code
left join {{ ref('csds_activity_code_lookup') }} as procedure_scheme
    on procedure_scheme.code_set_name = 'procedure_scheme' and upper(trim(a.procedure_scheme_in_use_community_care)) = procedure_scheme.code
left join {{ ref('csds_activity_code_lookup') }} as finding_scheme
    on finding_scheme.code_set_name = 'finding_scheme' and upper(trim(a.finding_scheme_in_use_community_care)) = finding_scheme.code
left join {{ ref('csds_activity_code_lookup') }} as observation_scheme
    on observation_scheme.code_set_name = 'observation_scheme' and upper(trim(a.observation_scheme_in_use_community_care)) = observation_scheme.code
