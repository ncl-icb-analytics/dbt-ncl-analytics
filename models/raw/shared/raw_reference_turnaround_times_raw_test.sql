-- Raw layer model for reference_analyst_managed.TURNAROUND_TIMES_RAW_TEST
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "submission_date" as submission_date,
    "data_type" as data_type,
    "month" as month,
    "year" as year,
    "ethnic_category" as ethnic_category,
    "person_gender" as person_gender,
    "general_medical_practice" as general_medical_practice,
    "patient_source_type" as patient_source_type,
    "referrer_code" as referrer_code,
    "referring_org" as referring_org,
    "diagnostic_test_request_date_time" as diagnostic_test_request_date_time,
    "diagnostic_test_request_received_date_time" as diagnostic_test_request_received_date_time,
    "diagnostic_test_date_time" as diagnostic_test_date_time,
    "service_report_issue_date_time" as service_report_issue_date_time,
    "imaging_code_nicip" as imaging_code_nicip,
    "imaging_code_snomed" as imaging_code_snomed,
    "provider_site_code" as provider_site_code,
    "priority_type_code" as priority_type_code,
    "cancer_pathway_flag" as cancer_pathway_flag,
    "file_name" as file_name,
    "month_year" as month_year,
    "submission_month" as submission_month,
    "submission_year" as submission_year,
    "data_period" as data_period,
    "trust_code" as trust_code,
    "combined_imaging_code" as combined_imaging_code,
    "priority_type_code_routine_default" as priority_type_code_routine_default,
    "TAT_scan" as tat_scan,
    "TAT_report" as tat_report,
    "TAT_overall" as tat_overall,
    "datedifftest" as datedifftest,
    "cancer_pathway_flag_string" as cancer_pathway_flag_string,
    "referring_organisation" as referring_organisation
from {{ source('reference_analyst_managed', 'TURNAROUND_TIMES_RAW_TEST') }}
