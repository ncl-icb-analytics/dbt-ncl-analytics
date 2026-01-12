{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.TURNAROUND_TIMES_RAW \ndbt: source(''reference_analyst_managed'', ''TURNAROUND_TIMES_RAW'') \nColumns:\n  submission_date -> submission_date\n  data_type -> data_type\n  month -> month\n  year -> year\n  ethnic_category -> ethnic_category\n  person_gender -> person_gender\n  general_medical_practice -> general_medical_practice\n  patient_source_type -> patient_source_type\n  referrer_code -> referrer_code\n  referring_org -> referring_org\n  diagnostic_test_request_date_time -> diagnostic_test_request_date_time\n  diagnostic_test_request_received_date_time -> diagnostic_test_request_received_date_time\n  diagnostic_test_date_time -> diagnostic_test_date_time\n  service_report_issue_date_time -> service_report_issue_date_time\n  imaging_code_nicip -> imaging_code_nicip\n  imaging_code_snomed -> imaging_code_snomed\n  provider_site_code -> provider_site_code\n  priority_type_code -> priority_type_code\n  cancer_pathway_flag -> cancer_pathway_flag\n  file_name -> file_name\n  month_year -> month_year\n  submission_month -> submission_month\n  submission_year -> submission_year\n  data_period -> data_period\n  trust_code -> trust_code\n  combined_imaging_code -> combined_imaging_code\n  priority_type_code_routine_default -> priority_type_code_routine_default\n  TAT_scan -> tat_scan\n  TAT_report -> tat_report\n  TAT_overall -> tat_overall\n  datedifftest -> datedifftest\n  cancer_pathway_flag_string -> cancer_pathway_flag_string\n  referring_organisation -> referring_organisation"
    )
}}
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
from {{ source('reference_analyst_managed', 'TURNAROUND_TIMES_RAW') }}
