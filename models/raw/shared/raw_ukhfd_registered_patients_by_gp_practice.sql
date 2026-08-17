{{
    config(
        description="Raw UKHFD registered patient counts by organisation, sex and 5-year age band. Source: UKHFD.Demography.fact_No_Of_Patients_Regd_At_GP_Prac_Regions_5Yr_AgeBand."
    )
}}

select
    "Org_Type" as org_type,
    "Org_Code" as org_code,
    "ONS_Code" as ons_code,
    "Sex" as sex,
    "Age_Band" as age_band,
    "Number_Of_Patients" as number_of_patients,
    "Effective_Snapshot_Date" as effective_snapshot_date,
    "DataSourceFileForThisSnapshot_Version" as source_file_version,
    "Report_Period_Length" as report_period_length,
    "Unique_ID" as unique_id,
    "AuditKey" as audit_key
from {{ source('ukhfd_demography', 'registered_patients_by_gp_practice') }}
