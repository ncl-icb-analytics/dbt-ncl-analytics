-- Raw layer model for reference_analyst_managed.turnaround_times_clean_write_test
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "DiagnosticTestDateTime" as diagnostic_test_date_time,
    "PatientSourceTypeName" as patient_source_type_name,
    "Modality" as modality,
    "SiteName" as site_name,
    "SiteNameShort" as site_name_short,
    "ProviderTrust" as provider_trust,
    "ProviderTrustShort" as provider_trust_short,
    "PriorityTypeCode" as priority_type_code,
    "CancerPathwayFlag" as cancer_pathway_flag,
    "BreachScan" as breach_scan,
    "BreachScan7Day" as breach_scan7_day,
    "TATScan" as tat_scan,
    "BreachReportNCL" as breach_report_ncl,
    "BreachReportNCLCancer7Day" as breach_report_ncl_cancer7_day,
    "BreachReport4Week" as breach_report4_week,
    "BreachReportNHSE" as breach_report_nhse,
    "BreachReportNHSECancer7Day" as breach_report_nhse_cancer7_day,
    "TATReport" as tat_report,
    "BreachOverall" as breach_overall,
    "BreachOverallCancer7Day" as breach_overall_cancer7_day,
    "TATOverall" as tat_overall,
    "TestAwaitingReport" as test_awaiting_report,
    "TestUnreported" as test_unreported,
    "DataType" as data_type
from {{ source('reference_analyst_managed', 'turnaround_times_clean_write_test') }}
