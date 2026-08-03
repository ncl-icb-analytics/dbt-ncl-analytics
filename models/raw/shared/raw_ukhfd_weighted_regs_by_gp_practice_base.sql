{{
    config(
        description="Raw UKHFD practice-level allocation weight inputs. Source: UKHFD.Comm_Allocations.fact_Weighted_Regs_By_GP_Practice_Base."
    )
}}

select
    "Title" as title,
    "GP_Practice_Code" as practice_code,
    "Metric_Name" as metric_name,
    "Metric_Value" as metric_value,
    "Metric_Value_Str" as metric_value_str,
    "Effective_Snapshot_Date" as effective_snapshot_date,
    "DataSourceFileForThisSnapshot_Version" as source_file_version,
    "Report_Period_Length" as report_period_length,
    "Unique_ID" as unique_id,
    "AuditKey" as audit_key
from {{ source('ukhfd_comm_allocations', 'weighted_regs_by_gp_practice_base') }}
