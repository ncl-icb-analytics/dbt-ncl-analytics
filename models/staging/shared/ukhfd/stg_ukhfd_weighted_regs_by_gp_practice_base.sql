-- UKHFD GP need-index base values. One row per practice, metric and base year.
select
    practice_code,
    metric_name,
    metric_value::float as metric_value,
    effective_snapshot_date as financial_year_start,
    source_file_version
from {{ ref('raw_ukhfd_comm_allocations_weighted_regs_by_gp_practice_base') }}
where title = 'gp_need_index_base'
  and practice_code is not null
  and metric_value is not null
