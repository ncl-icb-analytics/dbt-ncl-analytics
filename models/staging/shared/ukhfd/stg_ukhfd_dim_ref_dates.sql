-- Staging layer: UKHFD reference date dimension.
-- 1:1 with raw_ukhfd_dim_ref_dates - column selection only, no joins or filtering.
--
-- NOTE: only the columns consumed by int_ecds_consolidated are selected here.
-- Extend the list if other models need further date attributes. Verify the
-- column names against raw_ukhfd_dim_ref_dates before merging.

select
    full_date,
    fin_year,
    fin_month_no
from {{ ref('raw_ukhfd_dim_ref_dates') }}
