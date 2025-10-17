-- Raw layer model for reference_analyst_managed.IA_CERTIFICATES_MONTHLY
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "Certificate name" as certificate_name,
    "Course created by" as course_created_by,
    "Date issued" as date_issued,
    "Full name with link" as full_name_with_link,
    "Full name" as full_name,
    "Trust / Hospital" as trust_hospital,
    "Band/Grade" as band_grade,
    "What best describes your role?" as what_best_describes_your_role?,
    "Category name" as category_name
from {{ source('reference_analyst_managed', 'IA_CERTIFICATES_MONTHLY') }}
