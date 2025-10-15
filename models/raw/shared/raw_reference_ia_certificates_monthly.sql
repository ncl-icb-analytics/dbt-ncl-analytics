-- Raw layer model for reference_analyst_managed.IA_CERTIFICATES_MONTHLY
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "CERTIFICATE_NAME" as certificate_name,
    "COURSE_CREATED_BY" as course_created_by,
    "DATE_ISSUED" as date_issued,
    "FULL_NAME_WITH_LINK" as full_name_with_link,
    "FULL_NAME" as full_name,
    "TRUST_HOSPITAL" as trust_hospital,
    "BAND_GRADE" as band_grade,
    "WHAT_BEST_DESCRIBES_YOUR_ROLE" as what_best_describes_your_role,
    "CATEGORY_NAME" as category_name
from {{ source('reference_analyst_managed', 'IA_CERTIFICATES_MONTHLY') }}
