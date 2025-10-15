-- Raw layer model for reference_analyst_managed.IA_COMPLETION_MONTHLY
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "COURSE_FULL_NAME" as course_full_name,
    "COURSE_SHORT_NAME" as course_short_name,
    "FULL_NAME" as full_name,
    "ENROLMENT_METHOD" as enrolment_method,
    "TRUST_HOSPITAL" as trust_hospital,
    "BAND_GRADE" as band_grade,
    "WHAT_BEST_DESCRIBES_YOUR_ROLE" as what_best_describes_your_role,
    "DATE_AND_TIME_ENROLLED" as date_and_time_enrolled,
    "TIME_STARTED" as time_started,
    "TIME_COMPLETED" as time_completed,
    "STUDENT_PROGRESS" as student_progress,
    "COURSE_TYPE" as course_type
from {{ source('reference_analyst_managed', 'IA_COMPLETION_MONTHLY') }}
