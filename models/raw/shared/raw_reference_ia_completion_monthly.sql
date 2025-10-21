-- Raw layer model for reference_analyst_managed.IA_COMPLETION_MONTHLY
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "Course full name" as course_full_name,
    "Course short name" as course_short_name,
    "Full name" as full_name,
    "Enrolment method" as enrolment_method,
    "Trust / Hospital" as trust_hospital,
    "Band/Grade" as band_grade,
    "What best describes your role?" as what_best_describes_your_role,
    "Date and time enrolled" as date_and_time_enrolled,
    "Time started" as time_started,
    "Time completed" as time_completed,
    "Student progress" as student_progress,
    "Course Type" as course_type
from {{ source('reference_analyst_managed', 'IA_COMPLETION_MONTHLY') }}
