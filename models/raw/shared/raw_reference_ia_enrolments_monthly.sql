{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.IA_ENROLMENTS_MONTHLY \ndbt: source(''reference_analyst_managed'', ''IA_ENROLMENTS_MONTHLY'') \nColumns:\n  Course full name -> course_full_name\n  Course short name -> course_short_name\n  Course created by -> course_created_by\n  Full name -> full_name\n  Enrolment method -> enrolment_method\n  Trust / Hospital -> trust_hospital\n  Band/Grade -> band_grade\n  What best describes your role? -> what_best_describes_your_role\n  Date and time enrolled -> date_and_time_enrolled\n  Time started -> time_started\n  Time completed -> time_completed\n  Student progress -> student_progress"
    )
}}
select
    "Course full name" as course_full_name,
    "Course short name" as course_short_name,
    "Course created by" as course_created_by,
    "Full name" as full_name,
    "Enrolment method" as enrolment_method,
    "Trust / Hospital" as trust_hospital,
    "Band/Grade" as band_grade,
    "What best describes your role?" as what_best_describes_your_role,
    "Date and time enrolled" as date_and_time_enrolled,
    "Time started" as time_started,
    "Time completed" as time_completed,
    "Student progress" as student_progress
from {{ source('reference_analyst_managed', 'IA_ENROLMENTS_MONTHLY') }}
