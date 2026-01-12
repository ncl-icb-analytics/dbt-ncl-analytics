{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.IA_CERTIFICATES_MONTHLY \ndbt: source(''reference_analyst_managed'', ''IA_CERTIFICATES_MONTHLY'') \nColumns:\n  Certificate name -> certificate_name\n  Course created by -> course_created_by\n  Date issued -> date_issued\n  Full name with link -> full_name_with_link\n  Full name -> full_name\n  Trust / Hospital -> trust_hospital\n  Band/Grade -> band_grade\n  What best describes your role? -> what_best_describes_your_role\n  Category name -> category_name"
    )
}}
select
    "Certificate name" as certificate_name,
    "Course created by" as course_created_by,
    "Date issued" as date_issued,
    "Full name with link" as full_name_with_link,
    "Full name" as full_name,
    "Trust / Hospital" as trust_hospital,
    "Band/Grade" as band_grade,
    "What best describes your role?" as what_best_describes_your_role,
    "Category name" as category_name
from {{ source('reference_analyst_managed', 'IA_CERTIFICATES_MONTHLY') }}
