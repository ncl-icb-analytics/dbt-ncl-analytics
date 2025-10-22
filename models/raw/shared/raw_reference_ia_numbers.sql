-- Raw layer model for reference_analyst_managed.IA_NUMBERS
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "Training" as training,
    "Training Hours/Trainee Numbers" as training_hours_trainee_numbers,
    "Measure" as measure,
    "Month/Year" as month_year,
    "Data" as data
from {{ source('reference_analyst_managed', 'IA_NUMBERS') }}
