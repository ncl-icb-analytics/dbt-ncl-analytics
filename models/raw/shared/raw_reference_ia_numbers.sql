-- Raw layer model for reference_analyst_managed.IA_NUMBERS
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "TRAINING" as training,
    "TRAINING_HOURS_TRAINEE_NUMBERS" as training_hours_trainee_numbers,
    "MEASURE" as measure,
    "MONTH_YEAR" as month_year,
    "DATA" as data
from {{ source('reference_analyst_managed', 'IA_NUMBERS') }}
