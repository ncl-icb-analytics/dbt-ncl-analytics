{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.IA_NUMBERS \ndbt: source(''reference_analyst_managed'', ''IA_NUMBERS'') \nColumns:\n  Training -> training\n  Training Hours/Trainee Numbers -> training_hours_trainee_numbers\n  Measure -> measure\n  Month/Year -> month_year\n  Data -> data"
    )
}}
select
    "Training" as training,
    "Training Hours/Trainee Numbers" as training_hours_trainee_numbers,
    "Measure" as measure,
    "Month/Year" as month_year,
    "Data" as data
from {{ source('reference_analyst_managed', 'IA_NUMBERS') }}
