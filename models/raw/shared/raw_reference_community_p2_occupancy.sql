{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.COMMUNITY_P2_OCCUPANCY \ndbt: source(''reference_analyst_managed'', ''COMMUNITY_P2_OCCUPANCY'') \nColumns:\n  UNIT_NAME -> unit_name\n  BEDDED_CAPACITY -> bedded_capacity\n  ADMISSIONS_IN_MONTH -> admissions_in_month\n  DISCHARGES_IN_MONTH -> discharges_in_month\n  AVERAGE_LOS_OF_PATIENTS_DISCHARGED -> average_los_of_patients_discharged\n  BED_DAYS_FOR_DISCHARGED_PATIENTS -> bed_days_for_discharged_patients\n  BED_DAYS_AVAILABLE -> bed_days_available\n  BED_DAYS_OCCUPIED -> bed_days_occupied\n  OCCUPANCY_RATE -> occupancy_rate\n  MONTH -> month\n  PROVIDER -> provider"
    )
}}
select
    "UNIT_NAME" as unit_name,
    "BEDDED_CAPACITY" as bedded_capacity,
    "ADMISSIONS_IN_MONTH" as admissions_in_month,
    "DISCHARGES_IN_MONTH" as discharges_in_month,
    "AVERAGE_LOS_OF_PATIENTS_DISCHARGED" as average_los_of_patients_discharged,
    "BED_DAYS_FOR_DISCHARGED_PATIENTS" as bed_days_for_discharged_patients,
    "BED_DAYS_AVAILABLE" as bed_days_available,
    "BED_DAYS_OCCUPIED" as bed_days_occupied,
    "OCCUPANCY_RATE" as occupancy_rate,
    "MONTH" as month,
    "PROVIDER" as provider
from {{ source('reference_analyst_managed', 'COMMUNITY_P2_OCCUPANCY') }}
