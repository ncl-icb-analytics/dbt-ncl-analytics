-- Raw layer model for reference_analyst_managed.COMMUNITY_P2_OCCUPANCY
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
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
