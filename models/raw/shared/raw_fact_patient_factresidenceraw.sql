{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.FactresidenceRaw \ndbt: source(''fact_patient'', ''FactresidenceRaw'') \nColumns:\n  SK_DataSourceID -> sk_data_source_id\n  SK_PatientID -> sk_patient_id\n  SK_OutputAreaID -> sk_output_area_id\n  SK_PostcodeID -> sk_postcode_id\n  ResidenceStartDate -> residence_start_date\n  ResidenceEndDate -> residence_end_date"
    )
}}
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_PatientID" as sk_patient_id,
    "SK_OutputAreaID" as sk_output_area_id,
    "SK_PostcodeID" as sk_postcode_id,
    "ResidenceStartDate" as residence_start_date,
    "ResidenceEndDate" as residence_end_date
from {{ source('fact_patient', 'FactresidenceRaw') }}
