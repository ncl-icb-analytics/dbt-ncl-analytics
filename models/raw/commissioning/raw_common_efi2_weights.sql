{{
    config(
        description="Raw layer (Shared reference tables in the NCL data lake COMMON schema. Includes the eFI2 (electronic Frailty Index 2) codelists + weights that feed the native eFI2 pipeline (models/modelling/olids/risk_stratification/int_efi2_*).
). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.COMMON.EFI2_WEIGHTS \ndbt: source(''common'', ''EFI2_WEIGHTS'') \nColumns:\n  DEFICIT -> deficit\n  DETAIL_KEY -> detail_key\n  WEIGHT -> weight"
    )
}}
select
    "DEFICIT" as deficit,
    "DETAIL_KEY" as detail_key,
    "WEIGHT" as weight
from {{ source('common', 'EFI2_WEIGHTS') }}
