{{
    config(
        description="Raw layer. 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.COMMON.EFI2_WEIGHTS \ndbt: source(''common'', ''EFI2_WEIGHTS'') \nColumns:\n  DEFICIT -> deficit\n  DETAIL_KEY -> detail_key\n  WEIGHT -> weight"
    )
}}
-- eFI2 deficit weights, AIC-curated but published to the shared COMMON schema
-- (not AIC_DEV). Named *_aic_* to keep all eFI2 assets grouped; source is
-- `common` — see models/sources/sources.yml.
select
    "DEFICIT" as deficit,
    "DETAIL_KEY" as detail_key,
    "WEIGHT" as weight
from {{ source('common', 'EFI2_WEIGHTS') }}
