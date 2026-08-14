{{
    config(
        description="Raw layer (UKHD reference and dimension tables). 1:1 passthrough with cleaned column names. \nSource: UKHFD.dbo.dim_ref_Age \ndbt: source(''ukhfd'', ''dim_ref_Age'') \nColumns:\n  pk_Age -> pk_age\n  Age_Band_5_YRS -> age_band_5_yrs\n  Age_Band_10_YRS -> age_band_10_yrs\n  Age_Band_20_YRS -> age_band_20_yrs\n  NHS_Group -> nhs_group\n  NHS_Group_Shortname -> nhs_group_shortname\n  created_date -> created_date\n  import_date -> import_date"
    )
}}
select
    "pk_Age" as pk_age,
    "Age_Band_5_YRS" as age_band_5_yrs,
    "Age_Band_10_YRS" as age_band_10_yrs,
    "Age_Band_20_YRS" as age_band_20_yrs,
    "NHS_Group" as nhs_group,
    "NHS_Group_Shortname" as nhs_group_shortname,
    "created_date" as created_date,
    "import_date" as import_date
from {{ source('ukhfd', 'dim_ref_Age') }}
