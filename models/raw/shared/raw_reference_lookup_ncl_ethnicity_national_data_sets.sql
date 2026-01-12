-- Raw layer model for reference_lookup_ncl.ETHNICITY_NATIONAL_DATA_SETS
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_PATIENTID" as sk_patientid,
    "ETHNICITY_CODE" as ethnicity_code,
    "ETHNICITY_DESC" as ethnicity_desc,
    "ETHNICITY" as ethnicity,
    "ETHNICITY_DETAIL" as ethnicity_detail,
    "RECORD_DATE" as record_date
from {{ source('reference_lookup_ncl', 'ETHNICITY_NATIONAL_DATA_SETS') }}
