-- Staging model for reference_terminology.CHILDHOOD_IMMS_CODES
-- Source: "DATA_LAKE__NCL"."TERMINOLOGY"
-- Description: Reference terminology data including SNOMED, BNF, and other code sets

select
    "VACCINE" as vaccine,
    "DOSE" as dose,
    "PROPOSEDCLUSTER" as proposedcluster,
    "SOURCECLUSTERID" as sourceclusterid,
    "SOURCECLUSTERDESCRIPTION" as sourceclusterdescription,
    "SNOMEDCONCEPTID" as snomedconceptid,
    "CODEDESCRIPTION" as codedescription,
    "SOURCE" as source
from {{ source('reference_terminology', 'CHILDHOOD_IMMS_CODES') }}
