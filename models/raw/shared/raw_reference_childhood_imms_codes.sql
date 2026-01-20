{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.CHILDHOOD_IMMS_CODES \ndbt: source(''reference_terminology'', ''CHILDHOOD_IMMS_CODES'') \nColumns:\n  VACCINE -> vaccine\n  DOSE -> dose\n  PROPOSEDCLUSTER -> proposedcluster\n  SOURCECLUSTERID -> sourceclusterid\n  SOURCECLUSTERDESCRIPTION -> sourceclusterdescription\n  SNOMEDCONCEPTID -> snomedconceptid\n  CODEDESCRIPTION -> codedescription\n  SOURCE -> source"
    )
}}
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
