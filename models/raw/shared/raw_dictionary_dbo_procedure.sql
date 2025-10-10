-- Raw layer model for dictionary_dbo.Procedure
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_ProcedureCode" as sk_procedure_code,
    "Code" as code,
    "Alt_Code" as alt_code,
    "Category" as category,
    "Description" as description,
    "Status_Of_Operation" as status_of_operation,
    "Sex_Absolute" as sex_absolute,
    "Sex_Scrutiny" as sex_scrutiny,
    "Method_Of_Delivery_Code" as method_of_delivery_code,
    "OPCS_Version" as opcs_version,
    "IsOnlySecondaryCode" as is_only_secondary_code,
    "IsOnlyFemales" as is_only_females,
    "IsOnlyMales" as is_only_males,
    "IsMainlyFemales" as is_mainly_females,
    "IsMainlyMales" as is_mainly_males,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated,
    "Chapter" as chapter
from {{ source('dictionary_dbo', 'Procedure') }}
