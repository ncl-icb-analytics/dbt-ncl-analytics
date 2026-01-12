{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Procedure \ndbt: source(''dictionary_dbo'', ''Procedure'') \nColumns:\n  SK_ProcedureCode -> sk_procedure_code\n  Code -> code\n  Alt_Code -> alt_code\n  Category -> category\n  Description -> description\n  Status_Of_Operation -> status_of_operation\n  Sex_Absolute -> sex_absolute\n  Sex_Scrutiny -> sex_scrutiny\n  Method_Of_Delivery_Code -> method_of_delivery_code\n  OPCS_Version -> opcs_version\n  IsOnlySecondaryCode -> is_only_secondary_code\n  IsOnlyFemales -> is_only_females\n  IsOnlyMales -> is_only_males\n  IsMainlyFemales -> is_mainly_females\n  IsMainlyMales -> is_mainly_males\n  DateCreated -> date_created\n  DateUpdated -> date_updated\n  Chapter -> chapter"
    )
}}
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
