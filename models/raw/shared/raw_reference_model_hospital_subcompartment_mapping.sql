{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.MODEL_HOSPITAL_SUBCOMPARTMENT_MAPPING \ndbt: source(''reference_analyst_managed'', ''MODEL_HOSPITAL_SUBCOMPARTMENT_MAPPING'') \nColumns:\n  SubCompartment -> sub_compartment\n  SubCompartment_Mapping -> sub_compartment_mapping"
    )
}}
select
    "SubCompartment" as sub_compartment,
    "SubCompartment_Mapping" as sub_compartment_mapping
from {{ source('reference_analyst_managed', 'MODEL_HOSPITAL_SUBCOMPARTMENT_MAPPING') }}
