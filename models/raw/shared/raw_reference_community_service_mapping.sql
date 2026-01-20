{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.COMMUNITY_SERVICE_MAPPING \ndbt: source(''reference_analyst_managed'', ''COMMUNITY_SERVICE_MAPPING'') \nColumns:\n  PROVIDER -> provider\n  ADULT_CYP -> adult_cyp\n  PROVIDER_SERVICE_TEAM_TYPE -> provider_service_team_type\n  SERVICE_TEAM_CODE -> service_team_code\n  SERVICE_TEAM_DESCRIPTION -> service_team_description\n  CHS_SITREP_SERVICE_DESCRIPTION -> chs_sitrep_service_description"
    )
}}
select
    "PROVIDER" as provider,
    "ADULT_CYP" as adult_cyp,
    "PROVIDER_SERVICE_TEAM_TYPE" as provider_service_team_type,
    "SERVICE_TEAM_CODE" as service_team_code,
    "SERVICE_TEAM_DESCRIPTION" as service_team_description,
    "CHS_SITREP_SERVICE_DESCRIPTION" as chs_sitrep_service_description
from {{ source('reference_analyst_managed', 'COMMUNITY_SERVICE_MAPPING') }}
