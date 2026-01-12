-- Raw layer model for reference_analyst_managed.COMMUNITY_SERVICE_MAPPING
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "PROVIDER" as provider,
    "ADULT_CYP" as adult_cyp,
    "PROVIDER_SERVICE_TEAM_TYPE" as provider_service_team_type,
    "SERVICE_TEAM_CODE" as service_team_code,
    "SERVICE_TEAM_DESCRIPTION" as service_team_description,
    "CHS_SITREP_SERVICE_DESCRIPTION" as chs_sitrep_service_description
from {{ source('reference_analyst_managed', 'COMMUNITY_SERVICE_MAPPING') }}
