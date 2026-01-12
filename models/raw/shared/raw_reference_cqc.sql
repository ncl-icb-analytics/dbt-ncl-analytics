{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CQC \ndbt: source(''reference_analyst_managed'', ''CQC'') \nColumns:\n  LOCATION_ID -> location_id\n  LOCATION_ODS_CODE -> location_ods_code\n  LOCATION_NAME -> location_name\n  Care Home? -> care_home\n  LOCATION_TYPE -> location_type\n  LOCATION_PRIMARY_INSPECTION_CATEGORY -> location_primary_inspection_category\n  LOCATION_STREET_ADDRESS -> location_street_address\n  LOCATION_ADDRESS_LINE_2 -> location_address_line_2\n  LOCATION_CITY -> location_city\n  LOCATION_POST_CODE -> location_post_code\n  LOCATION_LOCAL_AUTHORITY -> location_local_authority\n  LOCATION_REGION -> location_region\n  LOCATION_NHS_REGION -> location_nhs_region\n  LOCATION_ONSPD_CCG_CODE -> location_onspd_ccg_code\n  LOCATION_ONSPD_CCG -> location_onspd_ccg\n  LOCATION_COMMISSIONING_CCG_CODE -> location_commissioning_ccg_code\n  LOCATION_COMMISSIONING_CCG_NAME -> location_commissioning_ccg_name\n  Service / Population Group -> service_population_group\n  DOMAIN -> domain\n  LATEST_RATING -> latest_rating\n  VALUE -> value\n  PUBLICATION_DATE -> publication_date\n  REPORT_TYPE -> report_type\n  Inherited Rating (Y/N) -> inherited_rating_y_n\n  URL -> url\n  PROVIDER_ID -> provider_id\n  PROVIDER_NAME -> provider_name"
    )
}}
select
    "LOCATION_ID" as location_id,
    "LOCATION_ODS_CODE" as location_ods_code,
    "LOCATION_NAME" as location_name,
    "Care Home?" as care_home,
    "LOCATION_TYPE" as location_type,
    "LOCATION_PRIMARY_INSPECTION_CATEGORY" as location_primary_inspection_category,
    "LOCATION_STREET_ADDRESS" as location_street_address,
    "LOCATION_ADDRESS_LINE_2" as location_address_line_2,
    "LOCATION_CITY" as location_city,
    "LOCATION_POST_CODE" as location_post_code,
    "LOCATION_LOCAL_AUTHORITY" as location_local_authority,
    "LOCATION_REGION" as location_region,
    "LOCATION_NHS_REGION" as location_nhs_region,
    "LOCATION_ONSPD_CCG_CODE" as location_onspd_ccg_code,
    "LOCATION_ONSPD_CCG" as location_onspd_ccg,
    "LOCATION_COMMISSIONING_CCG_CODE" as location_commissioning_ccg_code,
    "LOCATION_COMMISSIONING_CCG_NAME" as location_commissioning_ccg_name,
    "Service / Population Group" as service_population_group,
    "DOMAIN" as domain,
    "LATEST_RATING" as latest_rating,
    "VALUE" as value,
    "PUBLICATION_DATE" as publication_date,
    "REPORT_TYPE" as report_type,
    "Inherited Rating (Y/N)" as inherited_rating_y_n,
    "URL" as url,
    "PROVIDER_ID" as provider_id,
    "PROVIDER_NAME" as provider_name
from {{ source('reference_analyst_managed', 'CQC') }}
