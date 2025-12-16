-- Raw layer model for reference_analyst_managed.CQC
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
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
