-- Raw layer model for reference_analyst_managed.FA__MONTHLY_IAP_2526_WH
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "FINANCIAL_MONTH" as financial_month,
    "FINANCIAL_YEAR" as financial_year,
    "DATE_AND_TIME_DATA_SET_CREATED" as date_and_time_data_set_created,
    "ORGANISATION_IDENTIFIER_(CODE_OF_PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION_SITE_IDENTIFIER_(OF_TREATMENT)" as organisation_site_identifier_of_treatment,
    "ORGANISATION_IDENTIFIER_(GP_PRACTICE_RESPONSIBILITY)" as organisation_identifier_gp_practice_responsibility,
    "ORGANISATION_IDENTIFIER_(RESIDENCE_RESPONSIBILITY)" as organisation_identifier_residence_responsibility,
    "ORGANISATION_IDENTIFIER_(CODE_OF_COMMISSIONER)" as organisation_identifier_code_of_commissioner,
    "GENERAL_MEDICAL_PRACTICE_(PATIENT_REGISTRATION)" as general_medical_practice_patient_registration,
    "ACTIVITY_TREATMENT_FUNCTION_CODE" as activity_treatment_function_code,
    "LOCAL_SUB-SPECIALTY_CODE" as local_sub_specialty_code,
    "WARD_CODE" as ward_code,
    "COMMISSIONED_SERVICE_CATEGORY_CODE" as commissioned_service_category_code,
    "SERVICE_CODE" as service_code,
    "SPECIALISED_MENTAL_HEALTH_SERVICE_CATEGORY_CODE" as specialised_mental_health_service_category_code,
    "POINT_OF_DELIVERY_CODE" as point_of_delivery_code,
    "POINT_OF_DELIVERY_FURTHER_DETAIL_CODE" as point_of_delivery_further_detail_code,
    "POINT_OF_DELIVERY_FURTHER_DETAIL_DESCRIPTION" as point_of_delivery_further_detail_description,
    "LOCAL_POINT_OF_DELIVERY_CODE" as local_point_of_delivery_code,
    "LOCAL_POINT_OF_DELIVERY_DESCRIPTION" as local_point_of_delivery_description,
    "LOCAL_CONTRACT_CODE" as local_contract_code,
    "LOCAL_CONTRACT_CODE_DESCRIPTION" as local_contract_code_description,
    "LOCAL_CONTRACT_MONITORING_CODE" as local_contract_monitoring_code,
    "LOCAL_CONTRACT_MONITORING_DESCRIPTION" as local_contract_monitoring_description,
    "CONTRACT_MONITORING_ADDITIONAL_DETAIL" as contract_monitoring_additional_detail,
    "CONTRACT_MONITORING_ADDITIONAL_DESCRIPTION" as contract_monitoring_additional_description,
    "TARIFF_CODE" as tariff_code,
    "NATIONAL_TARIFF_INDICATOR" as national_tariff_indicator,
    "CONTRACT_MONITORING_PLANNED_ACTIVITY" as contract_monitoring_planned_activity,
    "CONTRACT_MONITORING_PLANNED_PRICE" as contract_monitoring_planned_price,
    "CONTRACT_MONITORING_PLANNED_MARKET_FORCES_FACTOR" as contract_monitoring_planned_market_forces_factor,
    "NAME_OF_SUBMITTER" as name_of_submitter
from {{ source('reference_analyst_managed', 'FA__MONTHLY_IAP_2526_WH') }}
