{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.IMD_2025 \ndbt: source(''reference_lookup_ncl'', ''IMD_2025'') \nColumns:\n  LSOA_CODE_2021 -> lsoa_code_2021\n  LSOA_NAME_2021 -> lsoa_name_2021\n  LOCAL_AUTHORITY_DISTRICT_CODE_2024 -> local_authority_district_code_2024\n  LOCAL_AUTHORITY_DISTRICT_NAME_2024 -> local_authority_district_name_2024\n  IMD25_DECILE -> imd25_decile\n  INCOME_DOMAINEMPLOYMENT_DOMAIN_DECILE -> income_domainemployment_domain_decile\n  EMPLOYMENT_DOMAIN_DECILE -> employment_domain_decile\n  EDUCATION_DOMAIN_DECILE -> education_domain_decile\n  HEALTH_DOMAIN_DECILE -> health_domain_decile\n  CRIME_DOMAIN_DECILE -> crime_domain_decile\n  BARRIERS_TO_HOUSING_DOMAIN_DECILE -> barriers_to_housing_domain_decile\n  LIVING_ENVIRONMENT_DOMAIN_DECILE -> living_environment_domain_decile\n  INCOME_DEPRIVATION_AFFECTING_CHILDREN_SUBDOMAIN_DECILE -> income_deprivation_affecting_children_subdomain_decile\n  INCOME_DEPRIVATION_AFFECTING_OLDER_PEOPLE_SUBDOMAIN_DECILE -> income_deprivation_affecting_older_people_subdomain_decile\n  CHILDREN_AND_YOUNG_PEOPLE_SUBDOMAIN_DECILE -> children_and_young_people_subdomain_decile\n  ADULT_SKILLS_SUBDOMAIN_DECILE -> adult_skills_subdomain_decile\n  GEOGRAPHICAL_BARRIERS_SUBDOMAIN_DECILE -> geographical_barriers_subdomain_decile\n  WIDER_BARRIERS_SUBDOMAIN_DECILE -> wider_barriers_subdomain_decile\n  INDOORS_SUBDOMAIN_DECILE -> indoors_subdomain_decile\n  OUTDOORS_SUBDOMAIN_DECILE -> outdoors_subdomain_decile"
    )
}}
select
    "LSOA_CODE_2021" as lsoa_code_2021,
    "LSOA_NAME_2021" as lsoa_name_2021,
    "LOCAL_AUTHORITY_DISTRICT_CODE_2024" as local_authority_district_code_2024,
    "LOCAL_AUTHORITY_DISTRICT_NAME_2024" as local_authority_district_name_2024,
    "IMD25_DECILE" as imd25_decile,
    "INCOME_DOMAINEMPLOYMENT_DOMAIN_DECILE" as income_domainemployment_domain_decile,
    "EMPLOYMENT_DOMAIN_DECILE" as employment_domain_decile,
    "EDUCATION_DOMAIN_DECILE" as education_domain_decile,
    "HEALTH_DOMAIN_DECILE" as health_domain_decile,
    "CRIME_DOMAIN_DECILE" as crime_domain_decile,
    "BARRIERS_TO_HOUSING_DOMAIN_DECILE" as barriers_to_housing_domain_decile,
    "LIVING_ENVIRONMENT_DOMAIN_DECILE" as living_environment_domain_decile,
    "INCOME_DEPRIVATION_AFFECTING_CHILDREN_SUBDOMAIN_DECILE" as income_deprivation_affecting_children_subdomain_decile,
    "INCOME_DEPRIVATION_AFFECTING_OLDER_PEOPLE_SUBDOMAIN_DECILE" as income_deprivation_affecting_older_people_subdomain_decile,
    "CHILDREN_AND_YOUNG_PEOPLE_SUBDOMAIN_DECILE" as children_and_young_people_subdomain_decile,
    "ADULT_SKILLS_SUBDOMAIN_DECILE" as adult_skills_subdomain_decile,
    "GEOGRAPHICAL_BARRIERS_SUBDOMAIN_DECILE" as geographical_barriers_subdomain_decile,
    "WIDER_BARRIERS_SUBDOMAIN_DECILE" as wider_barriers_subdomain_decile,
    "INDOORS_SUBDOMAIN_DECILE" as indoors_subdomain_decile,
    "OUTDOORS_SUBDOMAIN_DECILE" as outdoors_subdomain_decile
from {{ source('reference_lookup_ncl', 'IMD_2025') }}
