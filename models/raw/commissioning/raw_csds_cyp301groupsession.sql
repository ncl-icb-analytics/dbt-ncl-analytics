{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.CYP301GroupSession \ndbt: source(''csds'', ''CYP301GroupSession'') \nColumns:\n  SK -> sk\n  GROUP SESSION IDENTIFIER -> group_session_identifier\n  GROUP SESSION DATE -> group_session_date\n  ORGANISATION IDENTIFIER (CODE OF COMMISSIONER) -> organisation_identifier_code_of_commissioner\n  ORGANISATION CODE (CODE OF COMMISSIONER) -> organisation_code_code_of_commissioner\n  CLINICAL CONTACT DURATION OF GROUP SESSION -> clinical_contact_duration_of_group_session\n  GROUP SESSION TYPE (COMMUNITY CARE) -> group_session_type_community_care\n  GROUP SESSION TYPE CODE (COMMUNITY CARE) -> group_session_type_code_community_care\n  NUMBER OF GROUP SESSION PARTICIPANTS -> number_of_group_session_participants\n  ACTIVITY LOCATION TYPE CODE -> activity_location_type_code\n  ORGANISATION SITE IDENTIFIER (OF TREATMENT) -> organisation_site_identifier_of_treatment\n  SITE CODE (OF TREATMENT) -> site_code_of_treatment\n  CARE PROFESSIONAL LOCAL IDENTIFIER -> care_professional_local_identifier\n  NHS SERVICE AGREEMENT LINE IDENTIFIER -> nhs_service_agreement_line_identifier\n  NHS SERVICE AGREEMENT LINE NUMBER -> nhs_service_agreement_line_number\n  EFFECTIVE FROM -> effective_from\n  CYP301 UNIQUE ID -> cyp301_unique_id\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  ORGANISATION CODE (PROVIDER) -> organisation_code_provider\n  UNIQUE SUBMISSION ID -> unique_submission_id\n  BSP UNIQUE ID -> bsp_unique_id\n  UNIQUE MONTH ID -> unique_month_id\n  dmicImportLogId -> dmic_import_log_id\n  Unique_GroupSessionId -> unique_group_session_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicCCG -> dmic_ccg\n  Unique_CareProfessionalId_Local -> unique_care_professional_id_local\n  dmIcbCommissioner -> dm_icb_commissioner\n  dmSubIcbCommissioner -> dm_sub_icb_commissioner\n  dmCommissionerDerivationReason -> dm_commissioner_derivation_reason\n  FILE TYPE -> file_type\n  REPORTING PERIOD START DATE -> reporting_period_start_date\n  REPORTING PERIOD END DATE -> reporting_period_end_date"
    )
}}
select
    "SK" as sk,
    "GROUP SESSION IDENTIFIER" as group_session_identifier,
    "GROUP SESSION DATE" as group_session_date,
    "ORGANISATION IDENTIFIER (CODE OF COMMISSIONER)" as organisation_identifier_code_of_commissioner,
    "ORGANISATION CODE (CODE OF COMMISSIONER)" as organisation_code_code_of_commissioner,
    "CLINICAL CONTACT DURATION OF GROUP SESSION" as clinical_contact_duration_of_group_session,
    "GROUP SESSION TYPE (COMMUNITY CARE)" as group_session_type_community_care,
    "GROUP SESSION TYPE CODE (COMMUNITY CARE)" as group_session_type_code_community_care,
    "NUMBER OF GROUP SESSION PARTICIPANTS" as number_of_group_session_participants,
    "ACTIVITY LOCATION TYPE CODE" as activity_location_type_code,
    "ORGANISATION SITE IDENTIFIER (OF TREATMENT)" as organisation_site_identifier_of_treatment,
    "SITE CODE (OF TREATMENT)" as site_code_of_treatment,
    "CARE PROFESSIONAL LOCAL IDENTIFIER" as care_professional_local_identifier,
    "NHS SERVICE AGREEMENT LINE IDENTIFIER" as nhs_service_agreement_line_identifier,
    "NHS SERVICE AGREEMENT LINE NUMBER" as nhs_service_agreement_line_number,
    "EFFECTIVE FROM" as effective_from,
    "CYP301 UNIQUE ID" as cyp301_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION CODE (PROVIDER)" as organisation_code_provider,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "BSP UNIQUE ID" as bsp_unique_id,
    "UNIQUE MONTH ID" as unique_month_id,
    "dmicImportLogId" as dmic_import_log_id,
    "Unique_GroupSessionId" as unique_group_session_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "Unique_CareProfessionalId_Local" as unique_care_professional_id_local,
    "dmIcbCommissioner" as dm_icb_commissioner,
    "dmSubIcbCommissioner" as dm_sub_icb_commissioner,
    "dmCommissionerDerivationReason" as dm_commissioner_derivation_reason,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP301GroupSession') }}
