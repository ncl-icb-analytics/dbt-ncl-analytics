{{
    config(
        description="Raw layer (Community services dataset). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.CSDS.CYP604BloodSpotResult \ndbt: source(''csds'', ''CYP604BloodSpotResult'') \nColumns:\n  SK -> sk\n  LOCAL PATIENT IDENTIFIER (EXTENDED) -> local_patient_identifier_extended\n  BLOOD SPOT CARD COMPLETION DATE -> blood_spot_card_completion_date\n  NEWBORN BLOOD SPOT TEST RESULT RECEIVED DATE -> newborn_blood_spot_test_result_received_date\n  NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (PHENYLKETONURIA) -> newborn_blood_spot_test_outcome_status_code_phenylketonuria\n  NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (SICKLE CELL DISEASE) -> newborn_blood_spot_test_outcome_status_code_sickle_cell_disease\n  NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (CYSTIC FIBROSIS) -> newborn_blood_spot_test_outcome_status_code_cystic_fibrosis\n  NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (CONGENITAL HYPOTHYROIDISM) -> newborn_blood_spot_test_outcome_status_code_congenital_hypothyroidism\n  NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (MEDIUM CHAIN ACYL-COA DEHYDROGENASE DEFICIENCY) -> newborn_blood_spot_test_outcome_status_code_medium_chain_acyl_coa_dehydrogenase_deficiency\n  NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (HOMOCYSTINURIA) -> newborn_blood_spot_test_outcome_status_code_homocystinuria\n  NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (MAPLE SYRUP URINE DISEASE) -> newborn_blood_spot_test_outcome_status_code_maple_syrup_urine_disease\n  NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (GLUTARIC ACIDURIA TYPE 1) -> newborn_blood_spot_test_outcome_status_code_glutaric_aciduria_type_1\n  NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (ISOVALERIC ACIDURIA) -> newborn_blood_spot_test_outcome_status_code_isovaleric_aciduria\n  EFFECTIVE FROM -> effective_from\n  RECORD NUMBER -> record_number\n  CYP604 UNIQUE ID -> cyp604_unique_id\n  ORGANISATION IDENTIFIER (CODE OF PROVIDER) -> organisation_identifier_code_of_provider\n  ORGANISATION CODE (PROVIDER) -> organisation_code_provider\n  PERSON ID -> person_id\n  UNIQUE CSDS ID (PATIENT) -> unique_csds_id_patient\n  UNIQUE SUBMISSION ID -> unique_submission_id\n  BSP UNIQUE ID -> bsp_unique_id\n  AGE AT BLOOD SPOT COMPLETION DATE -> age_at_blood_spot_completion_date\n  IC_AGE_AT_BLOOD_SPOT_COMPLETION_DATE -> ic_age_at_blood_spot_completion_date\n  TIME TAKEN TO RECEIVE BLOOD SPOT RESULT -> time_taken_to_receive_blood_spot_result\n  RECORD START DATE -> record_start_date\n  RECORD END DATE -> record_end_date\n  UNIQUE MONTH ID -> unique_month_id\n  dmicImportLogId -> dmic_import_log_id\n  dmicSystemId -> dmic_system_id\n  dmicCCGCode -> dmic_ccg_code\n  dmicCCG -> dmic_ccg\n  Unique_LocalPatientId -> unique_local_patient_id\n  UniqueCYPHS_ID_Patient -> unique_cyphs_id_patient\n  FILE TYPE -> file_type\n  REPORTING PERIOD START DATE -> reporting_period_start_date\n  REPORTING PERIOD END DATE -> reporting_period_end_date"
    )
}}
select
    "SK" as sk,
    "LOCAL PATIENT IDENTIFIER (EXTENDED)" as local_patient_identifier_extended,
    "BLOOD SPOT CARD COMPLETION DATE" as blood_spot_card_completion_date,
    "NEWBORN BLOOD SPOT TEST RESULT RECEIVED DATE" as newborn_blood_spot_test_result_received_date,
    "NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (PHENYLKETONURIA)" as newborn_blood_spot_test_outcome_status_code_phenylketonuria,
    "NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (SICKLE CELL DISEASE)" as newborn_blood_spot_test_outcome_status_code_sickle_cell_disease,
    "NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (CYSTIC FIBROSIS)" as newborn_blood_spot_test_outcome_status_code_cystic_fibrosis,
    "NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (CONGENITAL HYPOTHYROIDISM)" as newborn_blood_spot_test_outcome_status_code_congenital_hypothyroidism,
    "NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (MEDIUM CHAIN ACYL-COA DEHYDROGENASE DEFICIENCY)" as newborn_blood_spot_test_outcome_status_code_medium_chain_acyl_coa_dehydrogenase_deficiency,
    "NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (HOMOCYSTINURIA)" as newborn_blood_spot_test_outcome_status_code_homocystinuria,
    "NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (MAPLE SYRUP URINE DISEASE)" as newborn_blood_spot_test_outcome_status_code_maple_syrup_urine_disease,
    "NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (GLUTARIC ACIDURIA TYPE 1)" as newborn_blood_spot_test_outcome_status_code_glutaric_aciduria_type_1,
    "NEWBORN BLOOD SPOT TEST OUTCOME STATUS CODE (ISOVALERIC ACIDURIA)" as newborn_blood_spot_test_outcome_status_code_isovaleric_aciduria,
    "EFFECTIVE FROM" as effective_from,
    "RECORD NUMBER" as record_number,
    "CYP604 UNIQUE ID" as cyp604_unique_id,
    "ORGANISATION IDENTIFIER (CODE OF PROVIDER)" as organisation_identifier_code_of_provider,
    "ORGANISATION CODE (PROVIDER)" as organisation_code_provider,
    "PERSON ID" as person_id,
    "UNIQUE CSDS ID (PATIENT)" as unique_csds_id_patient,
    "UNIQUE SUBMISSION ID" as unique_submission_id,
    "BSP UNIQUE ID" as bsp_unique_id,
    "AGE AT BLOOD SPOT COMPLETION DATE" as age_at_blood_spot_completion_date,
    "IC_AGE_AT_BLOOD_SPOT_COMPLETION_DATE" as ic_age_at_blood_spot_completion_date,
    "TIME TAKEN TO RECEIVE BLOOD SPOT RESULT" as time_taken_to_receive_blood_spot_result,
    "RECORD START DATE" as record_start_date,
    "RECORD END DATE" as record_end_date,
    "UNIQUE MONTH ID" as unique_month_id,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicSystemId" as dmic_system_id,
    "dmicCCGCode" as dmic_ccg_code,
    "dmicCCG" as dmic_ccg,
    "Unique_LocalPatientId" as unique_local_patient_id,
    "UniqueCYPHS_ID_Patient" as unique_cyphs_id_patient,
    "FILE TYPE" as file_type,
    "REPORTING PERIOD START DATE" as reporting_period_start_date,
    "REPORTING PERIOD END DATE" as reporting_period_end_date
from {{ source('csds', 'CYP604BloodSpotResult') }}
