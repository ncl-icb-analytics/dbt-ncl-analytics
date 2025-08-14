
{{ config(materialized='view') }}

/*
All emergency care activity from unified SUS ECDS data. Adding dictionary lookups
*/

SELECT a.*
    ,ORG2."Organisation_Name" AS ProviderName 
    ,ORG."Organisation_Code" AS ProviderSiteCode
    ,ORG."Organisation_Name" AS ProviderSiteName
    ,PCN."LEGACY_CCG_NAME"
    ,LSOA."CommissionerCode" as LSOA_CommissionerCode
    ,PCN."LEGACY_CCG_CODE"
    ,COM."CommissionerName"
    ,COM."CommissionerCode" as COM_CommissionerCode
    ,COM2."CommissionerCode" as COM2_CommissionerCode
    ,COM2."CommissionerName" as CommissionerName2
    ,ORG3."Organisation_Name"  AS CommissionerName
    ,ORG5."Organisation_Name" AS GPPractice
    ,ORG4."NetworkName" AS GPLocality 
    ,HRG."HRGDescription" AS HRGDescription
    ,DIAG.code AS DiagCode
    ,DIAG_NAME."SNOMED_Fully_Specified_Name" AS DiagDesc
    ,ETH."EthnicityDesc" AS EthnicityDesc
    ,GEN."GenderCode2" AS GenderDesc
    ,IMD."IMDDECILE" AS DeprivationDecile
    ,ARR_MODE."ECDS_Description" AS AEArrivalModeDesc
    ,DEPT_TYPE."DepartmentTypeDescription"  as DepartmentTypeDescription
    ,CC_GP."ECDS_Description" as ECDS_Description
    ,CC_GP."SNOMED_UK_Preferred_Term" as SNOMED_UK_Preferred_Term
    ,CC_GP."ECDS_Group1" AS AEChiefComplaintGroupDesc
    ,DIAG.code AS AEDiagCode
    ,DIAG_NAME."SNOMED_Fully_Specified_Name" AS AEDiagDesc
    ,INV.code AS AEInvestigationCode
    ,INV_DESC."SNOMED_Fully_Specified_Name" AS AEInvestigationDesc
    ,DISC_DEST."SNOMED_UK_Preferred_Term" AS AEDischargeDestinationDesc
    ,ATT_SOURCE."ECDS_Description" AS AEReferralSourceDesc
    ,TRE.code AS AETreatmentCode
    ,TRE_DESC."SNOMED_Fully_Specified_Name" AS AETreatmentDesc

FROM {{ ref('stg_sus_ae_emergency_care') }} AS a

LEFT JOIN "Dictionary"."ECDS_ETOS"."ArrivalMode" AS AM 
ON a.attendance_arrival_arrival_mode_code = AM."SNOMED_Code"
--"NCL"."dbo"."DD_ECDS_ARRIVAL MODE"

LEFT OUTER JOIN "Dictionary"."dbo"."Organisation" AS ORG2 
ON ORG2."Organisation_Code" = a.attendance_location_hes_provider_3

LEFT OUTER JOIN "Dictionary"."dbo"."Organisation" AS ORG 
ON ORG."Organisation_Code" = a.attendance_location_site

LEFT OUTER JOIN "Dictionary"."dbo"."Organisation" AS ORG3 
ON ORG3."Organisation_Code" = replace(LEFT(a.commissioning_service_agreement_commissioner,5),'00','') 

LEFT JOIN "Dictionary"."dbo"."Organisation" AS ORG5 
ON ORG5."Organisation_Code" = a.patient_gp_registration_general_practice -- GP Name

LEFT OUTER JOIN "Dictionary"."dbo"."HRG" AS HRG 
ON a.commissioning_grouping_health_resource_group = HRG."HRGCode"

LEFT OUTER JOIN {{ ref('stg_sus_ae_clinical_diagnoses_snomed') }} AS DIAG 
ON a.primarykey_id = DIAG.primarykey_id and DIAG.is_primary = 1

LEFT OUTER JOIN "Dictionary"."ECDS_ETOS"."Diagnosis" AS DIAG_NAME
ON DIAG.code = DIAG_NAME."SNOMED_Code"
--"NCL"."dbo"."DD_ECDS_DIAGNOSIS"

LEFT JOIN "Dictionary"."dbo"."Commissioner" AS COM 
ON LEFT(a.commissioning_service_agreement_commissioner,3) = LEFT(COM."CommissionerCode",3)

LEFT JOIN "DATA_LAB_NCL_TRAINING_TEMP"."MAINDATA"."NCL_PRACTICE_PCN_V3" AS PCN 
ON a.patient_gp_registration_pds_general_practice = PCN."GPCODE"
--"Data_Lab_NCL_Test"."dbo"."NCL_Practice_PCN_v3"

LEFT JOIN "Dictionary"."NELCSU"."Pre_2020/21_LSOA_To_Commissioner" AS LSOA 
ON a.patient_usual_address_lsoa_11 = LSOA."LSOACode"

LEFT JOIN "Dictionary"."dbo"."Commissioner" AS COM2 
ON LSOA."CommissionerCode" = COM2."CommissionerCode"

LEFT OUTER JOIN "Dictionary"."dbo"."OrganisationMatrixPracticeView" AS ORG4 
ON ORG4."PracticeCode" = a.patient_gp_registration_general_practice  -- PCN

LEFT JOIN {{ ref('stg_sus_ae_clinical_investigations_snomed') }} AS INV 
ON a.primarykey_id = INV.primarykey_id and INV.snomed_id = 1

LEFT JOIN "Dictionary"."ECDS_ETOS"."Investigation" AS INV_DESC 
ON INV.code = INV_DESC."SNOMED_Code"
--"NCL"."dbo"."DD_ECDS_INVESTIGATION"

LEFT JOIN {{ ref('stg_sus_ae_clinical_treatments_snomed') }} AS TRE 
ON a.primarykey_id = TRE.primarykey_id and TRE.snomed_id = 1

LEFT JOIN "Dictionary"."ECDS_ETOS"."Treatment" AS TRE_DESC 
ON TRE.code = TRE_DESC."SNOMED_Code"
--"NCL"."dbo"."DD_ECDS_TREATMENT"

LEFT JOIN "Dictionary"."ECDS_ETOS"."DischargeDestination" AS DISC_DEST 
ON a.attendance_discharge_destination_code = DISC_DEST."SNOMED_Code"
--"NCL"."dbo"."DD_ECDS_DISCHARGE DESTINATION"

LEFT JOIN "Dictionary"."ECDS_ETOS"."ArrivalMode" AS ARR_MODE 
ON a.attendance_arrival_arrival_mode_code = ARR_MODE."SNOMED_Code"
--"NCL"."dbo"."DD_ECDS_ARRIVAL MODE"

LEFT JOIN "Dictionary"."ECDS_ETOS"."AttendanceSource" AS ATT_SOURCE 
ON a.attendance_arrival_attendance_source_code = ATT_SOURCE."SNOMED_Code"
--"NCL"."dbo"."DD_ECDS_ATTENDANCE SOURCE"

LEFT JOIN "Dictionary"."AE"."DepartmentType" AS DEPT_TYPE 
ON a.attendance_location_department_type = DEPT_TYPE."BK_DepartmentTypeCode"

LEFT JOIN "Dictionary"."dbo"."Ethnicity" AS ETH 
ON a.patient_ethnic_category = ETH."BK_EthnicityCode"

LEFT JOIN "Dictionary"."dbo"."Gender" AS GEN 
ON a.patient_stated_gender = GEN."GenderCode"

LEFT JOIN "DATA_LAB_NCL_TRAINING_TEMP"."MAINDATA"."IMD2019"  AS IMD 
ON IMD."LSOACODE" = a.patient_usual_address_lsoa_11  -- link to get 2019 deprivation decile
--"NCL"."dbo"."IMD2019"

LEFT JOIN "Dictionary"."ECDS_ETOS"."ChiefComplaint" AS CC_GP 
ON a.clinical_chief_complaint_code = CC_GP."SNOMED_Code"
--"NCL"."dbo"."DD_ECDS_CHIEF COMPLAINT"
