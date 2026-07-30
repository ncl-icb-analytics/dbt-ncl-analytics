SELECT  

--Identifiers & Data Management Derived Fields 
att."PRIMARYKEY_ID" AS "Record_Identifier" 
,att."ROWNUMBER_ID" AS "Row_Number_ID" 
,att."PATIENT_NHS_NUMBER_VALUE_PSEUDO" AS "Patient_ID" 
,att."SYSTEM_TRANSACTION_CDS_UNIQUE_IDENTIFIER" AS "CDS_Unique_Identifier" 
,att."PATIENT_LOCAL_PATIENT_IDENTIFIER_VALUE" AS "Local_Patient_Identifier" 
,att."COMMISSIONING_SERVICE_AGREEMENT_PROVIDER_REFERENCE_NUMBER" AS "Provider_Reference_Number" 
,dt."Fin_Year" AS "Financial_Year" 
,dt."Fin_Month_No" AS "Financial_Month" 
,att."COMMISSIONING_NATIONAL_PRICING_COSTING_PERIOD" AS "Applicable_Costing_Period" 

--Patient Detail Fields 
--,NULL																			AS Year_Of_Birth  
,CASE																			  
WHEN att."PATIENT_PATIENT_TYPE" = 'ADU' THEN 'Adult'									  
WHEN att."PATIENT_PATIENT_TYPE" = 'CHI' THEN 'Child'									  
ELSE att."PATIENT_PATIENT_TYPE"														  
END																				AS "Patient_Type" 
,att."PATIENT_AGE_AT_ARRIVAL"													AS "Age_At_Arrival" 
,att."PATIENT_USUAL_ADDRESS_INDEX_OF_MULTIPLE_DEPRIVATION_DECILE"				AS "Index_Of_Multiple_Deprivation_Decile" 
,att."PATIENT_RESIDENCE_CCG_FROM_PATIENT_POSTCODE"								AS "Commissioner_Code_From_Postcode" 
,att."PATIENT_STATED_GENDER"													AS "Gender" 
,att."PATIENT_ETHNIC_CATEGORY"													AS "Ethnic_Category" 
,att."PATIENT_GP_REGISTRATION_GENERAL_PRACTITIONER"								AS "GP_Code" 
,att."PATIENT_GP_REGISTRATION_GENERAL_PRACTICE"									AS "GP_Practice_Code" 

--Attendance Fields 
,LEFT(att."COMMISSIONING_SERVICE_AGREEMENT_PROVIDER", 3)	AS "Provider_Code" 
,"ATTENDANCE_LOCATION_SITE" AS "Site_Code"
,att."COMMISSIONING_SERVICE_AGREEMENT_COMMISSIONER" AS "Commissioner_Code" 
,att."PATIENT_USUAL_ADDRESS_POSTCODE_DISTRICT" AS "Postcode_District_Usual_Address" 
,att."PATIENT_USUAL_ADDRESS_LSOA_11" AS "LSOA_11_Usual_Address" 
,CASE 
WHEN "ATTENDANCE_LOCATION_SITE" = 'RQM25' THEN '03'
ELSE "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" 
END	AS "Department_Type" 
,"ATTENDANCE_ARRIVAL_DATE" AS "Arrival_Date"
,"ATTENDANCE_ARRIVAL_TIME" AS "Arrival_Time" 
,"ATTENDANCE_ARRIVAL_ARRIVAL_MODE_CODE" AS "Arrival_Mode" 
,"ATTENDANCE_ARRIVAL_ATTENDANCE_CATEGORY"	AS "Attendance_Category" 
,"ATTENDANCE_ARRIVAL_PLANNED" AS "Arrival_Planned" 
,"ATTENDANCE_ARRIVAL_AMBULANCE_INCIDENT_NUMBER" AS "Ambulance_Incident_Number" 
,"ATTENDANCE_ARRIVAL_CONVEYING_AMBULANCE_TRUST" AS "Conveying_Ambulance_Trust_Code" 
,"ATTENDANCE_ARRIVAL_AMBULANCE_CARE_CONTACT_IDENTIFIER" AS "Ambulance_Care_Contact_Identifier" 
,"ATTENDANCE_ARRIVAL_ATTENDANCE_SOURCE_CODE" AS "Attendance_Source" 
,"ATTENDANCE_ARRIVAL_ATTENDANCE_SOURCE_ORGANISATION"	AS "Attendance_Source_For_Organistion_Site_Identifier" 
,"ATTENDANCE_INITIAL_ASSESSMENT_DATE"	AS "Initial_Assessment_Date" 
,"ATTENDANCE_INITIAL_ASSESSMENT_TIME"	AS "Initial_Assessment_Time" 
,"ATTENDANCE_INITIAL_ASSESSMENT_TIME_SINCE_ARRIVAL" AS "Initial_Assessment_Time_Since_Arrival" 
,CAST(et."TIMESTAMP" AS TIME)														AS "Expected_Treatment_Time"
,"ATTENDANCE_SEEN_FOR_TREATMENT_DATE"	AS "Seen_For_Treatment_Date" 
,"ATTENDANCE_SEEN_FOR_TREATMENT_TIME"	AS "Seen_For_Treatment_Time" 
,"ATTENDANCE_SEEN_FOR_TREATMENT_TIME_SINCE_ARRIVAL"	AS "Seen_For_Treatment_Time_Since_Arrival" 
,"ATTENDANCE_CONCLUSION_DATE"	AS "Conclusion_Date" 
,"ATTENDANCE_CONCLUSION_TIME"	AS "Conclusion_Time" 
,"ATTENDANCE_CONCLUSION_TIME_SINCE_ARRIVAL" AS "Conclusion_Time_Since_Arrival" 
,"ATTENDANCE_DEPARTURE_DATE"	AS "Departure_Date" 
,"ATTENDANCE_DEPARTURE_TIME"	AS "Departure_Time" 
,"ATTENDANCE_DEPARTURE_TIME_SINCE_ARRIVAL" AS "Departure_Time_Since_Arrival" 
,"ATTENDANCE_DECISION_TO_ADMIT_DATE" AS "Decided_To_Admit_Date" 
,"ATTENDANCE_DECISION_TO_ADMIT_TIME" AS "Decided_To_Admit_Time" 
,"ATTENDANCE_DECISION_TO_ADMIT_TIME_SINCE_ARRIVAL"	AS "Decided_To_Admit_Time_Since_Arrival" 
,"ATTENDANCE_DECISION_TO_ADMIT_TREATMENT_FUNCTION_CODE" AS "Decided_To_Admit_Treatment_Function_Code" 
,"ATTENDANCE_DECISION_TO_ADMIT_RECEIVING_SITE" AS "Organisation_Site_Identifier_Discharge_From_Emergency_Care" 
,"ATTENDANCE_DISCHARGE_STATUS_CODE" AS "Discharge_Status" 
,"ATTENDANCE_DISCHARGE_DESTINATION_CODE" AS "Discharge_Destination" 
,"ATTENDANCE_DISCHARGE_FOLLOW_UP_CODE" AS "Discharge_Follow_Up" 
,"ATTENDANCE_DISCHARGE_INFORMATION_GIVEN_CODE" AS "Discharge_Information_Given" 
,"ATTENDANCE_CLINICALLY_READY_TO_PROCEED_TIMESTAMP" AS "Clinically_Ready_To_Proceed_Time" 
,"ATTENDANCE_CLINICALLY_READY_TO_PROCEED_TIME_SINCE_ARRIVAL"	AS "Clinically_Ready_To_Proceed_Time_Since_Arrival"	--Attendance Referred To Fields

,ar."SERVICE" AS "Referred_To_Service"
,ar."ASSESSMENT_DATE" AS "Referred_To_Service_Assessment_Date" 
,ar."ASSESSMENT_TIME" AS "Referred_To_Service_Assessment_Time" 

--Clinical Fields 
,att."CLINICAL_CHIEF_COMPLAINT_CODE" AS "Chief_Complaint" 
,att."CLINICAL_CHIEF_COMPLAINT_IS_INJURY_RELATED" AS "Chief_Complaint_Is_Injury_Related" 
,att."CLINICAL_ACUITY_CODE" AS "Acuity" 
,att."COMMISSIONING_GROUPING_HEALTH_RESOURCE_GROUP" AS "HRG_Code" 
,att."COMMISSIONING_NATIONAL_PRICING_EXCLUDED" AS "National_Tariff_Excluded" 
,att."COMMISSIONING_NATIONAL_PRICING_TARIFF" AS "National_Tariff" 
,att."COMMISSIONING_NATIONAL_PRICING_FINAL_PRICE"	AS "National_Tariff_Final_Price" 
,att."COMMISSIONING_NATIONAL_PRICING_MARKET_FORCES_FACTOR" AS "MFF_Factor" 
,att."COMMISSIONING_NATIONAL_PRICING_MARKET_FORCES_ADJUSTMENT" AS "MFF_Adjustment" 
,att."CLINICAL_INJURY_INTENT_CODE" AS "Injury_Intent" 
,att."CLINICAL_INJURY_MECHANISM_CODE" AS "Injury_Mechanism" 
,att."CLINICAL_INJURY_PLACE_TYPE" AS "Place_Of_Injury" 
,att."CLINICAL_INJURY_DATE" AS "Injury_Date" 
,att."CLINICAL_INJURY_TIME" AS "Injury_Time" 
,att."CLINICAL_DISEASE_NOTIFICATION_CODE"	AS "Disease_Notification" 

--Clinical Diagnosis Fields 
,di."1" AS "Primary_Diagnosis" 
,di."2" AS "Secondary_Diagnosis1" 
,di."3" AS "Secondary_Diagnosis2" 
,di."4" AS "Secondary_Diagnosis3" 
,di."5" AS "Secondary_Diagnosis4"
,di."6" AS "Secondary_Diagnosis5" 
,di."7" AS "Secondary_Diagnosis6" 
,di."8" AS "Secondary_Diagnosis7" 
,di."9" AS "Secondary_Diagnosis8" 
,di."10" AS "Secondary_Diagnosis9" 
,di."11" AS "Secondary_Diagnosis10" 
,di."12" AS "Secondary_Diagnosis11" 
,di."13" AS "Secondary_Diagnosis12" 

--Alcohol Related Injury Field 
,ia."CODE" AS "Inury_Alcohol_Drug_Involvement"

--Clinical Investigation Fields 
,iv."1" AS "Clinical_Investigation1"
,iv."2" AS "Clinical_Investigation2" 
,iv."3" AS "Clinical_Investigation3" 
,iv."4" AS "Clinical_Investigation4" 
,iv."5" AS "Clinical_Investigation5" 
,iv."6" AS "Clinical_Investigation6" 
,iv."7" AS "Clinical_Investigation7" 
,iv."8" AS "Clinical_Investigation8" 
,iv."9" AS "Clinical_Investigation9" 
,iv."10" AS "Clinical_Investigation10" 
,iv."11" AS "Clinical_Investigation11" 
,iv."12" AS "Clinical_Investigation12" 

--Clinical Treatment Fields 
,ct."1" AS "Treatment1" 
,ct."2" AS "Treatment2" 
,ct."3" AS "Treatment3" 
,ct."4" AS "Treatment4" 
,ct."5" AS "Treatment5" 
,ct."6" AS "Treatment6" 
,ct."7" AS "Treatment7" 
,ct."8" AS "Treatment8" 
,ct."9" AS "Treatment9" 
,ct."10" AS "Treatment10" 
,ct."11" AS "Treatment11" 
,ct."12" AS "Treatment12" 

--Comorbidity Fields 
,Co."1" AS "Comorbidity1" 
,Co."2" AS "Comorbidity2" 
,Co."3" AS "Comorbidity3" 
,Co."4" AS "Comorbidity4"
,Co."5" AS "Comorbidity5"
,Co."6" AS "Comorbidity6" 
,Co."7" AS "Comorbidity7" 
,Co."8" AS "Comorbidity8" 
,Co."9" AS "Comorbidity9" 
,Co."10" AS "Comorbidity10" 

--Mental Health Legal Status Field 
,'MH.CLASSIFICATION' AS "Mental_Health_Legal_Status_Classification" 
--Referral Fields 
,att."REFERRAL_PATIENT_PATHWAY_IDENTIFIER_VALUE_PSEUDO" AS "Patient_Pathway_Identifier" 
,att."REFERRAL_PERIOD_STATUS"	AS "Referral_To_Treatment_Period_Status" 
,att."REFERRAL_PERIOD_START_DATE"	AS "Referral_To_Treatment_Period_Start_Date" 
,att."REFERRAL_PERIOD_END_DATE" AS "Referrral_To_Treatment_Period_End_Date" 
,att."REFERRAL_WAITING_TIME_MEASUREMENT_TYPE"	AS "Waiting_Time_Measurement_Type" 

--zDerived Fields 
,att."COMMISSIONING_SERVICE_AGREEMENT_PROVIDER" AS "zProvider_Code" 

--zBusinessRules 
,CAST(CASE 
--CURRENT UTC/WICs IN NWL 
WHEN "ATTENDANCE_LOCATION_SITE" = 'RQM25' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" = '03' THEN 'UTC_CW'					--Chelsea & Westminster UTC (Co-located) 
WHEN "ATTENDANCE_LOCATION_SITE" = 'AD915' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" = '03' THEN 'UTC_EHT'					--Ealing Hospital UTC (Totally) 
WHEN "ATTENDANCE_LOCATION_SITE" = 'R1K14' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" = '03' THEN 'UTC_EHT'					--Ealing Hospital UTC (Trust) 
WHEN "ATTENDANCE_LOCATION_SITE" = 'AD904' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" = '03' THEN 'UTC_THH'					--Hillingdon Hospital UTC (Totally)
WHEN "ATTENDANCE_LOCATION_SITE" = 'RAS01' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" = '03' THEN 'UTC_THH'					--Hillingdon Hospital UTC (Trust) 
WHEN "ATTENDANCE_LOCATION_SITE" = 'AD906' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" = '03' THEN 'UTC_NP'					--Northwick Park Hospital UTC (Totally) 
WHEN "ATTENDANCE_LOCATION_SITE" = 'R1K11' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" = '03' THEN 'UTC_NP'					--Northwick Park Hospital UTC (Trust) 
WHEN "ATTENDANCE_LOCATION_SITE" = 'NLO21' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" = '03' THEN 'UTC_SMH'					--St Mary's UTC (Totally) 
WHEN "ATTENDANCE_LOCATION_SITE" = 'RYJ01' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" = '03' THEN 'UTC_SMH_Streamed'			--St Mary's UTC (Trust) 
WHEN "ATTENDANCE_LOCATION_SITE" = 'AD918' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" = '03' THEN 'UTC_CMX'					--Central Middlesex Hospital UTC (Totally) 
WHEN "ATTENDANCE_LOCATION_SITE" = 'R1K12' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" = '03' THEN 'UTC_CMX'					--Central Middlesex Hospital UTC (Trust)
WHEN "ATTENDANCE_LOCATION_SITE" = 'RY901' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" = '03' THEN 'UTC_WMX'					--West Middlesex Hospital UTC (Totally / submitted by HRCH) 
WHEN "ATTENDANCE_LOCATION_SITE" = 'RYX24' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" IN ('03', '04') THEN 'WIC_EDG'			--Edgware Walk-In Centre 
WHEN "ATTENDANCE_LOCATION_SITE" = 'RYX23' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" IN ('03', '04') THEN 'WIC_FINCH'		--Finchley Walk-In Centre 
WHEN "ATTENDANCE_LOCATION_SITE" = 'RYX11' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" IN ('03', '04') THEN 'WIC_PGREEN'		--Parsons Green Walk-In Centre 
WHEN "ATTENDANCE_LOCATION_SITE" = 'RYX02' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" IN ('03', '04') THEN 'WIC_SOHO'			--Soho Walk-In-Centre 
WHEN "ATTENDANCE_LOCATION_SITE" = 'RYX01' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" IN ('03', '04') THEN 'WIC_STCharles'	--St Charles Walk-In Centre 
WHEN "ATTENDANCE_LOCATION_SITE" = 'RAS02' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" = '03' THEN 'MIU_MV'					--Mount Vernon Hospital 
WHEN "ATTENDANCE_LOCATION_SITE" = 'RYJ02' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" = '03' THEN 'UTC_CX'					--Charing Cross UTC

--NOT CURRENTLY SUBMITTING: 
WHEN "ATTENDANCE_LOCATION_SITE" = 'RYJ03' AND "ATTENDANCE_LOCATION_DEPARTMENT_TYPE" = '03' THEN 'UTC_HH'					--Hammersmith Hospital UTC 
ELSE NULL 
END																			AS VARCHAR(50))	AS zBusinessRule


FROM {{ref('raw_sus_ae_emergency_care')}} att 


LEFT JOIN "UKHFD"."dbo"."dim_ref_Dates" dt 
ON att."ATTENDANCE_ARRIVAL_DATE" = dt."Full_Date" 


LEFT JOIN {{ref('raw_sus_ae_attendance_referred_to')}} AS ar 
ON att."PRIMARYKEY_ID" = ar."PRIMARYKEY_ID"  
AND "REFERRED_TO_ID" = 1 

LEFT JOIN (SELECT
				*
			FROM (
						SELECT
							PRIMARYKEY_ID,
							"CODE",
							RowNum
						FROM
							(
							SELECT
								ROW_NUMBER() OVER (PARTITION BY
									PRIMARYKEY_ID
								ORDER BY "SEQUENCE_NUMBER" NULLS FIRST) AS RowNum,*
							FROM {{ref('raw_sus_ae_clinical_diagnoses_snomed') }}
							) a
						WHERE
							RowNum BETWEEN 1 AND 13 
						) d PIVOT (MAX(d."CODE") FOR d.RowNum IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)) P
						) as di
	ON att.PRIMARYKEY_ID = di.PRIMARYKEY_ID

LEFT JOIN
		{{ ref('raw_sus_ae_clinical_injury_alcohol_drug_involvements') }}  AS ia
	ON att.PRIMARYKEY_ID = ia.PRIMARYKEY_ID
		AND ia.ALCOHOL_DRUG_INVOLVEMENTS_ID =
 		                                     --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
 		                                     (SELECT
				MIN(ia.ALCOHOL_DRUG_INVOLVEMENTS_ID) FROM
				{{ ref('raw_sus_ae_clinical_injury_alcohol_drug_involvements') }}  ia
			WHERE
				att.PRIMARYKEY_ID = ia.PRIMARYKEY_ID
 		                                     )
LEFT JOIN (SELECT
				*
			FROM (
						SELECT
							PRIMARYKEY_ID,
							RowNum,
							"CODE"
						FROM
						(
						SELECT
								ROW_NUMBER() OVER (PARTITION BY
									PRIMARYKEY_ID
								ORDER BY SNOMED_ID NULLS FIRST) AS RowNum,
								*
						FROM
								{{ ref('raw_sus_ae_clinical_investigations_snomed') }}
							) ci
						WHERE
							RowNum BETWEEN 1 AND 12
						) ci
						PIVOT (MAX("CODE") FOR RowNum IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) P
						) iv
	ON att.PRIMARYKEY_ID = iv.PRIMARYKEY_ID

LEFT JOIN (SELECT
				*
			FROM (
					SELECT
							PRIMARYKEY_ID,
							"CODE",
							RowNum
					FROM
						(
						SELECT
								ROW_NUMBER() OVER (PARTITION BY
									PRIMARYKEY_ID
								ORDER BY "DATE" NULLS FIRST, "TIME" NULLS FIRST, SNOMED_ID NULLS FIRST) AS RowNum,
								*
						FROM
								{{ ref('raw_sus_ae_clinical_treatments_snomed') }}
						) a
						WHERE
							RowNum BETWEEN 1 AND 12) t
						PIVOT (MAX(t."CODE") FOR t.RowNum IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) P
						) ct
	ON att.PRIMARYKEY_ID = ct.PRIMARYKEY_ID

LEFT JOIN
	{{ref('raw_sus_ae_patient_mental_health_act_legal_status')}} AS mh
	ON att.PRIMARYKEY_ID = mh.PRIMARYKEY_ID
		AND mh.MENTAL_HEALTH_ACT_LEGAL_STATUS_ID = 1
		--AND mh.MENTAL_HEALTH_ACT_LEGAL_STATUS_ID = (SELECT MIN(mh.MENTAL_HEALTH_ACT_LEGAL_STATUS_ID) FROM [dmic_ECDS].[ECDS].[patient.mental_health_act_legal_status] AS mh WITH (NOLOCK) 
			--WHERE att.PRIMARYKEY_ID = mh.PRIMARYKEY_ID)
LEFT JOIN (
				SELECT
				*
				FROM (


				SELECT DISTINCT
							ROWNUMBER_ID
					  ,
							PRIMARYKEY_ID
					  ,
							COMORBIDITIES_ID
					  ,
							"CODE"
					  ,
							"IS_CODE_APPROVED"
					  ,
							"DMIC_IMPORT_LOG_ID"
				  FROM
							{{ ref('raw_sus_ae_clinical_comorbidities') }}
				  WHERE
							"CODE" IS NOT NULL

				) a --) a

				PIVOT (MAX("CODE")
					FOR COMORBIDITIES_ID IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10

					)) pvt
	) Co
	ON att.PRIMARYKEY_ID = Co.PRIMARYKEY_ID

LEFT JOIN
	{{ref('raw_sus_ae_attendance_expected_treatment_times')}} et
	ON att.PRIMARYKEY_ID = et.PRIMARYKEY_ID
	AND et.EXPECTED_TREATMENT_TIMES_ID = 1


