{{
    config(
        materialized='table',
        database='MODELLING',
        schema='LEGACY_NWL',
        tags=['legacy_nwl']
    )
}}

-- Transitional model preserving the existing NWL ECDS output.
-- Takes the 1:1 attendance record from stg_ecds_consolidated and joins the
-- date dimension and the ECDS child tables: referrals, pivoted diagnoses (13),
-- investigations (12), treatments (12) and comorbidities (10), plus alcohol /
-- drug involvement, mental health legal status and expected treatment time.
-- Transitional pending alignment with the shared Acute model design.

select

    -- Identifiers & data management derived fields
    c."Record_Identifier"
    ,c."Row_Number_ID"
    ,c."Patient_ID"
    ,c."CDS_Unique_Identifier"
    ,c."Local_Patient_Identifier"
    ,c."Provider_Reference_Number"
    ,dt.fin_year AS "Financial_Year"
    ,dt.fin_month_no AS "Financial_Month"
    ,c."Applicable_Costing_Period"

    -- Patient detail fields
    ,c."Patient_Type"
    ,c."Age_At_Arrival"
    ,c."Index_Of_Multiple_Deprivation_Decile"
    ,c."Commissioner_Code_From_Postcode"
    ,c."Gender"
    ,c."Ethnic_Category"
    ,c."GP_Code"
    ,c."GP_Practice_Code"

    -- Attendance fields
    ,c."Provider_Code"
    ,c."Site_Code"
    ,c."Commissioner_Code"
    ,c."Postcode_District_Usual_Address"
    ,c."LSOA_11_Usual_Address"
    ,c."Department_Type"
    ,c."Arrival_Date"
    ,c."Arrival_Time"
    ,c."Arrival_Mode"
    ,c."Attendance_Category"
    ,c."Arrival_Planned"
    ,c."Ambulance_Incident_Number"
    ,c."Conveying_Ambulance_Trust_Code"
    ,c."Ambulance_Care_Contact_Identifier"
    ,c."Attendance_Source"
    ,c."Attendance_Source_For_Organisation_Site_Identifier"
    ,c."Initial_Assessment_Date"
    ,c."Initial_Assessment_Time"
    ,c."Initial_Assessment_Time_Since_Arrival"
    ,et.timestamp AS "Expected_Treatment_Time"
    ,c."Seen_For_Treatment_Date"
    ,c."Seen_For_Treatment_Time"
    ,c."Seen_For_Treatment_Time_Since_Arrival"
    ,c."Conclusion_Date"
    ,c."Conclusion_Time"
    ,c."Conclusion_Time_Since_Arrival"
    ,c."Departure_Date"
    ,c."Departure_Time"
    ,c."Departure_Time_Since_Arrival"
    ,c."Decided_To_Admit_Date"
    ,c."Decided_To_Admit_Time"
    ,c."Decided_To_Admit_Time_Since_Arrival"
    ,c."Decided_To_Admit_Treatment_Function_Code"
    ,c."Organisation_Site_Identifier_Discharge_From_Emergency_Care"
    ,c."Discharge_Status"
    ,c."Discharge_Destination"
    ,c."Discharge_Follow_Up"
    ,c."Discharge_Information_Given"
    ,c."Clinically_Ready_To_Proceed_Time"
    ,c."Clinically_Ready_To_Proceed_Time_Since_Arrival"

    -- Attendance referred to fields
    ,ar.service AS "Referred_To_Service"
    ,ar.assessment_date AS "Referred_To_Service_Assessment_Date"
    ,ar.assessment_time AS "Referred_To_Service_Assessment_Time"

    -- Clinical fields
    ,c."Chief_Complaint"
    ,c."Chief_Complaint_Is_Injury_Related"
    ,c."Acuity"
    ,c."HRG_Code"
    ,c."National_Tariff_Excluded"
    ,c."National_Tariff"
    ,c."National_Tariff_Final_Price"
    ,c."MFF_Factor"
    ,c."MFF_Adjustment"
    ,c."Injury_Intent"
    ,c."Injury_Mechanism"
    ,c."Place_Of_Injury"
    ,c."Injury_Date"
    ,c."Injury_Time"
    ,c."Disease_Notification"

    -- Clinical diagnosis fields
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

    -- Alcohol related injury field
    ,ia.code AS "Injury_Alcohol_Drug_Involvement"

    -- Clinical investigation fields
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

    -- Clinical treatment fields
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

    -- Comorbidity fields
    ,co."1" AS "Comorbidity1"
    ,co."2" AS "Comorbidity2"
    ,co."3" AS "Comorbidity3"
    ,co."4" AS "Comorbidity4"
    ,co."5" AS "Comorbidity5"
    ,co."6" AS "Comorbidity6"
    ,co."7" AS "Comorbidity7"
    ,co."8" AS "Comorbidity8"
    ,co."9" AS "Comorbidity9"
    ,co."10" AS "Comorbidity10"

    -- Mental health legal status field
    ,mh."CLASSIFICATION" AS "Mental_Health_Legal_Status_Classification"

    -- Referral fields
    ,c."Patient_Pathway_Identifier"
    ,c."Referral_To_Treatment_Period_Status"
    ,c."Referral_To_Treatment_Period_Start_Date"
    ,c."Referral_To_Treatment_Period_End_Date"
    ,c."Waiting_Time_Measurement_Type"

    -- zDerived fields
    ,c."zProvider_Code"
    ,c.ZBUSINESSRULE

from {{ ref('stg_ecds_consolidated') }} c

LEFT JOIN {{ ref('stg_ukhfd_dim_ref_dates') }} dt
ON c."Arrival_Date" = dt.full_date

LEFT JOIN {{ ref('stg_sus_ecds_attendance_referred_to') }} AS ar
ON c."Record_Identifier" = ar."PRIMARYKEY_ID"
AND ar."REFERRED_TO_ID" = 1

LEFT JOIN (SELECT
				*
			FROM (
						SELECT
							PRIMARYKEY_ID,
							code,
							RowNum
						FROM
							(
							SELECT
								ROW_NUMBER() OVER (PARTITION BY
									PRIMARYKEY_ID
								ORDER BY sequence_number NULLS FIRST) AS RowNum,*
							FROM {{ ref('stg_sus_ecds_clinical_diagnoses_snomed') }}
							) a
						WHERE
							RowNum BETWEEN 1 AND 13
						) d PIVOT (MAX(d.code) FOR d.RowNum IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)) P
						) as di
	ON c."Record_Identifier" = di.PRIMARYKEY_ID

LEFT JOIN
		{{ ref('stg_sus_ecds_clinical_injury_alcohol_drug_involvements') }}  AS ia
	ON c."Record_Identifier" = ia.PRIMARYKEY_ID
		AND ia.ALCOHOL_DRUG_INVOLVEMENTS_ID =
 		                                     --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
 		                                     (SELECT
				MIN(ia2.ALCOHOL_DRUG_INVOLVEMENTS_ID) FROM
				{{ ref('stg_sus_ecds_clinical_injury_alcohol_drug_involvements') }}  ia2
			WHERE
				c."Record_Identifier" = ia2.PRIMARYKEY_ID
 		                                     )

LEFT JOIN (SELECT
				*
			FROM (
						SELECT
							PRIMARYKEY_ID,
							RowNum,
							code
						FROM
						(
						SELECT
								ROW_NUMBER() OVER (PARTITION BY
									PRIMARYKEY_ID
								ORDER BY SNOMED_ID NULLS FIRST) AS RowNum,
								*
						FROM
								{{ ref('stg_sus_ecds_clinical_investigations_snomed') }}
							) ci
						WHERE
							RowNum BETWEEN 1 AND 12
						) ci
						PIVOT (MAX(code) FOR RowNum IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) P
						) iv
	ON c."Record_Identifier" = iv.PRIMARYKEY_ID

LEFT JOIN (SELECT
				*
			FROM (
					SELECT
							PRIMARYKEY_ID,
							code,
							RowNum
					FROM
						(
						SELECT
								ROW_NUMBER() OVER (PARTITION BY
									PRIMARYKEY_ID
								ORDER BY date NULLS FIRST, time NULLS FIRST, SNOMED_ID NULLS FIRST) AS RowNum,
								*
						FROM
								{{ ref('stg_sus_ecds_clinical_treatments_snomed') }}
						) a
						WHERE
							RowNum BETWEEN 1 AND 12) t
						PIVOT (MAX(t.code) FOR t.RowNum IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) P
						) ct
	ON c."Record_Identifier" = ct.PRIMARYKEY_ID

LEFT JOIN
	{{ ref('stg_sus_ecds_patient_mental_health_act_legal_status') }} AS mh
	ON c."Record_Identifier" = mh.PRIMARYKEY_ID
		AND mh.MENTAL_HEALTH_ACT_LEGAL_STATUS_ID = 1

--Corrected pivot as per Coderabbit review
LEFT JOIN (
    SELECT *
    FROM (
        SELECT
            primarykey_id,
            comorbidities_id,
            code
        FROM {{ ref('stg_sus_ecds_clinical_comorbidities') }}
        WHERE code IS NOT null
    ) AS comorbidities
    PIVOT (
        max(code) FOR comorbidities_id IN (
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        )
    ) AS pivoted
) AS co
    ON c."Record_Identifier" = co.primarykey_id

LEFT JOIN
	{{ ref('stg_sus_ecds_attendance_expected_treatment_times') }} et
	ON c."Record_Identifier" = et.PRIMARYKEY_ID
	AND et.EXPECTED_TREATMENT_TIMES_ID = 1