-- Gets list of attendances and diagnoses in last 12 months (monthly grain)
 {{ config(materialized="table") }}

SELECT
        ip.VISIT_OCCURRENCE_ID AS primary_id,
        'IP' AS pod_group,
        ip.POD AS pod,
        REPORTING.MAIN_DATA.DETERMINE_FISCAL_YEAR(ip.END_DATE) AS FIN_YEAR,
        MOD(MONTH(ip.END_DATE) + 8, 12) + 1 AS fin_month,
        ip.SK_PATIENT_ID AS patient_id,
        ip.local_patient_identifier,
        ip.REG_PRACTICE_AT_EVENT AS gp_code,
        ip.ORGANISATION_ID AS provider_code,
        ip.ORGANISATION_NAME AS provider_name,
        ip.SITE_ID AS provider_site_code,
        ip.SITE_NAME AS provider_site_name,
        ip.END_DATE AS ACTIVITY_DATE,            
        dx.ICD_ID AS diag_n,
        dx.SOURCE_CONCEPT_CODE AS diag_code,
        ip.gender_at_event,
        ip.ethnicity_at_event,
        ip.age_at_event,
        ip.reg_practice_at_event,
        DATEDIFF(MM,ip.END_DATE,DATE_TRUNC('month',CURRENT_DATE)) as activity_months_ago -- use this in int_myria_conditions to flag 6 mth/1 year/2 year periods
        -- add col for how many months ago
    FROM
       {{ ref("int_sus_ip_encounters") }} ip
    LEFT JOIN {{ ref("int_sus_ip_diagnosis") }} dx
        ON ip.VISIT_OCCURRENCE_ID = dx.VISIT_OCCURRENCE_ID
     WHERE 
        ip.END_DATE < DATE_TRUNC('month',CURRENT_DATE) -- only activity before the start of this month

UNION ALL

SELECT
        op.VISIT_OCCURRENCE_ID AS primary_id,
        'OP' AS pod_group,
        op.POD AS pod,
        REPORTING.MAIN_DATA.DETERMINE_FISCAL_YEAR(op.START_DATE) AS FIN_YEAR,
        MOD(MONTH(op.START_DATE) + 8, 12) + 1 AS fin_month,
        op.SK_PATIENT_ID AS patient_id,
        op.local_patient_identifier,
        op.REG_PRACTICE_AT_EVENT AS gp_code,
        op.ORGANISATION_ID AS provider_code,
        op.ORGANISATION_NAME AS provider_name,
        op.SITE_ID AS provider_site_code,
        op.SITE_NAME AS provider_site_name,    
        op.START_DATE AS ACTIVITY_DATE,         
        dx.ICD_ID AS diag_n,
        dx.SOURCE_CONCEPT_CODE AS diag_code,
        op.gender_at_event,
        op.ethnicity_at_event,
        op.age_at_event,
        op.reg_practice_at_event,
        DATEDIFF(MM,op.START_DATE,DATE_TRUNC('month',CURRENT_DATE)) as activity_months_ago
    FROM 
       {{ ref("int_sus_op_encounters") }} op
    LEFT JOIN {{ ref("int_sus_op_diagnosis") }} dx
        ON op.VISIT_OCCURRENCE_ID = dx.VISIT_OCCURRENCE_ID
     WHERE
        op.START_DATE < DATE_TRUNC('month',CURRENT_DATE) -- only activity before the start of this month
        
    
UNION ALL

SELECT
        ae.VISIT_OCCURRENCE_ID AS primary_id,
        'AE' AS pod_group,
        ae.POD AS pod,
        REPORTING.MAIN_DATA.DETERMINE_FISCAL_YEAR(ae.START_DATE) AS FIN_YEAR,
        MOD(MONTH(ae.START_DATE) + 8, 12) + 1 AS fin_month,
        ae.SK_PATIENT_ID AS patient_id,
        ae.local_patient_identifier,
        ae.REG_PRACTICE_AT_EVENT AS gp_code,
        ae.ORGANISATION_ID AS provider_code,
        ae.ORGANISATION_NAME AS provider_name,
        ae.SITE_ID AS provider_site_code,
        ae.SITE_NAME AS provider_site_name, 
        ae.START_DATE AS ACTIVITY_DATE,           
        dx.SNOMED_ID AS diag_n,
        dx.MAPPED_ICD10_CODE AS diag_code,
        ae.gender_at_event,
        ae.ethnicity_at_event,
        ae.age_at_event,
        ae.reg_practice_at_event,
        DATEDIFF(MM,ae.START_DATE,DATE_TRUNC('month',CURRENT_DATE)) as activity_months_ago
    FROM 
       {{ ref("int_sus_ae_encounters") }} ae
    LEFT JOIN {{ ref("int_sus_ae_diagnosis") }} dx
        ON ae.VISIT_OCCURRENCE_ID = dx.VISIT_OCCURRENCE_ID
     WHERE
        ae.START_DATE < DATE_TRUNC('month',CURRENT_DATE) -- only activity before the start of this month