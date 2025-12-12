-- Gets list of attendances and diagnoses before 2025/26.
{{ config(materialized="table") }}

SELECT
        ip.VISIT_OCCURRENCE_ID AS primary_id,
        'IP' AS pod_group,
        ip.POD AS pod,
        REPORTING.MAIN_DATA.DETERMINE_FISCAL_YEAR(ip.END_DATE) AS FIN_YEAR,
        MOD(MONTH(ip.END_DATE) + 8, 12) + 1 AS fin_month,
        ip.SK_PATIENT_ID AS patient_id,
        ip.REG_PRACTICE_AT_EVENT AS gp_code,
        ip.ORGANISATION_ID AS provider_code,
        ip.ORGANISATION_NAME AS provider_name,
        ip.SITE_ID AS provider_site_code,
        ip.SITE_NAME AS provider_site_name,            
        dx.ICD_ID AS diag_n,
        dx.CONCEPT_CODE AS diag_code
    FROM
       {{ ref("int_sus_ip_encounters") }} ip --DEV__MODELLING.COMMISSIONING_MODELLING.INT_SUS_IP_ENCOUNTERS AS ip
    LEFT JOIN {{ ref("int_sus_ip_diagnosis") }} dx -- DEV__MODELLING.COMMISSIONING_MODELLING.INT_SUS_IP_DIAGNOSIS AS dx
        ON ip.VISIT_OCCURRENCE_ID = dx.VISIT_OCCURRENCE_ID
     WHERE 
        ip.END_DATE < '01-Apr-2025' -- only activity before April 2025

UNION ALL

SELECT
        op.VISIT_OCCURRENCE_ID AS primary_id,
        'OP' AS pod_group,
        op.POD AS pod,
        REPORTING.MAIN_DATA.DETERMINE_FISCAL_YEAR(op.START_DATE) AS FIN_YEAR,
        MOD(MONTH(op.START_DATE) + 8, 12) + 1 AS fin_month,
        op.SK_PATIENT_ID AS patient_id,
        op.REG_PRACTICE_AT_EVENT AS gp_code,
        op.ORGANISATION_ID AS provider_code,
        op.ORGANISATION_NAME AS provider_name,
        op.SITE_ID AS provider_site_code,
        op.SITE_NAME AS provider_site_name,            
        dx.ICD_ID AS diag_n,
        dx.CONCEPT_CODE AS diag_code
    FROM 
       {{ ref("int_sus_op_encounters") }} op -- DEV__MODELLING.COMMISSIONING_MODELLING.INT_SUS_OP_ENCOUNTERS AS op
    LEFT JOIN {{ ref("int_sus_op_diagnosis") }} dx -- DEV__MODELLING.COMMISSIONING_MODELLING.INT_SUS_OP_DIAGNOSIS AS dx
        ON op.VISIT_OCCURRENCE_ID = dx.VISIT_OCCURRENCE_ID
     WHERE
        op.START_DATE < '01-Apr-2025' -- only activity before April 2025
        
    
UNION ALL

SELECT
        ae.VISIT_OCCURRENCE_ID AS primary_id,
        'AE' AS pod_group,
        ae.POD AS pod,
        REPORTING.MAIN_DATA.DETERMINE_FISCAL_YEAR(ae.START_DATE) AS FIN_YEAR,
        MOD(MONTH(ae.START_DATE) + 8, 12) + 1 AS fin_month,
        ae.SK_PATIENT_ID AS patient_id,
        ae.REG_PRACTICE_AT_EVENT AS gp_code,
        ae.ORGANISATION_ID AS provider_code,
        ae.ORGANISATION_NAME AS provider_name,
        ae.SITE_ID AS provider_site_code,
        ae.SITE_NAME AS provider_site_name,            
        dx.SNOMED_ID AS diag_n,
        dx.MAPPED_ICD10_CODE AS diag_code
    FROM 
       {{ ref("int_sus_ae_encounters") }} ae -- DEV__MODELLING.COMMISSIONING_MODELLING.INT_SUS_AE_ENCOUNTERS AS ae
    LEFT JOIN {{ ref("int_sus_ae_diagnosis") }} dx -- DEV__MODELLING.COMMISSIONING_MODELLING.INT_SUS_AE_DIAGNOSIS AS dx
        ON ae.VISIT_OCCURRENCE_ID = dx.VISIT_OCCURRENCE_ID
     WHERE
        ae.START_DATE < '01-Apr-2025' -- only activity before April 2025