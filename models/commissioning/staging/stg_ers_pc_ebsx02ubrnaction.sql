{{
    config(materialized = 'view')
}}

select UBRN_ID
    ,E_REFERRAL_PATHWAY_START
    ,PATIENT_ID
    ,PATIENTS_REG_GP_PRACTICE_ID
    ,NHS_NUMBER_Pseudo as sk_patient_id
    ,PATIENT_AGE
    ,ORIGINAL_PRIORITY_CD
    ,ORIGINAL_PRIORITY_DESC
    ,SPECIALTY_CD
    ,SPECIALTY_DESC
    ,CLINIC_TYPE_CD
    ,CLINIC_TYPE_DESC
    ,PRIORITY_CD
    ,PRIORITY_DESC
    ,REFERRER_ORG_NAME
    ,REFERRING_ORG_ID
    ,APPT_TYPE_CD
    ,APPT_TYPE_DESC
    ,PROVIDER_ORG_ID
    ,provider_org_name
    ,PROVIDER_ORG_TYPE_DESC
    ,SERVICE_SPECIALTY_CD
    ,SERVICE_SPECIALTY_DESC
    ,action_dt_tm
    ,action_desc
from {{ ref('raw_ers_pc_ebsx02ubrnaction') }}