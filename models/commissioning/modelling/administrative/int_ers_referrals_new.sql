{{
    config(
        materialized='table')
}}


/*
All Recent referrals

Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.

*/
{% set years_from_now = -2 %}

WITH 
    accepted_referrals AS (
        select
            UBRN_ID
            ,APPT_TYPE_CD
            ,APPT_TYPE_DESC
            ,PROVIDER_ORG_ID
            ,provider_org_name
            ,PROVIDER_ORG_TYPE_DESC
            ,SERVICE_SPECIALTY_CD
            ,SERVICE_SPECIALTY_DESC
        from {{ ref('stg_ers_pc_ebsx02ubrnaction')}}
        where ACTION_CD = 1420 -- 'Referral accepted' code
            and E_REFERRAL_PATHWAY_START BETWEEN DATEADD(YEAR, {{years_from_now}}, CURRENT_DATE()) AND CURRENT_DATE()
            and E_REFERRAL_PATHWAY_START<=CURRENT_DATE()
    )

select
    a.UBRN_ID
    ,a.E_REFERRAL_PATHWAY_START
    ,a.PATIENT_ID
    ,a.PATIENTS_REG_GP_PRACTICE_ID
    ,a.sk_patient_id
    ,a.PATIENT_AGE
    ,a.ORIGINAL_PRIORITY_CD
    ,a.ORIGINAL_PRIORITY_DESC
    ,a.SPECIALTY_CD
    ,a.SPECIALTY_DESC
    ,a.CLINIC_TYPE_CD
    ,a.CLINIC_TYPE_DESC
    ,a.PRIORITY_CD
    ,a.PRIORITY_DESC
    ,a.REFERRER_ORG_NAME
    ,a.REFERRING_ORG_ID
    ,b.APPT_TYPE_CD
    ,b.APPT_TYPE_DESC
    ,b.PROVIDER_ORG_ID
    ,b.provider_org_name
    ,b.PROVIDER_ORG_TYPE_DESC
    ,b.SERVICE_SPECIALTY_CD
    ,b.SERVICE_SPECIALTY_DESC
from {{ ref('stg_ers_pc_ebsx02ubrnaction')}} as a
left join accepted_referrals as b on a.ubrn_id = b.ubrn_id 
where ACTION_CD = 1422 -- 'Referral Created' code
    and E_REFERRAL_PATHWAY_START BETWEEN DATEADD(YEAR, {{years_from_now}}, CURRENT_DATE()) AND CURRENT_DATE()
    and E_REFERRAL_PATHWAY_START<=CURRENT_DATE()

