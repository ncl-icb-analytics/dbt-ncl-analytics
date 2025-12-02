{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
/*
All Pulmonary Rehab observations from clinical records.
For SMI Longer Lives Enhanced Review:
- PULRHBATT_COD: Pulmonary Rehab Attended codes
- PULRHBPU_COD: Pulmonary Rehab Unsuitable codes  
- PULRHBDEC_COD: Pulmonary Rehab Declined codes
- PULRHBOFF_COD: Pulmonary Rehab Declined codes
*/

select *
FROM {{ ref('int_referral_pulmonary_rehab') }}

QUALIFY ROW_NUMBER()
        OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
    = 1

