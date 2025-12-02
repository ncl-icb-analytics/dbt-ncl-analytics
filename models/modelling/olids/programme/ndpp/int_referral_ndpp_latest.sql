{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--using PCDREFSET CODES FOR NDPP REFERRALS including invitations sent and declines - Selecting latest per person
SELECT
    person_id,
    clinical_effective_date,
    concept_code,
    concept_display
    FROM {{ ref('int_referral_ndpp_all') }}
QUALIFY
    ROW_NUMBER()
        OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
    = 1