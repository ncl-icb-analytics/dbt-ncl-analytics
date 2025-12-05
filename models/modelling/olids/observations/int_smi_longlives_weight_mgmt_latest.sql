{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--using mixed codes defined by SMI Longer Lives Campaign covers Weight Management Interventions
select *
FROM {{ ref('int_smi_longlives_weight_mgmt') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1