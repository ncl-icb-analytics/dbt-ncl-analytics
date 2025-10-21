{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
MH/SMI comprehensive care plan QOF Indicator. Date of the mental health care plan code MHP_COD
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Selects the latest care plan per person.
*/
select 
person_id
,clinical_effective_date
,MH_CARE_PLAN_CURRENT_12M
FROM {{ ref('int_smi_care_plan_all') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1