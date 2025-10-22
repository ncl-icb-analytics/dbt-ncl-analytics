{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}

/*
MH/SMI alcohol consumption QOF Indicator MH007. Date of the alchol consumption code ALC_COD
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Selects the latest code per person.
*/
select 
person_id
,gender
,clinical_effective_date
,concept_display
,result_value
,result_unit_display
,alcohol_risk_category
FROM {{ ref('int_smi_alcohol_all') }} a
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY clinical_effective_date DESC, CASE WHEN result_value IS NOT NULL THEN 1 ELSE 0 END DESC) = 1