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
,clinical_effective_date
,concept_display
,result_value
,result_unit_display
FROM {{ ref('int_smi_alcohol_all') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1