{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
    )
}}
/*
MH/SMI record of alcohol consumption QOF Indicator MH007. Date of the alcohol consumption code ALC_COD
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/
WITH base_observations AS (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.result_value,
    obs.result_unit_display,
    FROM ({{ get_observations("'ALC_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
),

observations_with_demographics AS (
    SELECT DISTINCT
        bo.person_id,
        pd.gender,
        date(bo.clinical_effective_date) as clinical_effective_date,
        bo.concept_code,
        bo.concept_display,
        bo.result_value,
        CASE
when bo.result_unit_display in ('{units}/wk','units/w','upw','unit/wk','u/ week','u(nits)/we','units/week','Units PW','Units per wk','Units/wk','Unt/Wk','U/wkly','U/wee0') THEN 'U/week'
when bo.result_unit_display ILIKE '%/uweek' THEN 'U/week'
when bo.result_unit_display ILIKE ('%U/week') THEN 'U/week'
WHEN bo.result_unit_display ILIKE ('U/week%') THEN 'U/week' 
WHEN bo.result_unit_display ILIKE ('%unit%per%week%') THEN 'U/week' 
WHEN bo.result_unit_display ILIKE ('%unit%week%') THEN 'U/week' 
ELSE bo.result_unit_display END AS result_unit_display
    FROM base_observations bo
    LEFT JOIN {{ ref('dim_person_demographics') }} pd
        ON bo.person_id = pd.person_id
)
select 
person_id,
gender,
clinical_effective_date,
concept_display,
result_value, 
result_unit_display,
CASE 
WHEN (concept_display ilike ('Ex%') OR concept_display = 'Stopped drinking alcohol') AND result_value = 0 AND result_unit_display = 'U/week' THEN 'Ex-Drinker'
WHEN concept_display in ('Alcohol units consumed per week','Alcoholic beverage intake','Alcohol intake') AND result_value = 0 AND result_unit_display = 'U/week' THEN 'Non-Drinker'
WHEN concept_display in ('Non - drinker', 'Lifetime non-drinker of alcohol','Lifetime non-drinker') AND result_value = 0 AND result_unit_display = 'U/week' THEN 'Non-Drinker'
WHEN concept_display in ('Alcohol units consumed per week','Alcoholic beverage intake','Alcohol intake') AND result_unit_display = 'U/week' AND result_value between 1 AND 14 THEN 'Low Risk'
WHEN concept_display in ('Alcohol units consumed per week','Alcoholic beverage intake','Alcohol intake') AND result_unit_display = 'U/week' AND result_value between 15 AND 34 AND GENDER = 'Female' THEN 'Increasing Risk' 
WHEN concept_display in ('Alcohol units consumed per week','Alcoholic beverage intake','Alcohol intake') AND result_unit_display = 'U/week' AND result_value between 15 AND 49 AND GENDER = 'Male' THEN 'Increasing Risk'
WHEN concept_display in ('Alcohol units consumed per week','Alcoholic beverage intake','Alcohol intake') AND result_unit_display = 'U/week' AND result_value >= 35 AND GENDER = 'Female' THEN 'High Risk' 
WHEN concept_display in ('Alcohol units consumed per week','Alcoholic beverage intake','Alcohol intake') AND result_unit_display = 'U/week' AND result_value >= 50 AND GENDER = 'Male' THEN 'High Risk' ELSE 'Unclear'
END AS alcohol_risk_category
from observations_with_demographics