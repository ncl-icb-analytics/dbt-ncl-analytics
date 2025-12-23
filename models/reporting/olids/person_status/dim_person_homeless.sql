{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'care_home', 'residence'],
        cluster_by=['person_id'])
}}

-- Get all residential status codes for each person using COVID and FLU CODE SETS

With homeless_codes as (
select distinct 
CLUSTER_ID
,CODE  
,replace(CODE_DESCRIPTION, ' (finding)','') as CODE_DESCRIPTION
FROM {{ ref('stg_reference_combined_codesets') }}
--from MODELLING.DBT_STAGING.STG_REFERENCE_COMBINED_CODESETS
WHERE cluster_id in ('RESIDE_COD', 'HOMELESS_COD')
)
--Get residential (latest) codes (including homeless) for either FLU OR COVID
,all_residential_codes AS (
    SELECT DISTINCT
        obs.person_id,
        DATE(obs.clinical_effective_date) AS latest_residential_date,
        obs.mapped_concept_code as concept_code,
        obs.mapped_concept_display as code_description,
        cc.cluster_id as code_type,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC ) as rn
       --FROM MODELLING.DBT_STAGING.STG_OLIDS_OBSERVATION obs
       FROM {{ ref('stg_olids_observation') }} obs
       INNER JOIN homeless_codes cc ON obs.mapped_concept_code = cc.code
       WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= CURRENT_DATE
        AND cc.cluster_id = 'RESIDE_COD' 
       QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC ) = 1
)
-- Identify people currently homeless by checking if the latest code is in HOMELESS_COD cluster
,currently_homeless as (
    SELECT 
    res.person_id,
    res.latest_residential_date,
    res.concept_code,
    res.code_description
    FROM all_residential_codes res
    INNER JOIN homeless_codes cc ON res.concept_code = cc.code
    AND cc.cluster_id = 'HOMELESS_COD'
) 
--identify people registered with the CAMDEN HEALTH IMPROVEMENT PRACTICE
,registered_CHIP as (
select person_id,
TRUE AS REGISTERED_CHIP
FROM REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS
where IS_ACTIVE = TRUE AND IS_DECEASED = FALSE AND PRACTICE_CODE = 'Y02674'
)

-- JOIN all people with homeless codes to be and/or registered with CHIP
select 
NVL(hom.person_id,reg.person_id) as person_id
,hom.latest_residential_date
,hom.concept_code
,hom.code_description
,reg.registered_chip
from currently_homeless hom
FULL OUTER JOIN registered_CHIP reg using (person_id)


