{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}
--Population demographics + Lifestyle enhanced metrics wide table
select
p.PERSON_ID
,p.HX_FLAKE
,p.PRACTICE_CODE
,p.practice_name
,p.RESIDENTIAL_LOC
,p.RESIDENTIAL_BOROUGH
-- ,p.RESIDENTIAL_NEIGHBOURHOOD
-- ,p.WARD_CODE
-- ,p.WARD_NAME
,p.AGE
,p.BIRTH_DATE_APPROX
,p.AGE_BAND_5Y
,p.GENDER
,p.ETHNICITY_CATEGORY
,p.ETHCAT_ORDER
,p.ETHNICITY_SUBCATEGORY
,p.ETHSUBCAT_ORDER
,p.IMD_QUINTILE
,p.IMDQUINTILE_ORDER
,p.MAIN_LANGUAGE
,p.is_homeless
--latest illicit drug use and subs misuse service referrals
,NVL2(ILLICIT_DRUG_DATE, 'Yes', 'No') AS drug_use_assessed_ever
,ILLICIT_DRUG_DATE as drug_assess_date
,drug_misuse_flag
,SM_INT_DATE
,subs_misuse_services
--smoking status ever and smoking cessation referrals
,smok_status_last_12m
,IFF(smok_status_DATE IS NULL, 'No', 'Yes') AS smoking_assessed_ever
,smok_status_DATE 
,SMOKER_FLAG
,SMOK_INT_DATE
,SMOKING_CESSATION_SERVICES
--alcohol use ever and alcohol interventions
,IFF(ALC_STAT_DATE IS NULL, 'No', 'Yes') AS alcohol_assessed_ever
,ALC_STAT_DATE
,high_alcohol_use_flag
,alcohol_advice_services
--nutrition assessment and diet
,NVL2(NUTR_REV_DATE, 'Yes', 'No') AS diet_assessed_ever
,NUTR_REV_DATE
,POOR_DIET_FLAG
,REFERRAL_DIET_ADVICE
--exercise assessment and referral
,NVL2(EX_STAT_DATE, 'Yes', 'No') AS exercise_assessed_ever
,EX_STAT_DATE
,LOW_EXERCISE_FLAG
,REFERRAL_EXERCISE_ADVICE
FROM {{ ref('int_smi_population_base')  }} p
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE p
LEFT JOIN {{ ref('int_smi_lifestyle')  }} l using (person_id)
--LEFT JOIN DEV__MODELLING.OLIDS_PROGRAMME.INT_SMI_LIFESTYLE l using (person_id)
WHERE p.HAS_ACTIVE_SMI_DIAGNOSIS = TRUE