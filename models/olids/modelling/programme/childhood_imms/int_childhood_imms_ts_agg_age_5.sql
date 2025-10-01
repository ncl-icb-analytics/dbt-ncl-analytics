{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

select 
p.analysis_month
,p.vaccination_metric
,'5 YEARS' as reporting_age
,CASE
WHEN p.Vaccination_metric ='All vaccinations 5 Years' THEN 12
WHEN p.Vaccination_metric ='6-in-1 (dose 1,2,3) 5 Years' THEN 13
WHEN p.Vaccination_metric ='4-in-1 (dose 1) 5 Years' THEN 14
WHEN p.Vaccination_metric ='Hib/MenC 5 Years' THEN 15
WHEN p.Vaccination_metric ='MMR (dose 1) 5 Years' THEN 16
WHEN p.Vaccination_metric ='MMR (dose 2) 5 Years' THEN 17
END As VACC_ORDER 
,p.practice_name AS GP_NAME
,p.practice_code
,p.ethnicity_category
,p.ethcat_order
,p.imd_quintile
,p.imdquintile_order
,p.numerator 
,p.denominator 
FROM (

------- 5 YEAR METRICS FROM HISTORICAL 
--sixin1_5y 
select 
 '6-in-1 (dose 1,2,3) 5 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(sixin1_comp_by_5) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_5') }}
group by
1, 2, 3, 4, 5, 6, 7, 8

UNION
--hibmenc_5y 
select 
 'Hib/MenC 5 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(hibmc_comp_by_5) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_5') }}
group by
1, 2, 3, 4, 5, 6, 7, 8

UNION
--fourin1_5y 
select 
 '4-in-1 (dose 1) 5 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(fourin1_comp_by_5) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_5') }}
group by
1, 2, 3, 4, 5, 6, 7, 8

UNION
--mmr1_5y 
select 
 'MMR (dose 1) 5 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(mmr1_comp_by_5) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_5') }}
group by
1, 2, 3, 4, 5, 6, 7, 8

UNION
--mmr2_5y  
select 
 'MMR (dose 2) 5 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(mmr2_comp_by_5) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_5') }}
group by
1, 2, 3, 4, 5, 6, 7, 8

UNION
--all_vacc5y 
select 
 'All vaccinations 5 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(all_comp_by_5) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_5') }}
group by
1, 2, 3, 4, 5, 6, 7, 8
) p
