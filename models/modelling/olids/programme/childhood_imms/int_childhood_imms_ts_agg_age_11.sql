{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

select 
p.analysis_month
,p.vaccination_metric
,'11 YEARS' as reporting_age
,CASE
WHEN p.Vaccination_metric ='All vaccinations 11 Years' THEN 1
WHEN p.Vaccination_metric ='6-in-1 (dose 1,2,3) 11 Years' THEN 2
WHEN p.Vaccination_metric ='4-in-1 (dose 1) 11 Years' THEN 3
WHEN p.Vaccination_metric ='Hib/MenC 11 Years' THEN 4
WHEN p.Vaccination_metric ='MMR (dose 1,2) 11 Years' THEN 5
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

------- 11 YEAR METRICS FROM HISTORICAL 
--sixin1_11y 
select 
 '6-in-1 (dose 1,2,3) 11 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(sixin1_comp_by_11) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_11') }}
group by
1, 2, 3, 4, 5, 6, 7, 8

UNION
--hibmenc_11y 
select 
 'Hib/MenC 11 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(hibmc_comp_by_11) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_11') }}
group by
1, 2, 3, 4, 5, 6, 7, 8

UNION
--fourin1_11y 
select 
 '4-in-1 (dose 1) 11 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(fourin1_comp_by_11) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_11') }}
group by
1, 2, 3, 4, 5, 6, 7, 8

UNION
--mmr1_11y 
select 
 'MMR (dose 1,2) 11 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(mmr_comp_by_11) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_11') }}
group by
1, 2, 3, 4, 5, 6, 7, 8

UNION
--all_vacc11y 
select 
 'All vaccinations 11 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(all_comp_by_11) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_11') }}
group by
1, 2, 3, 4, 5, 6, 7, 8

) p