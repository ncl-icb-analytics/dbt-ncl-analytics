{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

select 
p.analysis_month
,p.vaccination_metric
,'2 YEARS' as reporting_age
,CASE
WHEN p.Vaccination_metric ='All vaccinations 2 Years' THEN 6
WHEN p.Vaccination_metric ='6-in-1 (dose 1,2,3) 2 Years' THEN 7
WHEN p.Vaccination_metric ='Hib/MenC 2 Years' THEN 8
WHEN p.Vaccination_metric ='Men B (dose 3) 2 Years' THEN 9
WHEN p.Vaccination_metric ='MMR (dose 1) 2 Years' THEN 10
WHEN p.Vaccination_metric ='PCV (dose 2) 2 Years' THEN 11
END As VACC_ORDER 
,p.practice_name AS GP_NAME
,p.practice_code
,p.ethnicity_category
,p.ethcat_order
,p.imd_quintile
,p.numerator 
,p.denominator 
FROM (

------- 2 YEAR METRICS FROM HISTORICAL 
--sixin1_2y 
select 
 '6-in-1 (dose 1,2,3) 2 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile,
sum(sixin1_comp_by_2) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_2') }}
group by
1, 2, 3, 4, 5, 6, 7

UNION
--hibmenc_2y 
select 
 'Hib/MenC 2 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile,
sum(hibmc_comp_by_2) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_2') }}
group by
1, 2, 3, 4, 5, 6, 7

UNION
--menb_2y 
select 
 'Men B (dose 3) 2 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile,
sum(menb_comp_by_2) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_2') }}
group by
1, 2, 3, 4, 5, 6, 7

UNION
--mmr1_2y 
select 
 'MMR (dose 1) 2 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile,
sum(mmr1_comp_by_2) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_2') }}
group by
1, 2, 3, 4, 5, 6, 7

UNION
--pcv_2y 
select 
 'PCV (dose 2) 2 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile,
sum(pcv_comp_by_2) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_2') }}
group by
1, 2, 3, 4, 5, 6, 7

UNION
--all_vacc2y 
select 
 'All vaccinations 2 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile,
sum(all_comp_by_2) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_2') }}
group by
1, 2, 3, 4, 5, 6, 7
) p