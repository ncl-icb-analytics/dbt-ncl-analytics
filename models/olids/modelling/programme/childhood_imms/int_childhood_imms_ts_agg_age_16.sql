{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

select 
p.analysis_month
,p.vaccination_metric
,'16 YEARS' as reporting_age
,CASE
WHEN p.Vaccination_metric ='All vaccinations 16 Years' THEN 6
WHEN p.Vaccination_metric ='6-in-1 (dose 1,2,3) 16 Years' THEN 7
WHEN p.Vaccination_metric ='4-in-1 (dose 1) 16 Years' THEN 8
WHEN p.Vaccination_metric ='MMR (dose 1,2) 16 Years' THEN 9
WHEN p.Vaccination_metric ='HPV (dose 1) 16 Years' THEN 10
WHEN p.Vaccination_metric ='3-in-1 (dose 1) 16 Years' THEN 11
WHEN p.Vaccination_metric ='MenACWY (dose 1) 16 Years' THEN 12
END As VACC_ORDER 
,p.practice_name AS GP_NAME
,p.practice_code
,p.ethnicity_category
,p.ethcat_order
,p.imd_quintile
,p.numerator 
,p.denominator 
FROM (

------- 16 YEAR METRICS FROM HISTORICAL 
--sixin1_16y 
select 
 '6-in-1 (dose 1,2,3) 16 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile,
sum(sixin1_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_16') }}
group by
1, 2, 3, 4, 5, 6, 7

UNION
--fourin1_16y 
select 
 '4-in-1 (dose 1) 16 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile,
sum(fourin1_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_16') }}
group by
1, 2, 3, 4, 5, 6, 7

UNION
--mmr1_16y 
select 
 'MMR (dose 1,2) 16 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile,
sum(mmr_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_16') }}
group by
1, 2, 3, 4, 5, 6, 7

UNION
--HPV_16y 
select 
 'HPV (dose 1) 16 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile,
sum(hpv_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_16') }}
group by
1, 2, 3, 4, 5, 6, 7

UNION
--threein1_16y 
select 
 '3-in-1 (dose 1) 16 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile,
sum(threein1_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_16') }}
group by
1, 2, 3, 4, 5, 6, 7

UNION
--MenACWY_16y 
select 
 'MenACWY (dose 1) 16 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile,
sum(menacwy_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_16') }}
group by
1, 2, 3, 4, 5, 6, 7

UNION
--all_vacc16y 
select 
 'All vaccinations 16 Years' as vaccination_metric, analysis_month, practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile,
sum(all_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_16') }}
group by
1, 2, 3, 4, 5, 6, 7
) p
