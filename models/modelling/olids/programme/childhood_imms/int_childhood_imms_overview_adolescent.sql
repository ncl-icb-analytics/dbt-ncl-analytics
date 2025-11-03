{{
    config(
        materialized='table',
        tags=['childhood_imms']
    )
}}

select CASE
WHEN p.Vaccination_metric ='All vaccinations 11 Years' THEN 1
WHEN p.Vaccination_metric ='6-in-1 (dose 1, 2, 3) 11 Years' THEN 2
WHEN p.Vaccination_metric ='4-in-1 Booster 11 Years' THEN 3
WHEN p.Vaccination_metric ='Hib/MenC 11 Years' THEN 4
WHEN p.Vaccination_metric ='MMR (dose 1,2) 11 Years' THEN 5
WHEN p.Vaccination_metric ='All vaccinations 16 Years' THEN 6
WHEN p.Vaccination_metric ='6-in-1 (dose 1, 2, 3) 16 Years' THEN 7
WHEN p.Vaccination_metric ='4-in-1 Booster 16 Years' THEN 8
WHEN p.Vaccination_metric ='MMR (dose 1,2) 16 Years' THEN 9
WHEN p.Vaccination_metric ='HPV 16 Years' THEN 10
WHEN p.Vaccination_metric ='3-in-1 Booster 16 Years' THEN 11
WHEN p.Vaccination_metric ='MenACWY 16 Years' THEN 12
END As VACC_ORDER, p.*
FROM (
--UNION ALL LINE LEVEL VACC REPORTS AGGREGATING BY FILTERS around 220K rows
-------- 11 YEAR METRICS
--sixin1_11y 
select 
run_date, reporting_age, '6-in-1 (dose 1, 2, 3) 11 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order,  imd_decile,  practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code, residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code, ethnicity_granular, main_language, ethnicity_category, ethcat_order,  ethnicity_subcategory, ethsubcat_order, lac_flag,
sum(sixin1_comp_by_11) as numerator, count(*) as denominator  
FROM {{ ref('int_childhood_imms_vaccs_current_age_11') }}
group by all

UNION
--fourin1_11y  
select 
run_date, reporting_age, '4-in-1 Booster 11 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order,  imd_decile,  practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code, residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code, ethnicity_granular, main_language, ethnicity_category, ethcat_order,  ethnicity_subcategory, ethsubcat_order, lac_flag,
sum(fourin1_comp_by_11) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_11') }}
group by all

UNION
--hibmenc_11y 
select 
run_date, reporting_age, 'Hib/MenC 11 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order,  imd_decile,  practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code, residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code, ethnicity_granular, main_language, ethnicity_category, ethcat_order,  ethnicity_subcategory, ethsubcat_order, lac_flag,
sum(hibmc_comp_by_11) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_11') }}
group by all

UNION
--mmr12_11y 
select 
run_date, reporting_age, 'MMR (dose 1,2) 11 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order,  imd_decile,  practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code, residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code, ethnicity_granular, main_language, ethnicity_category, ethcat_order,  ethnicity_subcategory, ethsubcat_order, lac_flag,
sum(mmr_comp_by_11) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_11') }}
group by all

UNION
--all_vacc11y 
select 
run_date, reporting_age, 'All vaccinations 11 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order,  imd_decile,  practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code, residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code, ethnicity_granular, main_language, ethnicity_category, ethcat_order,  ethnicity_subcategory, ethsubcat_order, lac_flag,
sum(ALL_COMP_BY_11) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_11') }}
group by all
 

------- 16 YEAR METRICS

UNION
--sixin1_16y  
select 
run_date, reporting_age, '6-in-1 (dose 1, 2, 3) 16 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order,  imd_decile,  practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code, residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code, ethnicity_granular, main_language, ethnicity_category, ethcat_order,  ethnicity_subcategory, ethsubcat_order, lac_flag,
sum(sixin1_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_16') }}
group by all

UNION
--fourin1_16y  
select 
run_date, reporting_age, '4-in-1 Booster 16 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order,  imd_decile,  practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code, residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code, ethnicity_granular, main_language, ethnicity_category, ethcat_order,  ethnicity_subcategory, ethsubcat_order, lac_flag,
sum(fourin1_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_16') }}
group by all

UNION
--threein1_16y  
select 
run_date, reporting_age, '3-in-1 Booster 16 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order,  imd_decile,  practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code, residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code, ethnicity_granular, main_language, ethnicity_category, ethcat_order,  ethnicity_subcategory, ethsubcat_order, lac_flag,
sum(threein1_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_16') }}
group by all

UNION
--MMR2_16y  
select 
run_date, reporting_age, 'MMR (dose 1,2) 16 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order,  imd_decile,  practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code, residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code, ethnicity_granular, main_language, ethnicity_category, ethcat_order,  ethnicity_subcategory, ethsubcat_order, lac_flag,
sum(mmr_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_16') }}
group by all

UNION
--HPV_16y  
select 
run_date, reporting_age, 'HPV 16 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order,  imd_decile,  practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code, residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code, ethnicity_granular, main_language, ethnicity_category, ethcat_order,  ethnicity_subcategory, ethsubcat_order, lac_flag,
sum(hpv_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_16') }}
group by all

UNION
--menacwy_16y  
select 
run_date, reporting_age, 'MenACWY 16 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order,  imd_decile,  practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code, residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code, ethnicity_granular, main_language, ethnicity_category, ethcat_order,  ethnicity_subcategory, ethsubcat_order, lac_flag,
sum(menacwy_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_16') }}
group by all

UNION
--all_vacc16y 
select 
run_date, reporting_age, 'All vaccinations 16 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order,  imd_decile,  practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code, residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code, ethnicity_granular, main_language, ethnicity_category, ethcat_order,  ethnicity_subcategory, ethsubcat_order, lac_flag,
sum(all_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_16') }}
group by all
) p