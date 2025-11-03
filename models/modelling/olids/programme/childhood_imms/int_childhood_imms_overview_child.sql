{{
    config(
        materialized='table',
        tags=['childhood_imms']
    )
}}

select CASE
WHEN p.Vaccination_metric ='All vaccinations 1 Year' THEN 1
WHEN p.Vaccination_metric ='6-in-1 (dose 1, 2, 3) 1 Year' THEN 2
WHEN p.Vaccination_metric ='Men B (dose 1 and 2) 1 Year' THEN 3
WHEN p.Vaccination_metric ='Rotavirus (dose 1 & 2) 1 Year' THEN 4
WHEN p.Vaccination_metric ='PCV (dose 1) 1 Year' THEN 5
WHEN p.Vaccination_metric ='All vaccinations 2 Years' THEN 6
WHEN p.Vaccination_metric ='6-in-1 (dose 1, 2, 3) 2 Years' THEN 7
WHEN p.Vaccination_metric ='Hib/MenC 2 Years' THEN 8
WHEN p.Vaccination_metric ='Men B (dose 3) 2 Years' THEN 9
WHEN p.Vaccination_metric ='MMR (dose 1) 2 Years' THEN 10
WHEN p.Vaccination_metric ='PCV (dose 2) 2 Years' THEN 11
WHEN p.Vaccination_metric ='All vaccinations 5 Years' THEN 12
WHEN p.Vaccination_metric ='6-in-1 (dose 1, 2, 3) 5 Years' THEN 13
WHEN p.Vaccination_metric ='4-in-1 Booster 5 Years' THEN 14
WHEN p.Vaccination_metric ='Hib/MenC 5 Years' THEN 15
WHEN p.Vaccination_metric ='MMR (dose 1) 5 Years' THEN 16
WHEN p.Vaccination_metric ='MMR (dose 2) 5 Years' THEN 17 
END As VACC_ORDER 
,p.*
FROM (
--UNION ALL LINE LEVEL VACC REPORTS AGGREGATING BY FILTERS around 220K rows
-------- 1 YEAR METRICS
--sixin1_1y 
select 
run_date, reporting_age, '6-in-1 (dose 1, 2, 3) 1 Year' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(sixin1_comp_by_1) as numerator, count(*) as denominator  
FROM {{ ref('int_childhood_imms_vaccs_current_age_1') }}
group by all

UNION
--menb_1y 
select 
run_date, reporting_age, 'Men B (dose 1 and 2) 1 Year' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(menb_comp_by_1) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_1') }}
group by all

UNION
--rota_1y
select 
run_date, reporting_age, 'Rotavirus (dose 1 & 2) 1 Year' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(rota_comp_by_1) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_1') }}
group by all

UNION
--pcv_1y 
select 
run_date, reporting_age, 'PCV (dose 1) 1 Year' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(pcv_comp_by_1) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_1') }}
group by all

UNION
--all_vacc1y 
select 
run_date, reporting_age, 'All vaccinations 1 Year' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(ALL_COMP_BY_1) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_1') }}
group by all
 
------- 2 YEAR METRICS
UNION
--sixin1_2y 
select 
run_date, reporting_age, '6-in-1 (dose 1, 2, 3) 2 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(sixin1_comp_by_2) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_2') }}
group by all

UNION
--hibmenc_2y 
select 
run_date, reporting_age, 'Hib/MenC 2 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(hibmc_comp_by_2) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_2') }}
group by all

UNION
--menb_2y 
select 
run_date, reporting_age, 'Men B (dose 3) 2 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(menb_comp_by_2) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_2') }}
group by all

UNION
--mmr1_2y 
select 
run_date, reporting_age, 'MMR (dose 1) 2 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(mmr1_comp_by_2) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_2') }}
group by all

UNION
--pcv_2y 
select 
run_date, reporting_age, 'PCV (dose 2) 2 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(pcv_comp_by_2) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_2') }}
group by all

UNION
--all_vacc2y 
select 
run_date, reporting_age, 'All vaccinations 2 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(all_comp_by_2) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_2') }}
group by all

------- 5 YEAR METRICS

UNION
--sixin1_5y  
select 
run_date, reporting_age, '6-in-1 (dose 1, 2, 3) 5 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(sixin1_comp_by_5) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_5') }}
group by all

UNION
--fourin1_5y  
select 
run_date, reporting_age, '4-in-1 Booster 5 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(fourin1_comp_by_5) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_5') }}
group by all


UNION
--hibmenc_5y  
select 
run_date, reporting_age, 'Hib/MenC 5 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(hibmc_comp_by_5) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_5') }}
group by all

UNION
--MMR1_5y  
select 
run_date, reporting_age, 'MMR (dose 1) 5 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(mmr1_comp_by_5) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_5') }}
group by all

UNION
--MMR2_5y  
select 
run_date, reporting_age, 'MMR (dose 2) 5 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(mmr2_comp_by_5) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_5') }}
group by all

UNION
--all_vacc5y 
select 
run_date, reporting_age, 'All vaccinations 5 Years' as vaccination_metric, gender, imd_quintile, imdquintile_order, imd_decile, practice_borough,practice_neighbourhood, primary_care_network, 
gp_name, practice_code,residential_loc, residential_borough, residential_neighbourhood, ward_name, ward_code,  ETHNICITY_GRANULAR, MAIN_LANGUAGE, ethnicity_category, ethcat_order, ethnicity_subcategory, ethsubcat_order,  lac_flag,
sum(all_comp_by_5) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_current_age_5') }}
group by all
) p