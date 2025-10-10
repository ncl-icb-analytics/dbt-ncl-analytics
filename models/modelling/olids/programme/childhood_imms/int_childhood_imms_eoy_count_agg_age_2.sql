{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

SELECT
p.fiscal_year
,p.vaccination_dose
,'2 YEARS' as reporting_age
,CASE
WHEN p.vaccination_dose = '6-in-1 (dose 1) 2 Years' THEN 9
WHEN p.vaccination_dose = '6-in-1 (dose 2) 2 Years' THEN 10
WHEN p.vaccination_dose = '6-in-1 (dose 3) 2 Years' THEN 11
WHEN p.vaccination_dose ='HibMenC (dose 1) 2 Years' THEN 12
WHEN p.vaccination_dose ='MMR (dose 1) 2 Years' THEN 13
WHEN p.vaccination_dose ='MenB (dose 1) 2 Years' THEN 14
WHEN p.vaccination_dose ='MenB (dose 2) 2 Years' THEN 15
WHEN p.vaccination_dose ='MenB (dose 3) 2 Years' THEN 16
WHEN p.vaccination_dose ='PCV (dose 1) 2 Years' THEN 17
WHEN p.vaccination_dose ='PCV (dose 2) 2 Years' THEN 18
END As VACC_ORDER 
,p.GP_NAME
,p.practice_code
,p.month_year
,p.month_label
,p.vaccination_count 
FROM (
------- 2 YEAR METRICS FROM FISCAL IMMUNISATIONS
--sixin1_2y Dose 1
select 
 '6-in-1 (dose 1) 2 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, sixin1_dose1_sort as month_year, SIXIN1_DOSE1_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_2') }}
group by
1, 2, 3, 4, 5, 6

UNION

--sixin1_2y Dose 2
select 
 '6-in-1 (dose 2) 2 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, sixin1_dose2_sort as month_year, SIXIN1_DOSE2_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_2') }}
group by
1, 2, 3, 4, 5, 6

UNION

--sixin1_2y Dose 3
select 
 '6-in-1 (dose 3) 2 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, sixin1_dose3_sort as month_year, SIXIN1_DOSE3_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_2') }}
group by
1, 2, 3, 4, 5, 6

UNION

--HibMenC_2y Dose 1
select 
 'HibMenC (dose 1) 2 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, hibmc_dose1_sort as month_year, HIBMC_DOSE1_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_2') }}
group by
1, 2, 3, 4, 5, 6

UNION

--mmr1_2y Dose 2
select 
 'MMR (dose 1) 2 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, mmr_dose1_sort as month_year, MMR_DOSE1_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_2') }}
group by
1, 2, 3, 4, 5, 6

UNION

--menb_2y Dose 1
select 
 'MenB (dose 1) 2 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, menb_dose1_sort as month_year, MENB_DOSE1_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_2') }}
group by
1, 2, 3, 4, 5, 6

UNION

--menb_2y Dose 2
select 
 'MenB (dose 2) 2 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, menb_dose2_sort as month_year, MENB_DOSE2_MONTH_YEAR_LABEL as month_label,  count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_2') }}
group by
1, 2, 3, 4, 5, 6

UNION

--menb_2y Dose 3
select 
 'MenB (dose 3) 2 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, menb_dose3_sort as month_year, MENB_DOSE3_MONTH_YEAR_LABEL as month_label,  count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_2') }}
group by
1, 2, 3, 4, 5, 6

UNION

--pcv_2y Dose 1
select 
 'PCV (dose 1) 2 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, pcv_dose1_sort as month_year, PCV_DOSE1_MONTH_YEAR_LABEL as month_label,  count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_2') }}
group by
1, 2, 3, 4, 5, 6

UNION

--pcv_2y Dose 2
select 
 'PCV (dose 2) 2 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, pcv_dose2_sort as month_year, PCV_DOSE2_MONTH_YEAR_LABEL as month_label,  count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_2') }}
group by
1, 2, 3, 4, 5, 6

) p
