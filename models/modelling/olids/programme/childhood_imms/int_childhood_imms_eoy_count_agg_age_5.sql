{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

SELECT
p.fiscal_year
,p.vaccination_dose
,'5 YEARS' as reporting_age
,CASE
WHEN p.vaccination_dose = '6-in-1 (dose 1) 5 Years' THEN 19
WHEN p.vaccination_dose = '6-in-1 (dose 2) 5 Years' THEN 20
WHEN p.vaccination_dose = '6-in-1 (dose 3) 5 Years' THEN 21
WHEN p.vaccination_dose = '4-in-1 (dose 1) 5 Years' THEN 22
WHEN p.vaccination_dose = 'HibMenC (dose 1) 5 Years' THEN 23
WHEN p.vaccination_dose = 'MMR (dose 1) 5 Years' THEN 24
WHEN p.vaccination_dose = 'MMR (dose 2) 5 Years' THEN 25
END As VACC_ORDER 
,p.GP_NAME
,p.practice_code
,p.month_year
,p.month_label
,p.vaccination_count 
FROM (
------- 5 YEAR METRICS FROM FISCAL IMMUNISATIONS
--sixin1_5y Dose 1
select 
 '6-in-1 (dose 1) 5 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, sixin1_dose1_sort as month_year, SIXIN1_DOSE1_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_5') }}
group by
1, 2, 3, 4, 5, 6

UNION

--sixin1_5y Dose 2
select 
 '6-in-1 (dose 2) 5 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, sixin1_dose2_sort as month_year, SIXIN1_DOSE2_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_5') }}
group by
1, 2, 3, 4, 5, 6

UNION

--sixin1_5y Dose 3
select 
 '6-in-1 (dose 3) 5 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, sixin1_dose3_sort as month_year, SIXIN1_DOSE3_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_5') }}
group by
1, 2, 3, 4, 5, 6

UNION

--fourin1_5y Dose 1
select 
 '4-in-1 (dose 1) 5 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, fourin1_dose1_sort as month_year, fourin1_DOSE1_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_5') }}
group by
1, 2, 3, 4, 5, 6

UNION

--HibMenC_5y Dose 1
select 
 'HibMenC (dose 1) 5 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, hibmc_dose1_sort as month_year, HIBMC_DOSE1_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_5') }}
group by
1, 2, 3, 4, 5, 6

UNION

--mmr1_5y Dose 1
select 
 'MMR (dose 1) 5 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, mmr_dose1_sort as month_year, MMR_DOSE1_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_5') }}
group by
1, 2, 3, 4, 5, 6

UNION

--mmr1_5y Dose 2
select 
 'MMR (dose 2) 5 Years' as vaccination_dose, fiscal_year, gp_name, practice_code, mmr_dose2_sort as month_year, MMR_DOSE2_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_5') }}
group by
1, 2, 3, 4, 5, 6
) p