{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

SELECT 
p.fiscal_year
,p.vaccination_dose
,'1 YEAR' as reporting_age
,CASE
WHEN p.vaccination_dose = '6-in-1 (dose 1) 1 Year' THEN 1
WHEN p.vaccination_dose = '6-in-1 (dose 2) 1 Year' THEN 2
WHEN p.vaccination_dose = '6-in-1 (dose 3) 1 Year' THEN 3
WHEN p.vaccination_dose ='Rotavirus (dose 1) 1 Year' THEN 4
WHEN p.vaccination_dose ='Rotavirus (dose 2) 1 Year' THEN 5
WHEN p.vaccination_dose ='MenB (dose 1) 1 Year' THEN 6
WHEN p.vaccination_dose ='MenB (dose 2) 1 Year' THEN 7
WHEN p.vaccination_dose ='PCV (dose 1) 1 Year' THEN 8
END As VACC_ORDER 
,p.GP_NAME
,p.practice_code
,p.month_year
,p.month_label
,p.vaccination_count 
FROM (
------- 1 YEAR METRICS FROM FISCAL IMMUNISATIONS
--sixin1_1y Dose 1
select 
 '6-in-1 (dose 1) 1 Year' as vaccination_dose, fiscal_year, gp_name, practice_code, sixin1_dose1_sort as month_year, SIXIN1_DOSE1_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_1') }}
group by
1, 2, 3, 4, 5, 6

UNION

--sixin1_1y Dose 2
select 
 '6-in-1 (dose 2) 1 Year' as vaccination_dose, fiscal_year, gp_name, practice_code, sixin1_dose2_sort as month_year, SIXIN1_DOSE2_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_1') }}
group by
1, 2, 3, 4, 5, 6

UNION

--sixin1_1y Dose 3
select 
 '6-in-1 (dose 3) 1 Year' as vaccination_dose, fiscal_year, gp_name, practice_code, sixin1_dose3_sort as month_year, SIXIN1_DOSE3_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_1') }}
group by
1, 2, 3, 4, 5, 6

UNION

--rota_1y Dose 1
select 
 'Rotavirus (dose 1) 1 Year' as vaccination_dose, fiscal_year, gp_name, practice_code, rota_dose1_sort as month_year, ROTA_DOSE1_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_1') }}
group by
1, 2, 3, 4, 5, 6

UNION

--rota_1y Dose 2
select 
 'Rotavirus (dose 2) 1 Year' as vaccination_dose, fiscal_year, gp_name, practice_code, rota_dose2_sort as month_year, ROTA_DOSE2_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_1') }}
group by
1, 2, 3, 4, 5, 6

UNION

--menb_1y Dose 1
select 
 'MenB (dose 1) 1 Year' as vaccination_dose, fiscal_year, gp_name, practice_code, menb_dose1_sort as month_year, MENB_DOSE1_MONTH_YEAR_LABEL as month_label, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_1') }}
group by
1, 2, 3, 4, 5, 6

UNION

--menb_1y Dose 2
select 
 'MenB (dose 2) 1 Year' as vaccination_dose, fiscal_year, gp_name, practice_code, menb_dose2_sort as month_year, MENB_DOSE2_MONTH_YEAR_LABEL as month_label,  count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_1') }}
group by
1, 2, 3, 4, 5, 6

UNION

--pcv_1y Dose 1
select 
 'PCV (dose 1) 1 Year' as vaccination_dose, fiscal_year, gp_name, practice_code, pcv_dose1_sort as month_year, PCV_DOSE1_MONTH_YEAR_LABEL as month_label,  count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_eoy_age_1') }}
group by
1, 2, 3, 4, 5, 6
) p
