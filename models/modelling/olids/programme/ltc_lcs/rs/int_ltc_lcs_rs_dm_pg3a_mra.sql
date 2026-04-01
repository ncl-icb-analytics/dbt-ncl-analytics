-- LTC LCS: Diabetes Register - Priority Group 3A (Moderate Risk A)
-- Parent population: Diabetes register, excluding PG1 (HRC) and PG2 (HR)
--
-- EMIS final include path is:
-- - HbA1c > 58 and <= 75
-- - not in PG1 or PG2
-- - latest ABPM blood pressure >= 140 systolic or >= 90 diastolic

with diabetes_register as (
    select distinct person_id
    from {{ ref('fct_person_diabetes_register') }}
),

pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_dm_pg1_hrc') }}
),

pg2_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_dm_pg2_hr') }}
),

rule_2_hba1c_prerequisite as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_priority_group_3a_mra_vs1") }})
    where result_value > 58 and result_value <= 75
),

rule_3_bp_high as (
    select person_id
    from {{ ref('int_blood_pressure_all') }}
    where is_abpm_bp_event = true
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
      and (systolic_value >= 140 or diastolic_value >= 90)
)

select
    dr.person_id,
    'Included' as final_status,
    'Diabetes' as condition,
    '3a' as priority_group,
    'MRa' as risk_group
from diabetes_register dr
left join pg1_exclusions pg1 on dr.person_id = pg1.person_id
left join pg2_exclusions pg2 on dr.person_id = pg2.person_id
inner join rule_2_hba1c_prerequisite r2 on dr.person_id = r2.person_id
inner join rule_3_bp_high r3 on dr.person_id = r3.person_id
where pg1.person_id is null
  and pg2.person_id is null
