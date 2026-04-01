-- LTC LCS: Diabetes Register - Priority Group 3B (Moderate Risk B)
-- Parent population: Diabetes register, excluding PG1 (HRC), PG2 (HR), and PG3A (MRa)
--
-- EMIS final include path is:
-- - HbA1c > 48 and <= 58
-- - not in PG1, PG2, or PG3A
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

pg3a_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_dm_pg3a_mra') }}
),

rule_2_hba1c_prerequisite as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_priority_group_3b_mrb_vs1") }})
    where result_value > 48 and result_value <= 58
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
    '3b' as priority_group,
    'MRb' as risk_group
from diabetes_register dr
left join pg1_exclusions pg1 on dr.person_id = pg1.person_id
left join pg2_exclusions pg2 on dr.person_id = pg2.person_id
left join pg3a_exclusions pg3a on dr.person_id = pg3a.person_id
inner join rule_2_hba1c_prerequisite r2 on dr.person_id = r2.person_id
inner join rule_3_bp_high r3 on dr.person_id = r3.person_id
where pg1.person_id is null
  and pg2.person_id is null
  and pg3a.person_id is null
