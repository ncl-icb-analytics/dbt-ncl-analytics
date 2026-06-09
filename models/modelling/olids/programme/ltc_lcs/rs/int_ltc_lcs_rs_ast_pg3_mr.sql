-- LTC LCS: Asthma Adult Register - Priority Group 3 (Medium Risk)
-- Parent population: Asthma register, age >= 18, excluding PG1 (HRC) and PG2 (HR)
-- EMIS source: 'On Asthma(Adult) Register- LTC LCS Priority Group 3 (MR)*'
-- (docs/emis_specs/ltc_lcs_rs/on_asthma_adult_register_ltc_lcs_priority_group_3_mr.md)
--
-- Rule chain:
-- - Rule 1 (gate): excludes PG1 (HRC) and PG2 (HR) patients
-- - Rule 2: acute exacerbation of asthma code (vs1) in last 12 months -> include
-- - Rule 3: prednisolone (vs2) order in last 12 months -> include
-- - Rule 4: antibiotics (vs3) order in last 12 months -> include
-- - Rule 5: SABA (vs4: salbutamol, terbutaline) order in last 12 months -> include
-- - Rule 6: LABA (vs5 bambuterol, vs6 formoterol products) in last 6 months
--   and NOT on ICS (vs7) in last 6 months -> include
-- - Rule 7: SABA in last 12 months and NOT on ICS in last 6 months -> include,
--   else exclude (subsumed by rule 5 in the R5 XML, kept for fidelity)
--
-- Note: the source GitHub issue describes issue-count thresholds (>= 2
-- antibiotics, >= 6 SABA, >= 3 SABA); the R5 EMIS XML has no count
-- restrictions, so any issue in window qualifies.
--
-- Parent register note: the EMIS parent 'LTC LCS: Asthma Adult Register*' is
-- the asthma register restricted to age >= 18; fct_person_asthma_register is
-- age >= 6, so the age filter is applied here.

with
-- Parent population: Adults currently on asthma register
asthma_adult_register as (
    select distinct r.person_id
    from {{ ref('fct_person_asthma_register') }} r
    inner join {{ ref('dim_person_age') }} a on r.person_id = a.person_id
    where r.is_on_register = true
        and a.age >= 18
),

-- Rule 1: Exclude PG1 (HRC) and PG2 (HR)
pg1_pg2_exclusions as (
    select distinct person_id from {{ ref('int_ltc_lcs_rs_ast_pg1_hrc') }}
    union
    select distinct person_id from {{ ref('int_ltc_lcs_rs_ast_pg2_hr') }}
),

-- Rule 2: acute exacerbation of asthma code in last 12 months
rule_2_exacerbation as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_asthma_adult_reg_pg3_mr_vs1") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
),

-- Rule 3: prednisolone order in last 12 months
rule_3_prednisolone as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_adult_reg_pg3_mr_vs2") }})
    where order_date >= dateadd(month, -12, current_date())
),

-- Rule 4: antibiotics order in last 12 months
rule_4_antibiotics as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_adult_reg_pg3_mr_vs3") }})
    where order_date >= dateadd(month, -12, current_date())
),

-- Rule 5 / rule 7 SABA arm: SABA order in last 12 months
rule_5_saba as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_adult_reg_pg3_mr_vs4") }})
    where order_date >= dateadd(month, -12, current_date())
),

-- LABA order in last 6 months (rule 6)
laba_6m as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_adult_reg_pg3_mr_vs5, on_asthma_adult_reg_pg3_mr_vs6") }})
    where order_date >= dateadd(month, -6, current_date())
),

-- ICS order in last 6 months (rules 6-7 NOT arm)
ics_6m as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_asthma_adult_reg_pg3_mr_vs7") }})
    where order_date >= dateadd(month, -6, current_date())
),

-- Combine rule results for all adult asthma register patients
patient_rules as (
    select
        ar.person_id,
        (excl.person_id is not null) as rule_1_in_pg1_pg2,
        (r2.person_id is not null) as rule_2_exacerbation,
        (r3.person_id is not null) as rule_3_prednisolone,
        (r4.person_id is not null) as rule_4_antibiotics,
        (r5.person_id is not null) as rule_5_saba,
        (laba.person_id is not null and ics.person_id is null) as rule_6_laba_no_ics,
        (r5.person_id is not null and ics.person_id is null) as rule_7_saba_no_ics,
        case
            when excl.person_id is not null then 'Excluded'
            when r2.person_id is not null
                or r3.person_id is not null
                or r4.person_id is not null
                or r5.person_id is not null
                or (laba.person_id is not null and ics.person_id is null)
                then 'Included'
            else 'Excluded'
        end as final_status
    from asthma_adult_register ar
    left join pg1_pg2_exclusions excl on ar.person_id = excl.person_id
    left join rule_2_exacerbation r2 on ar.person_id = r2.person_id
    left join rule_3_prednisolone r3 on ar.person_id = r3.person_id
    left join rule_4_antibiotics r4 on ar.person_id = r4.person_id
    left join rule_5_saba r5 on ar.person_id = r5.person_id
    left join laba_6m laba on ar.person_id = laba.person_id
    left join ics_6m ics on ar.person_id = ics.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Asthma Adult' as condition,
    '3' as priority_group,
    'MR' as risk_group
from patient_rules
where final_status = 'Included'
