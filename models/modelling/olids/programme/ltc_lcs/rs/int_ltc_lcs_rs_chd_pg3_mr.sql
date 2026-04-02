-- LTC LCS: CHD Register - Priority Group 3 (Medium Risk)
-- Implements inclusion/exclusion rules for medium risk patient identification
-- Parent population: CHD register
--
-- Inclusion rules:
-- - Rule 1: Not in PG1 HRC or PG2 HR
-- - Rule 2: CHD recorded more than one year ago
-- - Rule 3: Statins ordered in last 6 month AND most recent Non HDL cholesterol level > 2.5

with
-- Parent population: Patients currently on CHD register
chd_register as (
    select distinct person_id
    from {{ ref('fct_person_chd_register') }}
),
-- Rule 1: Exclude patients already in PG1 (HRC) or PG2 (HR)
pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_chd_pg1_hrc') }}
),
pg2_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_chd_pg2_hr') }}
),
-- Rule 2: CHD recorded more than one year ago
rule_2_chd_over_one_year_ago as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_chd_reg_pg3_mr_vs1") }})
    where 
    clinical_effective_date < dateadd(month, -12, current_date())
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),
-- Rule 3a: Statins ordered in last 6 month
rule_3a_statins as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_chd_reg_pg3_mr_vs2") }})
    where order_date >= dateadd(month, -6, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1
),
rule_3b_non_hdl_cholesterol_level as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_chd_reg_pg3_mr_vs3") }})
    where result_value > 2.5
),
-- Combine rule results for all CHD register patients
patient_rules as (
    select
        cr.person_id,
        (r2.person_id is not null) as rule_2_chd_over_one_year_ago,
        (r3a.person_id is not null) as rule_3a_statins,
        (r3b.person_id is not null) as rule_3b_non_hdl_cholesterol_level,
        case
            when r1a.person_id is null --- not in pg1
                and r1b.person_id is null -- not in pg2
                and r2.person_id is not null -- has chd recorded >1 year ago
                and r3a.person_id is not null -- has statins ordered in last 6 months
                and r3b.person_id is not null -- latest non-HDL cholesterol level is >2.5
            then 'Included'
            else 'Excluded'
        end as final_status
    from chd_register cr
    left join pg1_exclusions r1a on cr.person_id = r1a.person_id
    left join pg2_exclusions r1b on cr.person_id = r1b.person_id
    left join rule_2_chd_over_one_year_ago r2 on cr.person_id = r2.person_id
    left join rule_3a_statins r3a on cr.person_id = r3a.person_id
    left join rule_3b_non_hdl_cholesterol_level r3b on cr.person_id = r3b.person_id
)
-- Final result: included patients only
select
    person_id,
    final_status,
    'CHD' as condition,
    '3' as priority_group,
    'MR' as risk_group
from patient_rules
where final_status = 'Included'
