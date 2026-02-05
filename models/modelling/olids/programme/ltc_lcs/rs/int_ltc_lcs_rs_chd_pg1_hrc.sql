-- LTC LCS: CHD Register - Priority Group 1 (High Risk & Complex)
-- Implements inclusion/exclusion rules for high-risk & complex patient identification
-- Parent population: CHD register
--
-- Inclusion rules:
-- - Rule 1: First/new or flare-up of CHD within last month or
-- - Rule 2: Significant CHD within last month

with
-- Parent population: Patients currently on CHD register
chd_register as (
    select distinct person_id
    from {{ ref('fct_person_chd_register') }}
),
-- Rule 1: First/new or flare-up of CHD within last month 
rule_1a_first_new_flare_up as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_chd_reg_pg1_hrc_vs1") }})
    where clinical_effective_date >= dateadd(month, -1, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1
),
rule_1b_first_new_flare_up as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_chd_reg_pg1_hrc_vs2") }})
    where clinical_effective_date >= dateadd(month, -1, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1
),
-- - Rule 2: Significant CHD within last month
rule_2a_significant as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_chd_reg_pg1_hrc_vs1") }})
    where clinical_effective_date >= dateadd(month, -1, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1
),
rule_2b_significant as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_chd_reg_pg1_hrc_vs3") }})
    where clinical_effective_date >= dateadd(month, -1, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1
),
-- Combine rule results for all CKD register patients
-- patient_rules as (
--     select
--         cr.person_id,
--         (r1.person_id is not null) as rule_1_egfr_low,
--         (r2.person_id is not null) as rule_2_acr_high,
--         case
--             when r1.person_id is not null then 'Included'  -- Rule 1 passed
--             when r2.person_id is not null then 'Included'  -- Rule 2 passed
--             else 'Excluded'                                 -- Both rules failed
--         end as final_status
--     from ckd_register cr
--     left join rule_1_egfr_low r1 on cr.person_id = r1.person_id
--     left join rule_2_acr_high r2 on cr.person_id = r2.person_id
-- )

-- Final result: included patients only
select
    person_id,
    final_status,
    'CHD' as condition,
    '1' as priority_group,
    'HRC' as risk_group
from patient_rules
where final_status = 'Included'
