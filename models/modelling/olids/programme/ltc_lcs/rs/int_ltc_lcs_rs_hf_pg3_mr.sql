-- LTC LCS: HF Register - Priority Group 3 (Medium Risk)
-- Implements inclusion/exclusion rules for medium-risk patient identification
-- Parent population: HF register
-- NB: there is no PG1.

-- Inclusion rules:
-- - Rule 1: Not in PG2 high-risk cohort
-- - Rule 2: Heart failure coded in last 6 months (include on pass)
-- - Rule 3: New York Heart Association Classification I - IV (include on pass)
-- - Rule 4: Oedema (not present or of feet) (include on pass)
-- - Rule 5: Latest left ventricular ejection fraction < 50 (exclude on fail)
-- - Rule 6: Rule 6: Dapagliflozin, Empagliflozin, Canagliflozin, Ertugliflozin L-pyroglutamic acid within 6 months (or adverse reaction) (include on fail)
-- - Rule 7: MRAs within 6 months (or adverse reaction) (include on fail)
-- - Rule 8: beta blockers within last 6 months (or adverse reaction) (include on fail)
-- - Rule 9: ace inhibitors within last 6 months (or adverse reaction) (include on fail)

with
-- Parent population: Patients currently on HF register
hf_register as (
    select distinct person_id
    from {{ ref('fct_person_heart_failure_register') }}
),

-- Rule 1: Exclude patients already in PG2 (HR)
pg2_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_hf_pg2_hr') }}
),
-- - Rule 2: Heart failure coded in last 6 months (include on pass)
rule_2_hf as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs1") }})
    where clinical_effective_date > dateadd(month, -6, current_date()) -- within last 6 months
    qualify row_number() over (partition by person_id order by clinical_effective_date asc) = 1
),

-- Rule 3: New York Heart Association Classification I - IV (Include)
rule_3_nyha as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs2") }})

    union

    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs3") }})
),
-- - Rule 4: Oedema (not present or of feet) (include on pass)
rule_4_oedema as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs4") }})

    union

    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs5") }})
),
-- Rule 5: Latest left ventricular ejection fraction < 50
rule_5_lvef as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs6") }})
    where result_value < 50
),
-- Rule 6: Dapagliflozin, Empagliflozin, Canagliflozin, Ertugliflozin L-pyroglutamic acid within 6 months (or adverse reaction)
rule_6_medications_sglt2_inhibitors as (
    -- medication courses
    select
    person_id 
    from ({{ get_ltc_lcs_medication_statements("on_hf_reg_pg3_mr_vs7") }})
    where statement_date >= dateadd(month, -6, current_date()) -- within last 6 months
    qualify row_number() over (partition by person_id order by statement_date desc) = 1
    
    union

    -- medication issues
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_hf_reg_pg3_mr_vs7") }})
    where order_date >= dateadd(month, -6, current_date()) -- within last year
    qualify row_number() over (partition by person_id order by order_date desc) = 1

    union

    -- adverse reactions
    select person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs8") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),
-- Rule 7: MRAs within 6 months (or adverse reaction)
rule_7_medications_mras as (
    -- medication courses
    select
    person_id 
    from ({{ get_ltc_lcs_medication_statements("on_hf_reg_pg3_mr_vs9") }})
    where statement_date >= dateadd(month, -6, current_date()) -- within last 6 months
    qualify row_number() over (partition by person_id order by statement_date desc) = 1
    
    union

    -- medication issues
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_hf_reg_pg3_mr_vs9") }})
    where order_date >= dateadd(month, -6, current_date()) -- within last 6 months
    qualify row_number() over (partition by person_id order by order_date desc) = 1

    union

    -- adverse reactions
    select person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs10") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),
-- Rule 8: beta blockers within last 6 months
rule_8_medications_beta_blockers as (
    -- medication issues
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_hf_reg_pg3_mr_vs11") }})
    where order_date >= dateadd(month, -6, current_date()) -- within 6 months
    qualify row_number() over (partition by person_id order by order_date desc) = 1

    union

    -- hypersensitivity to atenolol
    select person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs12") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1

    union

    select
    person_id 
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs13") }})
    where clinical_effective_date <= dateadd(month, -12, current_date()) -- on or before 1 year ago
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1

    union

    select
    person_id 
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs14") }})
    where clinical_effective_date <= dateadd(month, -12, current_date()) -- on or before 1 year ago
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),
rule_9_medications_ace_inhibitors as (
    -- medication issues
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_hf_reg_pg3_mr_vs15") }})
    where order_date > dateadd(month, -6, current_date()) -- within last 6 months
    qualify row_number() over (partition by person_id order by order_date desc) = 1

    union

    select
    person_id 
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs16") }})
    where clinical_effective_date > dateadd(month, -12, current_date()) -- within last year
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1

    union

    select
    person_id 
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs17") }})
    where clinical_effective_date > dateadd(month, -12, current_date()) -- within last year
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1

    union

    -- adverse reactionss
    select
    person_id 
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs18") }})
    where clinical_effective_date > dateadd(month, -12, current_date()) -- within last year
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1

    union

    select
    person_id 
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs19") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1

    union

    select
    person_id 
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs20") }})
    where clinical_effective_date > dateadd(month, -12, current_date()) -- within last year
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),
-- Combine rule results for all HF register patients
patient_rules as (
    select
        hr.person_id,
        (r2.person_id is not null) as rule_2_hf,
        (r3.person_id is not null) as rule_3_nyha,
        (r4.person_id is not null) as rule_4_oedema,
        (r5.person_id is not null) as rule_5_lvef,
        (r6.person_id is not null) as rule_6_medications_sglt2_inhibitors,
        (r7.person_id is not null) as rule_7_medications_mras,
        (r8.person_id is not null) as rule_8_medications_beta_blockers,
        (r9.person_id is not null) as rule_9_medications_ace_inhibitors,
        case
            when r1.person_id is not null then 'Excluded' -- Exclude patients in pg2
            when r2.person_id is not null then 'Included'  -- Rule 2 passed
            when r3.person_id is not null then 'Included' -- Rule 3 passed
            when r4.person_id is not null then 'Included' -- Rule 4 failed
            when r5.person_id is null then 'Excluded' -- Rule 5 failed
            when r6.person_id is null then 'Included' -- Rule 6 failed
            when r7.person_id is null then 'Included' -- Rule 7 failed
            when r8.person_id is null then 'Included' -- Rule 8 failed
            when r9.person_id is null then 'Included' -- Rule 9 failed
            else 'Excluded'
        end as final_status
    from hf_register hr
    left join pg2_exclusions r1 on hr.person_id = r1.person_id
    left join rule_2_hf r2 on hr.person_id = r2.person_id
    left join rule_3_nyha r3 on hr.person_id = r3.person_id
    left join rule_4_oedema r4 on hr.person_id = r4.person_id
    left join rule_5_lvef r5 on hr.person_id = r5.person_id
    left join rule_6_medications_sglt2_inhibitors r6 on hr.person_id = r6.person_id
    left join rule_7_medications_mras r7 on hr.person_id = r7.person_id
    left join rule_8_medications_beta_blockers r8 on hr.person_id = r8.person_id
    left join rule_9_medications_ace_inhibitors r9 on hr.person_id = r9.person_id
)
-- Final result: included patients only
select
    person_id,
    final_status,
    'HF' as condition,
    '3' as priority_group,
    'MR' as risk_group
from patient_rules
where final_status = 'Included'
