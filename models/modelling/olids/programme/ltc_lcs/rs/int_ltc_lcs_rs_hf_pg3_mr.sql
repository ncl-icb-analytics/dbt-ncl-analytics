-- LTC LCS: HF Register - Priority Group 3 (Medium Risk)
-- Parent population: HF register, excluding PG2 (HR)
-- EMIS source: 'On HF Register- LTC LCS Priority Group 3 (MR)*'
-- (docs/emis_specs/ltc_lcs_rs/on_hf_register_ltc_lcs_priority_group_3_mr.md)
--
-- Rule chain:
-- - Rule 1 (gate): excludes PG2 (HR) patients
-- - Rule 2: earliest HF code (vs1, HF_COD refset) in last 6 months -> include
--   (first HF diagnosis within 6 months)
-- - Rule 3: latest NYHA classification (vs2+vs3) is Class II (vs3) -> include
-- - Rule 4: latest oedema code (vs4+vs5) is ankle/feet oedema (vs5) -> include
-- - Rule 5 (gate): latest LVEF (vs6) < 50. Fail -> exclude.
-- - Rules 6-8 (SGLT2/MRA/beta-blocker arms) do not change who is included
--   (pass and fail both continue) and are not implemented.
-- - Rule 9: exclude if on the ACE inhibitor pathway - latest ACEi order (vs15)
--   in last 6 months, ACEi/AIIRA declined (vs16/vs17) or ACEi adverse reaction
--   (vs18) in last 12 months, XAII code ever (vs19), or TXAII (vs20) in last
--   12 months. Otherwise include.
--
-- Net logic: not pg2 and (rule_2 or rule_3 or rule_4 or (rule_5 and not rule_9))
--
-- Known gap: rule 9 also references EMIS library item
-- 80709f0e-d62c-4851-b8b5-22ce2fb29319, whose logic is not in the XML export;
-- that exclusion arm cannot be implemented.

with
-- Parent population: Patients currently on HF register
hf_register as (
    select distinct person_id
    from {{ ref('fct_person_heart_failure_register') }}
    where is_on_register = true
),

-- Rule 1: Exclude PG2 (HR)
pg2_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_hf_pg2_hr') }}
),

-- Rule 2: earliest HF code in last 6 months (first diagnosis within 6 months)
rule_2_new_hf_diagnosis as (
    select person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs1") }})
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date asc, observation_id asc
    ) = 1
        and clinical_effective_date > dateadd(month, -6, current_date())
),

-- Rule 3: latest NYHA classification is Class II.
-- vs3 (Class II) is a subset of the NYHA pool, so the class check matches
-- on observation_id.
rule_3_nyha_class_2 as (
    select latest_nyha.person_id
    from (
        select person_id, observation_id
        from (
            select distinct person_id, observation_id, clinical_effective_date
            from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs2, on_hf_reg_pg3_mr_vs3") }})
        )
        qualify row_number() over (
            partition by person_id
            order by clinical_effective_date desc, observation_id desc
        ) = 1
    ) latest_nyha
    inner join ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs3") }}) class_2
        on latest_nyha.observation_id = class_2.observation_id
),

-- Rule 4: latest oedema code is ankle/feet oedema
rule_4_oedema as (
    select latest_oedema.person_id
    from (
        select person_id, observation_id
        from (
            select distinct person_id, observation_id, clinical_effective_date
            from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs4, on_hf_reg_pg3_mr_vs5") }})
        )
        qualify row_number() over (
            partition by person_id
            order by clinical_effective_date desc, observation_id desc
        ) = 1
    ) latest_oedema
    inner join ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs5") }}) ankle_feet
        on latest_oedema.observation_id = ankle_feet.observation_id
),

-- Rule 5: latest LVEF < 50
rule_5_lvef as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs6") }})
    where result_value < 50
),

-- Rule 9: on the ACE inhibitor pathway
rule_9_ace_pathway as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_hf_reg_pg3_mr_vs15") }})
    where order_date > dateadd(month, -6, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs16") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs17") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs18") }})
    where clinical_effective_date > dateadd(year, -1, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs19") }})
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs20") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
),

-- Combine rule results for all HF register patients
patient_rules as (
    select
        hr.person_id,
        (pg2.person_id is not null) as rule_1_in_pg2,
        (r2.person_id is not null) as rule_2_new_hf_diagnosis,
        (r3.person_id is not null) as rule_3_nyha_class_2,
        (r4.person_id is not null) as rule_4_oedema,
        (r5.person_id is not null) as rule_5_lvef,
        (r9.person_id is not null) as rule_9_ace_pathway,
        case
            when pg2.person_id is not null then 'Excluded'
            when r2.person_id is not null then 'Included'
            when r3.person_id is not null then 'Included'
            when r4.person_id is not null then 'Included'
            when r5.person_id is null then 'Excluded'
            when r9.person_id is not null then 'Excluded'
            else 'Included'
        end as final_status
    from hf_register hr
    left join pg2_exclusions pg2 on hr.person_id = pg2.person_id
    left join rule_2_new_hf_diagnosis r2 on hr.person_id = r2.person_id
    left join rule_3_nyha_class_2 r3 on hr.person_id = r3.person_id
    left join rule_4_oedema r4 on hr.person_id = r4.person_id
    left join rule_5_lvef r5 on hr.person_id = r5.person_id
    left join rule_9_ace_pathway r9 on hr.person_id = r9.person_id
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
