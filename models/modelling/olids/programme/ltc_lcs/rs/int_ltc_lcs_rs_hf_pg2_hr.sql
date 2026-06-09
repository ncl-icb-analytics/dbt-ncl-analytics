-- LTC LCS: HF Register - Priority Group 2 (High Risk)
-- Parent population: HF register
-- NB: there is no PG1 for HF.
-- EMIS source: 'On HF Register- LTC LCS Priority Group 2 (HR)*'
-- (docs/emis_specs/ltc_lcs_rs/on_hf_register_ltc_lcs_priority_group_2_hr.md)
--
-- Rule chain:
-- - Rule 1: latest NYHA classification (vs1+vs2) is Class III or IV (vs2) -> include
-- - Rule 2: latest oedema code (vs3+vs4) is significant oedema (vs4), or
--   metolazone order (vs5) in last year -> include
-- - Rule 3 (gate): latest HbA1c (vs6) > 48 and <= 75. Fail -> exclude.
-- - Rule 4 (gate): latest LVEF (vs7) < 50, or HFLVSD/REDEJCFRAC code (vs8/vs9).
--   Fail -> exclude.
-- - Rules 5-7 (SGLT2/MRA/beta-blocker arms) do not change who is included
--   (pass and fail both continue) and are not implemented.
-- - Rule 8: exclude if on the ACE inhibitor pathway - latest ACEi order (vs18)
--   in last 6 months, ACEi/AIIRA declined (vs19/vs20) or ACEi adverse reaction
--   (vs21) in last 12 months, XAII code ever (vs22), or TXAII (vs23) in last
--   12 months. Otherwise include.
--
-- Net logic: rule_1 or rule_2 or (rule_3 and rule_4 and not rule_8)
--
-- Known gap: rule 8 also references EMIS library item
-- 80709f0e-d62c-4851-b8b5-22ce2fb29319, whose logic is not in the XML export;
-- that exclusion arm cannot be implemented.

with
-- Parent population: Patients currently on HF register
hf_register as (
    select distinct person_id
    from {{ ref('fct_person_heart_failure_register') }}
    where is_on_register = true
),

-- Rule 1: latest NYHA classification is Class III or IV.
-- vs2 (Class III/IV) is a subset of the NYHA pool, so the class check
-- matches on observation_id.
rule_1_nyha_class_3_4 as (
    select latest_nyha.person_id
    from (
        select person_id, observation_id
        from (
            select distinct person_id, observation_id, clinical_effective_date
            from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs1, on_hf_reg_pg2_hr_vs2") }})
        )
        qualify row_number() over (
            partition by person_id
            order by clinical_effective_date desc, observation_id desc
        ) = 1
    ) latest_nyha
    inner join ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs2") }}) class_3_4
        on latest_nyha.observation_id = class_3_4.observation_id
),

-- Rule 2: latest oedema code is significant oedema, or metolazone in last year
rule_2_oedema_or_metolazone as (
    select latest_oedema.person_id
    from (
        select person_id, observation_id
        from (
            select distinct person_id, observation_id, clinical_effective_date
            from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs3, on_hf_reg_pg2_hr_vs4") }})
        )
        qualify row_number() over (
            partition by person_id
            order by clinical_effective_date desc, observation_id desc
        ) = 1
    ) latest_oedema
    inner join ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs4") }}) significant
        on latest_oedema.observation_id = significant.observation_id
    union
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_hf_reg_pg2_hr_vs5") }})
    where order_date >= dateadd(year, -1, current_date())
),

-- Rule 3: latest HbA1c > 48 and <= 75
rule_3_hba1c as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs6") }})
    where result_value > 48 and result_value <= 75
),

-- Rule 4: latest LVEF < 50, or HFLVSD/REDEJCFRAC code
rule_4_lvef as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs7") }})
    where result_value < 50
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs8, on_hf_reg_pg2_hr_vs9") }})
),

-- Rule 8: on the ACE inhibitor pathway
rule_8_ace_pathway as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_hf_reg_pg2_hr_vs18") }})
    where order_date > dateadd(month, -6, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs19") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs20") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs21") }})
    where clinical_effective_date > dateadd(year, -1, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs22") }})
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs23") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
),

-- Combine rule results for all HF register patients
patient_rules as (
    select
        hr.person_id,
        (r1.person_id is not null) as rule_1_nyha_class_3_4,
        (r2.person_id is not null) as rule_2_oedema_or_metolazone,
        (r3.person_id is not null) as rule_3_hba1c,
        (r4.person_id is not null) as rule_4_lvef,
        (r8.person_id is not null) as rule_8_ace_pathway,
        case
            when r1.person_id is not null then 'Included'
            when r2.person_id is not null then 'Included'
            when r3.person_id is null then 'Excluded'
            when r4.person_id is null then 'Excluded'
            when r8.person_id is not null then 'Excluded'
            else 'Included'
        end as final_status
    from hf_register hr
    left join rule_1_nyha_class_3_4 r1 on hr.person_id = r1.person_id
    left join rule_2_oedema_or_metolazone r2 on hr.person_id = r2.person_id
    left join rule_3_hba1c r3 on hr.person_id = r3.person_id
    left join rule_4_lvef r4 on hr.person_id = r4.person_id
    left join rule_8_ace_pathway r8 on hr.person_id = r8.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'HF' as condition,
    '2' as priority_group,
    'HR' as risk_group
from patient_rules
where final_status = 'Included'
