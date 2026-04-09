-- LTC LCS: HF Register - Priority Group 2 (High Risk)
-- Implements inclusion/exclusion rules for high-risk patient identification
-- Parent population: HF register
-- NB: there is no PG1.

-- Inclusion rules:
-- - Rule 1: New York Heart Association Classification I - IV (include on pass)
-- - Rule 2: Oedema (event) or metolazone (medication isue) (include on pass)
-- - Rule 3: Latest hba1c > 48 and <= 75 (exclude on fail)
-- - Rule 4: left ventricular ejection fraction < 50 (exclude on fail)
-- - Rule 5: SGLT2 inhibitors within 6/12 months (or adverse reaction) (include on fail)
-- - Rule 6: MRAs within 6/12 months (or adverse reaction) (include on fail)
-- - Rule 7: beta blockers within 6/12 months (or adverse reaction) (include on fail)
-- - Rule 8: ACE inhibitors within 6/12 months (or adverse reaction) (include on fail)

with
-- Parent population: Patients currently on HF register
hf_register as (
    select distinct person_id
    from {{ ref('fct_person_heart_failure_register') }}
),

-- Rule 1: New York Heart Association Classification I - IV (Include)
rule_1_nyha as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs1") }})

    union

    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs2") }})
),

-- Rule 2: Oedema (event) or metolazone (medication isue) (Include)
rule_2_oedema as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs3") }})

    union

    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs4") }})

    union

    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_hf_reg_pg2_hr_vs5") }})
    where order_date >= dateadd(month, -12, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1
),
-- Rule 3: Latest hba1c > 48 and <= 75
rule_3_hba1c as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs6") }})
    where result_value > 48 and result_value <= 75
),

-- Rule 4: left ventricular ejection fraction < 50
rule_4_lvef as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs7") }})
    where result_value < 50

    union

    select person_id
    from
        (select
        *
        from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs8") }})) o
    left join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on o.episodicity_concept_id = ecm.source_code_id -- join episodiity for first/new/flare-up
    where
    ecm.source_display not in ('Review','End')
    and clinical_effective_date <= current_date()
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1

    union

    select person_id
    from
        (select
        *
        from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs9") }})) o
    left join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on o.episodicity_concept_id = ecm.source_code_id -- join episodiity for first/new/flare-up
    where
    ecm.source_display not in ('Review','End')
    and clinical_effective_date <= current_date()
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),

-- Rule 5: Dapagliflozin, Empagliflozin, Canagliflozin, Ertugliflozin L-pyroglutamic acid within 6/12 months (or adverse reaction)
rule_5_medications_sglt2_inhibitors as (
    -- medication courses
    select
    person_id 
    from ({{ get_ltc_lcs_medication_statements("on_hf_reg_pg2_hr_vs10") }})
    where statement_date >= dateadd(month, -6, current_date()) -- within last 6 months
    qualify row_number() over (partition by person_id order by statement_date desc) = 1
    
    union

    -- medication issues
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_hf_reg_pg2_hr_vs10") }})
    where order_date >= dateadd(month, -12, current_date()) -- within last year
    qualify row_number() over (partition by person_id order by order_date desc) = 1

    union

    -- adverse reactions
    select person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs11") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),

rule_6_medications_mras as (
    -- medication courses
    select
    person_id 
    from ({{ get_ltc_lcs_medication_statements("on_hf_reg_pg2_hr_vs12") }})
    where statement_date >= dateadd(month, -6, current_date()) -- within last 6 months
    qualify row_number() over (partition by person_id order by statement_date desc) = 1
    
    union

    -- medication issues
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_hf_reg_pg2_hr_vs12") }})
    where order_date >= dateadd(month, -12, current_date()) -- within last year
    qualify row_number() over (partition by person_id order by order_date desc) = 1

    union

    -- adverse reactions
    select person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs13") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),

rule_7_medications_beta_blockers as (
    -- medication issues
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_hf_reg_pg2_hr_vs14") }})
    where order_date >= dateadd(month, -6, current_date()) -- within 6 months
    qualify row_number() over (partition by person_id order by order_date desc) = 1

    union

    -- hypersensitivity to atenolol
    select person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs15") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1

    union

    select
    person_id 
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs16") }})
    where clinical_effective_date <= dateadd(month, -12, current_date()) -- on or before 1 year ago
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1

    union

    select
    person_id 
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs17") }})
    where clinical_effective_date <= dateadd(month, -12, current_date()) -- on or before 1 year ago
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),

rule_8_medications_ace_inhibitors as (
    -- medication issues
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_hf_reg_pg2_hr_vs18") }})
    where order_date > dateadd(month, -6, current_date()) -- within last 6 months
    qualify row_number() over (partition by person_id order by order_date desc) = 1

    union

    select
    person_id 
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs19") }})
    where clinical_effective_date > dateadd(month, -12, current_date()) -- within last year
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1

    union

    select
    person_id 
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs20") }})
    where clinical_effective_date > dateadd(month, -12, current_date()) -- within last year
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1

    union

    -- adverse reactionss
    select
    person_id 
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs21") }})
    where clinical_effective_date > dateadd(month, -12, current_date()) -- within last year
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1

    union

    select
    person_id 
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs22") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1

    union

    select
    person_id 
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs23") }})
    where clinical_effective_date > dateadd(month, -12, current_date()) -- within last year
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),

-- Combine rule results for all HF register patients
patient_rules as (
    select
        hr.person_id,
        (r1.person_id is not null) as rule_1_nyha,
        (r2.person_id is not null) as rule_2_oedema,
        (r3.person_id is not null) as rule_3_hba1c,
        (r4.person_id is not null) as rule_4_lvef,
        (r5.person_id is not null) as rule_5_medications_sglt2_inhibitors,
        (r6.person_id is not null) as rule_6_medications_mras,
        (r7.person_id is not null) as rule_7_medications_beta_blockers,
        (r8.person_id is not null) as rule_8_medications_ace_inhibitors,
        case
            when r1.person_id is not null then 'Included'  -- Rule 1 passed
            when r2.person_id is not null then 'Included' -- Rule 2 passed
            when r3.person_id is null then 'Excluded' -- Rule 3 failed
            when r4.person_id is null then 'Excluded' -- Rule 4 failed
            when r5.person_id is null then 'Included' -- Rule 5 failed
            when r6.person_id is null then 'Included' -- Rule 6 failed
            when r7.person_id is null then 'Included' -- Rule 7 failed
            when r8.person_id is null then 'Included' -- Rule 8 failed
            else 'Excluded'
        end as final_status
    from hf_register hr
    left join rule_1_nyha r1 on hr.person_id = r1.person_id
    left join rule_2_oedema r2 on hr.person_id = r2.person_id
    left join rule_3_hba1c r3 on hr.person_id = r3.person_id
    left join rule_4_lvef r4 on hr.person_id = r4.person_id
    left join rule_5_medications_sglt2_inhibitors r5 on hr.person_id = r5.person_id
    left join rule_6_medications_mras r6 on hr.person_id = r6.person_id
    left join rule_7_medications_beta_blockers r7 on hr.person_id = r7.person_id
    left join rule_8_medications_ace_inhibitors r8 on hr.person_id = r8.person_id
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
