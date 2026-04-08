-- LTC LCS: HF Register - Priority Group 2 (High Risk)
-- Implements inclusion/exclusion rules for high-risk patient identification
-- Parent population: HF register
-- NB: there is no PG1.

-- Inclusion rules:
-- - Rule 1: New York Heart Association Classification I - IV (include on pass)
-- - Rule 2: Oedema (event) or metolazone (medication isue) (include on pass)
-- - Rule 3: Latest hba1c > 48 and <= 75 (exclude on fail)
-- - Rule 4: left ventricular ejection fraction < 50 (exclude on fail)
-- - Rule 5: Dapagliflozin, Empagliflozin, Canagliflozin, Ertugliflozin L-pyroglutamic acid within 6 months (or adverse reaction) (include on fail)

with
-- Parent population: Patients currently on HF register
hf_register as (
    select distinct person_id
    from {{ ref('fct_person_hf_register') }}
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
-- Rule 5: Dapagliflozin, Empagliflozin, Canagliflozin, Ertugliflozin L-pyroglutamic acid within 6 months (or adverse reaction)
rule_5_medications_sglt2_inhibitors as (
    -- medication courses
    
    
    -- medication issues
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_hf_reg_pg2_hr_vs10") }})
    where order_date >= dateadd(month, -12, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1

    union

    -- adverse reactions
    select person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs11") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
)
rule_6_medications_mras as
(

    -- medication issues
    select person_id
    from ({{ get_ltc_lcs_medication_orders("on_hf_reg_pg2_hr_vs10") }})
    where order_date >= dateadd(month, -12, current_date())
    qualify row_number() over (partition by person_id order by order_date desc) = 1

    union

    -- adverse reactions
    select person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs11") }})
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
)


-- Combine rule results for all HF register patients
patient_rules as (
    select
        cr.person_id,
        (r1a.person_id is not null) as rule_1a_first_new_flare_up,
        case
            when r1a.person_id is not null then 'Included'  -- Rule 1a passed
            else 'Excluded'
        end as final_status
    from chd_register cr
    left join rule_1a_first_new_flare_up r1a on cr.person_id = r1a.person_id
)
-- Final result: included patients only
select
    person_id,
    final_status,
    'HF' as condition,
    '1' as priority_group,
    'HRC' as risk_group
from patient_rules
where final_status = 'Included'
