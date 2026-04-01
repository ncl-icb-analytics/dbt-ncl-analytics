-- LTC LCS: CKD Register - Priority Group 2 (High Risk)
-- Parent population: CKD register, excluding PG1 (HRC)
--
-- EMIS final gating requires:
-- - recent antihypertensive medication issue in the last 3 months
-- - exclusion of PG1
-- - latest BP in the last 12 months showing diastolic > 90 or systolic > 150

with ckd_register as (
    select distinct person_id
    from {{ ref('fct_person_ckd_register') }}
),

pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_ckd_pg1_hrc') }}
),

repeat_prescription_codes as (
    select distinct code
    from {{ ref('stg_reference_combined_codesets') }}
    where cluster_id = 'REPEAT_PRESCRIPTION'
),

recent_antihypertensive_repeat as (
    select am.person_id
    from {{ ref('int_antihypertensive_medications_all') }} am
    inner join {{ ref('stg_olids_medication_statement') }} ms
        on am.medication_statement_id = ms.id
    inner join repeat_prescription_codes rpc
        on ms.authorisation_type_code = rpc.code
    where am.order_date >= dateadd(month, -3, current_date())
    group by am.person_id
),

recent_bp as (
    select
        person_id,
        systolic_value,
        diastolic_value
    from {{ ref('int_blood_pressure_latest') }}
    where clinical_effective_date >= dateadd(month, -12, current_date())
),

bp_threshold_met as (
    select person_id
    from recent_bp
    where diastolic_value > 90
       or systolic_value > 150
)

select
    cr.person_id,
    'Included' as final_status,
    'CKD' as condition,
    '2' as priority_group,
    'HR' as risk_group
from ckd_register cr
left join pg1_exclusions pg1 on cr.person_id = pg1.person_id
inner join recent_antihypertensive_repeat meds on cr.person_id = meds.person_id
inner join bp_threshold_met bp on cr.person_id = bp.person_id
where pg1.person_id is null
