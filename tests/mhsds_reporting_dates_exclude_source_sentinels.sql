select
    'care_contact' as model_name,
    count(*) as sentinel_row_count
from {{ ref('fct_mhsds_care_contact') }}
where least_ignore_nulls(
    care_contact_date,
    earliest_reasonable_offer_date,
    earliest_clinically_appropriate_date,
    appointment_offer_date,
    appointment_booked_date,
    care_contact_cancellation_date,
    reporting_period_start_date,
    reporting_period_end_date
) < '1901-01-01'::date
having count(*) > 0

union all

select
    'referral' as model_name,
    count(*) as sentinel_row_count
from {{ ref('fct_mhsds_referral') }}
where least_ignore_nulls(
    referral_received_date,
    decision_to_treat_date,
    discharge_plan_created_date,
    discharge_plan_last_updated_date,
    referral_rejection_date,
    referral_discharge_date,
    discharge_letter_issued_date,
    record_start_date,
    record_end_date,
    reporting_period_start_date,
    reporting_period_end_date
) < '1901-01-01'::date
having count(*) > 0

union all

-- Spell and diagnosis dates are not listed: stg_mhsds_spell and
-- stg_mhsds_primdiag still pass their source values through uncast, so the
-- sentinel contract does not yet hold for latest_admission_date,
-- latest_discharge_date or latest_diagnosis_date.
select
    'person_mh_profile' as model_name,
    count(*) as sentinel_row_count
from {{ ref('dim_person_mh_profile') }}
where least_ignore_nulls(
    first_referral_date,
    latest_referral_date,
    latest_contact_date,
    latest_formal_mha_status_start_date
) < '1901-01-01'::date
having count(*) > 0
