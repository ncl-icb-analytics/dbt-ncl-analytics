-- LTC LCS: Model of Care activity - MoC declined
-- Flags persons with a Personalised Care and Support Planning declined event in the last 12 months.
-- Used by HRCS3 / HRCS4 / HRS3 / HRS4 / MRS3 / MRS4 / LRS3 / LRS4 EMIS searches
-- (OR'd with appointment-completed flags).

with events as (
    select
        person_id,
        clinical_effective_date
    from ({{ get_ltc_lcs_observations_latest("follow_up_appointment_completedmoc_declined_vs1") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
      and clinical_effective_date <= current_date()
)

select
    person_id,
    max(clinical_effective_date) as latest_declined_date,
    true as moc_declined
from events
group by person_id
