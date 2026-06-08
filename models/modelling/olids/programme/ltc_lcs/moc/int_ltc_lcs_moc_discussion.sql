-- LTC LCS: Model of Care activity - Discussion appointment completed (PCSP agreed)
-- Flags persons with a Personalised Care and Support Plan agreed event in the last 12 months.
-- Used by HRCS3 / HRS3 / MRS3 / LRS3 EMIS searches.

with events as (
    select
        person_id,
        clinical_effective_date
    from ({{ get_ltc_lcs_observations_latest("4_moc_discussion_vs1") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
      and clinical_effective_date <= current_date()
)

select
    person_id,
    max(clinical_effective_date) as latest_completed_date,
    true as discussion_completed
from events
group by person_id
