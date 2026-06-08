-- LTC LCS: Model of Care activity - Follow-up appointment completed (PCSP review)
-- Flags persons with a Review of Personalised Care and Support Plan event in the last 12 months.
-- Used by HRCS4 / HRS4 / MRS4 / LRS4 EMIS searches.

with events as (
    select
        person_id,
        clinical_effective_date
    from ({{ get_ltc_lcs_observations_latest("5_moc_follow_up_vs1") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
      and clinical_effective_date <= current_date()
)

select
    person_id,
    max(clinical_effective_date) as latest_completed_date,
    true as followup_completed
from events
group by person_id
