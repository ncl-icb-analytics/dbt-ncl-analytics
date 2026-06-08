-- LTC LCS: Model of Care activity - Remote Desktop Review (RDR) completed
-- Flags persons with a Chronic disease management annual review completed event in the last 12 months.
-- Used by HRCS2A / HRS2A EMIS searches (HRCS/HRS pathways only — MRS/LRS do not have an RDR step).

with events as (
    select
        person_id,
        clinical_effective_date
    -- 'mdt_review_completed_vs1' is the warehouse valueset identifier for the RDR concept; left unchanged.
    from ({{ get_ltc_lcs_observations_latest("mdt_review_completed_vs1") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
      and clinical_effective_date <= current_date()
)

select
    person_id,
    max(clinical_effective_date) as latest_completed_date,
    true as remote_desktop_review_completed
from events
group by person_id
