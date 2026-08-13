-- UKHFD registered patient counts. One row per organisation, sex and 5-year
-- age band per monthly snapshot. Includes GP practice, PCN, ICB and other
-- organisation types; sex and age band include ALL rollup rows.
select
    org_type,
    org_code,
    ons_code,
    sex,
    age_band,
    number_of_patients::int as number_of_patients,
    effective_snapshot_date as snapshot_date,
    source_file_version
from {{ ref('raw_ukhfd_registered_patients_by_gp_practice') }}
where org_code is not null
  and number_of_patients is not null
