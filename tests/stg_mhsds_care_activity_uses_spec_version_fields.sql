select a.mhs202_uniq_id
from {{ ref('stg_mhsds_care_activity') }} as a
inner join {{ ref('raw_mhsds_mhs202careactivity') }} as r
    on a.mhs202_uniq_id = r.mhs202_uniq_id
where a.procedure_code is distinct from iff(
        a.mhsds_version = 'V6'
        , r.procedure
        , r.code_proc_and_proc_status
    )
    or a.finding_code is distinct from iff(
        a.mhsds_version = 'V6'
        , r.finding
        , r.code_find
    )
    or a.observation_code is distinct from iff(
        a.mhsds_version = 'V6'
        , r.observation
        , r.code_obs
    )
    or a.observation_scheme_code is distinct from iff(
        a.mhsds_version = 'V6'
        , '03'
        , r.obs_scheme_in_use
    )
    or a.unit_of_measurement_code is distinct from iff(
        a.mhsds_version = 'V6'
        , r.unit_of_measurement_ucum
        , r.unit_measure
    )
