{{
    config(
        materialized = 'table',
        tags = ['mhsds']
    )
}}

-- Older reporting periods can be resubmitted after newer periods. The reporting
-- period therefore identifies the newest ward-stay state; file receipt time only
-- resolves records within that period.
select
    ward_stay.uniq_serv_req_id
    , ward_stay.person_id
    , ward_stay.uniq_hosp_prov_spell_num
    , ward_stay.org_id_prov
    , ward_stay.site_id_of_treat
    , ward_stay.hospital_bed_type_name
    , ward_stay.ward_type
    , ward_stay.ward_code
    , ward_stay.uniq_ward_stay_id
    , ward_stay.effective_from
    , ward_stay.start_date_ward_stay
    , ward_stay.end_date_ward_stay
    , ward_stay.mh_admitted_patient_class
    , ward_stay.dmic_ccg_code
    , ward_stay.uniq_submission_id
    , active_submission.reporting_period_end_date
from {{ ref('raw_mhsds_mhs502wardstay') }} as ward_stay
inner join {{ ref('raw_mhsds_activesubmission') }} as active_submission
    on ward_stay.uniq_submission_id = active_submission.uniq_submission_id
qualify row_number() over (
    partition by ward_stay.uniq_ward_stay_id
    order by
        active_submission.reporting_period_end_date desc
        , ward_stay.effective_from desc nulls last
        , ward_stay.uniq_submission_id desc
        , ward_stay.mhs502_uniq_id desc
) = 1
