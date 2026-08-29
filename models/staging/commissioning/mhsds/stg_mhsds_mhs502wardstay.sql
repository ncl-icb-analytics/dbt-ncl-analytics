{{
    config(
        materialized = 'table',
        tags = ['mhsds']
    )
}}

with latest_ward_stay as (
    {{
        select_latest_mhsds_state(
            mhsds_table = ref('raw_mhsds_mhs502wardstay'),
            partition_cols = ['uniq_ward_stay_id'],
            tie_breaker_cols = ['mhs502_uniq_id']
        )
    }}
)

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
    , ward_stay.reporting_period_end_date
from latest_ward_stay as ward_stay
