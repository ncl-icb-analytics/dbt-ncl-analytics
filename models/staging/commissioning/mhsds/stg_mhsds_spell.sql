{{
    config(
        materialized = 'table',
        tags = ['mhsds']
    )
}}

with latest_spell as (
    {{
        select_latest_mhsds_record(
            mhsds_table = ref('raw_mhsds_mhs501hospprovspell'),
            partition_cols = ['uniq_hosp_prov_spell_num'],
            tie_breaker_cols = ['mhs501_uniq_id']
        )
    }}
)

select
    spell.uniq_hosp_prov_spell_num
    , spell.uniq_submission_id
    , spell.person_id
    , spell.start_date_hosp_prov_spell
    , spell.source_adm_mh_hosp_prov_spell
    , spell.meth_adm_mh_hosp_prov_spell
    , spell.disch_date_hosp_prov_spell
    , spell.estimated_disch_date_hosp_prov_spell
    , spell.org_id_prov
    , spell.dmic_ccg_code as dm_icb_commissioner
    , spell.reporting_period_end_date
    , spell.uniq_serv_req_id
    , spell.age_hosp_start_date
from latest_spell as spell
