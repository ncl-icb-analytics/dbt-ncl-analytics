{{
    config(
        materialized = 'table',
        tags = ['mhsds']
    )
}}

-- Older reporting periods can be resubmitted after newer periods. The reporting
-- period therefore identifies the newest spell state; file receipt time only
-- resolves records within that period.
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
    , active_submission.reporting_period_end_date
    , spell.uniq_serv_req_id
    , spell.age_hosp_start_date
from {{ ref('raw_mhsds_mhs501hospprovspell') }} as spell
inner join {{ ref('raw_mhsds_activesubmission') }} as active_submission
    on spell.uniq_submission_id = active_submission.uniq_submission_id
qualify row_number() over (
    partition by spell.uniq_hosp_prov_spell_num
    order by
        active_submission.reporting_period_end_date desc
        , spell.effective_from desc nulls last
        , spell.uniq_submission_id desc
        , spell.mhs501_uniq_id desc
) = 1
