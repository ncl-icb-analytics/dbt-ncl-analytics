{{
    config(
        materialized = 'table',
        tags=['mhsds']
        )
}}

WITH deduplicated AS (
{{
    deduplicate_mhsds(
        mhsds_table =ref('raw_mhsds_mhs501hospprovspell'),
        partition_cols = ['uniq_hosp_prov_spell_num']
    )
}} )

-- End of the last reporting period this spell appeared in an active submission;
-- providers must resubmit every active spell each period, so this is the last
-- evidence the spell was genuinely open. Taken as the max across every active
-- submission carrying the spell rather than from the deduplicated row:
-- deduplicate_mhsds() ranks on effective_from, a load timestamp that can tie
-- across reporting periods, so the surviving row is not reliably the latest
-- submission. int_mhsds_spell_encounters classifies open vs orphaned spells on
-- this date, so it has to be the true last period.
, last_active_submission as (
    select
        spell.uniq_hosp_prov_spell_num
        , max(sub.reporting_period_end_date) as reporting_period_end_date
    from {{ ref('raw_mhsds_mhs501hospprovspell') }} as spell
    inner join {{ ref('raw_mhsds_activesubmission') }} as sub
        on spell.uniq_submission_id = sub.uniq_submission_id
    group by 1
)

select
    d.uniq_hosp_prov_spell_num
    , d.uniq_submission_id
    , d.person_id
    , d.start_date_hosp_prov_spell
    , d.source_adm_mh_hosp_prov_spell
    , d.meth_adm_mh_hosp_prov_spell
    , d.disch_date_hosp_prov_spell
    , d.estimated_disch_date_hosp_prov_spell
     --adding additional fields
    , d.org_id_prov
    , d.dmic_ccg_code as dm_icb_commissioner
    , las.reporting_period_end_date
from deduplicated as d
left join last_active_submission as las
    on d.uniq_hosp_prov_spell_num = las.uniq_hosp_prov_spell_num

-- NB: De-deuplicated layer MHSDS.docx shared by Shak recommends to deduplicate using
-- PARTITION BY [UniqServReqID],[UniqHospProvSpellNum]
-- however this leaves duplicate records on uniq_hosp_prov_spell_num which seem to be for the same event (same admission date, provider, patient)
-- using partition on uniq_hosp_prov_spell_num only for now