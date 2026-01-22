{{
    config(materialized = 'table')
}}

select 
    uniq_hosp_prov_spell_num
    , uniq_submission_id
    , person_id
    , start_date_hosp_prov_spell
    , source_adm_mh_hosp_prov_spell
    , meth_adm_mh_hosp_prov_spell
    , disch_date_hosp_prov_spell
    , estimated_disch_date_hosp_prov_spell
from {{ ref('raw_mhsds_mhs501hospprovspell') }}
qualify row_number() over (
    partition by uniq_hosp_prov_spell_num
    order by effective_from desc
) = 1

-- NB: De-deuplicated layer MHSDS.docx shared by Shak recommends to deduplicate using
-- PARTITION BY [UniqServReqID],[UniqHospProvSpellNum]
-- however this leaves duplicate records on uniq_hosp_prov_spell_num which seem to be for the same event (same admission date, provider, patient)
-- using partition on uniq_hosp_prov_spell_num only for now