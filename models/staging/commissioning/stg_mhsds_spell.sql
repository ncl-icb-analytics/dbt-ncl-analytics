{{
    config(materialized = 'view')
}}

select hosp_prov_spell_id
    , start_date_hosp_prov_spell
    , source_adm_mh_hosp_prov_spell
    , meth_adm_mh_hosp_prov_spell
    , disch_date_hosp_prov_spell
    , estimated_disch_date_hosp_prov_spell
from {{ ref('raw_mhsds_mhs501hospprovspell') }}
qualify row_number() over (
    partition by hosp_prov_spell_id
    order by dmic_date_added desc, rownumber_id desc
) = 1