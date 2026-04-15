{{
    config(
        materialized = 'table',
        tags=['mhsds']
        )
}}

WITH deduplicated AS (
{{
    deduplicate_mhsds(
        mhsds_table = ref('raw_mhsds_mhs502wardstay'),
        partition_cols = ['uniq_serv_req_id']
    )
}} )
select 
    uniq_serv_req_id
    ,person_id
    ,uniq_hosp_prov_spell_num
    ,org_id_prov
    ,site_id_of_treat
    ,hospital_bed_type_name
    ,ward_type
    ,uniq_ward_stay_id
    ,effective_from
    ,start_date_ward_stay
    ,end_date_ward_stay
    ,mh_admitted_patient_class
    ,dmic_ccg_code
from deduplicated 