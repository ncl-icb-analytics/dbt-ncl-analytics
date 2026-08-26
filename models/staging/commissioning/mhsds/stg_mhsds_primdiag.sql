{{
    config(
        materialized = 'table',
        tags=['mhsds']
    )
}}

with deduplicated as (
    {{
        deduplicate_mhsds(
            mhsds_table = ref('raw_mhsds_mhs604primdiag'),
            partition_cols = ['uniq_serv_req_id', 'coded_diag_timestamp']
        )
    }}
)

, snomed_to_icd10 as (
    select
        referenced_component_id
        , map_target
    from {{ ref('raw_dictionary_snomed_ref_set_complex_map') }}
    where ref_set_id = '999002271000000101'
        and active = true
    qualify row_number() over (
        partition by referenced_component_id
        order by map_group, map_priority
    ) = 1
)

, resolved as (
    select
        d.uniq_serv_req_id
        , d.person_id
        , d.coded_diag_timestamp
        , d.diag_scheme_in_use
        , d.prim_diag
        , case
            when d.diag_scheme_in_use = '02'
                and left(d.prim_diag, 1) in ('F', 'G', 'Q', 'R') then d.prim_diag
            when d.diag_scheme_in_use = '06' then s.map_target
        end as icd10_code
        , d.org_id_prov
    from deduplicated as d
    left join snomed_to_icd10 as s
        on to_varchar(d.prim_diag) = to_varchar(s.referenced_component_id)
)

select
    uniq_serv_req_id
    , person_id
    , coded_diag_timestamp
    , diag_scheme_in_use
    , prim_diag
    , icd10_code
    , left(replace(replace(icd10_code, '.', ''), 'X', '0'), 3) as icd10_3
    , org_id_prov
from resolved
-- ~0.2% of rows have no coded_diag_timestamp (and no diag_date fallback);
-- unusable for as-at diagnosis selection
where coded_diag_timestamp is not null
