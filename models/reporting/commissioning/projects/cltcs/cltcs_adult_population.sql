{{ config(materialized='table', tags=['cltcs']) }}

/*
Shared adult-registrant population for the C-LTCS sub-score models.

One row per living adult sk_patient_id from dim_person_demographics_basic (PDS as
the source of truth for registrant data), enriched with:
- person_id: OLIDS person key via dim_person_pseudo, resolved to one row per
  sk_patient_id with a latest-registered-record tiebreak.
- neighbourhood_code: C-LTCS local neighbourhood
  (stg_cltcs_emis_cltcs_local_mapping_nh_gp), used as the z-score scaling group in
  the sub-score models. Null for practices outside the Haringey/Camden local mapping.

NOTE: the sk_patient_id <-> person_id resolution here is a local stopgap. It should
be handled centrally (e.g. in dim_person_pseudo or a shared bridge/macro), and it
does NOT handle fragmented records well: it silently keeps one person_id and discards
the others' data, and does nothing for the reverse case (one person_id spanning
multiple sk_patient_ids). See flag-and-exclude in cltcs_patient_list and
aggregate-to-sk_patient_id in int_resource_index_olids_profile for robust approaches.

Downstream: cltcs_score_activation / _coordination / _treatment / _frailty.
*/

select
    db.sk_patient_id
    , pp.person_id
    , nh.neighbourhood_code
    , db.practice_code
    , db.practice_name
from {{ ref('dim_person_demographics_basic') }} db -- PDS as the source of truth for registrant data
left join {{ ref('dim_person_pseudo') }} pp
    on db.sk_patient_id = pp.sk_patient_id
left join {{ ref('dim_person_demographics') }} reg
    on pp.person_id = reg.person_id
left join {{ ref('stg_cltcs_emis_cltcs_local_mapping_nh_gp') }} nh
    on db.practice_code = nh.practice_code
where db.date_of_death is null -- living patients only
    and db.date_of_birth < date_trunc('month', dateadd(year, -18, current_date)) -- adults only
    and db.sk_patient_id is not null 
    and pp.person_id is not null
-- Latest-record tiebreak when an sk_patient_id maps to >1 person_id: keep the most
-- recently registered person record (person_id as a stable final tiebreak).
qualify row_number() over (
    partition by db.sk_patient_id
    order by reg.registration_start_date desc nulls last, pp.person_id
) = 1
