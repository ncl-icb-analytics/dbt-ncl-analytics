{{ config(materialized='view') }}

/*
Consolidated person-level frailty view: one row per person_id, combining the three
frailty signals into a single lookup:
- AIC eFI2 (electronic Frailty Index 2): score + category band
- Rockwood Clinical Frailty Scale: score + level/category
- Frailty register: latest coded severity + severity counts

Built primarily to feed `int_person_frailty_snapshot` (SCD2 frailty trajectory for
control-cohort matching), but reusable as a single frailty lookup. Population is any
person with a signal in at least one feeder.

Note: depends on fct_person_frailty_register (reporting layer). If frailty
consolidation is promoted to a reusable reporting dim later, move it there.
*/

with persons as (
    select person_id from {{ ref('stg_aic_int_efi2_scores') }}
    union
    select person_id from {{ ref('int_rockwood_latest') }}
    union
    select person_id from {{ ref('fct_person_frailty_register') }}
)

select
    p.person_id
    -- AIC eFI2
    , efi.efi_score as efi2_score
    , efi.category as efi2_category
    -- Rockwood Clinical Frailty Scale
    , rock.rockwood_score
    , rock.frailty_level as rockwood_frailty_level
    , rock.frailty_category as rockwood_frailty_category
    -- Frailty register (coded severity)
    , fr.latest_frailty_severity
    , fr.mild_frailty_count
    , fr.moderate_frailty_count
    , fr.severe_frailty_count
from persons p
left join {{ ref('stg_aic_int_efi2_scores') }} efi
    on p.person_id = efi.person_id
left join {{ ref('int_rockwood_latest') }} rock
    on p.person_id = rock.person_id
left join {{ ref('fct_person_frailty_register') }} fr
    on p.person_id = fr.person_id
