{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['medication_adherence'])
}}

/*
Reporting-layer medicines adherence: rolling and overall PDC (Proportion of
Days Covered) per person, drug and monthly 12-month window.

int_medication_adherence_pdc joined to dim_person — only persons with at
least one linked patient record appear here. Both the faithful port of the
NCL meds_adherence Snowpark pipeline (pdc, overall_pdc) and the corrected
variants (*_corrected — final order of each refill chain counts in full) are
exposed; see int_medication_adherence_pdc for the divergence documentation.

The modelling layer groups refill chains on the VTM code (stable identifier);
this mart re-presents drug_name as the readable VTM name — matching the
original pipeline's output — via a deduplicated code -> name map (one name
per code, lowest alphabetically, so the join can never fan out). vtm_code is
retained as the unique grain key: distinct codes could in principle share a
name string, so uniqueness is tested on the code, not the name. Rows whose
chain key never resolved to a VTM (drug_name_source = CONCEPT_DISPLAY
upstream) pass their concept display string through unchanged.

No opt-out filter is applied at this layer. For secondary use, consumers
should INNER JOIN to REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_SECONDARY_USE_ALLOWED
ON person_id per the project convention.
*/

with vtm_names as (
    select
        to_varchar(vtm) as vtm_code,
        vtm_name
    from {{ ref('stg_reference_bnf_latest') }}
    where vtm is not null
        and vtm_name is not null
    qualify row_number() over (
        partition by to_varchar(vtm)
        order by vtm_name
    ) = 1
)

select
    f.person_id,
    f.drug_name as vtm_code,
    coalesce(v.vtm_name, f.drug_name) as drug_name,
    f.drug_class,
    f.window_start,
    f.window_end,
    f.exposure_start,
    f.exposure_end,
    f.covered_days,
    f.covered_days_corrected,
    f.window_days,
    f.pdc,
    f.pdc_corrected,
    f.overall_start,
    f.overall_end,
    f.total_exposure_days,
    f.overall_pdc,
    f.overall_pdc_corrected,
    f.pdc_type,
    f.exclusive,
    f.window_length_months
from {{ ref('int_medication_adherence_pdc') }} f
inner join {{ ref('dim_person') }} p
    on f.person_id = p.person_id
left join vtm_names v
    on f.drug_name = v.vtm_code
