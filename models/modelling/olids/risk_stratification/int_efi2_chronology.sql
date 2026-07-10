{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['efi2'])
}}

/*
eFI2 chronology — attaches deficit weights to the per-person deficit flags and
applies the special-case handling (max dementia, latest alcohol, missing BMI /
alcohol defaults, polypharmacy bands, latest smoking status), then sums the
weights to the raw eFI2 score per person.

Port of lds-pipelines stg_efi2__chronology. Substitutions vs lds (OLIDS-native):
- lds stg_efi2__rules                -> int_efi2_rules
- lds base_efi2__weights             -> stg_aic_base_efi2_weights
- lds stg_gp__medication_order        -> stg_olids_medication_order

person_unique (the population over which missing-lifestyle deficits and
polypharmacy are evaluated) now draws from int_efi2_patient_list, the full scored
spine, rather than int_efi2_cohort_snomed_codes. See the person_unique CTE for why
- this is a deliberate paper-alignment deviation from lds.

Polypharmacy counts distinct BNF sub-subchapters (level 3 = first 6 chars of the
BNF code) among meds prescribed in the 90 days before end_date, per the eFI2
paper. The deprecated phenolab definition store is removed. No BNF code exclusions
are applied (unlike this repo's int_polypharmacy_* pipeline) - the paper counts
across all sub-subchapters; see the polypharmacy CTE note. No phenolab dependency
remains.
*/

with
    deficit_weights as (
        select
            id.person_id,
            upper(id.deficit) as deficit,
            id.other_instructions,
            id.has_deficit,
            id.sub_deficit,
            id.end_date,
            id.last_date,
            ew.detail_key,
            ew.weight
        from {{ ref("int_efi2_rules") }} id
        left join {{ ref("stg_aic_base_efi2_weights") }} ew
            on lower(id.deficit) = lower(ew.deficit)
        where weight is not null
    ),

    -- Annoying dementia rules
    cognitive as (
        select person_id, end_date, max(weight) as weight, 'MAX_DEMENTIA' as deficit
        from deficit_weights
        where
            deficit in ('MEMORY CONCERNS', 'COGNITIVE IMPAIRMENT', 'DEMENTIA')
            and has_deficit = true
        group by person_id, end_date
    ),

    max_dementia as (
        select
            c.person_id,
            c.deficit,
            dw.other_instructions,
            dw.has_deficit,
            dw.sub_deficit,
            dw.end_date,
            dw.last_date,
            dw.detail_key,
            c.weight
        from cognitive c
        left join
            deficit_weights dw
            on c.person_id = dw.person_id
            and c.end_date = dw.end_date
            and c.weight = dw.weight
        where
            c.deficit = 'MAX_DEMENTIA'
            and sub_deficit in (
                'Dementia', 'COGNITIVE_SCORE', 'COGNITIVE_IMPAIRMENT', 'DEMENTIA', 'MEMORY_CONCERNS'
            )
    ),

    -- Alcohol
    alcohol as (
        select person_id, end_date, max(last_date) as last_date, 'MAX_ALCOHOL' as deficit
        from deficit_weights
        where deficit = 'ALCOHOL' and has_deficit = true
        group by person_id, end_date
    ),

    max_alcohol as (
        select
            a.person_id,
            a.deficit,
            dw.other_instructions,
            dw.has_deficit,
            dw.sub_deficit,
            dw.end_date,
            dw.last_date,
            dw.detail_key,
            dw.weight
        from alcohol a
        left join
            deficit_weights dw
            on a.person_id = dw.person_id
            and a.end_date = dw.end_date
            and a.last_date = dw.last_date
        where a.deficit = 'MAX_ALCOHOL'
    ),

    -- Full scored population (one row per person), sourced from the patient_list
    -- spine, NOT int_efi2_cohort_snomed_codes. The cohort table is an inner join to
    -- the codelist, so it only contains persons with >= 1 eFI2 code. The paper
    -- assigns a "missing" lifestyle category (BMI, alcohol) so that patients with no
    -- lifestyle data still receive a score, and evaluates polypharmacy across the
    -- whole population. Drawing from the cohort table instead would deny the
    -- missing-lifestyle weights and polypharmacy to code-sparse persons, understating
    -- their score. Patient_list is already the correct denominator (living, age >= 65).
    person_unique as (
        select distinct person_id, end_date from {{ ref("int_efi2_patient_list") }}
    ),

    missing_bmi as (
        select
            pu.person_id,
            'BMI' as deficit,
            dw.other_instructions,
            true as has_deficit,
            dw.sub_deficit,
            pu.end_date,
            dw.last_date,
            'MISSING' as detail_key
        from person_unique pu
        left join
            (select * from deficit_weights where deficit = 'BMI') dw
            on pu.person_id = dw.person_id
            and pu.end_date = dw.end_date
        where deficit is null
    ),

    missing_bmi_score as (
        select
            person_id,
            mb.deficit,
            other_instructions,
            has_deficit,
            sub_deficit,
            end_date,
            end_date as last_date,
            mb.detail_key,
            ew.weight
        from missing_bmi mb
        left join
            {{ ref("stg_aic_base_efi2_weights") }} ew
            on lower(mb.deficit) = lower(ew.deficit)
            and mb.detail_key = ew.detail_key
    ),

    missing_alcohol as (
        select
            pu.person_id,
            'ALCOHOL' as deficit,
            dw.other_instructions,
            true as has_deficit,
            dw.sub_deficit,
            pu.end_date,
            dw.last_date,
            'MISSING' as detail_key
        from person_unique pu
        left join
            (select * from deficit_weights where deficit = 'ALCOHOL') dw
            on pu.person_id = dw.person_id
            and pu.end_date = dw.end_date
        where deficit is null
    ),

    missing_alcohol_score as (
        select
            person_id,
            ma.deficit,
            other_instructions,
            has_deficit,
            sub_deficit,
            end_date,
            end_date as last_date,
            ma.detail_key,
            ew.weight
        from missing_alcohol ma
        left join
            {{ ref("stg_aic_base_efi2_weights") }} ew
            on lower(ma.deficit) = lower(ew.deficit)
            and ma.detail_key = ew.detail_key
    ),

    -- Polypharmacy — count of distinct BNF sub-subchapters (level 3 - note not 
    -- real level so this is the agreed interpretation) among meds
    -- prescribed in the 90 days before end_date, per the eFI2 paper: "Number of
    -- medications from different BNF sub-subchapters (level 3) prescribed in
    -- previous 90 days". Level 3 = BNF paragraph = the first 6 characters of the
    -- BNF code (chars 1-2 chapter, 3-4 section, 5-6 paragraph) - the same
    -- definition as int_medication_order_bnf.bnf_paragraph.
    --
    -- NOTE - no code exclusions applied here (differs from this repo's
    -- int_polypharmacy_* pipeline). That pipeline scopes to NHSBSA in-scope BNF
    -- chapters via the bnf_polypharmacy_exclusions seed; the lds pipeline
    -- counts across ALL BNF sub-subchapters, so we deliberately do NOT apply those
    -- exclusions - every prescribed med with a BNF code counts. Applying
    -- the exclusion list as minimising divergence.
    relevant_meds as (
        select
            im.person_id,
            pu.end_date,
            left(im.bnf_code, 6) as bnf_sub_subchapter
        from {{ ref("stg_olids_medication_order") }} im
        left join person_unique pu on im.person_id = pu.person_id
        where
            im.bnf_code is not null
            and pu.end_date is not null
            and datediff('day', im.clinical_effective_date, pu.end_date) between 0 and 90
    ),

    polypharmacy_counts as (
        select
            person_id,
            end_date,
            count(distinct bnf_sub_subchapter) as sub_subchapter_count
        from relevant_meds
        group by person_id, end_date
    ),

    polypharmacy as (
        select
            person_id,
            end_date,
            'POLYPHARMACY' as deficit,
            case
                when sub_subchapter_count between 5 and 9 then '5-9'
                when sub_subchapter_count >= 10 then '10+'
            end as detail_key,
            -- detail_key is non-null exactly when sub_subchapter_count >= 5
            sub_subchapter_count >= 5 as has_deficit
        from polypharmacy_counts
    ),

    polypharmacy_score as (
        select
            p.person_id,
            p.deficit,
            'Polypharmacy: 0-4, 5-9, 10+ meds from different BNF Chapters' as otherinstructions,
            p.has_deficit,
            'POLYPHARMACY' as sub_deficit,
            p.end_date,
            p.end_date as last_date,
            p.detail_key,
            ew.weight
        from polypharmacy p
        left join
            {{ ref("stg_aic_base_efi2_weights") }} ew
            on lower(p.deficit) = lower(ew.deficit)
            and p.detail_key = ew.detail_key
    ),

    -- Now do smoking score - have to have most recent smoking status
    smoking as (
        select distinct person_id, end_date, deficit, last_date
        from deficit_weights
        where upper(deficit) in ('SMOKER (CURRENT)', 'SMOKER (EX)') and has_deficit = true
        qualify row_number() over (partition by person_id, end_date order by last_date desc) = 1
    ),

    smoking_score as (
        select
            s.person_id,
            s.deficit,
            'Cannot be current and ex smoker' as otherinstructions,
            case when s.deficit = 'SMOKER (CURRENT)' then true else false end as has_deficit,
            'SMOKING' as sub_deficit,
            s.end_date,
            s.last_date,
            null as detail_key,
            ew.weight
        from smoking s
        left join {{ ref("stg_aic_base_efi2_weights") }} ew on lower(s.deficit) = lower(ew.deficit)
    ),

    -- Now union all the scores
    all_scores as (
        select *
        from deficit_weights
        where
            upper(deficit) not in (
                'SMOKER (CURRENT)',
                'SMOKER (EX)',
                'MEMORY CONCERNS',
                'COGNITIVE IMPAIRMENT',
                'DEMENTIA',
                'ALCOHOL',
                'BMI'
            )
            and has_deficit = true

        union all

        {% for rule_cte in [
            "max_dementia",
            "max_alcohol",
            "missing_bmi_score",
            "missing_alcohol_score",
            "polypharmacy_score",
            "smoking_score",
        ] %}
            select * from {{ rule_cte }} where has_deficit = true {{ "union all" if not loop.last }}
        {% endfor %}
    ),
    -- as some of the rules are not properly handled upstream, a deficit that isn't explicitly
    -- handled can be added as many times as it is mentioned. One example of this is `Skin
    -- Ulcers`. To get around this we need to take an aggregated value for each person, deficit,
    -- end date, and weight.
    -- TODO: we could fix this in the future however should probably go hand in hand with a larger
    -- bit of work in refactoring the eFIv2 code in general
    aggregated_values as (
        select distinct person_id, deficit, end_date, detail_key, weight from all_scores
    )

select person_id, end_date, sum(weight) as efi
from aggregated_values
group by person_id, end_date
