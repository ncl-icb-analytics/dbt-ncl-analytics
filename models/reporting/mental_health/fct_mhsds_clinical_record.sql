with labelled as (
    select
        r.* exclude (
            source_coding_scheme_description, source_clinical_description,
            source_clinical_label_status, source_standardised_snomed_description
        )
        , case
            when r.coding_scheme_kind = 'diagnosis' then scheme.description
            else r.source_coding_scheme_description
        end as coding_scheme_description
        , coalesce(
            r.source_standardised_snomed_description, mapped.preferred_term
        ) as standardised_snomed_description
        , case
            when r.source_table = 'MHS202' then coalesce(
                r.source_clinical_description
                , iff(r.source_clinical_label_status = 'mapped_to_snomed',
                    r.source_standardised_snomed_description, null)
            )
            when r.coding_scheme_kind = 'diagnosis' and r.coding_scheme_code = '02'
                then icd.description
            when r.coding_scheme_kind = 'fixed_snomed' or r.coding_scheme_code = '06'
                then snomed.preferred_term
            when r.coding_scheme_kind = 'diagnosis' and r.coding_scheme_code in ('03', '04', '05')
                then mapped.preferred_term
        end as clinical_description
        , case
            when r.source_table = 'MHS202' then r.source_clinical_label_status
            when r.clinical_code is null then 'code_missing'
            when r.coding_scheme_kind = 'diagnosis' and r.coding_scheme_code = '07'
                then 'reference_not_available'
            when r.coding_scheme_kind = 'diagnosis' and r.coding_scheme_code in ('03', '04', '05')
                then iff(mapped.snomed_code is null, 'reference_not_available', 'mapped_to_snomed')
            when clinical_description is not null then 'labelled'
            else 'code_or_expression_unmatched'
        end as clinical_label_status
        , b.sk_patient_id
        , provider.organisation_name as provider_organisation_name
    from {{ ref('int_mhsds_clinical_record') }} as r
    left join {{ ref('mhsds_diagnosis_scheme') }} as scheme
        on r.coding_scheme_kind = 'diagnosis'
        and upper(trim(r.coding_scheme_code)) = scheme.code
    left join {{ ref('stg_dictionary_snomed_concept') }} as snomed
        on trim(r.clinical_code) = snomed.snomed_code
        and r.source_table <> 'MHS202'
        and (r.coding_scheme_kind = 'fixed_snomed' or r.coding_scheme_code = '06')
    left join {{ ref('stg_dictionary_dbo_diagnosis') }} as icd
        on {{ clean_icd10_code('upper(trim(r.clinical_code))') }} = upper(icd.code)
        and r.coding_scheme_kind = 'diagnosis'
        and r.coding_scheme_code = '02'
    left join {{ ref('stg_dictionary_snomed_concept') }} as mapped
        on trim(r.standardised_snomed_code) = mapped.snomed_code
        and r.source_table <> 'MHS202'
    left join {{ ref('stg_mhsds_bridging') }} as b
        on r.person_id = b.person_id
    left join {{ ref('int_mhsds_organisation') }} as provider
        on upper(r.provider_organisation_code) = upper(provider.organisation_code)
)

select
    {{ dbt_utils.generate_surrogate_key([
        'source_table', 'source_record_id', 'clinical_record_type'
    ]) }} as clinical_record_id
    , 'MHSDS' as source_dataset
    , *
    , clinical_at::date as clinical_date
    , iff(clinical_time_precision = 'date', null, clinical_at::time) as clinical_time
    , try_to_decimal(clinical_value, 38, 9) as clinical_value_numeric
    , case
        when clinical_value is null then 'value_missing'
        when clinical_value_numeric is null then 'not_numeric_or_out_of_range'
        when try_to_double(clinical_value) <> clinical_value_numeric::double
            then 'numeric_rounded'
        else 'numeric'
    end as clinical_value_parse_status
    , coalesce(clinical_at::date > reporting_period_end_date, false)
        as is_clinical_date_after_reporting_period
    , iff(
        source_table = 'MHS606'
        , coalesce(clinical_at::date < reporting_period_start_date, false)
        , null
    ) as is_assessment_before_reporting_period
from labelled
