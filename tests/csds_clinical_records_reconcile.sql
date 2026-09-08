with expected as (
    select 'immunisation' as clinical_record_type, cyp501_unique_id::varchar as source_row_id
    from {{ ref('int_csds_coded_immunisation') }}
    union all
    select 'referral_assessment', cyp609_unique_id::varchar
    from {{ ref('stg_csds_referral_assessment') }}
    union all
    select 'activity_assessment', cyp612_unique_id::varchar
    from {{ ref('stg_csds_activity_assessment') }}
    union all
    select 'procedure', source_row_id::varchar
    from {{ ref('fct_csds_care_activity') }}
    where coded_procedure_clinical_terminology is not null
    union all
    select 'finding', source_row_id::varchar
    from {{ ref('fct_csds_care_activity') }}
    where coded_finding_coded_clinical_entry is not null
    union all
    select 'observation', source_row_id::varchar
    from {{ ref('fct_csds_care_activity') }}
    where coded_observation_clinical_terminology is not null or observation_value is not null
)
select coalesce(e.clinical_record_type, a.clinical_record_type) as clinical_record_type,
    e.source_row_id as expected_source_row_id, a.source_row_id as actual_source_row_id,
    a.source_record_id
from expected as e
full outer join {{ ref('fct_csds_clinical_record') }} as a
    on e.clinical_record_type = a.clinical_record_type and e.source_row_id = a.source_row_id
where e.source_row_id is null or a.source_row_id is null
