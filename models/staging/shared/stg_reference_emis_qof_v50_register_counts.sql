with source as (
    select *
    from {{ ref('raw_reference_emis_qof_v50_register_counts') }}
    where reference_date = {{ qof_reference_date() }}
        and (
            indicator_description like '%on the%register%'
            or indicator_code = 'DEP1_REG'
            or indicator_code = 'HF1 Register'
        )
        and cdb is not null
),

mapped as (
    select
        reference_date,
        indicator_code,
        indicator_description,
        cdb as practice_code,
        organisation as practice_name,
        case
            when indicator_code = 'AF001' then 'Atrial Fibrillation'
            when indicator_code = 'AST005' then 'Asthma'
            when indicator_code = 'CAN001' then 'Cancer'
            when indicator_code = 'CHD001' then 'CHD'
            when indicator_code = 'CKD005' then 'CKD'
            when indicator_code = 'COPD015' then 'COPD'
            when indicator_code = 'DEM001' then 'Dementia'
            when indicator_code = 'DEP1_REG' then 'Depression'
            when indicator_code = 'DM017' then 'Diabetes'
            when indicator_code = 'EP001' then 'Epilepsy'
            when indicator_code = 'HF1 Register' then 'Heart Failure'
            when indicator_code = 'HYP001' then 'Hypertension'
            when indicator_code = 'LD005' then 'Learning Disability'
            when indicator_code = 'MH001' then 'SMI'
            when indicator_code = 'OST004' then 'Osteoporosis'
            when indicator_code = 'PAD001' then 'PAD'
            when indicator_code = 'PC001' then 'Palliative Care'
            when indicator_code = 'RA001' then 'Rheumatoid Arthritis'
            when indicator_code = 'STIA001' then 'Stroke/TIA'
        end as register_name,
        population_count,
        parent_population,
        percentage,
        males,
        females,
        excluded,
        status
    from source
)

select * from mapped
where register_name is not null
