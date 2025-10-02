{{
    config(materialized = 'table')
}}

{{
    deduplicate_csds(
        csds_table = 'dev__modelling.dbt_staging.stg_csds_cyp609codedassessmentreferral',
        partition_cols = ['coded_assessment_tool_type_snomed_ct', 'person_score', 'assessment_tool_completion_date']
    )
}}
