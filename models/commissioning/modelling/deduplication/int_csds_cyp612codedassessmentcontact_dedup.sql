{{
    config(materialized = 'table')
}}

{{
    deduplicate_csds(
        csds_table = 'dev__modelling.dbt_staging.stg_csds_cyp612codedassessmentcontact',
        partition_cols = ['coded_assessment_tool_type_snomed_ct', 'person_score']
    )
}}