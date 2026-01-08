{{
    config(
        materialized='view',
        tags=['qof', 'asof', 'reporting']
    )
}}

WITH register_data AS (
    {{ calculate_atrial_fibrillation_register(reference_date_expr=get_reference_date()) }}
)

SELECT
    person_id,
    register_name,
    is_on_register,
    {{ get_reference_date() }} AS reference_date
FROM register_data
