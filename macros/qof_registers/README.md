# QOF Register Calculation Macros

Macros for calculating QOF disease register status at a given reference date.

## Pattern

Each macro:
1. Accepts `reference_date_expr` parameter (defaults to `CURRENT_DATE()`)
2. Assumes `active_registrations` CTE exists with (person_id, practice_code)
3. Filters diagnosis/observation data to reference date
4. Applies condition-specific business rules
5. Returns (person_id, practice_code, register_name, is_on_register)

## Example Structure

{% raw %}
```sql
{% macro calculate_{condition}_register(reference_date_expr='CURRENT_DATE()') %}
    {#
    Calculates {Condition} register status at a given reference date.

    Business Logic:
    - [List criteria]

    Parameters:
        reference_date_expr: SQL expression for reference date (default: CURRENT_DATE())

    Returns: CTE with person_id, practice_code, register_name, is_on_register

    Assumes: active_registrations CTE exists with (person_id, practice_code)
    #}

    WITH {condition}_diagnoses_filtered AS (
        SELECT person_id, ...
        FROM {{ ref('int_{condition}_diagnoses_all') }}
        WHERE clinical_effective_date <= {{ reference_date_expr }}
    ),

    -- Aggregate, apply business rules

    {condition}_register_logic AS (
        SELECT
            ar.person_id,
            ar.practice_code,
            '{Condition Display Name}' AS register_name,
            COALESCE(..., FALSE) AS is_on_register
        FROM active_registrations ar
        LEFT JOIN ...
    )

    SELECT person_id, practice_code, register_name, is_on_register
    FROM {condition}_register_logic

{% endmacro %}
```
{% endraw %}

## Register Types

### Simple (Diagnosis Only, Lifelong)
- CHD, Cancer, Stroke/TIA, PAD, Heart Failure, Atrial Fibrillation, Palliative Care
- Logic: Presence of diagnosis = on register
- No resolution codes or age restrictions

### Age Restricted
- Diabetes (≥17), Asthma (≥6), CKD (≥18), Depression (≥18), Epilepsy (≥18), Rheumatoid Arthritis (≥16)
- Hypertension (≤79) - upper age limit
- Logic: Age threshold + active diagnosis

### External Validation Required
- Asthma (requires medication in last 12 months)
- COPD (complex spirometry rules)
- Logic: Diagnosis + supporting data

### Complex Business Rules
- COPD (Rules 1-4 with date bifurcation)
- Diabetes (Type classification)
- Obesity (BMI-based, not diagnosis codes)
