{{
    config(
        materialized='view'
    )
}}

/*
Medication orders enriched with BNF classification and prescription (statement) details.
View — not materialised. Can be materialised later if performance requires it.

Grain: one row per medication order (issue).

Joins:
- stg_reference_bnf_latest: SNOMED → BNF code mapping (96% coverage)
- stg_olids_medication_statement: prescription details (acute/repeat, active status, expiry)
- int_patient_person_unique: patient_id → person_id

BNF hierarchy exposed as:
- bnf_code: full BNF code (e.g. 0212000B0AAAAAA)
- bnf_chapter: 2-digit chapter (e.g. 02 = Cardiovascular)
- bnf_section: 4-digit section (e.g. 0212 = Lipid-Regulating Drugs)
- bnf_paragraph: 6-digit paragraph (e.g. 021200 = Lipid-Regulating Drugs)
- bnf_name: human-readable BNF product name
*/

SELECT
    -- Order identifiers
    mo.id AS medication_order_id,
    mo.medication_statement_id,
    pp.person_id,
    mo.patient_id,
    mo.organisation_id,
    mo.record_owner_organisation_code AS practice_code,

    -- Order details
    mo.clinical_effective_date AS order_date,
    mo.medication_name,
    mo.mapped_concept_code,
    mo.mapped_concept_display,
    mo.dose,
    mo.quantity_value,
    mo.quantity_unit,
    mo.duration_days,
    mo.estimated_cost,
    mo.age_at_event,

    -- BNF classification (from SNOMED → BNF reference mapping)
    bnf.bnf_code,
    bnf.bnf_name,
    LEFT(bnf.bnf_code, 2) AS bnf_chapter,
    LEFT(bnf.bnf_code, 4) AS bnf_section,
    LEFT(bnf.bnf_code, 6) AS bnf_paragraph,

    -- Prescription (statement) details
    -- authorisation_type_source_code is the clean classification: Acute, Repeat, Repeat Dispensing, Automatic
    ms.authorisation_type_source_code AS issue_type,
    ms.is_active AS prescription_is_active,
    ms.cancellation_date AS prescription_cancellation_date,
    ms.expiry_date AS prescription_expiry_date,

    -- Issue method (how the order was issued)
    mo.issue_method_description AS issue_method,

    -- Fiscal year for cost trending
    CASE
        WHEN MONTH(mo.clinical_effective_date) >= 4
        THEN YEAR(mo.clinical_effective_date)
        ELSE YEAR(mo.clinical_effective_date) - 1
    END AS fiscal_year_start

FROM {{ ref('stg_olids_medication_order') }} mo

-- Person mapping
INNER JOIN {{ ref('int_patient_person_unique') }} pp
    ON mo.patient_id = pp.patient_id

-- BNF classification via SNOMED code
LEFT JOIN {{ ref('stg_reference_bnf_latest') }} bnf
    ON mo.mapped_concept_code = bnf.snomed_code

-- Prescription details (statement = the prescription, order = each issue)
LEFT JOIN {{ ref('stg_olids_medication_statement') }} ms
    ON mo.medication_statement_id = ms.id

WHERE mo.clinical_effective_date IS NOT NULL
