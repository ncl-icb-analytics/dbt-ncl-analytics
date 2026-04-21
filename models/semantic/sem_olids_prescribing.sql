{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS Prescribing Semantic View
    ================================

    Medication order-level semantic model for prescribing analysis. OLIDS is
    the One London Integrated Data Set — primary care data from system suppliers
    (currently EMIS Web, with TPP to follow), unified by the One London team.

    Grain: One row per medication order (issue)

    Core table (int_medication_order_bnf) pre-joins:
    - SNOMED → BNF code mapping (96% coverage) for chapter/section filtering
    - Medication statement for prescription type (acute/repeat) and active status

    BNF hierarchy: bnf_chapter (2-digit) → bnf_section (4-digit) → bnf_paragraph (6-digit) → bnf_code (full).
    The chatbot has a BNF lookup tool — ask it to resolve drug class names to BNF codes.

    Pre-defined medication sets (joined via medication_order_id):
    - Statins: with intensity classification (high/moderate/combination)
    - Antihypertensives, ACE inhibitors, ARBs, beta-blockers
    - Anticoagulants: with DOAC/VKA classification
    - Antiplatelets
    - Antipsychotics, antidepressants, lithium, epilepsy drugs
    - Inhaled corticosteroids (ICS)
    - Valproate: with product type, indication, dose category (clinical safety)
    - Diabetes medications
    - Respiratory: asthma meds, inhaled corticosteroids
    - NSAIDs, PPIs, systemic corticosteroids
    - Antibacterials

    Use Cases:
    - Prescribing volume/cost by BNF chapter, practice, PCN
    - Statin prescribing rates and intensity mix
    - Antibiotic prescribing patterns
    - Valproate safety monitoring (pregnancy risk)
    - Repeat vs acute prescribing patterns
    - Cost per patient by therapeutic area
    - Equity: prescribing by deprivation/ethnicity
#}

TABLES(
    rx AS {{ ref('int_medication_order_bnf') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'All medication orders enriched with BNF classification and prescription type. bnf_chapter is the top-level filter (e.g. 02=Cardiovascular, 04=CNS, 06=Endocrine). The chatbot has a BNF lookup tool to resolve drug names to codes.',

    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Patient demographics, geography, ethnicity, deprivation (current snapshot)',

    conditions AS {{ ref('dim_person_conditions') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Long-term condition flags and diabetes type',

    status AS {{ ref('dim_person_status_summary') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Polypharmacy, smoking, vulnerability status',

    practice AS {{ ref('dim_practice') }}
        PRIMARY KEY (practice_code)
        COMMENT = 'Practice details for the prescribing practice',

    -- Pre-defined medication category models (each filtered to specific drug class)
    statins AS {{ ref('int_statin_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Statin orders (BNF 2.12) with intensity classification: HIGH_INTENSITY (atorvastatin, rosuvastatin), MODERATE_INTENSITY (simvastatin, pravastatin, fluvastatin), COMBINATION (statin+ezetimibe)',

    antihypertensives AS {{ ref('int_antihypertensive_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Antihypertensive medication orders (SNOMED cluster: ANTIHYPERTENSIVE_MEDICATIONS)',

    anticoagulants AS {{ ref('int_anticoagulant_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Anticoagulant orders with DOAC/VKA classification',

    antiplatelets AS {{ ref('int_antiplatelet_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Antiplatelet medication orders',

    antipsychotics AS {{ ref('int_antipsychotic_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Antipsychotic medication orders (BNF 4.2, excludes lithium)',

    antidepressants AS {{ ref('int_antidepressant_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Antidepressant medication orders',

    valproate AS {{ ref('int_valproate_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Valproate orders with product type (sodium valproate, valproic acid), indication (anti-epileptic, mood stabiliser), dose category, and teratogenic risk flag',

    diabetes_meds AS {{ ref('int_diabetes_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Diabetes medication orders',

    antibacterials AS {{ ref('int_antibacterial_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Antibacterial medication orders with class classification (penicillins, cephalosporins, macrolides, etc.)',

    ace_inhibitors AS {{ ref('int_ace_inhibitor_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'ACE inhibitor orders (BNF 2.5.5.1). Cornerstone of HTN, HF, CKD management.',

    arbs AS {{ ref('int_arb_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'ARB (angiotensin II receptor blocker) orders (BNF 2.5.5.2). Alternative to ACE inhibitors.',

    beta_blockers AS {{ ref('int_beta_blocker_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Beta-blocker orders. Used for AF rate control, HF, post-MI, hypertension.',

    lithium AS {{ ref('int_lithium_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Lithium orders (BNF 4.2.3). Requires therapeutic drug monitoring. Small cohort, high safety relevance.',

    ics AS {{ ref('int_inhaled_corticosteroid_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Inhaled corticosteroid (ICS) orders (BNF 3.2). Cornerstone of asthma/COPD preventer therapy.',

    epilepsy_meds AS {{ ref('int_epilepsy_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Anti-epileptic drug orders. Some also used as mood stabilisers (SMI crossover).',

    nsaids AS {{ ref('int_nsaid_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'NSAID medication orders (BNF 10.1)',

    ppis AS {{ ref('int_ppi_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Proton pump inhibitor orders (BNF 1.3.5)'
)

RELATIONSHIPS(
    rx (person_id) REFERENCES demographics,
    rx (person_id) REFERENCES conditions,
    rx (person_id) REFERENCES status,
    rx (practice_code) REFERENCES practice (practice_code),
    statins (medication_order_id) REFERENCES rx,
    antihypertensives (medication_order_id) REFERENCES rx,
    anticoagulants (medication_order_id) REFERENCES rx,
    antiplatelets (medication_order_id) REFERENCES rx,
    antipsychotics (medication_order_id) REFERENCES rx,
    antidepressants (medication_order_id) REFERENCES rx,
    valproate (medication_order_id) REFERENCES rx,
    diabetes_meds (medication_order_id) REFERENCES rx,
    antibacterials (medication_order_id) REFERENCES rx,
    ace_inhibitors (medication_order_id) REFERENCES rx,
    arbs (medication_order_id) REFERENCES rx,
    beta_blockers (medication_order_id) REFERENCES rx,
    lithium (medication_order_id) REFERENCES rx,
    ics (medication_order_id) REFERENCES rx,
    epilepsy_meds (medication_order_id) REFERENCES rx,
    nsaids (medication_order_id) REFERENCES rx,
    ppis (medication_order_id) REFERENCES rx
)

FACTS(
    -- Order details
    rx.estimated_cost AS estimated_cost COMMENT = 'Estimated cost of this medication order (GBP). Populated for ~96% of orders.',
    rx.quantity_value AS quantity_value COMMENT = 'Quantity prescribed',
    rx.duration_days AS duration_days COMMENT = 'Duration of prescription in days',
    rx.age_at_event AS age_at_event COMMENT = 'Patient age at time of order',
    conditions.total_conditions AS total_conditions COMMENT = 'Patient total active long-term conditions',
    status.medication_count AS medication_count COMMENT = 'Patient current repeat medication count (from polypharmacy model)'
)

DIMENSIONS(
    -- Order time
    rx.order_date AS order_date WITH SYNONYMS = ('prescription date', 'date') COMMENT = 'Date the medication was issued',
    rx.fiscal_year_start AS fiscal_year_start COMMENT = 'UK fiscal year start (Apr-Mar). Use for annual cost comparisons.',

    -- BNF classification (use chatbot BNF tool to resolve drug names to codes)
    rx.bnf_chapter AS bnf_chapter WITH SYNONYMS = ('BNF chapter', 'therapeutic area') COMMENT = 'BNF chapter (2-digit). Key chapters: 01=GI, 02=Cardiovascular, 03=Respiratory, 04=CNS, 05=Infections, 06=Endocrine, 07=Obstetrics/Gynae/UTI, 08=Malignancy, 09=Nutrition/Blood, 10=Musculoskeletal, 11=Eye, 12=ENT, 13=Skin, 14=Vaccines',
    rx.bnf_section AS bnf_section COMMENT = 'BNF section (4-digit, e.g. 0212=Lipid-Regulating Drugs)',
    rx.bnf_paragraph AS bnf_paragraph COMMENT = 'BNF paragraph (6-digit)',
    rx.bnf_code AS bnf_code COMMENT = 'Full BNF product code (e.g. 0212000B0AAAAAA). 15-character format: chapter(2) + section(2) + paragraph(2) + chemical(3) + product(4) + formulation(2).',
    rx.bnf_name AS bnf_name COMMENT = 'BNF product name',
    rx.medication_name AS medication_name COMMENT = 'Medication name as recorded',
    rx.dose AS dose COMMENT = 'Dose as recorded',

    -- Prescription type (from statement)
    rx.issue_type AS issue_type WITH SYNONYMS = ('acute', 'repeat', 'prescription type') COMMENT = 'Prescription type (Acute, Repeat, Repeat Dispensing, Automatic). Acute = one-off; Repeat = ongoing repeat prescription; Repeat Dispensing = pharmacy-managed repeat; Automatic = auto-issued.',
    rx.prescription_is_active AS prescription_is_active COMMENT = 'Whether the parent prescription (statement) is currently active (TRUE/FALSE)',
    rx.issue_method AS issue_method COMMENT = 'How the order was issued (Electronic, Print, Outside Other, Handwritten, Outside Hospital, Outside Out Of Hours, Automatic, Over The Counter)',

    -- Prescribing practice
    rx.practice_code AS practice_code WITH SYNONYMS = ('practice code', 'ODS code') COMMENT = 'ODS code of the prescribing practice',
    practice.practice_name AS practice_name COMMENT = 'Name of the prescribing practice',
    practice.pcn_code AS pcn_code COMMENT = 'PCN code of the prescribing practice',
    practice.pcn_name AS pcn_name COMMENT = 'PCN name of the prescribing practice',
    practice.pcn_name_with_borough AS pcn_name_with_borough COMMENT = 'PCN with borough prefix',
    practice.borough_registered AS borough_registered COMMENT = 'Borough of the prescribing practice',
    practice.sub_icb_code AS sub_icb_code COMMENT = 'Sub-ICB / place-based partnership ODS code of the prescribing practice: QMJ = NHS North Central London (Camden, Islington, Barnet, Enfield, Haringey); QRV = NHS North West London (Brent, Ealing, Hammersmith and Fulham, Harrow, Hillingdon, Hounslow, Kensington and Chelsea, Westminster). NULL outside the WNL footprint.',
    practice.sub_icb_name AS sub_icb_name COMMENT = 'Sub-ICB display name (NHS North Central London or NHS North West London) of the prescribing practice. NULL outside the WNL footprint.',

    -- Patient demographics (current snapshot)
    demographics.gender AS gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    demographics.age_band_5y AS age_band_5y COMMENT = 'Current 5-year age band (drifts — use age_at_event for historical cohorting)',
    demographics.age_band_10y AS age_band_10y COMMENT = 'Current 10-year age band (drifts)',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'Current NHS standard age band (drifts)',
    demographics.age_band_esp AS age_band_esp COMMENT = 'Current ESP 2013 age band (drifts — use age_at_event for historical)',
    demographics.age_life_stage AS age_life_stage COMMENT = 'Life stage (Infant, Toddler, Child, Adolescent, Young Adult, Adult, Older Adult, Elderly, Very Elderly, Unknown)',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category (Asian or Asian British, Black or Black British, Mixed, Other, White, Unknown)',
    demographics.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory (White: British, White: Irish, White: Roma, White: Traveller, White: Other White, Mixed: White and Black Caribbean, Mixed: White and Black African, Mixed: White and Asian, Mixed: Other Mixed, Asian: Indian, Asian: Pakistani, Asian: Bangladeshi, Asian: Chinese, Asian: Other Asian, Black: African, Black: Caribbean, Black: Other Black, Other: Arab, Other: Other, Unknown, Not Stated, Not Recorded, Recorded Not Known, Refused)',
    demographics.main_language AS main_language COMMENT = 'Main spoken language (Not Recorded if unknown)',
    demographics.is_active AS is_active COMMENT = 'Patient currently registered',

    -- Patient geography (residence)
    demographics.lsoa_code_21 AS lsoa_code_21 COMMENT = 'Patient LSOA 2021 code (residence-based)',
    demographics.ward_code AS ward_code COMMENT = 'Patient electoral ward 2025 code (residence-based)',
    demographics.ward_name AS ward_name COMMENT = 'Patient electoral ward 2025 name (residence-based)',
    demographics.borough_resident AS borough_resident COMMENT = 'Patient borough of residence',
    demographics.is_london_resident AS is_london_resident COMMENT = 'Patient resides in Greater London',
    demographics.neighbourhood_resident AS neighbourhood_resident COMMENT = 'Patient NCL neighbourhood of residence',

    -- Deprivation (patient residence)
    demographics.imd_decile_19 AS imd_decile_19 COMMENT = 'IMD 2019 decile (1=most deprived, 10=least). NULL if LSOA not mapped.',
    demographics.imd_quintile_19 AS imd_quintile_19 COMMENT = 'IMD 2019 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)',
    demographics.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived, 10=least). Preferred over 2019.',
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)',

    -- Multimorbidity
    conditions.total_qof_conditions AS total_qof_conditions COMMENT = 'Number of QOF-registered conditions',
    conditions.cardiovascular_conditions AS cardiovascular_conditions COMMENT = 'Count of cardiovascular conditions (AF, CHD, HF, HTN, PAD, Stroke/TIA)',
    conditions.respiratory_conditions AS respiratory_conditions COMMENT = 'Count of respiratory conditions (Asthma, COPD)',
    conditions.mental_health_conditions AS mental_health_conditions COMMENT = 'Count of mental health conditions (Depression, SMI, Dementia, Anxiety)',
    conditions.metabolic_conditions AS metabolic_conditions COMMENT = 'Count of metabolic conditions (Diabetes, NDH, CKD, Obesity)',

    -- Key conditions (for prescribing-condition analysis)
    conditions.diabetes_type AS diabetes_type COMMENT = 'Diabetes type (Type 1, Type 2, Unknown, Not Diabetic)',
    conditions.has_diabetes AS has_diabetes WITH SYNONYMS = ('DM') COMMENT = 'On diabetes register',
    conditions.has_hypertension AS has_hypertension WITH SYNONYMS = ('HTN') COMMENT = 'On hypertension register',
    conditions.has_coronary_heart_disease AS has_coronary_heart_disease WITH SYNONYMS = ('CHD') COMMENT = 'On CHD register',
    conditions.has_atrial_fibrillation AS has_atrial_fibrillation WITH SYNONYMS = ('AF') COMMENT = 'On AF register',
    conditions.has_heart_failure AS has_heart_failure WITH SYNONYMS = ('HF') COMMENT = 'On HF register',
    conditions.has_stroke_tia AS has_stroke_tia WITH SYNONYMS = ('stroke', 'TIA') COMMENT = 'On stroke/TIA register',
    conditions.has_peripheral_arterial_disease AS has_peripheral_arterial_disease WITH SYNONYMS = ('PAD') COMMENT = 'On PAD register',
    conditions.has_copd AS has_copd COMMENT = 'On COPD register',
    conditions.has_asthma AS has_asthma COMMENT = 'On asthma register',
    conditions.has_depression AS has_depression COMMENT = 'On depression register',
    conditions.has_severe_mental_illness AS has_severe_mental_illness WITH SYNONYMS = ('SMI') COMMENT = 'On SMI register',
    conditions.has_anxiety AS has_anxiety COMMENT = 'On anxiety register',
    conditions.has_dementia AS has_dementia COMMENT = 'On dementia register',
    conditions.has_epilepsy AS has_epilepsy COMMENT = 'On epilepsy register',
    conditions.has_chronic_kidney_disease AS has_chronic_kidney_disease WITH SYNONYMS = ('CKD') COMMENT = 'On CKD register',
    conditions.has_non_diabetic_hyperglycaemia AS has_non_diabetic_hyperglycaemia WITH SYNONYMS = ('NDH', 'prediabetes') COMMENT = 'Non-diabetic hyperglycaemia',
    conditions.has_obesity AS has_obesity COMMENT = 'Recorded obesity',
    conditions.has_cancer AS has_cancer COMMENT = 'On cancer register',
    conditions.has_frailty AS has_frailty COMMENT = 'Recorded frailty',
    conditions.has_learning_disability AS has_learning_disability WITH SYNONYMS = ('LD') COMMENT = 'On LD register',
    conditions.has_palliative_care AS has_palliative_care COMMENT = 'On palliative care register',

    -- Polypharmacy and risk behaviours (from dim_person_status_summary)
    status.is_polypharmacy_5plus AS is_polypharmacy_5plus COMMENT = 'Has 5+ current repeat medications',
    status.is_polypharmacy_10plus AS is_polypharmacy_10plus COMMENT = 'Has 10+ current repeat medications (severe polypharmacy)',
    status.medication_count_band AS medication_count_band COMMENT = 'Medication count band (0, 1-4, 5-9, 10-14, 15+)',
    status.smoking_status AS smoking_status COMMENT = 'Smoking status (Never Smoked, Ex-Smoker, Current Smoker, Unknown)',
    status.is_care_home_resident AS is_care_home_resident COMMENT = 'Residing in care home',

    -- Statin classification (only populated for statin orders)
    statins.statin_intensity AS statin_intensity COMMENT = 'Statin intensity (HIGH_INTENSITY, MODERATE_INTENSITY, COMBINATION, OTHER_STATIN). Only for statin orders.',
    statins.is_combination_therapy AS is_combination_therapy COMMENT = 'Statin + ezetimibe combination. Only for statin orders.',

    -- Anticoagulant classification (only populated for anticoagulant orders)
    anticoagulants.anticoagulant_type AS anticoagulant_type COMMENT = 'Anticoagulant type (DOAC, VKA, Other). Only for anticoagulant orders.',
    anticoagulants.is_doac AS is_doac COMMENT = 'Direct oral anticoagulant. Only for anticoagulant orders.',

    -- Valproate classification (only populated for valproate orders — clinical safety)
    valproate.valproate_product_type AS valproate_product_type COMMENT = 'Valproate type (SODIUM_VALPROATE, VALPROIC_ACID, EPILIM, DEPAKOTE). Only for valproate orders.',
    valproate.clinical_indication AS clinical_indication COMMENT = 'Valproate indication (ANTI_EPILEPTIC, MOOD_STABILISER). Only for valproate orders.',
    valproate.dose_category AS dose_category COMMENT = 'Valproate dose category (LOW, MODERATE, HIGH, UNKNOWN). Only for valproate orders.'
)

METRICS(
    -- Volume
    rx.order_count AS COUNT(rx.medication_order_id) COMMENT = 'Total medication orders (issues)',
    rx.patient_count AS COUNT(DISTINCT rx.person_id) COMMENT = 'Distinct patients',

    -- Cost
    rx.total_cost AS SUM(rx.estimated_cost) COMMENT = 'Total estimated prescribing cost (GBP)',
    rx.avg_cost_per_order AS AVG(rx.estimated_cost) COMMENT = 'Average cost per order (GBP)',

    -- Prescription type
    rx.repeat_order_count AS COUNT(CASE WHEN rx.issue_type IN ('Repeat', 'Repeat Dispensing') THEN rx.medication_order_id END) COMMENT = 'Repeat prescription orders (includes Repeat Dispensing)',
    rx.acute_order_count AS COUNT(CASE WHEN rx.issue_type = 'Acute' THEN rx.medication_order_id END) COMMENT = 'Acute (one-off) prescription orders',

    -- Category counts (non-null when order is in that category)
    statins.statin_order_count AS COUNT(statins.medication_order_id) COMMENT = 'Statin orders',
    statins.statin_patient_count AS COUNT(DISTINCT statins.person_id) COMMENT = 'Patients with statin orders',
    antihypertensives.antihypertensive_order_count AS COUNT(antihypertensives.medication_order_id) COMMENT = 'Antihypertensive orders',
    anticoagulants.anticoagulant_order_count AS COUNT(anticoagulants.medication_order_id) COMMENT = 'Anticoagulant orders',
    antipsychotics.antipsychotic_order_count AS COUNT(antipsychotics.medication_order_id) COMMENT = 'Antipsychotic orders',
    antidepressants.antidepressant_order_count AS COUNT(antidepressants.medication_order_id) COMMENT = 'Antidepressant orders',
    antibacterials.antibacterial_order_count AS COUNT(antibacterials.medication_order_id) COMMENT = 'Antibacterial orders',
    diabetes_meds.diabetes_med_order_count AS COUNT(diabetes_meds.medication_order_id) COMMENT = 'Diabetes medication orders',
    ace_inhibitors.ace_inhibitor_order_count AS COUNT(ace_inhibitors.medication_order_id) COMMENT = 'ACE inhibitor orders',
    arbs.arb_order_count AS COUNT(arbs.medication_order_id) COMMENT = 'ARB orders',
    beta_blockers.beta_blocker_order_count AS COUNT(beta_blockers.medication_order_id) COMMENT = 'Beta-blocker orders',
    lithium.lithium_order_count AS COUNT(lithium.medication_order_id) COMMENT = 'Lithium orders',
    ics.ics_order_count AS COUNT(ics.medication_order_id) COMMENT = 'Inhaled corticosteroid orders',
    epilepsy_meds.epilepsy_med_order_count AS COUNT(epilepsy_meds.medication_order_id) COMMENT = 'Anti-epileptic drug orders',
    nsaids.nsaid_order_count AS COUNT(nsaids.medication_order_id) COMMENT = 'NSAID orders',
    ppis.ppi_order_count AS COUNT(ppis.medication_order_id) COMMENT = 'PPI orders',
    valproate.valproate_order_count AS COUNT(valproate.medication_order_id) COMMENT = 'Valproate orders'
)

COMMENT = 'OLIDS Prescribing Semantic View - All medication orders with BNF classification, prescription type, practice attribution, demographics, conditions, and pre-defined drug category flags. Source: OLIDS (One London Integrated Data Set). Grain: one row per medication order. BNF chapter is the primary therapeutic filter — the chatbot has a BNF lookup tool to resolve drug class names.'
AI_SQL_GENERATION 'BNF CODE FORMAT: bnf_chapter is a 2-digit compact code (e.g. 02 = Cardiovascular), bnf_section is 4-digit (e.g. 0205 = Hypertension and heart failure), bnf_paragraph is 6-digit, bnf_code is the full 15-character product code (e.g. 0212000B0AAAAAA). These are compact codes NOT dotted codes — use the BNF lookup tool to resolve drug class names to the correct prefix. For top-level breakdowns, GROUP BY bnf_chapter. For specific drug classes, WHERE bnf_section = tool_result or WHERE bnf_code LIKE tool_result || ''%''. Pre-defined category tables (statins, antibacterials, etc.) are joined — their metrics count only orders in that category; use these for known drug classes rather than BNF filtering. For statin intensity, use statin_intensity. For anticoagulant DOAC/VKA split, use anticoagulant_type. For valproate safety, use valproate_product_type and valproate_indication. Cost: SUM(estimated_cost) for total prescribing cost (~96% populated). issue_type: Repeat, Acute, Repeat Dispensing, Automatic. Use fiscal_year_start for annual trends. Patient demographics are current snapshot — use age_at_event for historical age cohorting.'
AI_QUESTION_CATEGORIZATION 'Use this view for: prescribing volume and cost by BNF chapter/practice/PCN, statin prescribing rates and intensity, antibiotic stewardship, antipsychotic/antidepressant prescribing, valproate safety monitoring, repeat vs acute prescribing, cost per patient by therapeutic area, prescribing equity by deprivation/ethnicity, and any medication-related questions. For current population health (conditions, demographics) without prescribing use sem_olids_population. For clinical biomarkers use sem_olids_observations.'
