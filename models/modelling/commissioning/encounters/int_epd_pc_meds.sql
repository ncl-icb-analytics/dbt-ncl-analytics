SELECT 
    m.age_bands
    , c.chapter_name as nbf_chapter_name
    , CASE WHEN LENGTH(c.chapter_path) - LENGTH(REPLACE(c.chapter_path, '>', '')) >= 0 
           THEN SUBSTR(c.chapter_path, 1, POSITION('>' IN c.chapter_path || '>') - 1)
           ELSE NULL 
      END AS bnf_chapter_level1
    , CASE WHEN LENGTH(c.chapter_path) - LENGTH(REPLACE(c.chapter_path, '>', '')) >= 1 
           THEN SUBSTR(c.chapter_path, POSITION('>' IN c.chapter_path) + 2, 
                       POSITION('>' IN SUBSTR(c.chapter_path || '>', POSITION('>' IN c.chapter_path) + 1)) 
                       - POSITION('>' IN c.chapter_path) - 2)
           ELSE NULL 
      END AS bnf_chapter_level2
    , CASE WHEN LENGTH(c.chapter_path) - LENGTH(REPLACE(c.chapter_path, '>', '')) >= 2 
           THEN SUBSTR(c.chapter_path, 
                       POSITION('>' IN SUBSTR(c.chapter_path || '>', POSITION('>' IN c.chapter_path) + 1)) + 2, 
                       POSITION('>' IN SUBSTR(c.chapter_path || '>', 
                       POSITION('>' IN SUBSTR(c.chapter_path || '>', POSITION('>' IN c.chapter_path) + 1)) + 1)) 
                       - POSITION('>' IN SUBSTR(c.chapter_path || '>', POSITION('>' IN c.chapter_path) + 1)) - 2)
           ELSE NULL 
      END AS bnf_chapter_level3
    , CASE WHEN LENGTH(c.chapter_path) - LENGTH(REPLACE(c.chapter_path, '>', '')) >= 3 
           THEN SUBSTR(c.chapter_path, 
                       POSITION('>' IN SUBSTR(c.chapter_path || '>', 
                       POSITION('>' IN SUBSTR(c.chapter_path || '>', POSITION('>' IN "Chapter_Path") + 1)) + 1)) + 2, 
                       LENGTH(c.chapter_path) - 
                       POSITION('>' IN SUBSTR(c.chapter_path || '>', 
                       POSITION('>' IN SUBSTR(c.chapter_path || '>', POSITION('>' IN "Chapter_Path") + 1)) + 1)))
           ELSE NULL 
      END AS bnf_chapter_level4
    , m.cost_centre_ods_code
    , CASE 
        WHEN m.cost_centre_ods_code = '-' THEN 'Does not apply'
        WHEN m.cost_centre_ods_code = '01' THEN 'Walk-in-Centre'
        WHEN m.cost_centre_ods_code = '02' THEN 'Out-of-hours service'
        WHEN m.cost_centre_ods_code = '03' THEN 'Walk-in Centre and Out-of-hours service'
        WHEN m.cost_centre_ods_code = '04' THEN 'GP Practice'
        WHEN m.cost_centre_ods_code = '05' THEN 'Health & Justice'
        WHEN m.cost_centre_ods_code = '06' THEN 'Private Controlled Drug practice'
        WHEN m.cost_centre_ods_code = '07' THEN 'Other'
        WHEN m.cost_centre_ods_code = '08' THEN 'Public health service'
        WHEN m.cost_centre_ods_code = '09' THEN 'Community health service'
        WHEN m.cost_centre_ods_code = '10' THEN 'Hospital service'
        WHEN m.cost_centre_ods_code = '11' THEN 'Optometry service'
        WHEN m.cost_centre_ods_code = '12' THEN 'Urgent & emergency care'
        WHEN m.cost_centre_ods_code = '13' THEN 'Hospice'
        WHEN m.cost_centre_ods_code = '14' THEN 'Care home/nursing home'
        WHEN m.cost_centre_ods_code = '15' THEN 'PCN'
        ELSE 'Unknown'
      END AS "CostCentreSubType"
    , m.cost_centre_sub_type
    , m.dispensed_pharmacy_lsoa
    , m.dispensed_pharmacy_ods_code
    , m.dispensed_pharmacy_type
    , m.exemption_code
    , CASE 
        WHEN m.exemption_code = '-' THEN 'Unknown'
        WHEN m.exemption_code = 'A' THEN 'Exempt - Under 16 / Aged 60 Or Over'
        WHEN m.exemption_code = 'B' THEN 'Exempt - Aged 16-18 And In Full Time Education'
        WHEN m.exemption_code = 'C' THEN 'Exempt - Aged 60 Or Over'
        WHEN m.exemption_code = 'D' THEN 'Exempt - Maternity Exemption'
        WHEN m.exemption_code = 'E' THEN 'Exempt - Medical Exemption'
        WHEN m.exemption_code = 'F' THEN 'Exempt - Pre-Payment Certificate'
        WHEN m.exemption_code = 'G' THEN 'Exempt - War/MOD pensioner exemption'
        WHEN m.exemption_code = 'H' THEN 'Exempt - Income Support'
        WHEN m.exemption_code = 'K' THEN 'Exempt - Income Based Job-seekers Allowance'
        WHEN m.exemption_code = 'L' THEN 'Exempt - HC2 Charges'
        WHEN m.exemption_code = 'M' THEN 'Exempt - NHS Tax Credit Exemption Certificate'
        WHEN m.exemption_code = 'S' THEN 'Exempt - Pension Guarantee Credit'
        WHEN m.exemption_code = 'U' THEN 'Exempt - Universal Credit'
        WHEN m.exemption_code = 'W' THEN 'Exempt - HRT Pre-payment Certificate'
        WHEN m.exemption_code = 'X' THEN 'Exempt - Non-Chargeable Contraceptive'
        WHEN m.exemption_code = 'Y' THEN 'Exempt - Sexual Health'
        WHEN m.exemption_code = 'Z' THEN 'Exempt - No Declaration/Declaration Not Specific'
        ELSE 'Unknown Exemption Code'
      END AS exemption_description
    , m.item_actual_cost / 100 as item_actual_cost --NB: raw cost is in penc
    , m.item_count
    , m.item_id
    , m.item_nic / 100 as item_nic
    , m.maternity_exemption_flag
    , m.dmic_pseudo_nhs_number as sk_patient_id
    , m.not_dispensed_indicator
    , m.paid_bnf_code
    , m.paid_bnf_name
    , m.paid_drug_strength
    , m.paid_quantity
    , m.paid_supplier_name
    , m.patient_age
    , m.patient_gpods
    , m.patient_gender
    , m.patient_la
    , m.patient_lsoa
    , m.prescriber_id
    , m.prescriber_type
    , m.private_prescription_indicator
    , m.processing_period_date
FROM 
    {{ ref('stg_epd_pc_meds') }} AS m
LEFT JOIN 
    {{ ref('stg_dictionary_dbo_bnf_substance_product_presentation') }} AS bnf
    ON m."PaidBNFCode" = bnf.code
LEFT JOIN 
    {{ ref('stg_dictionary_dbo_bnf_chapter') }} AS c
    ON bnf.sk_bnf_chapter_id = c.sk_bnf_chapter_id
