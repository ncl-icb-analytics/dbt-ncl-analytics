{{ config(materialized='view') }}

WITH

-- Pre-filter dictionaries
treatment_specs AS (
    SELECT BK_SpecialtyCode, SpecialtyName
    FROM {{ ref('stg_dictionary_dbo_specialties') }}
    WHERE IsTreatmentFunction = 1
),
main_specs AS (
    SELECT BK_SpecialtyCode, SpecialtyName
    FROM {{ ref('stg_dictionary_dbo_specialties') }}
    WHERE IsMainSpecialty = 1
)


SELECT
    base.*,
    Spec.SpecialtyName AS tfc_desc,
    Spec2.SpecialtyName AS main_spec_name,
    HRG.HRGDescription,
    Dia.Description AS diag_desc,
    Pro.Category AS proc_category,
    Pro.Description AS proc_desc,
    Org."Organisation_Name" AS provider_site_name,
    Org2."Organisation_Name" AS provider_name,
    Org3."Organisation_Name" AS commissioner_name,
    Org5."Organisation_Name" AS gp_practice,
    Org4."NetworkName" AS gp_locality,
    Gen.GenderCode2 AS gender_desc,
    Eth.EthnicityDesc AS ethnicity_desc,
    AM.AdmissionMethodName AS admit_method_desc,
    SA.SourceOfAdmissionName AS source_adm_desc,
    DM.DischargeMethodName AS discharge_method_desc,
    DS.DischargeDestinationName AS discharge_dest_desc,
    IMD.IMDDECILE AS deprivation_decile
FROM {{ ref('int_spell_with_episodes') }} base
LEFT JOIN {{ ref('stg_specialty_tfc') }} Spec
    ON base.dom_tfc = Spec.BK_SpecialtyCode
LEFT JOIN {{ ref('stg_specialty_main') }} Spec2
    ON base.dom_main_spec = Spec2.BK_SpecialtyCode
LEFT JOIN {{ source('dictionary', 'HRG') }} HRG
    ON base."spell.commissioning.grouping.core_hrg" = HRG.HRGCode
LEFT JOIN {{ source('dictionary', 'Diagnosis') }} Dia
    ON Dia.Code = REPLACE(base."spell.clinical_coding.grouper_derived.primary_diagnosis", '-', '')
LEFT JOIN {{ source('dictionary', 'Procedure') }} Pro
    ON Pro.Code = LEFT(base."spell.clinical_coding.grouper_derived.dominant_procedure", 4)
LEFT JOIN {{ source('dictionary', 'Organisation') }} Org
    ON Org.Organisation_Code = base."spell.care_location.site_code_of_treatment"
LEFT JOIN {{ source('dictionary', 'Organisation') }} Org2
    ON Org2.Organisation_Code = base.provider_code3
LEFT JOIN {{ source('dictionary', 'Commissioner') }} Org3
    ON base.comm_code5 = Org3.CommissionerCode
LEFT JOIN {{ source('dictionary', 'OrganisationMatrixPracticeView') }} Org4
    ON Org4.PracticeCode = base."spell.patient.registration.general_practice"
LEFT JOIN {{ source('dictionary', 'Organisation') }} Org5
    ON Org5.Organisation_Code = base."spell.patient.registration.general_practice"
LEFT JOIN {{ source('dictionary', 'Gender') }} Gen
    ON base."spell.patient.identity.gender" = Gen.GenderCode
LEFT JOIN {{ source('dictionary', 'Ethnicity') }} Eth
    ON base."spell.patient.identity.ethnic_category" = Eth.BK_EthnicityCode
LEFT JOIN {{ source('ip', 'AdmissionMethods') }} AM
    ON base."spell.admission.method" = AM.BK_AdmissionMethodCode
LEFT JOIN {{ source('ip', 'SourceOfAdmissions') }} SA
    ON base."spell.admission.source" = SA.BK_SourceOfAdmissionCode
LEFT JOIN {{ source('ip', 'DischargeMethod') }} DM
    ON base."spell.discharge.method" = DM.BK_DischargeMethodCode
LEFT JOIN {{ source('ip', 'DischargeDestination') }} DS
    ON base."spell.discharge.destination" = DS.BK_DischargeDestinationCode
LEFT JOIN {{ source('imd', 'IMD2019') }} IMD
    ON IMD.LSOACODE = base."spell.patient.residence_derived.lsoa_11"
