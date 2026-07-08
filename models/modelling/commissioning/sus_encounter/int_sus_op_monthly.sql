{{
    config(materialized = 'table')
}}

-- Converted from SQL Server view [ETL].[VW_SUSOPCurrent]
-- Aliases:  b = encounterdenormalised_daterange, c = encounterbilling,
--           d = encounterpatient,               e = encounterunbundled
--
-- NOTE 1: ETL.udf_ACPFromActivityDate() has no Snowflake equivalent. Left as a
--         placeholder call below — replace with the migrated UDF / macro or NULL.
-- NOTE 2: The original [Year of Birth] logic (YEAR(dv_FinYear)) applies YEAR() to
--         a string; preserved intent but see the flagged lines.

select
      b.administrative_category
    , b.age
    , case
        when year(b.dv_year_of_birth) = 1990 or b.dv_year_of_birth is null then null
        else year(b.appointment_date) - year(b.dv_year_of_birth)
      end as age_at_record_end
    , case
        when year(b.dv_year_of_birth) = 1990 or b.dv_year_of_birth is null then null
        else year(b.appointment_date) - year(b.dv_year_of_birth)
      end as age_at_record_start
    , case
        when b.age = 0 then 1
        when b.age between 1 and 4 then 2
        when b.age between 5 and 9 then 3
        when b.age between 10 and 14 then 4
        when b.age between 15 and 19 then 5
        when b.age between 20 and 24 then 6
        when b.age between 25 and 29 then 7
        when b.age between 30 and 34 then 8
        when b.age between 35 and 39 then 9
        when b.age between 40 and 44 then 10
        when b.age between 45 and 49 then 11
        when b.age between 50 and 54 then 12
        when b.age between 55 and 59 then 13
        when b.age between 60 and 64 then 14
        when b.age between 65 and 69 then 15
        when b.age between 70 and 74 then 16
        when b.age between 75 and 79 then 17
        when b.age between 80 and 84 then 18
        when b.age >= 85 then 19
        else null
      end as age_range_derived
    , null as aggregate_unbundled_adjustment_local
    , e.cost as aggregate_unbundled_adjustment_national
    , null as aggregate_unbundled_adjustment_non_mandatory
    ,{{calculate_acp_from_activity_date('b.appointment_date') }} as applicable_costing_period
    , null as applicable_date
    , round(b.dv_mff_index_applied, 6) as applied_mff_national
    , null as applied_mff_non_mandatory
    , b.appointment_date
    , null as area_code_derived
    , null as attendance_date
    , b.attendance_identifier
    , null as attendance_organisation_code_type
    , b.attended_or_did_not_attend
    , b.first_attendance + 2 as attender_type_derived
    , null as bulk_replacement_cds_group
    , null as carer_support_indicator
    , b.dv_cds_activity_date as cds_activity_date
    , null as cds_group_derived
    , null as cds_group_indicator
    , null as cds_record_type
    , 'CDS062' as cds_schema_version
    , null as census_output_area_2001
    , null as code_cleaning
    , b.organisation_code_code_of_commissioner as commissioner_code_original_data
    , b.commissioner_reference_no
    , null as commissioner_site_code
    , b.commissioning_serial_no_agreement_no
    , null as confidentiality_category
    , null as configurable_indicator
    , b.consultant_code
    , null as consultant_code_type
    , null as consultant_organisation_code
    , b.core_hrg
    , '4' as core_hrg_version_calculated
    , null as costed_indicator
    , null as costing_batch_sequence
    , null as country
    , null as county_code
    , null as current_period_number
    , b.age as derived_age
    , null as diagnosis_scheme_in_use
    , null as ed_county_code
    , null as ed_district_code
    , d.ward_code as electoral_ward_1998
    , null as electoral_ward_division
    , b.ethnic_category_code
    , null as exclusion_reason
    , null as extract_date
    , b.dv_total_cost_inc_mff as final_tariff_applied
    , null as finished_indicator
    , case
        when b.first_attendance = '-1' then null
        else b.first_attendance
      end as first_attendance
    , b.gp_practice_code_original_data as first_gp_organisation_code
    , b.gender_code
    , b.generated_record_id
    , null as government_office_region_code
    , b.gp_code
    , null as gp_code_type
    , null as gp_consortium_code
    , null as gp_pct_type_derived
    , b.gp_practice_code_derived as gp_practice_code
    , b.gp_practice_code_derived
    , case
        when length(b.gp_practice_code_original_data) > 6 then left(b.gp_practice_code_original_data, 6)
        else b.gp_practice_code_original_data
      end as gp_practice_code_original_data
    , b.dv_practice_code_validated as gp_practice_derived_from_pds
    , 'P_' || right(b.dv_fin_year, 4) as grouping_algorithm_version
    , null as grouping_hrg_version
    , null as grouping_reference_data_version
    , null as hes_identifier
    , null as hierarchy
    , null as hrg_submitted
    , null as hrg_dominant_grouping_variable
    , null as hrg_dominant_grouping_variable_procedure
    , null as hrg_procedure_scheme
    , b.dv_hrg_used_for_tariff as hrg_used_for_tariff
    , null as hrg_version_submitted
    , null as intended_procedure_status
    , null as interchange_id
    , null as last_did_not_arrive_date
    , null as last_dna_or_patient_cancelled_date
    , null as lead_care_activity_indicator
    , null as local_authority_code
    , null as local_core_tariff_with_ub
    , null as location_class
    , b.location_type_code
    , b.main_specialty_code
    , null as marital_status
    , null as market_forces_factor_id
    , null as match_criterion_indicator
    , b.medical_staff_type_seeing_patient
    , case
        when b.dv_mff_index_applied is null then null
        when b.pbr_final_tariff is null then null
        else round(b.pbr_final_tariff - (b.pbr_final_tariff / b.dv_mff_index_applied), 0)
      end as mff_adjustment
    , null as mff_adjustment_non_mandatory
    , month(d.date_of_birth) as month_of_birth  -- flagged 'Incorrect' in source
    , null as nhs_number_status_indicator
    , null as nhs_rid_from_provider
    , b.nhs_service_agreement_line_no
    , null as non_mandatory_core_tariff_with_ub
    , null as number_bpt_indicators
    , b.number_diagnosis
    , b.number_procedures
    , b.dv_number_unbundled_hr_gs as number_unbundled_hrgs
    , b.dv_number_unbundled_non_priced_hr_gs as number_unbundled_non_priced_hrgs
    , b.dv_number_unbundled_priced_hr_gs as number_unbundled_priced_hrgs
    , null as op_episode_type
    , b.operation_status
    , null as org_code_local_patient_identifier
    , b.organisation_code_code_of_commissioner
    , left(b.organisation_code_code_of_provider, 3) as organisation_code_code_of_provider
    , left(b.organisation_code_pct_of_residence, 3) as organisation_code_pct_of_residence
    , left(b.organisation_code_code_of_provider, 3) as organisation_code_sender
    , b.gp_code as organisation_code_of_gp
    , b.organisation_code_patient_pathway_identifier
    , null as organisation_code_type_commissioner
    , null as organisation_code_type_consultant
    , null as organisation_code_type_gp
    , null as organisation_code_type_pct_of_residence
    , null as organisation_code_type_prime_recipient
    , null as organisation_code_type_provider
    , null as organisation_code_type_referrer
    , null as organisation_code_type_sender
    , null as other_indicator
    , case
        when b.outcome_of_attendance = '-1' then null
        else b.outcome_of_attendance
      end as outcome_of_attendance
    , null as outpatient_tariff
    , b.patient_postcode_derived_pct
    , null as patient_postcode_derived_pct_type
    , null as patient_postcode_electoral_ward
    , null as patient_type
    , b.pbr_final_tariff
    , null as pbr_generated_interchange_id
    , null as pbr_spell_cost_id
    , null as pbr_spell_frozen_indicator
    , null as pbr_spell_status_indicator
    , null as pct_derived_from_gp
    , b.pct_derived_from_gp_practice
    , b.organisation_code_pct_of_residence as pct_of_residence_original
    , b.organisation_code_code_of_commissioner as pct_responsible
    , b.postcode_of_usual_address as postcode_sector_of_usual_address
    , b.primary_diagnosis_code
    , b.primary_procedure_code
    , b.primary_procedure_date
    , null as prime_recipient
    , case
        when b.priority_type = '-1' then null
        else b.priority_type
      end as priority_type
    , null as procedure_scheme_in_use
    , null as protocol_identifier
    , null as provider_location
    , b.provider_reference_no
    , b.site_code_of_treatment as provider_site_code
    , null as pseudonymised_status
    , null as query_date
    , null as reason_access_provided
    , null as record_extraction_indicator
    , null as re_costing_requested_flag
    , b.referral_request_received_date
    , b.referrer_code
    , null as referrer_code_type
    , b.referring_organisation_code
    , null as registered_gmp_code
    , null as report_period_end_date
    , null as report_period_start_date
    , null as request_received_date_status
    , b.dv_rtt_length_derived as rtt_length_derived
    , b.rtt_patient_pathway_identifier
    , b.rtt_period_end_date
    , b.rtt_period_start_date
    , b.rtt_status
    , b.secondary_diagnosis_code_1
    , b.secondary_diagnosis_code_2
    , b.secondary_diagnosis_code_3
    , b.secondary_diagnosis_code_4
    , b.secondary_diagnosis_code_5
    , b.secondary_diagnosis_code_6
    , b.secondary_diagnosis_code_7
    , b.secondary_diagnosis_code_8
    , b.secondary_diagnosis_code_9
    , b.secondary_diagnosis_code_10
    , b.secondary_diagnosis_code_11
    , b.secondary_diagnosis_code_12
    , b.secondary_procedure_code_1
    , b.secondary_procedure_code_2
    , b.secondary_procedure_code_3
    , b.secondary_procedure_code_4
    , b.secondary_procedure_code_5
    , b.secondary_procedure_code_6
    , b.secondary_procedure_code_7
    , b.secondary_procedure_code_8
    , b.secondary_procedure_code_9
    , b.secondary_procedure_code_10
    , b.secondary_procedure_code_11
    , b.secondary_procedure_code_12
    , b.secondary_procedure_date_1
    , b.secondary_procedure_date_2
    , b.secondary_procedure_date_3
    , b.secondary_procedure_date_4
    , b.secondary_procedure_date_5
    , b.secondary_procedure_date_6
    , b.secondary_procedure_date_7
    , b.secondary_procedure_date_8
    , b.secondary_procedure_date_9
    , b.secondary_procedure_date_10
    , b.secondary_procedure_date_11
    , b.secondary_procedure_date_12
    , b.service_type_requested
    , null as sha_commissioner
    , null as sha_from_gp_derived
    , null as sha_from_patient_postcode
    , null as sha_old_org_code
    , null as sha_provider
    , null as sha_type_from_gp_derived
    , null as sha_type_from_patient_postcode
    , b.site_code_of_treatment
    , b.source_of_referral_for_outpatients
    , null as spare_1
    , null as spare_2
    , null as spare_3
    , null as spare_4
    , null as spare_5
    , null as spell_const_version_no
    , null as spell_cost_version_date
    , null as spell_error_status
    , b.spell_identifier
    , b.spell_in_pb_r_not_in_pb_r as spell_in_pbr_not_in_pbr
    , null as spell_version_as_at_date_and_time
    , null as staging_loaded_date
    , b.dv_hrg_used_for_tariff as sus_hrg
    , null as sus_version
    , null as tariff_adjustment_future_use_1_national
    , null as tariff_adjustment_future_use_1_non_mandatory
    , null as tariff_adjustment_future_use_2_national
    , null as tariff_adjustment_future_use_2_non_mandatory
    , null as tariff_financial_adjustment_national
    , null as tariff_financial_adjustment_non_mandatory
    , b.tariff_initial_amount_national
    , null as tariff_initial_amount_non_mandatory
    , b.tariff_pre_mff_adjusted_national
    , null as tariff_pre_mff_adjusted_non_mandatory
    , null as tariff_total_payment_local
    , coalesce(b.tariff_total_payment_national, b.pbr_final_tariff) as tariff_total_payment_national
    , null as tariff_total_payment_non_mandatory
    , null as tarrif_initial_amount_local
    , null as temporary_cost_period_status
    , null as test_indicator
    , b.treatment_function_code
    , b.dv_unbundled_hrg_1 as unbundled_hrg_1
    , b.dv_unbundled_hrg_2 as unbundled_hrg_2
    , b.dv_unbundled_hrg_3 as unbundled_hrg_3
    , b.dv_unbundled_hrg_4 as unbundled_hrg_4
    , b.dv_unbundled_hrg_5 as unbundled_hrg_5
    , b.dv_unbundled_hrg_6 as unbundled_hrg_6
    , b.dv_unbundled_hrg_7 as unbundled_hrg_7
    , b.dv_unbundled_hrg_8 as unbundled_hrg_8
    , b.dv_unbundled_hrg_9 as unbundled_hrg_9
    , b.dv_unbundled_hrg_10 as unbundled_hrg_10
    , b.dv_unbundled_hrg_11 as unbundled_hrg_11
    , b.dv_unbundled_hrg_12 as unbundled_hrg_12
    , null as unique_booking_reference_number_converted
    , b.unique_cds_identifier
    , b.dv_query_id as unique_query_id
    , null as update_type
    , null as version_sequence_number
    , year(d.date_of_birth) as year_of_birth
    , case
        when b.organisation_code_code_of_commissioner in (
            '07P00','07W00','07Y00','08C00','08E00','08G00','08Y00','09A00','13R00','Q7100',
            '07P','07W','07Y','08C','08E','08G','08Y','09A','13R','Q71','W2U3Z'
        ) then 1
        else 0
      end as zcommissioning_access
    , null as zlsoa01
    , b.dw_nhs_number as znhs_number_pseudo
    , c.pod_description as zpod_level_4
    , null as zsensitive_data_category
    , null as zccgcode
    , null as zpctcode
    -- Original: CONCAT(RIGHT(YEAR(LEFT(dv_FinYear,4)),2), RIGHT(YEAR(RIGHT(dv_FinYear,4)),2))
    -- YEAR() applied to a string is invalid in Snowflake; preserving intent (last 2 digits of each 4-char half).
    , right(left(b.dv_fin_year, 4), 2) || right(right(b.dv_fin_year, 4), 2) as zfinancialyear
    , null as zcarehome
    , b.dv_total_cost_inc_mff as zderivedprice
    , null as zderivedpriceflag
    , null as zderivedpricedt
    , null as zderivedpricerowid
    , null as zbusinessrule
    , null as zlocalpatientidentifier
    , b.dv_lsoa as zlsoa11
    , null as zcontracttype
    , null as direct_access_tariff_flag
    , null as unbundled_exclusion_reason
    , case
        when b.first_attendance in ('1','3') then 'New'
        when b.first_attendance in ('2','4') then 'FUp'
        when b.first_attendance = '5' then 'N/A'
        else 'Unknown'
      end as zderappointmenttype
    , case
        when b.first_attendance = '5' then 'Admin Event'
        when b.attended_or_did_not_attend = '0' then 'Unknown'
        when b.attended_or_did_not_attend = '2' then 'Cancel (Pat)'
        when b.attended_or_did_not_attend = '4' then 'Cancel (Hos)'
        when b.attended_or_did_not_attend in ('3','7') then 'DNA'
        when b.attended_or_did_not_attend in ('5','6') then 'Attend'
        else 'Unknown'
      end as zderattendancetype
    , null as ztfccategory
    , null as zhostccgcode
    , null as znhsecategory
    , null as znhsesubcategory
    , null as zresponsibleccgcode
    , null as zcamorganisationcode
    , null as zspecialised
    , null as zhighlyspecialised
    , b.local_patient_identifier
    , b.spell_npoc
    , null as spell_service_line
    , null as commissioning_region
    , null as zimd2015londonquintile
    , null as zimd2015nationaldecile
    , null as znfa
    , case
        when length(b.dv_fin_month) = 1
            then cast(left(b.dv_fin_year, 4) as varchar) || '0' || cast(b.dv_fin_month as varchar)
        else cast(left(b.dv_fin_year, 4) as varchar) || cast(b.dv_fin_month as varchar)
      end as zfinancialmonth
    , b.sk_patient_id
    , 'CURRENT' as zdatatype
    , b.sk_encounter_id
    , 'Current' as susversion
    , b.dv_spec_com_service_code_national
    , b.dv_spec_com_service_code_local
from {{ ref('stg_sus_op_monthly_encounterdenormalised_daterange') }} b
left outer join {{ ref('stg_sus_op_monthly_encounterbilling') }}  c on b.sk_encounter_id = c.sk_encounter_id
left outer join {{ ref('stg_sus_op_monthly_encounterpatient') }}   d on b.sk_encounter_id = d.sk_encounter_id
left outer join {{ ref('stg_sus_op_monthly_encounterunbundled') }} e on b.sk_encounter_id = e.sk_encounter_id