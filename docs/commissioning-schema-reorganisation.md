# Commissioning schema re-organisation

The `COMMISSIONING_MODELLING` and `COMMISSIONING_REPORTING` schemas are replaced by domain schemas.
Each dbt folder under `models/modelling/` and `models/reporting/` now maps to a schema of the same
name (e.g. `models/modelling/acute` → `MODELLING.ACUTE`). A new `models/reference/` layer builds
derived reference datasets into the `REFERENCE` database. OLIDS schemas are unchanged, except
`OLIDS_RISK_STRATIFICATION`, which is renamed `RISK_STRATIFICATION` in both MODELLING and REPORTING.

When this change deploys, the objects in the old schemas stop refreshing. They are dropped
automatically 35 days later. Repoint anything that reads from an old location before then —
same object name, new schema, unless listed in the renames below.

## Renamed models

These models change name as well as schema.

| Old | New |
|---|---|
| MODELLING.COMMISSIONING_MODELLING.INT_COMM_AMBULATORY_SENSITIVE_NEL | MODELLING.ACUTE.INT_ACTIVITY_AMBULATORY_SENSITIVE_NEL |
| MODELLING.COMMISSIONING_MODELLING.INT_COMM_CANCER | MODELLING.ACUTE.INT_ACTIVITY_CANCER |
| MODELLING.COMMISSIONING_MODELLING.INT_COMM_CHRONIC_LOWER_RESPIRATORY | MODELLING.ACUTE.INT_ACTIVITY_CHRONIC_LOWER_RESPIRATORY |
| MODELLING.COMMISSIONING_MODELLING.INT_COMM_DIALYSIS | MODELLING.ACUTE.INT_ACTIVITY_DIALYSIS |
| MODELLING.COMMISSIONING_MODELLING.INT_COMM_MATERNITY | MODELLING.ACUTE.INT_ACTIVITY_MATERNITY |
| MODELLING.COMMISSIONING_MODELLING.INT_COMM_PRIMARY_SUMMARY | MODELLING.ACUTE.INT_ACTIVITY_PRIMARY_SUMMARY |
| MODELLING.COMMISSIONING_MODELLING.INT_COMM_RESPIRATORY | MODELLING.ACUTE.INT_ACTIVITY_RESPIRATORY |
| MODELLING.COMMISSIONING_MODELLING.INT_COMMISSIONING_OBSERVATIONS | MODELLING.CROSS_SYSTEM.INT_ENCOUNTER_OBSERVATIONS |
| MODELLING.COMMISSIONING_MODELLING.INT_COMMISSIONING_ENCOUNTER_OBSERVATIONS_SUMMARY | MODELLING.CROSS_SYSTEM.INT_ENCOUNTER_OBSERVATIONS_SUMMARY |
| MODELLING.COMMISSIONING_MODELLING.INT_COMMISSIONING_DEMOGRAPHICS_AT_EVENT | MODELLING.POPULATION.INT_PERSON_DEMOGRAPHICS_AT_EVENT |
| MODELLING.COMMISSIONING_MODELLING.PROVIDER_DAILY_APC_ACTIVITY_DBT | MODELLING.DATA_QUALITY.PROVIDER_DAILY_APC_ACTIVITY |
| MODELLING.COMMISSIONING_MODELLING.PROVIDER_DAILY_ECDS_ACTIVITY_DBT | MODELLING.DATA_QUALITY.PROVIDER_DAILY_ECDS_ACTIVITY |
| MODELLING.COMMISSIONING_MODELLING.PROVIDER_DAILY_OP_ACTIVITY_DBT | MODELLING.DATA_QUALITY.PROVIDER_DAILY_OP_ACTIVITY |
| MODELLING.COMMISSIONING_MODELLING.DICT_ORGANISATION_NHS_PROVIDER | REFERENCE.ORGANISATION.ORGANISATION_NHS_PROVIDER |
| MODELLING.COMMISSIONING_MODELLING.DICT_ORGANISATIONS_LOCAL_AUTHORITY | REFERENCE.ORGANISATION.ORGANISATIONS_LOCAL_AUTHORITY |
| MODELLING.COMMISSIONING_MODELLING.INT_PRACTICE_WEIGHTED_POPULATION | REFERENCE.PRIMARY_CARE.PRACTICE_WEIGHTED_POPULATION |

## Where models moved

Models keep their names. Previous location is `MODELLING.COMMISSIONING_MODELLING` for MODELLING
schemas and `REPORTING.COMMISSIONING_REPORTING` for REPORTING schemas.

**MODELLING.ACUTE**

- int_activity_ambulatory_sensitive_nel, int_activity_cancer, int_activity_chronic_lower_respiratory, int_activity_dialysis, int_activity_maternity, int_activity_primary_summary, int_activity_respiratory (renamed, see above)
- int_sus_apc_diagnosis, int_sus_op_diagnosis, int_sus_uec_diagnosis
- int_sus_apc_encounter, int_sus_apc_imputed_spells, int_sus_apc_merged_spells, int_sus_op_appointment, int_sus_op_encounter, int_sus_uec_encounter
- int_sus_apc_procedure, int_sus_apc_procedure_hrg, int_sus_op_procedure, int_sus_op_procedure_hrg, int_sus_uec_procedure

**MODELLING.POPULATION**

- int_person_pds_demographics, int_person_pds_latest_record, int_person_pds_population_flags, int_snapshot_person_pds_records
- int_person_pmi_combined, int_person_pmi_dataset_pds, int_person_pmi_dataset_sus, int_person_pmi_dataset_prescribing, int_person_pmi_dataset_ethnicity_national_data_sets
- int_person_demographics_at_event (renamed, see above)

**MODELLING.MENTAL_HEALTH**

- int_mhsds_carecontact_encounters, int_mhsds_spell_encounters, int_mhsds_contact_currency, int_mhsds_spell_currency
- int_cost_index_mhsds_activity_monthly, int_cost_index_slam_activity_monthly

**MODELLING.COMMUNITY**

- int_csds_encounters, int_csds_contacts_summary, int_csds_contacts_activity_summary, int_csds_activities_summary, int_csds_referrals_summary, int_csds_contact_currency
- int_cost_index_csds_activity_monthly

**MODELLING.CONTRACTING**

- int_nhse_currency_price_resolution

**MODELLING.CROSS_SYSTEM**

- int_person_cost_index_actual_monthly, int_cost_index_source_coverage, int_cost_index_epd_prescribing_monthly
- int_person_resource_index_exposure_monthly
- int_encounter_observations, int_encounter_observations_summary (renamed, see above)

**MODELLING.WAITING_LISTS**

- int_wl_current

**MODELLING.ADULT_SOCIAL_CARE**

- int_asc_cld_service_most_recent

**MODELLING.DIAGNOSTICS**

- int_tat_turnaround_times

**MODELLING.DATA_QUALITY**

- provider_daily_apc_activity, provider_daily_ecds_activity, provider_daily_op_activity (renamed, see above), provider_missing_summary

**MODELLING.MYRIA**

- int_myria_attendances_diagnoses, int_myria_conditions, int_myria_eligible_propensity_match, int_myria_evaluation, int_myria_monthly_output

**REPORTING.ACUTE**

- obt_encounter_apc, obt_encounter_op, obt_encounter_uec, obt_appointment_op
- fct_person_sus_apc_recent, fct_person_sus_op_recent, fct_person_sus_uec_recent

**REPORTING.POPULATION**

- dim_person_demographics_basic, dim_snapshot_person_pds_demographics

**REPORTING.MENTAL_HEALTH**

- dim_person_mh_profile, fct_mhsds_referral_episodes, fct_mhsds_currency_bed_days, fct_mhsds_currency_contacts, fct_mhsds_current_inpatients

**REPORTING.COMMUNITY**

- fct_csds_currency_contacts

**REPORTING.CROSS_SYSTEM**

- fct_person_cost_index_monthly, fct_cost_index_by_org_month
- fct_person_resource_index, fct_resource_index_by_area, fct_resource_index_by_imd_quintile, fct_resource_index_by_residence_borough, fct_resource_index_by_ward
- obt_person_activity, fct_person_activity_by_month

**REPORTING.WAITING_LISTS**

- fct_person_wl_current_count_total, fct_person_wl_current_count_tfc, fct_person_wl_current_count_provider, fct_provider_wl_current_count_total

**REPORTING.ADULT_SOCIAL_CARE**

- fct_person_asc_service_recent

**REPORTING.CLTCS**

- cltcs_adult_population, cltcs_monthly_covariates, cltcs_activity_monthly_capture, cltcs_asc_monthly_capture, cltcs_medications_monthly_capture, cltcs_population_monthly_capture
- cltcs_score_activation, cltcs_score_coordination, cltcs_score_frailty, cltcs_score_treatment, plus their four `_snapshot_input` models

**REPORTING.MYRIA**

- fct_person_myria_control_group, fct_person_myria_high_risk_patients

**REFERENCE.ORGANISATION**

- organisation_nhs_provider, organisations_local_authority (renamed, see above)

**REFERENCE.PRIMARY_CARE**

- practice_weighted_population (renamed, see above)

## OLIDS

- Schemas unchanged, except `OLIDS_RISK_STRATIFICATION` → `RISK_STRATIFICATION` (both databases; models keep their names).
- Four OLIDS-derived models moved from the commissioning schemas into existing OLIDS schemas:
  - fct_person_gp_recent, fct_person_resource_index_olids → `REPORTING.OLIDS_PERSON_ANALYTICS`
  - int_resource_index_olids_profile, int_resource_index_olids_enrichment_monthly → `MODELLING.OLIDS_PERSON_ATTRIBUTES`
