{{
    config(
        description="Raw layer (SUS admitted patient care episodes and procedures). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_APC.spell.critical_care_consolidated \ndbt: source(''sus_apc'', ''spell.critical_care_consolidated'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  CRITICAL_CARE_CONSOLIDATED_ID -> critical_care_consolidated_id\n  identifier.local_identifier -> identifier_local_identifier\n  identifier.sus_cc_period -> identifier_sus_cc_period\n  identifier.submitted_on_episode -> identifier_submitted_on_episode\n  identifier.cc_period_type -> identifier_cc_period_type\n  identifier.cc_period_type_numeric -> identifier_cc_period_type_numeric\n  admission.date -> admission_date\n  admission.time -> admission_time\n  admission.unit_function -> admission_unit_function\n  admission.unit_bed_configuration -> admission_unit_bed_configuration\n  admission.source -> admission_source\n  admission.source_location -> admission_source_location\n  discharge.date -> discharge_date\n  discharge.time -> discharge_time\n  discharge.status -> discharge_status\n  discharge.location -> discharge_location\n  organ_support_days.advanced_respiratory -> organ_support_days_advanced_respiratory\n  organ_support_days.basic_respiratory -> organ_support_days_basic_respiratory\n  organ_support_days.advanced_cardiovascular -> organ_support_days_advanced_cardiovascular\n  organ_support_days.basic_cardiovascular -> organ_support_days_basic_cardiovascular\n  organ_support_days.renal -> organ_support_days_renal\n  organ_support_days.neurological -> organ_support_days_neurological\n  organ_support_days.gastro_intestinal -> organ_support_days_gastro_intestinal\n  organ_support_days.dermatological -> organ_support_days_dermatological\n  organ_support_days.liver -> organ_support_days_liver\n  organ_support_days.maximum_systems -> organ_support_days_maximum_systems\n  intensity_support_days.level_2 -> intensity_support_days_level_2\n  intensity_support_days.level_3 -> intensity_support_days_level_3\n  admission.gestation_length_at_delivery -> admission_gestation_length_at_delivery\n  admission.type -> admission_type\n  discharge.ready_date -> discharge_ready_date\n  discharge.ready_time -> discharge_ready_time\n  discharge.destination -> discharge_destination\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "CRITICAL_CARE_CONSOLIDATED_ID" as critical_care_consolidated_id,
    "identifier.local_identifier" as identifier_local_identifier,
    "identifier.sus_cc_period" as identifier_sus_cc_period,
    "identifier.submitted_on_episode" as identifier_submitted_on_episode,
    "identifier.cc_period_type" as identifier_cc_period_type,
    "identifier.cc_period_type_numeric" as identifier_cc_period_type_numeric,
    "admission.date" as admission_date,
    "admission.time" as admission_time,
    "admission.unit_function" as admission_unit_function,
    "admission.unit_bed_configuration" as admission_unit_bed_configuration,
    "admission.source" as admission_source,
    "admission.source_location" as admission_source_location,
    "discharge.date" as discharge_date,
    "discharge.time" as discharge_time,
    "discharge.status" as discharge_status,
    "discharge.location" as discharge_location,
    "organ_support_days.advanced_respiratory" as organ_support_days_advanced_respiratory,
    "organ_support_days.basic_respiratory" as organ_support_days_basic_respiratory,
    "organ_support_days.advanced_cardiovascular" as organ_support_days_advanced_cardiovascular,
    "organ_support_days.basic_cardiovascular" as organ_support_days_basic_cardiovascular,
    "organ_support_days.renal" as organ_support_days_renal,
    "organ_support_days.neurological" as organ_support_days_neurological,
    "organ_support_days.gastro_intestinal" as organ_support_days_gastro_intestinal,
    "organ_support_days.dermatological" as organ_support_days_dermatological,
    "organ_support_days.liver" as organ_support_days_liver,
    "organ_support_days.maximum_systems" as organ_support_days_maximum_systems,
    "intensity_support_days.level_2" as intensity_support_days_level_2,
    "intensity_support_days.level_3" as intensity_support_days_level_3,
    "admission.gestation_length_at_delivery" as admission_gestation_length_at_delivery,
    "admission.type" as admission_type,
    "discharge.ready_date" as discharge_ready_date,
    "discharge.ready_time" as discharge_ready_time,
    "discharge.destination" as discharge_destination,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_apc', 'spell.critical_care_consolidated') }}
