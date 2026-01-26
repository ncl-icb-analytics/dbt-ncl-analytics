# dim_person_demographics_basic pipeline

## Overview

This documentation covers versioning for the prod state of the dim_person_demographics_basic model ([DIM_PERSON_DEMOGRAPHICS_BASIC | Table](https://app.snowflake.com/atkjncu/ncl/#/data/databases/REPORTING/schemas/COMMISSIONING_REPORTING/table/DIM_PERSON_DEMOGRAPHICS_BASIC)).

A more raw view of the combined demographic data is available in the intermediate combined model ([INT_PERSON_PMI_COMBINED | Table](https://app.snowflake.com/atkjncu/ncl/#/data/databases/MODELLING/schemas/COMMISSIONING_MODELLING/table/INT_PERSON_PMI_COMBINED/data-preview)). This table contains the data source and event date for each patient used in the final table.

Currently the following datasets are used in the Demographics Basic table:
- PDS ([INT_PERSON_PMI_DATASET_PDS | Table](https://app.snowflake.com/atkjncu/ncl/#/data/databases/MODELLING/schemas/COMMISSIONING_MODELLING/table/INT_PERSON_PMI_DATASET_PDS))
- SUS ([INT_PERSON_PMI_DATASET_SUS | Table](https://app.snowflake.com/atkjncu/ncl/#/data/databases/MODELLING/schemas/COMMISSIONING_MODELLING/table/INT_PERSON_PMI_DATASET_SUS))
- Ethnicity National Data Sets ([INT_PERSON_PMI_DATASET_ETHNICITY_NATIONAL_DATA_SETS | Table](https://app.snowflake.com/atkjncu/ncl/#/data/databases/MODELLING/schemas/COMMISSIONING_MODELLING/table/INT_PERSON_PMI_DATASET_ETHNICITY_NATIONAL_DATA_SETS))

Note that the logic for the PDS Snapshot table largely follows the same logic as the Demographics Basic table but is limited to data available in PDS only ([DIM_SNAPSHOT_PERSON_PDS_DEMOGRAPHICS | Table](https://app.snowflake.com/atkjncu/ncl/#/data/databases/REPORTING/schemas/COMMISSIONING_REPORTING/table/DIM_SNAPSHOT_PERSON_PDS_DEMOGRAPHICS)).

## Version History
### V1.0 - 22/01/2026
* Initial version of dim_person_demographics_basic created.
* Initial data sources included:
    - PDS
    - SUS
    - Ethnicity National Data Sets ([ETHNICITY_NATIONAL_DATA_SETS | Table](https://app.snowflake.com/atkjncu/ncl/#/data/databases/MODELLING/schemas/LOOKUP_NCL/table/ETHNICITY_NATIONAL_DATA_SETS))
* Core demographic attributes (note these core fields are expanded using joins to lookup tables in the final table):
    - SK Patient ID (Pseudonymised NHS Number)
    - Gender
    - Date of Birth
    - Date of Death
    - Ethnicity Code (BK Ethnic)
    - Preferred Language (PDS only)
    - Interpreter Required Flag (PDS only)
    - LSOA 2021 Code of Residence
    - GP Practice Code of Registration
    - NCL Registered Flag (At time of refresh)
    - NCL Resident Flag (At time of refresh)

### V1.1 - 26/01/2026
* The logic to determine which dataset to pull from is more advanced:
  * Previous logic: Use PDS where it exists otherwise SUS (expect for ethnicity data where ETHNICITY_NATIONAL_DATA_SETS | Table is used when possible)
  * New logic: Custom per field but typically:
    * Use detailed values over unknowns/nulls (i.e. for gender, use 'Male' 'Female', 'Not specified' over 'Unknown' or NULL if possible) 
    * Use the most recent record across the datasets (2024 SUS data > 2022 PDS data)
* Cleaned up some of the ethnicity codes in the ethnicity data set and truncated to 1 digit (as 95% of codes only used 1 digit simplified ethnicity codes so why mix and match)
* Renamed resident fields to all include residence_ as a prefix for consistency
* Added some logic for registered borough and neighbourhood to replace NULLs with more informative values like 'Non-NCL' or 'Unknown due to practice closure'