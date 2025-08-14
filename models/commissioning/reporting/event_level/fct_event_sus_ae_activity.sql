SELECT 
    GETDATE() AS RefreshDate 
    ,primarykey_id
    ,system_transaction_cds_unique_identifier AS UniqueID 
    ,patient_nhs_number_value_pseudo AS PatientID -- NB this is the sk_patientID
    ,attendance_location_hes_provider_3 AS ProviderCode
    ,'SUS-Faster' AS Dataset
    ,'ECDS' AS PODGroup
    ,CASE 
        WHEN attendance_location_department_type = '01' THEN 'AE-T1'
        WHEN attendance_location_department_type = '02' THEN 'AE-Other'
        WHEN attendance_location_department_type = '03' THEN 'UCC'
        WHEN attendance_location_department_type = '04' THEN 'WiC'
        WHEN attendance_location_department_type = '05' THEN 'SDEC'
        ELSE 'Others' END AS POD
    -- missing some location imputations
    -- missing some commissioner imputations
    -- missing some renaming (function in wrong area)
    ,"DETERMINE_FISCAL_YEAR__CH_TEMP"(attendance_arrival_date) AS FinYear
    ,monthname(attendance_arrival_date) AS FinMonthText
    ,"DETERMINE_FISCAL_MONTH__CH_TEMP"(attendance_arrival_date) AS FinMonth

    ,TIMESTAMP_NTZ_FROM_PARTS(
            TO_DATE(attendance_arrival_date)
        ,coalesce(to_time(TO_CHAR(attendance_arrival_date, 'HH24:MI:SS')), to_time('00:00:00'))
        ) AS StartDate

    ,TIMESTAMP_NTZ_FROM_PARTS(
            TO_DATE(attendance_arrival_date)
        , coalesce(to_time(TO_CHAR(attendance_arrival_date, 'HH24:MI:SS')), to_time('00:00:00'))
        ) AS EndDate

    , DATEADD(
        day
        , (8 - DAYOFWEEK(attendance_arrival_date))
        , attendance_arrival_date
        ) AS week_EndDate

    ,ProviderCode
    ,ProviderName 
    ,ProviderSiteCode
    ,ProviderSiteName
    --,RegisteredBoroughCode -- need to reimpute
    --,RegisteredBoroughName  -- need to reimpute
    ,replace(LEFT(commissioning_service_agreement_commissioner,5),'00','') AS CommissionerCode
    ,CommissionerName
    ,patient_gp_registration_general_practice AS GPCode
    ,GPPractice
    ,GPLocality 
    ,'180' AS TFCCode
    ,'Emergency Medicine Service' AS TFCDesc
    ,commissioning_grouping_health_resource_group AS HRGCode
    ,HRGDescription
    ,DiagCode
    ,DiagDesc
    ,CASE WHEN attendance_location_department_type   = '05' THEN 'Same Day Emergency Care Attendance' ELSE DepartmentTypeDescription END AS AEAttendanceTypeDesc
    ,CAST('' AS varchar(10)) AS ProCode
    ,CAST('' AS varchar(10)) AS ProCategory
    ,CAST('' AS varchar(10)) AS ProDesc
    ,patient_age_at_arrival AS Age
    ,patient_ethnic_category AS EthnicityCode
    ,EthnicityDesc
    ,patient_stated_gender AS GenderCode
    ,GenderDesc
    ,patient_usual_address_lsoa_11 AS LSOA
    ,DeprivationDecile  -- table needs to be recreated
    ,patient_usual_address_postcode_pseudo AS PostCodeID
    ,1 AS Activity
    ,commissioning_national_pricing_tariff AS BaseCost
    ,commissioning_national_pricing_tariff + commissioning_national_pricing_market_forces_adjustment AS TotalCost
    ,CASE WHEN commissioning_national_pricing_tariff_description = 'MAND' THEN 1 ELSE 0 end AS PBRFlag
    ,CAST('' AS varchar(10)) AS AdminCategoryCode
    ,CAST('' AS varchar(10)) AS AdminCategoryDesc
    ,commissioning_service_agreement_commissioning_serial_number AS CommissioneRSerialNumber
    ,'N' AS SpecCommFlag
    ,0 AS BedDays
    ,0 AS CriticalCareDays
    ,0 AS excess_BedDays
    ,attendance_arrival_arrival_mode_code AS AEArrivalModeCode
    ,AEArrivalModeDesc
    ,attendance_arrival_time AS AEArrivalTime
    ,attendance_location_department_type  AS AEAttendanceTypeCode
    ,CASE WHEN attendance_location_department_type   = '05' THEN 'Same Day Emergency Care Attendance' ELSE DepartmentTypeDescription END AS AEAttendanceTypeDesc
    ,clinical_chief_complaint_code AS AEChiefComplaintCode
    ,CASE WHEN ECDS_Description = 'Code deprecated' THEN SNOMED_UK_Preferred_Term ELSE  ECDS_Description END AS AEChiefComplaintDesc
    ,attendance_departure_time AS AEDepartureTime
    ,AEDiagCode
    ,AEDiagDesc
    ,AEInvestigationCode
    ,AEInvestigationDesc
    ,attendance_discharge_destination_code AS AEDischargeDestinationCode
    ,AEDischargeDestinationDesc
    ,attendance_arrival_attendance_source_code AS AEReferralSourceCode
    ,AEReferralSourceDesc
    ,AETreatmentCode
    ,AETreatmentDesc

FROM {{ ref('int_sus_ae_activity_enriched')}}