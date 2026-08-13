{{
    config(
        materialized='table'
    )
}}

-- Intermediate layer: ECDS consolidated emergency care attendances.
-- Currently a 1:1 passthrough of stg_ecds_consolidated. Business logic,
-- derivations and filtering belong here rather than in staging.

select

    -- Identifiers & data management derived fields
    "Record_Identifier"
    ,"Row_Number_ID"
    ,"Patient_ID"
    ,"CDS_Unique_Identifier"
    ,"Local_Patient_Identifier"
    ,"Provider_Reference_Number"
    ,"Financial_Year"
    ,"Financial_Month"
    ,"Applicable_Costing_Period"

    -- Patient detail fields
    ,"Patient_Type"
    ,"Age_At_Arrival"
    ,"Index_Of_Multiple_Deprivation_Decile"
    ,"Commissioner_Code_From_Postcode"
    ,"Gender"
    ,"Ethnic_Category"
    ,"GP_Code"
    ,"GP_Practice_Code"

    -- Attendance fields
    ,"Provider_Code"
    ,"Site_Code"
    ,"Commissioner_Code"
    ,"Postcode_District_Usual_Address"
    ,"LSOA_11_Usual_Address"
    ,"Department_Type"
    ,"Arrival_Date"
    ,"Arrival_Time"
    ,"Arrival_Mode"
    ,"Attendance_Category"
    ,"Arrival_Planned"
    ,"Ambulance_Incident_Number"
    ,"Conveying_Ambulance_Trust_Code"
    ,"Ambulance_Care_Contact_Identifier"
    ,"Attendance_Source"
    ,"Attendance_Source_For_Organisation_Site_Identifier"
    ,"Initial_Assessment_Date"
    ,"Initial_Assessment_Time"
    ,"Initial_Assessment_Time_Since_Arrival"
    ,"Expected_Treatment_Time"
    ,"Seen_For_Treatment_Date"
    ,"Seen_For_Treatment_Time"
    ,"Seen_For_Treatment_Time_Since_Arrival"
    ,"Conclusion_Date"
    ,"Conclusion_Time"
    ,"Conclusion_Time_Since_Arrival"
    ,"Departure_Date"
    ,"Departure_Time"
    ,"Departure_Time_Since_Arrival"
    ,"Decided_To_Admit_Date"
    ,"Decided_To_Admit_Time"
    ,"Decided_To_Admit_Time_Since_Arrival"
    ,"Decided_To_Admit_Treatment_Function_Code"
    ,"Organisation_Site_Identifier_Discharge_From_Emergency_Care"
    ,"Discharge_Status"
    ,"Discharge_Destination"
    ,"Discharge_Follow_Up"
    ,"Discharge_Information_Given"
    ,"Clinically_Ready_To_Proceed_Time"
    ,"Clinically_Ready_To_Proceed_Time_Since_Arrival"

    -- Attendance referred to fields
    ,"Referred_To_Service"
    ,"Referred_To_Service_Assessment_Date"
    ,"Referred_To_Service_Assessment_Time"

    -- Clinical fields
    ,"Chief_Complaint"
    ,"Chief_Complaint_Is_Injury_Related"
    ,"Acuity"
    ,"HRG_Code"
    ,"National_Tariff_Excluded"
    ,"National_Tariff"
    ,"National_Tariff_Final_Price"
    ,"MFF_Factor"
    ,"MFF_Adjustment"
    ,"Injury_Intent"
    ,"Injury_Mechanism"
    ,"Place_Of_Injury"
    ,"Injury_Date"
    ,"Injury_Time"
    ,"Disease_Notification"

    -- Clinical diagnosis fields
    ,"Primary_Diagnosis"
    ,"Secondary_Diagnosis1"
    ,"Secondary_Diagnosis2"
    ,"Secondary_Diagnosis3"
    ,"Secondary_Diagnosis4"
    ,"Secondary_Diagnosis5"
    ,"Secondary_Diagnosis6"
    ,"Secondary_Diagnosis7"
    ,"Secondary_Diagnosis8"
    ,"Secondary_Diagnosis9"
    ,"Secondary_Diagnosis10"
    ,"Secondary_Diagnosis11"
    ,"Secondary_Diagnosis12"

    -- Alcohol related injury field
    ,"Injury_Alcohol_Drug_Involvement"

    -- Clinical investigation fields
    ,"Clinical_Investigation1"
    ,"Clinical_Investigation2"
    ,"Clinical_Investigation3"
    ,"Clinical_Investigation4"
    ,"Clinical_Investigation5"
    ,"Clinical_Investigation6"
    ,"Clinical_Investigation7"
    ,"Clinical_Investigation8"
    ,"Clinical_Investigation9"
    ,"Clinical_Investigation10"
    ,"Clinical_Investigation11"
    ,"Clinical_Investigation12"

    -- Clinical treatment fields
    ,"Treatment1"
    ,"Treatment2"
    ,"Treatment3"
    ,"Treatment4"
    ,"Treatment5"
    ,"Treatment6"
    ,"Treatment7"
    ,"Treatment8"
    ,"Treatment9"
    ,"Treatment10"
    ,"Treatment11"
    ,"Treatment12"

    -- Comorbidity fields
    ,"Comorbidity1"
    ,"Comorbidity2"
    ,"Comorbidity3"
    ,"Comorbidity4"
    ,"Comorbidity5"
    ,"Comorbidity6"
    ,"Comorbidity7"
    ,"Comorbidity8"
    ,"Comorbidity9"
    ,"Comorbidity10"

    -- Mental health legal status field
    ,"Mental_Health_Legal_Status_Classification"

    -- Referral fields
    ,"Patient_Pathway_Identifier"
    ,"Referral_To_Treatment_Period_Status"
    ,"Referral_To_Treatment_Period_Start_Date"
    ,"Referral_To_Treatment_Period_End_Date"
    ,"Waiting_Time_Measurement_Type"

    -- zDerived fields
    ,"zProvider_Code"
    ,ZBUSINESSRULE

from {{ ref('stg_ecds_consolidated') }}