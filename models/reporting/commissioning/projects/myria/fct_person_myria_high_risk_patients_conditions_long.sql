SELECT
PATIENT_ID,
HOSPITAL_NUMBER,
CONDITION,
FLAG AS CONDITION_COUNT
from DEV__REPORTING.COMMISSIONING_REPORTING.fct_person_myria_high_risk_patients
UNPIVOT (flag for condition in (heart_failure as "Heart Failure",
                                copd as "COPD",
                                dementia as "Dementia",
                                end_stage_renal_failure as "End Stage Renal Failure",
                                severe_interstitial_lung_disease as "Severe Interstitial Lung Disease",
                                parkinsons_disease as "Parkinson's Disease",
                                chronic_kidney_disease as "Chronic Kidney Disease",
                                liver_failure as "Liver Failure",
                                alcohol_dependence as "Alcohol Dependence",
                                bronchiectasis as "Bronchiectasis",
                                atrial_fibrillation as "Atrial Fibrillation",
                                cerebrovascular_disease as "Cerebrovascular Disease",
                                peripheral_vascular_disease as "Peripheral Vascular Disease",
                                pulmonary_heart_disease as "Pulmonary Heart Disease",
                                coronary_heart_disease as "Coronary Heart Disease",
                                osteoporosis as "Osteoporosis",
                                rheumatoid_arthritis as "Rheumatoid Arthritis",
                                chronic_liver_disease as "Chronic Liver Disease",
                                --- The following are not high risk 'conditions' but helpful flags for risk and multimorbidity
                                hypertension as "Hypertension",
                                frailty_falls as "Frailty/Falls")
        )
WHERE
flag = 1
AND RFL_COUNT > 0
AND LOCAL_AUTHORITY IN ('Barnet','Enfield')