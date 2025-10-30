-- Raw layer model for c_ltcs.PERSON_SUMMARY
-- Source: "DEV__PUBLISHED_REPORTING__DIRECT_CARE"."C_LTCS"
-- Description: C-LTCS tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "PATIENT_ID" as patient_id,
    "PCN" as pcn,
    "ELIGIBLE" as eligible,
    "FEMALE" as female,
    "AGE" as age,
    "WEIGHT" as weight,
    "DISEASE_PROB" as disease_prob,
    "HAS_DISEASE_1" as has_disease_1,
    "BIOMARKER_1" as biomarker_1,
    "BIOMARKER_2" as biomarker_2,
    "CV_EVENTS" as cv_events,
    "AV_EVENTS_PER_YEAR" as av_events_per_year,
    "SCORE" as score,
    "RANK_PCT_EVENTS" as rank_pct_events,
    "RANK_PCT_CV_EV" as rank_pct_cv_ev
from {{ source('c_ltcs', 'PERSON_SUMMARY') }}
