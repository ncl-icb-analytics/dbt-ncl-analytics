-- LTC LCS DBT extract
--
--  Stroke - HR - Overaching
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
-- 1.1              19/3/26     CS              Simplify join logic

-- Rule 1: Exclude patients from Priority Group 1 (HRC)
-- Rule 2: Stroke/TIA codes (Refset Id: 999005531000230105, 999005291000230109) dated 30-365 days before search date with Episode = First, New, Flare Up OR Problem Significance = Significant
-- Rule 3: Stroke/TIA codes dated >1 year before search date (if passed, go to next rule)
-- Rule 4: (Age ≤80, 24hr BP) BP monitoring codes in last 12 months with DBP 1-90 AND SBP 1-140 → exclude if passed (BP controlled)
-- Rule 5: (Age ≤80, Standard BP) Standard BP codes in last 12 months with DBP 1-85 AND SBP 1-135 → exclude if passed (BP controlled)
-- Rule 6: Age >80 years → go to next rule
-- Rule 7: (Age >80, 24hr BP) BP monitoring codes in last 12 months with DBP 1-90 AND SBP 1-150 → exclude if passed (BP controlled)
-- Rule 8: (Age >80, Standard BP) Standard BP codes in last 12 months with DBP 1-85 AND SBP 1-145 → include if failed (BP uncontrolled)





with stroketia_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_STROKE_TIA_REGISTER
    )  
,
  patient_age as (
select distinct person_id, age
from  DEV__REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS
)
,
  HR_subgroup as (
--Rule 1: Exclude patients from Priority Group 1 (HRC)
select DR.person_id, AGE.age from stroketia_reg DR
inner join patient_age AGE
on DR.person_id = AGE.person_id
left join DEV__MODELLING.DBT_DEV.STROKE_HRC_Test_CS HRC
on DR.person_id = HRC.person_id
where HRC.person_id is null
)

-- OUTPUT

select HR.person_id
 from HR_subgroup HR
left join DEV__MODELLING.DBT_DEV.STROKE_HR_Rule2_Test_CS R2
on HR.person_id = R2.person_id
left join DEV__MODELLING.DBT_DEV.STROKE_HR_Rule3_Test_CS R3
on HR.person_id = R3.person_id
left join DEV__MODELLING.DBT_DEV.STROKE_HR_Rule4_Test_CS R4
on HR.person_id = R4.person_id
left join DEV__MODELLING.DBT_DEV.STROKE_HR_Rule4_Test_CS R5
on HR.person_id = R5.person_id
left join DEV__MODELLING.DBT_DEV.STROKE_HR_Rule4_Test_CS R7
on HR.person_id = R7.person_id
left join DEV__MODELLING.DBT_DEV.STROKE_HR_Rule4_Test_CS R8
on HR.person_id = R8.person_id

where
R2.person_ID is not null
or
(
    R3.person_ID is not null  
    and
    R4.person_id is null  -- exclude if included
    and 
    R5.person_id is null
    and
    (   
        age >80
        or
        (
            R7.person_id is null
            and 
            R8.person_id is null
        )
    )
)
