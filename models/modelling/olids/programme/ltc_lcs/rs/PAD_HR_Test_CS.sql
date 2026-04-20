

-- LTC LCS DBT extract
--
--  PAD - HR - Overarching
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--
--Rule 1: Excludes Priority Group 1 (HRC)
--Rule 2: Peripheral ischaemia codes dated 90-365 days before search date
--Rule 3: Peripheral ischaemia codes before 12 months (go to next rule if passed)
--Rule 4: BP monitoring codes (Refset) in last 12 months, then for same date:
--Rule 5: 24hr BP monitoring codes (Refset) in last 12 months, then for same date:
--Rule 6: Age >80 years (go to next rule if passed, include if failed)
--Rule 7: BP monitoring codes (Refset) in last 12 months, then for same date:
--Rule 8: 24hr BP monitoring codes (Refset) in last 12 months, then for same date:

 
with pad_reg as (
    select distinct DR.person_id
     from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_PAD_REGISTER DR
     --Rule 1: Excludes Priority Group 1 (HRC)
       left join DEV__MODELLING.DBT_DEV.PAD_HRC_Test_CS HRC
    on DR.person_id = HRC.person_id
    where HRC.person_id is null
    )  


--OUTPUT

select distinct DR.person_id from pad_reg DR
left join DEV__MODELLING.DBT_DEV.PAD_HR_Rule2_Test_CS R2
on DR.person_id = R2.person_id
left join DEV__MODELLING.DBT_DEV.PAD_HR_Rule3_Test_CS R3
on DR.person_id = R3.person_id
left join DEV__MODELLING.DBT_DEV.PAD_HR_Rule4_Test_CS R4
on DR.person_id = R4.person_id
left join DEV__MODELLING.DBT_DEV.PAD_HR_Rule5_Test_CS R5
on DR.person_id = R5.person_id
left join DEV__MODELLING.DBT_DEV.PAD_HR_Rule6_Test_CS R6
on DR.person_id = R6.person_id
left join DEV__MODELLING.DBT_DEV.PAD_HR_Rule7_Test_CS R7
on DR.person_id = R7.person_id
left join DEV__MODELLING.DBT_DEV.PAD_HR_Rule8_Test_CS R8
on DR.person_id = R8.person_id

where
(
R2.PERSON_ID is not null
    or 
    (
        R3.PERSON_ID is not null  -- exclude if failed 
        and
        R4.PERSON_ID is null   ---  exclude if PASSED
        and
        R5.PERSON_ID is null   ---  exclude if PASSED
        and
        (
            R6.PERSON_ID is null
            or
            (
                R7.PERSON_ID is null   ---  exclude if PASSED
                and 
                R8.PERSON_ID is null   ---  exclude if PASSED
            )
        )
    )
)