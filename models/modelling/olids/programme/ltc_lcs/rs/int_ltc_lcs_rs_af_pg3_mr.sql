
-- LTC LCS DBT extract
--
--  AF - MR - Overarching
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--

--Rule 1: Excludes Priority Group 2 (HR) patients
--Rule 2: On anticoagulants in last 6 months OR age >65 OR Cockcroft-Gault 40-60 OR weight ≥50kg OR Rockwood Frailty 1-4 (fit to vulnerable) OR latest Frailty level 6 (moderately frail)
--Rules 3-4: On oral anticoagulants with serum creatinine overdue by >15 months
--Rules 5-7: On DOACs with Cockcroft-Gault 40-60 OR weight 50-60kg OR age ≥75
--Rules 8-9: On DOACs with Rockwood Frailty 1-4 OR latest Frailty score 6-7 in last 6 months
--Rules 10-14: On anticoagulants with various gender-specific conditions (Male with value ≥1, Female with value ≥2) and age >65 with readings before 2 years

with

  MR_subgroup as (
-- Rule 1: Excludes Priority Group 2 (HR) patients
select DR.person_id from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ASTHMA_REGISTER DR
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg2_hr HR
on DR.person_id = HR.person_id
where HR.person_id is null
)


select distinct MR.person_ID from MR_subgroup MR
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg3_mr_r2 R2
on MR.person_id = R2.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg3_mr_r3 R3
on MR.person_id = R3.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg3_mr_r4 R4
on MR.person_id = R4.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg3_mr_r5 R5
on MR.person_id = R5.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg3_mr_r6 R6
on MR.person_id = R6.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg3_mr_r7 R7
on MR.person_id = R7.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg3_mr_r8 R8
on MR.person_id = R8.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg3_mr_r9 R9
on MR.person_id = R9.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg3_mr_r10 R10
on MR.person_id = R10.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg3_mr_r11 R11
on MR.person_id = R11.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg3_mr_r12 R12
on MR.person_id = R12.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg3_mr_r13 R13
on MR.person_id = R13.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_af_pg3_mr_r14 R14
on MR.person_id = R14.person_id

where

R2.PERSON_ID is not null
and 
(
    R3.PERSON_ID is not null
            or
    R4.PERSON_ID is not null
            or
    R5.PERSON_ID is not null
            or
    R6.PERSON_ID is not null
            or
    R7.PERSON_ID is not null
            or
    R8.PERSON_ID is not null
            or
    R9.PERSON_ID is not null
            or 
    (
        R10.PERSON_ID is null  -- exclude if included
        and
        (
        R11.PERSON_ID is not null
          or
        R12.PERSON_ID is not null
          or
            (
            R13.PERSON_ID is not null
            and
            R14.PERSON_ID is null  -- exclude if included
            )
        )
    )
)
