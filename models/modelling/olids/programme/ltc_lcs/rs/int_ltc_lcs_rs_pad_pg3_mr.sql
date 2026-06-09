

-- LTC LCS DBT extract
--
--  PAD - MR - Overarching
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--
--Rule 1: Excludes Priority Groups 1 (HRC) and 2 (HR)
--Rule 2: Peripheral ischaemia codes before 12 months
--Rule 3: Non-HDL cholesterol >2.5
--Rule 4: Repeat High-intensity statins (Atorvastatin 80mg, Lipitor 80mg, Crestor 20-40mg, Rosuvastatin 20-40mg) in last 6 months (excludes if passed - on optimal therapy)
--Rule 5: Any statin medication in last 12 months OR statin clinical code in last 12 months OR statin declined in last 1 year

with pad_reg as (
    select distinct DR.person_id
     from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_PAD_REGISTER DR
     -- Rule 1: Excludes Priority Groups 1 (HRC) and 2 (HR)
       left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_pad_pg1_hrc HRC
    on DR.person_id = HRC.person_id
        left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_pad_pg2_hr HR
    on DR.person_id = HR.person_id
    where 
        HRC.person_id is null
        and
        HR.person_id is null
    )  


--OUTPUT

select distinct DR.person_id from pad_reg DR
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_pad_pg3_mr_r2 R2
on DR.person_id = R2.person_id
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_pad_pg3_mr_r3 R3
on DR.person_id = R3.person_id
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_pad_pg3_mr_r4 R4
on DR.person_id = R4.person_id
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_pad_pg3_mr_r5 R5
on DR.person_id = R5.person_id
where
R2.PERSON_ID is not null
and
    (
    R3.PERSON_ID is not null
    or
        (   
          R4.PERSON_ID is null
          or
          R5.PERSON_ID is null
        )

    )
