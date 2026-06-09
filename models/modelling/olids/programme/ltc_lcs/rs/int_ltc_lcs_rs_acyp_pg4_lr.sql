
-- LTC LCS DBT extract
--
--  ASTHMA CYP - LR - Overarching
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with
  LR_subgroup as (
--Rule 1: Exclude patients from Priority Group 1 (HRC)
select DR.person_id from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_CYP_ASTHMA_REGISTER DR
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_acyp_pg2_hr HR
on DR.person_id = HR.person_id
where HR.person_id is null
)


select distinct LR.person_ID from LR_subgroup LR
