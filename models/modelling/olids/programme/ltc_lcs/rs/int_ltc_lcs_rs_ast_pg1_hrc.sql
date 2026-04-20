-- LTC LCS DBT extract
--
--  Asthma - HRC - Overarching
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--
-- pull together HRC rules



with asthma_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ASTHMA_REGISTER
    )  
,

      on_asthma_adult_reg_pg1_hrc_vs1 as (
       {{	get_ltc_lcs_medication_orders_latest ("'on_asthma_adult_reg_pg1_hrc_vs1'") }} 
      )


select DR.person_ID from asthma_reg DR
join on_asthma_adult_reg_pg1_hrc_vs1 VS1
on DR.person_id = VS1.person_ID

   where datediff(d,VS1.order_date,current_date())  <365.25

