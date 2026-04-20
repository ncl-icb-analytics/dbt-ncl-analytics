-- LTC LCS DBT extract
--
--  ASTHMA CYP - HR - Rule 8
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--



with astCYP_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_CYP_ASTHMA_REGISTER
    )  
,
--Rule 8: Exclude if on Beclometasone in last 12 months AND ≥1 SABA in last 3 months



   on_asthma_cyp_reg_pg2_hr_vs12 as (
       {{	get_ltc_lcs_medication_orders_latest ("'on_asthma_cyp_reg_pg2_hr_vs12'") }} 
      )
,
   on_asthma_cyp_reg_pg2_hr_vs9 as (
       {{	get_ltc_lcs_medication_orders_latest ("'on_asthma_cyp_reg_pg2_hr_vs9'") }} 
      )

-- OUTPUT

select distinct DR.person_ID from astCYP_reg DR
 
join on_asthma_cyp_reg_pg2_hr_vs12 vs12
on DR.person_ID = vs12.person_ID


join on_asthma_cyp_reg_pg2_hr_vs9 vs9
on DR.person_ID = vs9.person_ID

where DATEDIFF(day, vs12.order_date, CURRENT_DATE())<=365.25
and DATEDIFF(day, vs9.order_date, CURRENT_DATE())<=91.3