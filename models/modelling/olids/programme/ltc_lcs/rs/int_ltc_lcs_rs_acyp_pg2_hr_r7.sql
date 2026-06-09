-- LTC LCS DBT extract
--
--  ASTHMA CYP - HR - Rule 7
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with astCYP_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_CYP_ASTHMA_REGISTER
    )  
,
--Rule 7: LABA (Bambuterol, Atimos, Foradil) in last 12 months

   on_asthma_cyp_reg_pg2_hr_vs10 as (
       {{	get_ltc_lcs_medication_orders_latest ("'on_asthma_cyp_reg_pg2_hr_vs10'") }} 
      )
,
   on_asthma_cyp_reg_pg2_hr_vs11 as (
       {{	get_ltc_lcs_medication_orders_latest ("'on_asthma_cyp_reg_pg2_hr_vs11'") }} 
      )

-- OUTPUT

select distinct DR.person_ID from astCYP_reg DR
 
left join on_asthma_cyp_reg_pg2_hr_vs10 vs10
on DR.person_ID = vs10.person_ID


left join on_asthma_cyp_reg_pg2_hr_vs11 vs11
on DR.person_ID = vs11.person_ID

where 
    (vs10.person_id is not null
    and 
    DATEDIFF(day, vs10.order_date, CURRENT_DATE())<=365.25
)
or 
    (vs11.person_id is not null
    and 
    DATEDIFF(day, vs11.order_date, CURRENT_DATE())<=365.25
)