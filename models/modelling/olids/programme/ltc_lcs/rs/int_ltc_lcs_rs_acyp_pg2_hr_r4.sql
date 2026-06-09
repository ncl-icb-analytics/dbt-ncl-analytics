-- LTC LCS DBT extract
--
--  ASTHMA CYP - HR - Rule 4
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--



with astCYP_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_CYP_ASTHMA_REGISTER
    )  
,
--Rule 4: ≥2 issues of Prednisolone in last 12 months


   on_asthma_cyp_reg_pg2_hr_vs7 as (
       {{	get_ltc_lcs_medication_orders ("'on_asthma_cyp_reg_pg2_hr_vs7'") }} 
      )
,
-- take count
vs7_count as(
    select vs7.person_id, count(*) as num
    from on_asthma_cyp_reg_pg2_hr_vs7 vs7
     where   DATEDIFF(day, vs7.order_date, CURRENT_DATE())<=365.25
    group by vs7.person_id
)

select distinct DR.person_ID, num from astCYP_reg DR
 
left join vs7_count
on DR.person_ID = vs7_count.person_ID
where vs7_count.num >=2


