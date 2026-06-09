-- LTC LCS DBT extract
--
--  ASTHMA CYP - HR - Rule 6
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--



with astCYP_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_CYP_ASTHMA_REGISTER
    )  
,
--Rule 6: ≥3 issues of SABA (Salbutamol, Terbutaline) in last 3 months



   on_asthma_cyp_reg_pg2_hr_vs9 as (
       {{	get_ltc_lcs_medication_orders ("'on_asthma_cyp_reg_pg2_hr_vs9'") }} 
      )
,
-- take count
vs9_count as(
    select vs9.person_id, count(*) as num
    from on_asthma_cyp_reg_pg2_hr_vs9 vs9
     where   DATEDIFF(day, vs9.order_date, CURRENT_DATE())<=91.3 -- 3months
    group by vs9.person_id
)

select distinct DR.person_ID, num from astCYP_reg DR
 
left join vs9_count
on DR.person_ID = vs9_count.person_ID
where vs9_count.num >=3


