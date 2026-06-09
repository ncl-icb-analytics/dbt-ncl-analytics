-- LTC LCS DBT extract
--
--  Asthma - MR - Rule 4
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with asthma_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ASTHMA_REGISTER
    )  
,
--Rule 4: ≥2 issues of antibiotics in last 12 months

   on_asthma_adult_reg_pg3_mr_vs3 as (
       {{	get_ltc_lcs_medication_orders ("'on_asthma_adult_reg_pg3_mr_vs3'") }} 
      )
,
-- take count
VS3_count as(
    select VS3.person_id, count(*) as num
    from on_asthma_adult_reg_pg3_mr_vs3 VS3
     where   DATEDIFF(day, VS3.order_date, CURRENT_DATE())<=365.25
    group by VS3.person_id
)

select distinct DR.person_ID, num from asthma_reg DR
left join VS3_count
on DR.person_ID = VS3_count.person_ID
where VS3_count.num >=2


