-- LTC LCS DBT extract
--
--  Asthma - HR - Rule 4
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with asthma_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ASTHMA_REGISTER
    )  
,
--Rule 4: ≥3 issues of oral Prednisolone in last 12 months

   on_asthma_adult_reg_pg2_hr_vs5 as (
       {{	get_ltc_lcs_medication_orders ("'on_asthma_adult_reg_pg2_hr_vs5'") }} 
      )
,
-- take count
VS5_count as(
    select VS5.person_id, count(*) as num
    from on_asthma_adult_reg_pg2_hr_vs5 VS5
     where   DATEDIFF(day, VS5.order_date, CURRENT_DATE())<=365.25
    group by VS5.person_id
)

select distinct DR.person_ID, num from asthma_reg DR
 
left join VS5_count
on DR.person_ID = VS5_count.person_ID
where VS5_count.num >=3


