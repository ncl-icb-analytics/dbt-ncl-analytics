-- LTC LCS DBT extract
--
--  Asthma - MR - Rule 5
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--

with asthma_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ASTHMA_REGISTER
    )  
,
--Rule 5: ≥6 issues of SABA (Salbutamol, Terbutaline) in last 12 months


   on_asthma_adult_reg_pg3_mr_vs4 as (
       {{	get_ltc_lcs_medication_orders ("'on_asthma_adult_reg_pg3_mr_vs4'") }} 
      )
,
-- take count
VS4_count as(
    select VS4.person_id, count(*) as num
    from on_asthma_adult_reg_pg3_mr_vs4 VS4
     where   DATEDIFF(day, VS4.order_date, CURRENT_DATE())<=365.25
    group by VS4.person_id
)

select distinct DR.person_ID, num from asthma_reg DR
left join VS4_count
on DR.person_ID = VS4_count.person_ID
where VS4_count.num >=6


