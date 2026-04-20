-- LTC LCS DBT extract
--
--  ASTHMA CYP - HR - Rule 3
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--



with astCYP_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_CYP_ASTHMA_REGISTER
    )  
,
--Rule 3: >2 acute exacerbation codes in last 12 months


   on_asthma_cyp_reg_pg2_hr_vs6 as (
       {{	get_ltc_lcs_observations ("'on_asthma_cyp_reg_pg2_hr_vs6'") }} 
      )
,
-- take count
VS6_count as(
    select VS6.person_id, count(*) as num
    from on_asthma_cyp_reg_pg2_hr_vs6 VS6
     where   DATEDIFF(day, VS6.clinical_effective_date, CURRENT_DATE())<=365.25
    group by VS6.person_id
)

select distinct DR.person_ID, num from astCYP_reg DR
 
left join VS6_count
on DR.person_ID = VS6_count.person_ID
where VS6_count.num >=2


