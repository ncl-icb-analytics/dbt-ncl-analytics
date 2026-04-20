-- LTC LCS DBT extract
--
--  Asthma - HR - Rule 3
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with asthma_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ASTHMA_REGISTER
    )  
,
--Rule 3: Tiotropium in last 6 months OR Montelukast in last 12 months OR 
--Theophylline/Aminophylline in last 12 months
 
 --Tiotropium 
   on_asthma_adult_reg_pg2_hr_vs2 as (
       {{	get_ltc_lcs_medication_orders_latest ("'on_asthma_adult_reg_pg2_hr_vs2'") }} 
      )
,
--Montelukast
   on_asthma_adult_reg_pg2_hr_vs3 as (
       {{	get_ltc_lcs_medication_orders_latest ("'on_asthma_adult_reg_pg2_hr_vs3'") }} 
      )
,
--Theophylline/Aminophylline
   on_asthma_adult_reg_pg2_hr_vs4 as (
       {{	get_ltc_lcs_medication_orders_latest ("'on_asthma_adult_reg_pg2_hr_vs4'") }} 
      )


select distinct DR.person_ID from asthma_reg DR
 
left join on_asthma_adult_reg_pg2_hr_vs2 VS2
on DR.person_ID = VS2.person_ID
left join on_asthma_adult_reg_pg2_hr_vs3 VS3
on DR.person_ID = VS3.person_ID
left join on_asthma_adult_reg_pg2_hr_vs4 VS4
on DR.person_ID = VS4.person_ID


--Tiotropium in last 6 months
where 
(   VS2.person_ID is not null
    and
    DATEDIFF(day, VS2.order_date, CURRENT_DATE())<=186 -- 6 months
)

or 
--Montelukast in last 12 months
(   VS3.person_ID is not null
    and
    DATEDIFF(day, VS3.order_date, CURRENT_DATE())<=365.25
)
or
 --Theophylline/Aminophylline
 (   VS4.person_ID is not null
    and
    DATEDIFF(day, VS4.order_date, CURRENT_DATE())<=365.25
)
