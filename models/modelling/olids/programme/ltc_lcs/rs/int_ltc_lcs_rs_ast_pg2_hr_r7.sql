-- LTC LCS DBT extract
--
--  Asthma - HR - Rule 7
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with asthma_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ASTHMA_REGISTER
    )  
,
-- Rule 7: Beclometasone/Formoterol combination inhalers AND Beclazone in last 6 months


--Beclometasone/Formoterol combination inhalers
   on_asthma_adult_reg_pg2_hr_vs8 as (
       {{	get_ltc_lcs_medication_orders_latest ("'on_asthma_adult_reg_pg2_hr_vs8'") }} 
      )
,
      --Beclazone  
       on_asthma_adult_reg_pg2_hr_vs9 as (
       {{	get_ltc_lcs_medication_orders_latest ("'on_asthma_adult_reg_pg2_hr_vs9'") }} 
      )

select distinct DR.person_id,
VS8.person_id as VS8,
VS8.order_date as VS8_date,
VS9.person_id as VS9,
VS9.order_date as VS9_date

 from 
asthma_reg DR 
join on_asthma_adult_reg_pg2_hr_vs8 VS8 
on DR.person_id = VS8.person_id
join  on_asthma_adult_reg_pg2_hr_vs9 VS9 
on DR.person_id = VS9.person_id
where     DATEDIFF(day, VS8.order_date, CURRENT_DATE())<=182.6 -- 6 months
and     DATEDIFF(day, VS9.order_date, CURRENT_DATE())<=182.6 -- 6 months