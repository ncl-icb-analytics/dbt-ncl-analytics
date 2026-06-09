-- LTC LCS DBT extract
--
--  Asthma - MR - Rule 6
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with asthma_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ASTHMA_REGISTER
    )  
,
--Rule 6: LABA medications (Bambuterol, Atimos, Foradil) in last 6 months 
--AND NOT on ICS (Beclometasone Dipropionate) in last 6 months

 
 --Bambuterol
   on_asthma_adult_reg_pg3_mr_vs5 as (
       {{	get_ltc_lcs_medication_orders_latest ("'on_asthma_adult_reg_pg3_mr_vs5'") }} 
      )
,
-- Atimos, Foradil
   on_asthma_adult_reg_pg3_mr_vs6 as (
       {{	get_ltc_lcs_medication_orders_latest ("'on_asthma_adult_reg_pg3_mr_vs6'") }} 
      )

,
-- AND NOT on ICS (Beclometasone Dipropionate) in last 6 months
    on_asthma_adult_reg_pg3_mr_vs7 as (
       {{	get_ltc_lcs_medication_orders_latest ("'on_asthma_adult_reg_pg3_mr_vs7'") }} 
      )


select distinct DR.person_ID from asthma_reg DR
 
left join on_asthma_adult_reg_pg3_mr_vs5 VS5
on DR.person_ID = VS5.person_ID
left join on_asthma_adult_reg_pg3_mr_vs6 VS6
on DR.person_ID = VS6.person_ID
left join on_asthma_adult_reg_pg3_mr_vs7 VS7
on DR.person_ID = VS7.person_ID

 --Bambuterol
where
( 
    (   VS5.person_ID is not null
        and
        DATEDIFF(day, VS5.order_date, CURRENT_DATE())<=182.6 -- 6 months
    )

    or 
-- Atimos, Foradil
    (       VS6.person_ID is not null
        and
        DATEDIFF(day, VS6.order_date, CURRENT_DATE())<=182.6
    )
and
-- AND NOT on ICS (Beclometasone Dipropionate) in last 6 months
 (   VS7.person_ID is null
        or
    DATEDIFF(day, VS7.order_date, CURRENT_DATE())>182.6
 )
)
