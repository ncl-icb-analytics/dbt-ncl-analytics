-- LTC LCS DBT extract
--
--  Asthma - MR - Rule 7
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
-- 1.1              17/3/26     Colin Styles    Updated code following QA


-- Rule 7: ≥3 issues of SABA in last 12 months AND NOT on ICS in last 6 months



with asthma_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ASTHMA_REGISTER
    )  ,

    -- SABA
   on_asthma_adult_reg_pg3_mr_vs4 as (
       {{	get_ltc_lcs_medication_orders ("'on_asthma_adult_reg_pg3_mr_vs4'") }} 
      )
,
    --Beclometasone
   on_asthma_adult_reg_pg3_mr_vs7 as (
       {{	get_ltc_lcs_medication_orders_latest ("'on_asthma_adult_reg_pg3_mr_vs7'") }} 
      )
      ,
--  SABA_COUNT
   vs4_count as (
   select VS4.person_id, count(*) as num
   from on_asthma_adult_reg_pg3_mr_vs4 VS4
     where 
        DATEDIFF(day, VS4.order_date, CURRENT_DATE())<365.256 -- 12 months
        group by VS4.person_id
   )
    

    -- OUTPUT

select distinct DR.person_ID, 

from asthma_reg DR
 
 left join vs4_count VS4_count
on DR.person_ID = VS4_count.person_ID
left join on_asthma_adult_reg_pg3_mr_vs7 VS7
on DR.person_ID = VS7.person_ID

where
    -- Rule 7: ≥3 issues of SABA in last 12 months
( 
    (   VS4_count.person_ID is not null
        and VS4_Count.num >=3

    )


and 
-- AND NOT on ICS (Beclometasone Dipropionate) in last 6 months
    (       VS7.person_ID is null
        or
        DATEDIFF(day, VS7.order_date, CURRENT_DATE())>182.6    --- exclude if within 6 months
    )

)