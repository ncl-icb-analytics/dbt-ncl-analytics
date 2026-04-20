-- LTC LCS DBT extract
--
--  Stroke - MR - Overaching
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
-- 1.1              19/3/26     CS              Simplified join logic


with stroketia_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_STROKE_TIA_REGISTER
    )  
,
-- valueset  Latest Non-HDL cholesterol - is on_stroketia_reg_pg3_mr_vs3
  on_stroketia_reg_pg3_mr_vs3 as (
       {{ get_ltc_lcs_observations_latest("'on_stroketia_reg_pg3_mr_vs3'") }}
    )
    ,
      on_stroketia_reg_pg3_mr_vs4 as (
       {{	get_ltc_lcs_medication_orders ("'on_stroketia_reg_pg3_mr_vs4'") }}
    )
,
      on_stroketia_reg_pg3_mr_vs5 as (
       {{	get_ltc_lcs_medication_orders ("'on_stroketia_reg_pg3_mr_vs5'") }}
      )
,
      on_stroketia_reg_pg3_mr_vs6 as (
       {{	get_ltc_lcs_observations_latest ("'on_stroketia_reg_pg3_mr_vs6'") }}
      )

,
      on_statin as (
       select *  FROM ({{ get_medication_orders(cluster_id='STAT_COD') }}) mo
   where datediff(d,mo.order_date,current_date())  <365.25
      )


,  MR_subgroup as (
--Rule 1: Exclude patients from Priority Group 1 or 2
select DR.person_id from stroketia_reg DR
left join DEV__MODELLING.DBT_DEV.STROKE_HRC_Test_CS HRC
on DR.person_id = HRC.person_id
left join DEV__MODELLING.DBT_DEV.STROKE_HR_Test_CS HR
on DR.person_id = HR.person_id
where HRC.person_id is null  -- exclude HRC patients
and 
HR.person_id is null   -- exclude HR patients
)


select distinct MR.person_id
 from MR_subgroup MR
left join DEV__MODELLING.DBT_DEV.STROKE_MR_rule2_Test_CS R2
 on MR.person_id = R2.person_id

left join on_stroketia_reg_pg3_mr_vs3 VS3
 on MR.person_id = VS3.person_id
left join on_stroketia_reg_pg3_mr_vs4 VS4
on MR.person_id = VS4.person_id
left join on_stroketia_reg_pg3_mr_vs5 VS5
on MR.person_id = VS5.person_id
left join on_stroketia_reg_pg3_mr_vs6 VS6
on MR.person_id = VS6.person_id
left join on_statin STATIN
on MR.person_id = STATIN.person_id


 where 
 
R2.person_id is not null
and
(
      VS3.result_value >2.5
      or
      (
            VS4.person_id is null
            and
            VS5.person_id is null
            and 
            (     
                  STATIN.person_id is null
                  or
                  VS6.person_id is not null   --  "Statin declined" code in last 1 year
            )
      )
)



