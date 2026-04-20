-- LTC LCS DBT extract
--
--  Asthma - MR - Overarching
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--

-- pull together MR rules

--Rule 1: Excludes Priority Groups 1 (HRC) and 2 (HR)
with
  MR_subgroup as (

select DR.person_id from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ASTHMA_REGISTER DR
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_ast_pg1_hrc HRC
on DR.person_id = HRC.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_ast_pg2_hr HR
on DR.person_id = HR.person_id
where HRC.person_id is null
and HR.person_id is null

)

,
--Rule 2: Acute exacerbation of asthma code in last 12 months

 on_asthma_adult_reg_pg3_mr_vs1  as (
       {{ get_ltc_lcs_observations_latest("'on_asthma_adult_reg_pg3_mr_vs1'") }}
    )
,
--Rule 3: Prednisolone issue in last 12 months
on_asthma_adult_reg_pg3_mr_vs2 as
 (
       {{ get_ltc_lcs_medication_orders_latest ("'on_asthma_adult_reg_pg3_mr_vs2'") }} 
      )

select distinct MR.person_ID from MR_subgroup MR
left join  on_asthma_adult_reg_pg3_mr_vs1 VS1
on MR.person_id = VS1.person_id
left join on_asthma_adult_reg_pg3_mr_vs2 VS2
on MR.person_id = VS2.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_ast_pg3_mr_r4 R4
on MR.person_id = R4.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_ast_pg3_mr_r5 R5
on MR.person_id = R5.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_ast_pg3_mr_r6 R6
on MR.person_id = R6.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_ast_pg3_mr_r7 R7
on MR.person_id = R7.person_id


--Rule 2: Acute exacerbation of asthma code in last 12 months
where
( VS1.person_ID is not null
and
    DATEDIFF(day, VS1.clinical_effective_date, CURRENT_DATE())<=365.25
)

--Rule 3: Prednisolone issue in last 12 months
or 
( VS2.person_ID is not null
and
    DATEDIFF(day, VS2.order_date, CURRENT_DATE())<=365.25
)

--Rule 4: ≥2 issues of antibiotics in last 12 months
or
( R4.person_ID is not null)

--Rule 5: ≥6 issues of SABA (Salbutamol, Terbutaline) in last 12 months
or
( R5.person_ID is not null)

--Rule 6: LABA medications (Bambuterol, Atimos, Foradil) in last 6 months AND NOT on ICS (Beclometasone Dipropionate) in last 6 months
or
( R6.person_ID is not null)

--Rule 7: ≥3 issues of SABA in last 12 months AND NOT on ICS in last 6 months
or
( R7.person_ID is not null)





