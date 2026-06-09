-- LTC LCS DBT extract
--
--  Asthma - LR - Overarching
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--
--
--Rule 1: Excludes Priority Groups 1, 2, and 3
with
  LR_subgroup as (

select DR.person_id from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ASTHMA_REGISTER DR
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_ast_pg1_hrc HRC
on DR.person_id = HRC.person_id
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_ast_pg2_hr HR
on DR.person_id = HR.person_id
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_ast_pg3_mr MR
on DR.person_id = MR.person_id
where HRC.person_id is null
and HR.person_id is null
and MR.person_id is null
)
,
--Rule 2: Beclometasone Dipropionate in last 6 months

 on_asthma_adult_reg_pg4_lr_vs1  as (
       {{ get_ltc_lcs_observations_latest("'on_asthma_adult_reg_pg4_lr_vs1'") }}
    )
,
--Rule 3: ≥1 issue of SABA (Salbutamol, Terbutaline) in last 12 months
on_asthma_adult_reg_pg4_lr_vs2 as
 (
       {{ get_ltc_lcs_medication_orders_latest ("'on_asthma_adult_reg_pg4_lr_vs2'") }} 
      )


-- OUTPUT

select 

distinct LR.person_ID,
VS1.person_id as VS1_ID,
VS1.clinical_effective_date as VS1_CLIN_EFF,
VS2.person_id as VS2_ID,
VS2.order_date as VS2_ORDER

 from LR_subgroup LR


left join on_asthma_adult_reg_pg4_lr_vs1 VS1
on LR.person_id = VS1.person_id
left join on_asthma_adult_reg_pg4_lr_vs2 VS2
on LR.person_id = VS2.person_id


where
--Rule 2: Beclometasone Dipropionate in last 6 months

    (   VS1.person_ID is not null
        and
        DATEDIFF(day, VS1.clinical_effective_date, CURRENT_DATE())<=182.6 -- 6 months
    )
-- Rule 3: ≥1 issue of SABA (Salbutamol, Terbutaline) in last 12 months
or

    (   VS2.person_ID is not null
        and
        DATEDIFF(day, VS2.order_date, CURRENT_DATE())<=365.25 -- 12 months
    )
