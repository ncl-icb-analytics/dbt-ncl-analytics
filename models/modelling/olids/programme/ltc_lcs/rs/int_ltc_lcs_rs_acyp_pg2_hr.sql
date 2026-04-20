-- LTC LCS DBT extract
--
--  ASTHMA CYP - HR - Overarching
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--



with astCYP_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_CYP_ASTHMA_REGISTER
    )  
,
 on_asthma_cyp_reg_pg2_hr_vs5 as (
       {{ get_ltc_lcs_observations_latest("'on_asthma_cyp_reg_pg2_hr_vs5'") }}
    )
,
-- Rule 1: Child on protection register OR Child in need (latest code = "in need")
--
      on_cypcare1 as     (
       {{	get_ltc_lcs_observations ("'on_asthma_cyp_reg_pg2_hr_vs1'") }} 
      )
      ,
      on_cypcare2 as     (
       {{	get_ltc_lcs_observations ("'on_asthma_cyp_reg_pg2_hr_vs2'") }} 
      )
      ,
      on_cypcare3 as     (
       {{	get_ltc_lcs_observations ("'on_asthma_cyp_reg_pg2_hr_vs3'") }} 
      )
      ,     
       on_cypcare4 as     (
       {{	get_ltc_lcs_observations ("'on_asthma_cyp_reg_pg2_hr_vs4'") }} 
      )
      , 
      on_asthma_cyp_reg_pg2_hr_vs1234 as (
      select * from   on_cypcare1 
      union select * from on_cypcare2 
      union select * from on_cypcare3 
      union select * from on_cypcare4
      )
,
-- OUTPUT

--Rule 5: Theophylline/Aminophylline in last 12 months
  on_asthma_cyp_reg_pg2_hr_vs8 as (
     {{	get_ltc_lcs_medication_orders ("'on_asthma_cyp_reg_pg2_hr_vs8'") }} 
     )



select distinct DR.person_id from astCYP_reg DR
left join on_asthma_cyp_reg_pg2_hr_vs1234 R1
on DR.person_id = R1.person_id
left join on_asthma_cyp_reg_pg2_hr_vs5 VS5
on DR.person_id = VS5.person_id
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_acyp_pg2_hr_r3 R3
on DR.person_id = R3.person_id
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_acyp_pg2_hr_r4 R4
on DR.person_id = R4.person_id
left join on_asthma_cyp_reg_pg2_hr_vs5 VS8
on DR.person_id = VS8.person_id
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_acyp_pg2_hr_r6 R6
on DR.person_id = R6.person_id
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_acyp_pg2_hr_r7 R7
on DR.person_id = R7.person_id
left join DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_acyp_pg2_hr_r8 R8
on DR.person_id = R8.person_id

where
-- Rule 1: Child on protection register OR Child in need (latest code = "in need")
(
    R1.person_id is not null

or     
--Rule 2: Emergency asthma admission/visit in last 12 months
    (
    VS5.person_id is not null
    and
    DATEDIFF(day, VS5.clinical_effective_date, CURRENT_DATE())<=365.25
    )


--Rule 3: >2 acute exacerbation codes in last 12 months
or R3.person_id is not null

--Rule 4: ≥2 issues of Prednisolone in last 12 months
or R4.person_id is not null

--Rule 5: Theophylline/Aminophylline in last 12 months
 or
    (
    VS8.person_id is not null
    and
    DATEDIFF(day, VS8.clinical_effective_date, CURRENT_DATE())<=365.25
    )


--Rule 6: ≥3 issues of SABA (Salbutamol, Terbutaline) in last 3 months
or R6.person_id is not null

--Rule 7: LABA (Bambuterol, Atimos, Foradil) in last 12 months
or R7.person_id is not null

--Rule 8: Exclude if on Beclometasone in last 12 months AND ≥1 SABA in last 3 months
)

AND R8.person_id is null