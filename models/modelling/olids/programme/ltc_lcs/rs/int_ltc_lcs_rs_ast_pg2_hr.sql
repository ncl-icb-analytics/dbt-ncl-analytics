
-- LTC LCS DBT extract
--
--  Asthma - HR - Overarching
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--

with
  HR_subgroup as (
--Rule 1: Exclude patients from Priority Group 1 (HRC)
select DR.person_id from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ASTHMA_REGISTER DR
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_ast_pg1_hrc HRC
on DR.person_id = HRC.person_id
where HRC.person_id is null
)
,
--Rule 2: Emergency hospital admission for asthma code in last 12 months
 on_asthma_adult_reg_pg2_hr_vs1 as (
       {{ get_ltc_lcs_observations_latest("'on_asthma_adult_reg_pg2_hr_vs1'") }}
    )
,

--Rule 6: Fostair inhalers in last 6 months
on_asthma_adult_reg_pg2_hr_vs7 as
 (
       {{ get_ltc_lcs_medication_orders ("'on_asthma_adult_reg_pg2_hr_vs7'") }} 
      )

select distinct HR.person_ID from HR_subgroup HR
left join  on_asthma_adult_reg_pg2_hr_vs1 VS1
on HR.person_id = VS1.person_id

left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_ast_pg2_hr_r3 R3
on HR.person_id = R3.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_ast_pg2_hr_r4 R4
on HR.person_id = R4.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_ast_pg2_hr_r5 R5
on HR.person_id = R5.person_id
left join  on_asthma_adult_reg_pg2_hr_vs7 VS7
on HR.person_id = VS7.person_id
left join  DEV__MODELLING.OLIDS_PROGRAMME.int_ltc_lcs_rs_ast_pg2_hr_r7 R7
on HR.person_id = R7.person_id


where 
--Rule 2: Emergency hospital admission for asthma code in last 12 months
( VS1.person_ID is not null
and
    DATEDIFF(day, VS1.clinical_effective_date, CURRENT_DATE())<=365.25
)
or
--Rule 3: Tiotropium in last 6 months OR Montelukast in last 12 months OR Theophylline/Aminophylline in last 12 months
( R3.person_ID is not null)

--Rule 4: ≥3 issues of oral Prednisolone in last 12 months
or
( R4.person_ID is not null)

--Rule 5: ≥3 issues of antibiotics (Amoxicillin, Doxycycline) in last 12 months
or
( R5.person_ID is not null)

--Rule 6: Fostair inhalers in last 6 months
or
( VS7.person_ID is not null
and
    DATEDIFF(day, VS7.order_date, CURRENT_DATE())<=182.6 -- 6 months
)
--Rule 7: Beclometasone/Formoterol combination inhalers AND Beclazone in last 6 months
or
( R7.person_ID is not null)



