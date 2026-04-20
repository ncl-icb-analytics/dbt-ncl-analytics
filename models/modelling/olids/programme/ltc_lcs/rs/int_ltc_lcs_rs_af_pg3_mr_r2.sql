
-- LTC LCS DBT extract
--
--  AF - MR - Rule 2
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--


with 

  patient_age as (
select distinct person_id, age
from  DEV__REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS
)
,

AF_reg as (
    select distinct DR.person_id, AGE.AGE
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_ATRIAL_FIBRILLATION_REGISTER DR
    inner join patient_age AGE
    on DR.person_id = AGE.person_id
    )
,
--Rule 2: On anticoagulants in last 6 months OR age >65 OR Cockcroft-Gault 40-60 OR weight ≥50kg OR Rockwood Frailty 1-4 (fit to vulnerable) OR latest Frailty level 6 (moderately frail)


--On anticoagulants in last 6 months
   on_af_reg_pg3_mr_vs1 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg3_mr_vs1'") }} 
     )
,
   on_af_reg_pg3_mr_vs2 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg3_mr_vs2'") }} 
     )
,
   on_af_reg_pg3_mr_vs3 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg3_mr_vs3'") }} 
     )
,
    on_af_reg_pg3_mr_vs4 as (
     {{	get_ltc_lcs_medication_orders_latest ("'on_af_reg_pg3_mr_vs4'") }} 
     )
,



--Cockcroft-Gault 40-60
   on_af_reg_pg3_mr_vs5 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg3_mr_vs5'") }} 
     )
,

-- weight ≥50kg
   on_af_reg_pg3_mr_vs6 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg3_mr_vs6'") }} 
     )
,
--  Rockwood Frailty 1-4 (fit to vulnerable) 
   on_af_reg_pg3_mr_vs7 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg3_mr_vs7'") }} 
     )
,
--  latest Frailty level 6 (moderately frail)
   on_af_reg_pg3_mr_vs8 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg3_mr_vs8'") }} 
     )
,
-- frailty 6-7 within six months
   on_af_reg_pg3_mr_vs9 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg3_mr_vs9'") }} 
     )
,

-- CHADVASC 
   on_af_reg_pg3_mr_vs10 as (
     {{	get_ltc_lcs_observations_latest ("'on_af_reg_pg3_mr_vs10'") }} 
     )

--


select DR.person_id from AF_reg DR

left join on_af_reg_pg3_mr_vs1 VS1
on DR.person_id = VS1.person_id
left join on_af_reg_pg3_mr_vs2 VS2
on DR.person_id = VS2.person_id
left join on_af_reg_pg3_mr_vs3 VS3
on DR.person_id = VS3.person_id
left join on_af_reg_pg3_mr_vs4 VS4
on DR.person_id = VS4.person_id
left join on_af_reg_pg3_mr_vs5 VS5
on DR.person_id = VS5.person_id
left join on_af_reg_pg3_mr_vs6 VS6
on DR.person_id = VS6.person_id
left join on_af_reg_pg3_mr_vs7 VS7
on DR.person_id = VS7.person_id
left join on_af_reg_pg3_mr_vs8 VS8
on DR.person_id = VS8.person_id
left join on_af_reg_pg3_mr_vs9 VS9
on DR.person_id = VS9.person_id
left join on_af_reg_pg3_mr_vs10 VS10
on DR.person_id = VS10.person_id

where
--On anticoagulants in last 6 months
(VS1.person_id is not null
and
  DATEDIFF(day, VS1.order_date, CURRENT_DATE())<=182.6
)
or
(VS2.person_id is not null
and
  DATEDIFF(day, VS2.order_date, CURRENT_DATE())<=182.6
)
or
(VS3.person_id is not null
and
  DATEDIFF(day, VS3.order_date, CURRENT_DATE())<=182.6
)
or
(VS4.person_id is not null
and
  DATEDIFF(day, VS4.order_date, CURRENT_DATE())<=182.6
)

-- OR age >65 
or AGE >65


--Cockcroft-Gault 40-60
or
(VS5.person_id is not null
and VS5.result_value >= 40
and VS5.result_value < 60
)

-- weight ≥50kg
or
(VS6.person_id is not null
and VS6.result_value >= 50
)
or
--  Rockwood Frailty 1-4 (fit to vulnerable) or
(VS7.person_id is not null
and VS7.result_value = 6
)
or
--  latest Frailty level 6 (moderately frail)
(VS8.person_id is not null
and VS8.result_value = 6
)
or

(VS9.person_id is not null
and VS9.result_value = 6
and   DATEDIFF(day, VS9.clinical_effective_date, CURRENT_DATE())<=182.6
)

-- CHADVASC 
or 
(VS10.person_id is not null
and VS10.result_value >=1
)