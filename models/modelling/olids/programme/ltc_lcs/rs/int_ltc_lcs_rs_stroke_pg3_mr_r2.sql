-- LTC LCS DBT extract
--
--  Stroke - MR - Rule 2
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--

-- Rule 2: Stroke/TIA codes (Refset Id: 999005531000230105, 999005291000230109) dated >1 year before search date (if passed, go to next rule)


with stroketia_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_STROKE_TIA_REGISTER
    )  
,
    
--Rule 1: Stroke/TIA clinical codes (Refset Id: 999005531000230105, 999005291000230109) 
-- valueset 999005531000230105 is on_stroketia_reg_pg1_hrc_vs1
  on_stroketia_reg_pg1_hrc_vs1 as (
       {{ get_ltc_lcs_observations("'on_stroketia_reg_pg1_hrc_vs1'") }}
    )

    ,
-- valueset 999005291000230109 is on_stroketia_reg_pg1_hrc_vs2
  on_stroketia_reg_pg1_hrc_vs2 as (
       {{ get_ltc_lcs_observations("'on_stroketia_reg_pg1_hrc_vs2'") }}
    )
    


select DR.person_id, 
VS1.clinical_effective_date as VS1_date,
VS2.clinical_effective_date as VS2_date,
 from stroketia_reg DR
left join 
on_stroketia_reg_pg1_hrc_vs1 VS1
on DR.person_id = VS1.person_id
left join 
on_stroketia_reg_pg1_hrc_vs2 VS2
on DR.person_id = VS2.person_id

where 
(
    DATEDIFF(day, VS1.clinical_effective_date, CURRENT_DATE())<365.25  -- <1 year
    or 
    VS1.clinical_effective_date is null
)
 and
(
    DATEDIFF(day, VS2.clinical_effective_date, CURRENT_DATE())>=365.25  -- <1 year
    or 
    VS2.clinical_effective_date is null
)
and not (VS1.clinical_effective_date is null
and VS2.clinical_effective_date is null)
