-- LTC LCS DBT extract
--
--  Stroke - HR - Rule 3
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--



with stroketia_reg as (
    select distinct person_id
    from DEV__REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_STROKE_TIA_REGISTER
    )  
,
    
 
--  Stroke/TIA codes (Refset Id: 999005531000230105, 999005291000230109) dated 30-365 days before search date with Episode = First, New, Flare Up OR Problem Significance = Significant

-- valueset 999005531000230105 is on_stroketia_reg_pg1_hrc_vs1
  on_stroketia_reg_pg1_hrc_vs1 as (
       {{ get_ltc_lcs_observations("'on_stroketia_reg_pg1_hrc_vs1'") }}
    )
    ,
-- valueset 999005291000230109 is on_stroketia_reg_pg1_hrc_vs2
  on_stroketia_reg_pg1_hrc_vs2 as (
       {{ get_ltc_lcs_observations("'on_stroketia_reg_pg1_hrc_vs2'") }}
    )
        

select distinct VS1.PERSON_ID, 'VS1' as rule
from on_stroketia_reg_pg1_hrc_vs1 VS1
inner join stroketia_reg DR
on DR.PERSON_ID = VS1.PERSON_ID
 where 
    (
    DATEDIFF(day, VS1.clinical_effective_date, CURRENT_DATE())>365.25
  
    )


-- UNION VS2
union
select distinct VS2.PERSON_ID , 'VS2' as rule
from on_stroketia_reg_pg1_hrc_vs2 VS2
inner join stroketia_reg DR
on DR.PERSON_ID = VS2.PERSON_ID
 where 
    (
    DATEDIFF(day, VS2.clinical_effective_date, CURRENT_DATE())>365.25

    )