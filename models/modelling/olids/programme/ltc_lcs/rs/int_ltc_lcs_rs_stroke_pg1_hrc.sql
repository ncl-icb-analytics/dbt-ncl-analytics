-- LTC LCS DBT extract
--
--  Stroke - HRC - Overaching
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
--



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
    ,


-- concept codes for episode
    episodecodes as 
    (
    select distinct person_id, mapped_concept_code
 from  DATA_LAKE.OLIDS.OBSERVATION A
join DATA_LAKE.OLIDS.CONCEPT_MAP B
ON A.EPISODICITY_CONCEPT_ID = B.SOURCE_CODE_ID
where B.source_display  in ('New','Flare Up','First')
    )
    
-- Rule 1: with date in last 30 days 
-- AND Episode = First, New, Flare Up
select distinct VS1.PERSON_ID, 'VS1' as rule
from on_stroketia_reg_pg1_hrc_vs1 VS1
inner join stroketia_reg DR
on DR.PERSON_ID = VS1.PERSON_ID
inner join episodecodes EPI
on VS1.PERSON_ID = EPI.PERSON_ID
 where DATEDIFF(day, VS1.clinical_effective_date, CURRENT_DATE())<=30 
-- OR Problem Significance = Significant
-- CANT DO CURRENTLY

-- UNION VS2
union
select distinct VS2.PERSON_ID , 'VS2' as rule
from on_stroketia_reg_pg1_hrc_vs2 VS2
inner join stroketia_reg DR
on DR.PERSON_ID = VS2.PERSON_ID
inner join episodecodes EPI
on VS2.PERSON_ID = EPI.PERSON_ID
 where DATEDIFF(day, VS2.clinical_effective_date, CURRENT_DATE())<=30 