-- LTC LCS DBT extract
--
--  NAFLD - MR - Overarching
--
-- Version          Date        Author          Comments
-- 1.0              3/3/26      Colin Styles    Initial version
-- 1.1              18/3/26     CS              Updated following QA


-- note no partition by required as all values within last 3 years to be tested
with on_nafld_reg_pg3_mr_vs1 as (
        {{ get_ltc_lcs_observations_latest("'on_nafld_reg_pg3_mr_vs1'") }}
),
on_nafld_reg_pg3_mr_vs3 as (
        {{ get_ltc_lcs_observations_latest("'on_nafld_reg_pg3_mr_vs3'") }}
)

select distinct NR.person_id from DEV__MODELLING.DBT_DEV.NAFLD_Register_Test_CS NR

left outer join on_nafld_reg_pg3_mr_vs1 VS1
on NR.PERSON_ID = VS1.PERSON_ID

left outer join on_nafld_reg_pg3_mr_vs3 VS3
on NR.PERSON_ID = VS3.PERSON_ID
where
    (VS1.PERSON_ID is not null
    and
    DATEDIFF(day, VS1.clinical_effective_date, CURRENT_DATE())<=365.25 * 3   -- within past 3 years
    and
    -- Rule 1: Latest NAFLD fibrosis score >3.25 in last 3 years
    VS1.result_value  >3.25 
    )
or
-- Rule 2: Latest NAFLD fibrosis score 1.3-3.25 in last 3 years AND ELF score ≥9.8
    (VS1.PERSON_ID is not null
    and
    DATEDIFF(day, VS1.clinical_effective_date, CURRENT_DATE())<=365.25 * 3   -- within past 3 years
    and
    VS1.result_value  >1.3   --  rule says between
    and
    VS1.result_value  <=3.25 
    and
    VS3.PERSON_ID is not null
    and
    DATEDIFF(day, VS3.clinical_effective_date, CURRENT_DATE())<=365.25 * 3   -- within past 3 years
    and
    VS3.result_value >=9.8
    )




