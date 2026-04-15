{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}
/*Person Level Intermediate table holding Case Finding data for people on the SMI register who have had an inpatient spell at NLFT in the last 6 months or who have a current inpatient spell. 
This includes number of checks missed and vulnerabilities. */

--find local patient ids for active submission. One sk_patient_id and one person_id can have multiple local patient ids. MPI_PERSON_ID is the unique identifier in the MHSD record set.
with LOCAL_ID as (
select distinct 
nhs_number_pseudo as sk_patient_id, 
mpi.PERSON_ID as mpi_person_id, 
local_patient_id,  
org_id_prov,
'NLFT' as provider
FROM {{ ref('raw_mhsds_mhs001mpi') }} mpi
--FROM MODELLING.DBT_RAW.RAW_MHSDS_MHS001MPI mpi
INNER JOIN {{ ref('stg_mhsds_activesubmission') }} a ON mpi.uniq_submission_id = a.uniq_submission_id
--inner join MODELLING.DBT_STAGING.STG_MHSDS_ACTIVESUBMISSION a  on mpi.uniq_submission_id = a.uniq_submission_id
INNER JOIN {{ ref('int_smi_population_base') }} smi on TO_VARCHAR(smi.sk_patient_id) = mpi.nhs_number_pseudo
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE smi on TO_VARCHAR(smi.sk_patient_id) = mpi.nhs_number_pseudo
where ORG_ID_PROV in ('G6V2S') --,'TAF') use NLFT code only C&I legacy patients are not found in the NLFT EPR system
and DMIC_CCG_CODE = '93C'
and HAS_ACTIVE_SMI_DIAGNOSIS
--and mpi.person_id = 'MYCG3Y42UHE991Q' - this person has two C&I ids
order by 1
)
--DEFINE SMI POP
,SMIPOPULATION as (
SELECT DISTINCT
TO_VARCHAR(smi.sk_patient_id) AS sk_patient_id
,b.mpi_person_id
,smi.person_id
,smi.hx_flake
,smi.age
,smi.age_band_5y
,smi.gender
,smi.birth_date_approx
,smi.ethnicity_category
,smi.ethcat_order
,smi.ethnicity_subcategory
,smi.ethsubcat_order
,smi.ethnicity_granular
,smi.practice_code
,smi.practice_name
,smi.main_language
,IFF(smi.is_homeless, 'Yes', 'No') AS is_homeless
,smi.imd_quintile
,smi.imdquintile_order
,IFF(smi.interpreter_needed, 'Yes', 'No') AS interpreter_needed
,smi.interpreter_type
,cf.is_smoker
,cf.drug_use
,cf.alcohol_use
,cf.ltc_count
,cf.ltc_2plus
,cf.ltc_summary
,cf.is_on_lithium
--health_check_completed
,cf.has_declined
,cf.incomp12m_ct
,cf.incomp12m_list
,cf.smok_miss
,cf.alc_miss
,cf.bp_miss
,cf.chol_miss
,cf.bmi_miss
,cf.hba1c_miss
FROM {{ ref('int_smi_population_base') }} smi
--FROM MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE smi
--add in case finding data for people on the SMI register
LEFT JOIN {{ ref('int_smi_casefinding') }} cf on smi.person_id = cf.person_id
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_CASEFINDING cf on smi.person_id = cf.person_id
--this bridging is in DBT and includes any person who has had activity at NLFT.
INNER JOIN (SELECT DISTINCT mpi_person_id, sk_patient_id FROM LOCAL_ID) b ON TO_VARCHAR(smi.sk_patient_id) = b.sk_patient_id
WHERE HAS_ACTIVE_SMI_DIAGNOSIS
)
--Inpatient stays
,SPELL as (
    select distinct 
    p.sk_patient_id
    ,p.mpi_person_id
    ,'NLFT' as provider
    ,sp.uniq_hosp_prov_spell_num
    ,sp.uniq_submission_id
    ,DATE(sp.start_date_hosp_prov_spell) as SPELL_START_DATE
    ,CASE WHEN sp.start_date_hosp_prov_spell  >= DATEADD('month', -6, CURRENT_DATE) THEN 'Yes' ELSE 'No' END AS LAST_6MTHS_FLAG
    ,coalesce(sp.disch_date_hosp_prov_spell, sp.estimated_disch_date_hosp_prov_spell) as spell_end_date
    ,CASE
    WHEN sp.meth_adm_mh_hosp_prov_spell in ('11','12','13') THEN 'Elective'
    WHEN sp.meth_adm_mh_hosp_prov_spell in ('81') THEN 'Other transfer'
    ELSE 'Emergency' END AS admission_type
    ,DATE(ws.effective_from) as ward_date_from
    ,CASE 
    WHEN ws.ward_type = '01' THEN 'Child and Adolescent Mental Health'
    WHEN ws.ward_type = '02' THEN 'Paediatric'
    WHEN ws.ward_type = '03' THEN 'Adult Mental Health'
    WHEN ws.ward_type = '04' THEN 'Non Mental Health'
    WHEN ws.ward_type = '05' THEN 'Learning Disabilities'
    WHEN ws.ward_type = '06' THEN 'Older People Mental Health'
    END AS ward_type
FROM {{ ref('stg_mhsds_spell') }} sp
--FROM MODELLING.DBT_STAGING.STG_MHSDS_spell sp
INNER JOIN SMIPOPULATION p ON p.mpi_person_id = sp.person_id
LEFT JOIN {{ ref('raw_mhsds_mhs502wardstay') }} ws on sp.uniq_hosp_prov_spell_num = ws.uniq_hosp_prov_spell_num
--LEFT JOIN MODELLING.DBT_RAW.RAW_MHSDS_MHS502WARDSTAY ws on sp.uniq_hosp_prov_spell_num = ws.uniq_hosp_prov_spell_num
WHERE sp.DM_ICB_COMMISSIONER = '93C'
AND sp.ORG_ID_PROV in ('G6V2S')--,'TAF','RNK','RRP') use NLFT code only C&I legacy patients are not found in the NLFT EPR system
)
--select people who are inpatients currently or who have been admitted in the last 6 months
,SPELL_DEDUP AS (
select 
sk_patient_id
,mpi_person_id
,provider
,admission_type
,ward_type
,SPELL_START_DATE
,LAST_6MTHS_FLAG
,DATE(spell_end_date) as SPELL_END_DATE
FROM SPELL
where LAST_6MTHS_FLAG = 'Yes' OR spell_end_date  is NULL
--deduplicate ward stays by selecting the latest
QUALIFY ROW_NUMBER() OVER (PARTITION BY SK_PATIENT_ID, uniq_hosp_prov_spell_num ORDER BY ward_date_from DESC) = 1
order by 1,4
)
--final add back in population characteristics and local patient id for NFLT.
select 
p.person_id
,p.hx_flake
,loc.local_patient_id 
,max(sp.admission_type) as admission_type
,max(sp.ward_type) as ward_type
,max(sp.spell_start_date) as latest_spell_start
,max(sp.spell_end_date) as latest_spell_end
,p.age
,p.age_band_5y
,p.gender
,p.birth_date_approx
,p.ethnicity_category
,p.ethcat_order
,p.ethnicity_subcategory
,p.ethsubcat_order
,p.ethnicity_granular
,p.practice_code
,p.practice_name
,p.main_language
,p.is_homeless
,p.imd_quintile
,p.imdquintile_order
,p.interpreter_needed
,p.interpreter_type
,p.is_smoker
,p.drug_use
,p.alcohol_use
,p.ltc_count
,p.ltc_2plus
,p.ltc_summary
,p.is_on_lithium
--health_check_completed
,p.has_declined
,p.incomp12m_ct
,p.incomp12m_list
,p.smok_miss
,p.alc_miss
,p.bp_miss
,p.chol_miss
,p.bmi_miss
,p.hba1c_miss
from spell_dedup sp
left join smipopulation p on p.mpi_person_id = sp.mpi_person_id
left join (select distinct mpi_person_id, local_patient_id from LOCAL_ID) loc on loc.mpi_person_id = p.mpi_person_id --and loc.provider = p.provider
--where s.provider in (  'NLFT') --'C&I' C&I legacy patients are not found in NLFT systems
group by all