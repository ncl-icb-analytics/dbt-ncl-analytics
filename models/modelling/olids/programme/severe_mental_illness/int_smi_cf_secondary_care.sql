{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}
/*Person Level Intermediate table holding Case Finding data for people on the SMI register who have had an inpatient spell at NLFT in the last 6 months or who have a current inpatient spell. 
This includes number of checks missed and vulnerabilities. The MHSDS active submission is 6 weeks behind current date.
*/

--find local patient ids for active submission. One sk_patient_id and one person_id can have multiple local patient ids. MPI_PERSON_ID is the unique identifier in the MHSD record set.
with LOCAL_ID as (
select distinct 
mpi.sk_patient_id 
,mpi.PERSON_ID as mpi_person_id 
,local_patient_id  
,org_id_prov
,'NLFT' as provider
FROM {{ ref('stg_mhsds_mpi') }} mpi
--FROM MODELLING.DBT_STAGING.STG_MHSDS_MPI mpi
INNER JOIN {{ ref('stg_mhsds_activesubmission') }} a ON mpi.uniq_submission_id = a.uniq_submission_id
--inner join MODELLING.DBT_STAGING.STG_MHSDS_ACTIVESUBMISSION a  on mpi.uniq_submission_id = a.uniq_submission_id
INNER JOIN {{ ref('int_smi_population_base') }} smi on TO_VARCHAR(smi.sk_patient_id) = mpi.sk_patient_id
--INNER JOIN MODELLING.OLIDS_PROGRAMME.INT_SMI_POPULATION_BASE smi on TO_VARCHAR(smi.sk_patient_id) = mpi.sk_patient_id
where ORG_ID_PROV in ('G6V2S') --,'TAF') use NLFT code only C&I legacy patients are not found in the NLFT EPR system
and DMIC_CCG_CODE = '93C'
and HAS_ACTIVE_SMI_DIAGNOSIS

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
--Inpatient stays that started in the last 6 months or are currently active for people on the SMI register. Some people have multiple spells and ward stays, so we select the latest ward stay only.
,SPELL as (
    select distinct 
    p.sk_patient_id
    ,p.mpi_person_id
    ,'NLFT' as provider
    ,sp.uniq_hosp_prov_spell_num as spell_number
    ,ws.uniq_ward_stay_id
    ,DATE(sp.start_date_hosp_prov_spell) as spell_start_date
    ,DATE(sp.disch_date_hosp_prov_spell) as spell_discharge_date
    ,sp.disch_date_hosp_prov_spell is null as is_current_spell  
    ,DATE(ws.start_date_ward_stay) as start_date_ward_stay
    ,DATE(end_date_ward_stay) as end_date_ward_stay
    ,CASE WHEN sp.start_date_hosp_prov_spell  >= DATEADD('month', -6, CURRENT_DATE) THEN 'Yes' ELSE 'No' END AS last_6mths_flag
    ,CASE
    WHEN sp.meth_adm_mh_hosp_prov_spell in ('11','12','13') THEN 'Elective'
    WHEN sp.meth_adm_mh_hosp_prov_spell in ('81') THEN 'Other transfer'
    ELSE 'Emergency' END AS admission_type
    ,CASE 
    WHEN ws.hospital_bed_type_name = 'Adult Psychiatric Intensive Care Unit (Acute Mental Health Care)' THEN 'Adult Psychiatric Intensive Care Unit'
    WHEN ws.hospital_bed_type_name = 'Acute Older Adult Mental Health Care (Organic and Functional)' THEN 'Acute Older Adult Mental Health Care'
    WHEN ws.hospital_bed_type_name = 'Adult Mental Health Rehabilitation (Mainstream Service)' THEN 'Adult Mental Health Rehabilitation'
    WHEN ws.hospital_bed_type_name = 'General Child and Young Person - Young Person (13 years up to and including 17 years)' THEN 'General Child and Young Person'
    ELSE ws.hospital_bed_type_name END AS ward_type
    ,ws.ward_code
FROM {{ ref('stg_mhsds_spell') }} sp
--FROM MODELLING.DBT_STAGING.STG_MHSDS_spell sp
INNER JOIN SMIPOPULATION p ON p.mpi_person_id = sp.person_id
LEFT JOIN {{ ref('stg_mhsds_mhs502wardstay') }} ws on sp.uniq_hosp_prov_spell_num = ws.uniq_hosp_prov_spell_num
--LEFT JOIN MODELLING.DBT_STAGING.STG_MHSDS_MHS502WARDSTAY ws on sp.uniq_hosp_prov_spell_num = ws.uniq_hosp_prov_spell_num
WHERE sp.DM_ICB_COMMISSIONER = '93C'
AND sp.ORG_ID_PROV in ('G6V2S')--,'TAF','RNK','RRP') use NLFT code only C&I legacy patients are not found in the NLFT EPR system
--deduplicate selecting latest ward_start_date only
QUALIFY ROW_NUMBER() OVER (PARTITION BY sp.person_id, sp.uniq_hosp_prov_spell_num ORDER BY start_date_ward_stay DESC) = 1
)
--select people who are inpatients currently or who have been admitted in the last 6 months
,SPELL_6M AS (
select 
sk_patient_id
,mpi_person_id
,provider
,uniq_ward_stay_id
,admission_type
,ward_type
,ward_code
,spell_number
,spell_start_date
,start_date_ward_stay
,spell_discharge_date
,is_current_spell
,last_6mths_flag
FROM SPELL
where last_6mths_flag = 'Yes' OR is_current_spell
)
--select latest spell if multiple reported.
,latest_spell as (
select *
from spell_6m sp
QUALIFY ROW_NUMBER() OVER (PARTITION BY sp.mpi_person_id ORDER BY spell_start_date DESC, start_date_ward_stay DESC) = 1
)
--final add back in population characteristics and health check flags and local patient id for NFLT.
select 
p.person_id
,p.hx_flake
,loc.local_patient_id 
,sp.spell_number
,sp.spell_start_date
,sp.spell_discharge_date
,sp.is_current_spell
,sp.admission_type
,sp.ward_type
,sp.ward_code
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
from  latest_spell sp
left join smipopulation p on p.mpi_person_id = sp.mpi_person_id
--some people have multiple MPI_PERSON_IDs to each sk_patient_id/local_patient_id.
left join (select distinct mpi_person_id, local_patient_id from LOCAL_ID) loc on loc.mpi_person_id = sp.mpi_person_id 
