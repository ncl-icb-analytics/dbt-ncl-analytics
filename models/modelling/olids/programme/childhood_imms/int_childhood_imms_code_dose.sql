{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}


select * from (
select 
sched.VACCINE_ORDER,
sched.VACCINE_ID, 
clut.VACCINE, 
CAST(clut.SNOMEDCONCEPTID AS VARCHAR) AS CODE,
clut.PROPOSEDCLUSTER as CODECLUSTERID, 
sched.ADMINISTERED_CLUSTER_ID, 
sched.DRUG_CLUSTER_ID,
sched.DECLINED_CLUSTER_ID,
sched.CONTRAINDICATED_CLUSTER_ID , 
sched.DOSE_NUMBER AS SCHEDULE_DOSE,
clut.DOSE AS CODE_DOSE,
CASE 
WHEN clut.VACCINE = 'DTaP/IPV/Hib/HepB/6-in-1' and clut.DOSE = '1,2,3,4' and sched.DOSE_NUMBER = '1' THEN '1'
WHEN clut.VACCINE = 'DTaP/IPV/Hib/HepB/6-in-1' and clut.DOSE = '1,2,3,4' and sched.DOSE_NUMBER = '2' THEN '2'
WHEN clut.VACCINE = 'DTaP/IPV/Hib/HepB/6-in-1' and clut.DOSE = '1,2,3,4' and sched.DOSE_NUMBER = '3'THEN '3'
WHEN clut.VACCINE = 'DTaP/IPV/Hib/HepB/6-in-1' and clut.DOSE = '1,2,3,4' and sched.DOSE_NUMBER = '4'THEN '4'
WHEN clut.VACCINE = 'MenB' and clut.DOSE = '1,2,3' and sched.DOSE_NUMBER = '1' THEN '1'   
WHEN clut.VACCINE = 'MenB' and clut.DOSE = '1,2,3' and sched.DOSE_NUMBER = '2' THEN '2'
WHEN clut.VACCINE = 'MenB' and clut.DOSE = '1,2,3' and sched.DOSE_NUMBER = '3' THEN '3'
WHEN clut.VACCINE = 'Hib/MenC' and clut.DOSE = '1,2' and sched.DOSE_NUMBER = '1' THEN '1'    
WHEN clut.VACCINE = 'HPV' and clut.DOSE in ('1,2,3') and sched.DOSE_NUMBER = '1' THEN '1' 
WHEN clut.VACCINE = 'HPV' and clut.DOSE in ('1,2,3') and sched.DOSE_NUMBER = '2' THEN '2'    
WHEN clut.VACCINE = 'MMR' and clut.DOSE = '1,2' and sched.DOSE_NUMBER = '1' THEN '1'    
WHEN clut.VACCINE = 'MMR' and clut.DOSE = '1,2' and sched.DOSE_NUMBER = '2' THEN '2' 
WHEN clut.VACCINE = 'MMRV' and clut.DOSE = '1,2' and sched.DOSE_NUMBER = '1' THEN '1'    
WHEN clut.VACCINE = 'MMRV' and clut.DOSE = '1,2' and sched.DOSE_NUMBER = '2' THEN '2' 
WHEN clut.VACCINE = 'PCV' and clut.DOSE = '1,2' and sched.DOSE_NUMBER = '1' THEN '1'    
WHEN clut.VACCINE = 'PCV' and clut.DOSE = '1,2' and sched.DOSE_NUMBER = '2' THEN '2' 
WHEN clut.VACCINE = 'Rotavirus' and clut.DOSE = '1,2' and sched.DOSE_NUMBER = '1' THEN '1'    
WHEN clut.VACCINE = 'Rotavirus' and clut.DOSE = '1,2' and sched.DOSE_NUMBER = '2' THEN '2'
WHEN clut.VACCINE = sched.VACCINE_NAME AND clut.DOSE = sched.DOSE_NUMBER THEN clut.DOSE
END as DOSE_MATCH
FROM {{ ref('stg_reference_childhood_imms_codes') }} clut
INNER JOIN {{ ref('stg_reference_imms_schedule_latest') }} sched ON
            (sched.administered_cluster_id = clut.proposedcluster OR
            sched.drug_cluster_id = clut.proposedcluster OR
            sched.declined_cluster_id = clut.proposedcluster OR
            sched.contraindicated_cluster_id = clut.proposedcluster) 
            
order by vaccine, code, dose_match
) a
where a.dose_match is not null