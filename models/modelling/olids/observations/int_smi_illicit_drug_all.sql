{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}

/*
ILLICIT DRUG USE defined by PCD REFSET CLUSTER
*/
WITH base_observations AS (
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,

   -- Illicit Drug Use in the last year 
    CASE
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS illicit_drug_assessed_last_12m

FROM ({{ get_observations("'ILLSUB_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL 
AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)
--final select with categorisations
select o.*
,CASE 
WHEN CONCEPT_DISPLAY is null THEN 'Unknown'
WHEN CONCEPT_DISPLAY in ('Does not misuse drugs','Has never misused drugs') THEN 'Does not misuse drugs'
WHEN CONCEPT_DISPLAY in ('Poly-drug misuser', 'Long-term drug misuser','Combined opioid with non-opioid substance dependence, continuous','Notified addict') THEN 'Dependence' 
WHEN CONCEPT_DISPLAY ILIKE ANY ('%dependence','%Glue sniffing dependence%','%drug dependence%') THEN 'Dependence'
WHEN CONCEPT_DISPLAY in ('Misused drugs in past', 'Date stopped drug misuse', 'Time since stopped drug misuse','Time spent recovering from drugs','Abstinent from drug misuse') THEN 'Abstinence/Remission'
WHEN CONCEPT_DISPLAY ILIKE ANY ('Abstinen%','%remission') THEN 'Abstinence/Remission'
WHEN CONCEPT_DISPLAY in ('Shares drug injecting equipment','Cleaning of drug injection equipment','Drug injecting equipment hygiene','Drug injection behaviour') THEN 'Injecting drug user'
WHEN CONCEPT_DISPLAY ILIKE ANY ('Intravenous%','%needle%','Injects drugs%','%injector%') THEN 'Injecting drug user'
WHEN CONCEPT_DISPLAY ILIKE ANY ('Overdose%','%overdose','%intoxication%','%poison%') THEN 'Overdose or Poisoning' 
WHEN CONCEPT_DISPLAY ilike '%disorder%' THEN 'Drug-Induced Mental Disorders'
WHEN CONCEPT_DISPLAY in ('Duplicative flashbacks') THEN 'Drug-Induced Mental Disorders'
WHEN CONCEPT_DISPLAY in ('Not using heroin on top of substitution therapy') THEN 'Withdrawal/Treatment'
WHEN CONCEPT_DISPLAY ilike '%withdrawal%' THEN 'Withdrawal/Treatment'
WHEN CONCEPT_DISPLAY in ('Misuses drugs', 'Occasional drug user', 'Episodic use of drugs', 'Current drug user','Illicit drug use','Never injecting drug user','Recreational drug user','Details of drug misuse behaviour','Misuses drugs sublingually','Drug seeking behaviour') THEN 'Misuse/Harmful Use'
WHEN CONCEPT_DISPLAY ILIKE ANY ('%drug abuse%','%drug misuse','Misuse%','%Harmful pattern%','%inject%','%drug-related activities%','Smokes%','%Inject%','%needle%') THEN 'Misuse/Harmful Use'
ELSE 'Misuse/Harmful Use'
END AS ILLICIT_DRUG_PATTERN
,CASE 
WHEN CONCEPT_DISPLAY in ('Methadone dependence', 'Opium dependence') THEN 'Opioids'
WHEN CONCEPT_DISPLAY ILIKE ANY ('Opioid%','%opioid%', 'Heroin%', '%heroin%','Morphine%','%Opiate%','Narcotic%') THEN 'Opioids'
WHEN CONCEPT_DISPLAY ILIKE ANY ('%cocaine%', '%stimulant%', '%amphetamine%','%amfetamine%') THEN 'Cocaine and Stimulants'
WHEN CONCEPT_DISPLAY ilike '%cannabis%' THEN 'Cannabis'
WHEN CONCEPT_DISPLAY ILIKE ANY ('%benzo%','%hypnotic%','%over-the-counter%','%steroid%','Diazepam%','Barbiturate%','Sedative%','Anxiolytic%','%prescription%') THEN 'Prescription drugs'
WHEN CONCEPT_DISPLAY in ('Misuse of analgesic','Current non recreational drug user') THEN 'Prescription drugs'
WHEN CONCEPT_DISPLAY ILIKE ANY ('Ecstasy%','%ecstasy%', '%glue%','%Lysergic%','%psychoactive substance%','%Hallucinogen%','Mescaline%','Nitrous%') THEN 'Other Psychoactive Substances'
WHEN CONCEPT_DISPLAY in ('Inhalant-induced organic mental disorder') THEN 'Other Psychoactive Substances'
WHEN CONCEPT_DISPLAY in ('Does not misuse drugs','Has never misused drugs') THEN NULL
ELSE 'Unknown Substance'
END AS ILLICIT_DRUG_CLASS
FROM base_observations o