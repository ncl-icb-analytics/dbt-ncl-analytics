{{
    config(
        materialized='table',
        tags=['myria_eval'])
}}
--Testing latest version of Myria evaluation script v18. Aggregated by week prior to intervention -53 to current week by myria_status
--Use HEX_FLAKE macro.

with date_range as (-- Generate days for 2 years
    SELECT 
         dateadd('day', seq4() - 700, current_date) as date
    FROM 
        TABLE(generator(rowcount => 700))
    WHERE 
        date <= current_date()
)
, deaths as ( 
    SELECT
        sk_patient_id,
        MIN(REG_DATE_OF_DEATH) AS death_date
    FROM {{ ref('stg_registries_deaths') }}
    --FROM MODELLING.DBT_STAGING.STG_REGISTRIES_DEATHS
    GROUP BY 1
)
,pds_deaths as
(
    SELECT
    pp.sk_patient_id,
    pp.date_of_death
    FROM {{ ref('stg_pds_pds_person') }} pp
    --FROM MODELLING.DBT_STAGING.STG_PDS_PDS_PERSON pp
    WHERE
    pp.event_to_date IS NULL -- current patient state
    )
, eligible as ( -- 5,737
    SELECT
        PATIENT_ID,
        DATE('22-Jan-2026') as eligibility_date
    FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }} 
    --FROM MODELLING.DBT_SNAPSHOTS.FCT_PERSON_MYRIA_HIGH_RISK_PATIENTS_PUBLISHED_SNAPSHOT
    WHERE DATE('22-Jan-2026') between DBT_VALID_FROM AND COALESCE(DBT_VALID_TO,CURRENT_DATE())

    UNION

    SELECT
        PATIENT_ID,
        DATE('16-Feb-2026') as eligibility_date
    FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }} 
    --FROM MODELLING.DBT_SNAPSHOTS.FCT_PERSON_MYRIA_HIGH_RISK_PATIENTS_PUBLISHED_SNAPSHOT
    WHERE DATE('16-Feb-2026') between DBT_VALID_FROM AND COALESCE(DBT_VALID_TO,CURRENT_DATE())

    UNION

    SELECT
        PATIENT_ID,
        DATE('17-Mar-2026') as eligibility_date
    FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }} 
    --FROM MODELLING.DBT_SNAPSHOTS.FCT_PERSON_MYRIA_HIGH_RISK_PATIENTS_PUBLISHED_SNAPSHOT
    WHERE DATE('17-Mar-2026') between DBT_VALID_FROM AND COALESCE(DBT_VALID_TO,CURRENT_DATE())

    UNION

    SELECT
        PATIENT_ID,
        DATE('15-Apr-2026') as eligibility_date
    FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }} 
    --FROM MODELLING.DBT_SNAPSHOTS.FCT_PERSON_MYRIA_HIGH_RISK_PATIENTS_PUBLISHED_SNAPSHOT
    WHERE DATE('15-Apr-2026') between DBT_VALID_FROM AND COALESCE(DBT_VALID_TO,CURRENT_DATE())

    UNION

    SELECT
        PATIENT_ID,
        DATE('15-May-2026') as eligibility_date
    FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }} 
    --FROM MODELLING.DBT_SNAPSHOTS.FCT_PERSON_MYRIA_HIGH_RISK_PATIENTS_PUBLISHED_SNAPSHOT
    WHERE DATE('15-May-2026') between DBT_VALID_FROM AND COALESCE(DBT_VALID_TO,CURRENT_DATE())
)

, eligible_date as (
    SELECT
        patient_id,
        {{ hxflake_pseudo_generation('patient_id') }} AS hex_id,
        -- LEFT(SUBSTR(TRIM(REVERSE(RIGHT(LPAD(
        --                         TO_CHAR(CAST(patient_id AS INT), 'XXXXXXXXX'),
        --                         9, 0), 10))) || '0000000000', 1, 2) || '-' || 
        --     RPAD(SUBSTR(TRIM(REVERSE(RIGHT(LPAD(
        --                         TO_CHAR(CAST(patient_id AS INT), 'XXXXXXXXX'),
        --                         9, 0), 10))) || '0000000000', 3, 4), 3, '0') || '-' || 
        --     SUBSTR(TRIM(REVERSE(RIGHT(LPAD(
        --                         TO_CHAR(CAST(patient_id AS INT), 'XXXXXXXXX'),
        --                         9, 0), 10))) || '0000000000', 6, 3) || '0000000000', 10
        -- ) AS hex_id,
        MIN(eligibility_date) AS eligibility_date
    FROM ELIGIBLE
    GROUP BY 1,2
)
, care_homes as (
    SELECT SK_PATIENT_ID
    FROM {{ ref('stg_pds_pds_address') }} 
    --FROM MODELLING.DBT_STAGING.STG_PDS_PDS_ADDRESS 
    --WHERE '01-JAN-2026' BETWEEN C.USUAL_ADDRESS_BUSINESS_EFFECTIVE_FROM_DATE AND C.USUAL_ADDRESS_BUSINESS_EFFECTIVE_TO_DATE
        --OR '30-APR-2026' BETWEEN C.USUAL_ADDRESS_BUSINESS_EFFECTIVE_FROM_DATE AND C.USUAL_ADDRESS_BUSINESS_EFFECTIVE_TO_DATE
    WHERE '01-JAN-2026' BETWEEN event_from_date AND event_to_date
        OR '30-APR-2026' BETWEEN event_from_date AND event_to_date
    AND DER_CQC_CARE_HOME_CODE IS NOT NULL
    QUALIFY ( ROW_NUMBER() OVER (PARTITION BY SK_PATIENT_ID ORDER BY event_from_date DESC) = 1 )
)
, member_spine as (
    SELECT
        e.patient_id,
        e.hex_id,
        e.eligibility_date,
        COALESCE(d.death_date, pd.date_of_death) as death_date,
        CASE 
            WHEN m.discharged_date IS NOT NULL THEN 'Discharged'
            WHEN DATEDIFF(DD, m.ONBOARDED_DATE, DATEADD(DD, -7, CURRENT_DATE)) >= 60 THEN 'Onboarded 60+ days'
            WHEN m.ONBOARDED_DATE IS NOT NULL THEN 'Onboarded less than 60 days'
            ELSE 'Not onboarded'
            END AS myria_status
    FROM eligible_date AS e
    LEFT JOIN deaths AS d 
        ON e.patient_id = d.sk_patient_id
    LEFT JOIN pds_deaths AS pd 
        ON e.patient_id = pd.sk_patient_id
    --LEFT JOIN FROM {{ ref('myria_enrolled_patients_20250507') }} m -- myria uploads are not in raw dbt yet. Failed upload.
    LEFT JOIN DATA_LAKE__NCL.ANALYST_MANAGED.MYRIA_ENROLLED_PATIENTS_20250507 as m
        ON e.hex_id = m.hx_id
    LEFT JOIN care_homes AS c 
        ON e.patient_id = c.SK_PATIENT_ID -- identify care home patients patients
    WHERE m.hx_id IS NOT NULL OR c.SK_PATIENT_ID IS NULL -- include myria patients or, not onboarded patients (control) excluding care home patients
)

, ae_encounter_summary as (
    select
        sk_patient_id
        , start_date as activity_date
        , end_date as end_date
        , start_date as activity_date_range
        , 'A&E' as activity_type
        , pod as activity_subtype
        , count(*) as encounters
        , sum(cost) as cost
        , sum(duration) as duration
    FROM {{ ref('obt_encounter_uec') }} 
    --from REPORTING.COMMISSIONING_REPORTING.OBT_ENCOUNTER_UEC
    where 
        sk_patient_id is not null and sk_patient_id != '1' 
        and START_DATE <= dateadd(dd, -7, current_date)
    group by 
        sk_patient_id
        ,start_date
        , pod
        , end_date
)

, apc_encounter_summary as (
    select
        e.sk_patient_id
        , e.start_date as activity_date
        , e.end_date
        , d.date as activity_date_range
        , 'Inpatient' as activity_type
        , case when e.spec_comm_flag = 'Y' then 'spec_comm' else e.admission_method_group end as activity_subtype
        , sum(case when d.date = e.start_date then 1 end) as encounters
        , sum(cost / datediff(day, e.start_date, coalesce(dateadd(dd, 1, e.end_date), dateadd(dd, -6, current_date))) ) as cost -- spreads costs against all days in spell, if open spell, all days except last 7
        --, sum(cost / datediff(day, e.start_date,dateadd(dd, 1, e.end_date))) as cost
        , sum(case when d.date = e.start_date then 0 else 1 end) as duration -- each row is 1 day. 0 day los would be 0, else 1. so 3 rows would give 2 day los - which is correct for admitted on first day and discharged 2 days later.
    FROM {{ ref('obt_encounter_apc') }} e
    --from REPORTING.COMMISSIONING_REPORTING.OBT_ENCOUNTER_APC as e
    left join -- join all days during the spell
        date_range as d
        on d.date <= coalesce(e.end_date, current_date)
        and d.date >= e.start_date
    where 
        e.sk_patient_id is not null and e.sk_patient_id != '1'
        AND START_DATE <= dateadd(dd, -7, current_date)
    group by 
        e.sk_patient_id, d.date, start_date, end_date, case when e.spec_comm_flag = 'Y' then 'spec_comm' else e.admission_method_group end
)
--select * from apc_encounter_summary order by SK_PATIENT_ID, activity_date, activity_date_range; 
, combined as (
    select * from ae_encounter_summary
    union all
    select * from apc_encounter_summary
)
, combined_hex as (
    select
    *,
     {{ hxflake_pseudo_generation('sk_patient_id') }} AS hex_id
    -- LEFT(SUBSTR(TRIM(REVERSE(RIGHT(LPAD(
    --                          TO_CHAR(CAST(sk_patient_id AS INT), 'XXXXXXXXX'),
    --                          9, 0), 10))) || '0000000000', 1, 2) || '-' || 
    --     RPAD( SUBSTR(TRIM(REVERSE(RIGHT(LPAD(
    --                          TO_CHAR(CAST(sk_patient_id AS INT), 'XXXXXXXXX'),
    --                          9, 0), 10))) || '0000000000', 3, 4), 3, '0') || '-' || 
    --     SUBSTR(TRIM(REVERSE(RIGHT(LPAD(
    --                          TO_CHAR(CAST(sk_patient_id AS INT), 'XXXXXXXXX'),
    --                          9, 0), 10))) || '0000000000', 6, 3) || '0000000000', 10
    --  ) AS hex_id
    from combined
)

, fct_person_activity_by_day_ral as (
    select 
        --sk_patient_id
        hex_id
        , activity_date_range
        -- A&E
        , sum(case when activity_type = 'A&E' then encounters else 0 end) as ae_encounters
        , sum(case when activity_type = 'A&E' then cost else 0 end) as ae_cost
        -- Inpatient NEL
        , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - emergency' then encounters else 0 end) as ip_nel_emergency_encounters
        , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - emergency' then cost else 0 end) as ip_nel_emergency_cost
        , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - emergency' then duration else 0 end) as ip_nel_emergency_duration
    from
        combined_hex
    group by 
        hex_id, activity_date_range
)
--select top 1000 * from fct_person_activity_by_day_ral order by hex_id, activity_date;
, member_daily_costs as (
    select
        d.date,
        m.hex_id ,
        m.myria_status,
        case when d.date <= m.eligibility_date then 'Pre-intervention' else 'Post-intervention' end as activity_status,
        FLOOR(datediff(DAY, dateadd(dd, 1, m.eligibility_date), d.date)/7) AS week_n,
        -- cost
        round (sum (ip_nel_emergency_cost), 10) as ip_nel_emergency_cost,
        round (sum (ae_cost), 10) as ae_cost,
        -- encounters
        round (sum (ip_nel_emergency_encounters), 10) as ip_nel_emergency_encounters,
        round (sum (ae_encounters), 10) as ae_encounters,
        -- bed days
        round (sum (ip_nel_emergency_duration), 10) as ip_nel_emergency_duration
    from
        member_spine as m
    left join date_range as d -- date spine between 1 year before eligibility date and date of death/current date - 7 days (to allow for incomplete data)
        on d.date between dateadd(dd, -365, eligibility_date) 
            and case when m.death_date < dateadd(dd, -7, current_date()) then m.death_date else dateadd(dd, -7, current_date()) end
    left join fct_person_activity_by_day_ral as c 
        on m.hex_id = c.hex_id
        and d.date = c.activity_date_range
    group by
        1, 2, 3, 4, 5--, 6, 7
)

--SELECT * FROM MEMBER_DAILY_COSTS ORDER BY HEX_ID, DATE;
select
    week_n,
    myria_status,
    activity_status,
    count(*) as n_days,
    COUNT(*)/7 as n_patients,
    -- costs
    sum (ifnull(ip_nel_emergency_cost,0)) as ip_nel_emergency_cost,
    sum (ifnull(ae_cost,0)) as ae_cost,
    -- encounters
    sum (ifnull(ip_nel_emergency_encounters,0)) as ip_nel_emergency_encounters,
    sum (ifnull(ae_encounters,0))  as ae_encounters,
    -- bed days
    sum (ifnull(ip_nel_emergency_duration,0)) as ip_nel_emergency_duration 
from
     member_daily_costs as m 
     where week_n is not null
group by
    1, 2, 3
order by 
    1, 2, 3
    

-- jess updates:
-- x cut out legacy bits and condensed hex_id blocks for readability
-- x switched pre- post- date to be eligibility date instead of onboarded date
-- x pre aggregated deaths and care home CTEs, thought there might be a chance it's taking different rows each time with the qualify (multiple deaths and care home records)
-- x added in the 7 day buffer throughout, including the 60+ day logic
-- x spread NEL costs over all stay days in open encounters except last 7 days
-- x counting week 0 from day after eligibility 


-- now fixed:
-- Pds deaths - IN ASSESSMENT SCRIPT TOO!
-- div/0 error? change the end date of the date diff to be the actual most recent data of SUS - use a freshness table?
-- tweak the week numbers such that all week 0 is post-intervention
-- date spine back into fct_person_activity_by_day_ral. this allows the cost to be spread over days rather tha assigned start date

