{{
    config(
        materialized='table',
        tags=['myria_eval'])
}}
/*Testing latest version of Myria evaluation script v18. Aggregated by week prior to intervention -53 to current week by myria_status
THIS does not use the PROPENSITY MATCH GROUPS. REFER TO 260604_early_evaluation_diff_of_diff_v2_Propensity_Matched_KH.sql which uses the PROPENSITY MATCHED INPUT.
--KH Adjustments to the V18 script: 
1) Use HEX_FLAKE macro on eligible population
2) Combined CTE for registry and PDS deaths selecting the earliest date for each person.
3) Care home from address expanded to include people in a care home with a null end date (may have moved in before 2026 and still there).
4) Join hospital activity CTEs to inner join with member spine to reduce processing on millions of records.
5) APC date range join changed to Use COALESCE(end_date, current_date) to handle ongoing admissions, and INNER JOIN to date_range to ensure only valid overlapping activity is kept.
*/
with date_range as (-- Generate days for 2 years
    SELECT 
         dateadd('day', seq4() - 700, current_date) as date
    FROM 
        TABLE(generator(rowcount => 700))
    WHERE 
        date <= current_date()
)
, all_deaths as ( 
--combined choose MIN Date if > death date per person (earliest one) 
    select a.sk_patient_id,
    MIN(a.death_date) as death_date
    from (
     --registry_deaths
    SELECT
        sk_patient_id,
        MIN(REG_DATE_OF_DEATH) AS death_date
    FROM {{ ref('stg_registries_deaths') }}
    --FROM MODELLING.DBT_STAGING.STG_REGISTRIES_DEATHS
    GROUP BY 1
UNION
 --PDS_deaths (status 1 or 2)
    SELECT
        sk_patient_id,
        MIN(date_of_death) AS death_date
     FROM {{ ref('stg_pds_pds_person') }}
    --FROM MODELLING.DBT_STAGING.STG_PDS_PDS_PERSON
    where date_of_death is not null
    GROUP BY 1
    ) a
    group by a.sk_patient_id
 )   
 --eligible population from historical snapshots to date
, eligible as ( 
    --Jan no results
    SELECT PATIENT_ID, DATE('22-Jan-2026') as eligibility_date
    FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }} 
    --FROM MODELLING.DBT_SNAPSHOTS.FCT_PERSON_MYRIA_HIGH_RISK_PATIENTS_PUBLISHED_SNAPSHOT
    WHERE DATE('22-Jan-2026') between DBT_VALID_FROM AND COALESCE(DBT_VALID_TO,CURRENT_DATE())

    UNION
    --feb extract n=5015
    SELECT PATIENT_ID, DATE('16-Feb-2026') as eligibility_date
    FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }} 
    --FROM MODELLING.DBT_SNAPSHOTS.FCT_PERSON_MYRIA_HIGH_RISK_PATIENTS_PUBLISHED_SNAPSHOT
    WHERE DATE('16-Feb-2026') between DBT_VALID_FROM AND COALESCE(DBT_VALID_TO,CURRENT_DATE())

    UNION
    --mar extract n=5257
    SELECT PATIENT_ID, DATE('17-Mar-2026') as eligibility_date
    FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }} 
    --FROM MODELLING.DBT_SNAPSHOTS.FCT_PERSON_MYRIA_HIGH_RISK_PATIENTS_PUBLISHED_SNAPSHOT
    WHERE DATE('17-Mar-2026') between DBT_VALID_FROM AND COALESCE(DBT_VALID_TO,CURRENT_DATE())

    UNION
    --apr extract n=5638
    SELECT PATIENT_ID, DATE('15-Apr-2026') as eligibility_date
    FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }} 
    --FROM MODELLING.DBT_SNAPSHOTS.FCT_PERSON_MYRIA_HIGH_RISK_PATIENTS_PUBLISHED_SNAPSHOT
    WHERE DATE('15-Apr-2026') between DBT_VALID_FROM AND COALESCE(DBT_VALID_TO,CURRENT_DATE())

    UNION
    --may extract n=5710
    SELECT PATIENT_ID, DATE('15-May-2026') as eligibility_date
    FROM {{ ref('fct_person_myria_high_risk_patients_published_snapshot') }} 
    --FROM MODELLING.DBT_SNAPSHOTS.FCT_PERSON_MYRIA_HIGH_RISK_PATIENTS_PUBLISHED_SNAPSHOT
    WHERE DATE('15-May-2026') between DBT_VALID_FROM AND COALESCE(DBT_VALID_TO,CURRENT_DATE())
)
--select the earliest eligibility date per person where they appear in >1 snapshot n=6061. Use HEX_FLAKE macro to generate hex_id at this stage.
--AI suggestion to use qualify rather than MIN + Group By as more accurate, safer, and more flexible for deduplication
, eligible_date as (
    SELECT
        patient_id,
         CASE
  WHEN TRY_TO_NUMBER(s.PATIENT_ID) IS NULL THEN NULL
  ELSE
    CONCAT(
      SUBSTR(
        RPAD(REVERSE(TRIM(TO_CHAR(TRY_TO_NUMBER(s.PATIENT_ID), 'XXXXXXXXXXXXXXXX'))), 8, '0'),
        1, 2
      ),
      '-',
      SUBSTR(
        RPAD(REVERSE(TRIM(TO_CHAR(TRY_TO_NUMBER(s.PATIENT_ID), 'XXXXXXXXXXXXXXXX'))), 8, '0'),
        3, 3
      ),
      '-',
      SUBSTR(
        RPAD(REVERSE(TRIM(TO_CHAR(TRY_TO_NUMBER(s.PATIENT_ID), 'XXXXXXXXXXXXXXXX'))), 8, '0'),
        6, 3
      )
    )
END AS HEX_ID,
        eligibility_date
    FROM ELIGIBLE
    qualify row_number() over(partition by patient_id order by eligibility_date) = 1
)
/*Current logic checks that January 1, 2026 is inside the effective date range OR April 30, 2026 is inside the effective date range. 
AI comment:If the row range overlaps your range but doesn't include either endpoint, it could fail 
Added to include where someone moves into a care home before '2026-01-01' and their latest address period is null so they are still there.
Note: This PDS address table is still unreliable and a proxy for care homes. Coding the GP record is a better */
, care_homes as (
    SELECT SK_PATIENT_ID
    ,event_from_date
    ,event_to_date,
    ROW_NUMBER() OVER (PARTITION BY SK_PATIENT_ID ORDER BY COALESCE(event_to_date, '9999-12-31') DESC, event_from_date DESC) as rownum
    FROM {{ ref('stg_pds_pds_address') }} 
    --FROM MODELLING.DBT_STAGING.STG_PDS_PDS_ADDRESS 
  -- WHERE ('01-JAN-2025' BETWEEN event_from_date AND event_to_date OR '30-APR-2026' BETWEEN event_from_date AND event_to_date)
    WHERE event_from_date <= '2026-04-30' AND (event_to_date >= '2026-01-01' OR event_to_date IS NULL)
    AND DER_CQC_CARE_HOME_CODE IS NOT NULL
   --This QUALIFY prioritises:Ongoing records (NULL end date) Records lasting longest into/through your window. So someone who is not in  a care home during that date period is excluded.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SK_PATIENT_ID ORDER BY COALESCE(event_to_date, '9999-12-31') DESC, event_from_date DESC) = 1
)
--join eligible to enrolled patients, adding deaths and care home status.
, member_spine as (
     SELECT
        e.patient_id,
        e.hex_id,
        e.eligibility_date,
        d.death_date,
        case when ch.sk_patient_id is not null then TRUE else FALSE end as is_care_home,
        CASE 
            WHEN m.discharged_date IS NOT NULL THEN 'Discharged'
            WHEN DATEDIFF(DD, m.ONBOARDED_DATE, DATEADD(DD, -7, CURRENT_DATE)) >= 60 THEN 'Onboarded 60+ days'
            WHEN m.ONBOARDED_DATE IS NOT NULL THEN 'Onboarded less than 60 days'
            ELSE 'Not onboarded'
            END AS myria_status
    FROM eligible_date AS e
    LEFT JOIN all_deaths AS d ON e.patient_id = d.sk_patient_id
    LEFT JOIN {{ref('raw_reference_myria_enrolled_patients_20250507')}} AS m ON e.hex_id = m.hx_id
    --LEFT JOIN MODELLING.DBT_RAW.RAW_REFERENCE_MYRIA_ENROLLED_PATIENTS_20250507 m ON e.hex_id = m.hx_id
    LEFT JOIN care_homes AS ch ON e.patient_id = ch.SK_PATIENT_ID -- identify care home patients patients
)
/*Build activity data (A&E): ae_encounter_summary as Counts:encounters, cost,duration
limit to eligible group 66K rows by INNER JOIN with member spine. Added date range limits to reduce rows from 15 million!
*/
, ae_encounter_summary as (
    select
        uec.sk_patient_id
        ,m.hex_id
        , start_date as activity_date
        , end_date as end_date
        , date(start_date) as activity_date_range
        , 'A&E' as activity_type
        , pod as activity_subtype
        , count(*) as encounters
        , sum(cost) as cost
        , sum(duration) as duration
    from {{ref('obt_encounter_uec') }} as uec
        --FROM REPORTING.COMMISSIONING_REPORTING.OBT_ENCOUNTER_UEC uec
        INNER JOIN member_spine m on uec.sk_patient_id = m.patient_id
        INNER JOIN date_range d ON d.date >= uec.start_date  AND d.date <= COALESCE(uec.end_date, current_date)
    where 
        sk_patient_id is not null and sk_patient_id != '1' 
        and START_DATE <= dateadd(dd, -7, current_date)
    group by 
        sk_patient_id
        ,hex_id
        ,start_date
        , pod
        , end_date
)
/* Build Inpatient activity: apc_encounter_summary
Expands each stay into daily rows via date_range eg: Admission = 01-Jan to 03-Jan becomes 3 rows 01-Jan, 02-Jan, 03-Jan
Calculates: encounters (on admission day), cost per day, duration (length of stay)
limit by eligible patient activity by INNER JOIN with member spine.
testing alternative which includes: completed admissions if they overlap date_range, 
Ongoing admissions up to current_date and Non-overlapping admissions dropped (no NULL rows)
*/
, apc_encounter_summary as (
    select
        apc.sk_patient_id
        , m.hex_id
        , apc.start_date as activity_date
        , apc.end_date
        , d.date as activity_date_range
        , 'Inpatient' as activity_type
        --Overrides subtype if it's specialised commissioning
        , case when apc.spec_comm_flag = 'Y' then 'spec_comm' else apc.admission_method_group end as activity_subtype
        --Counts 1 per admission, only on first day.
        , sum(case when d.date = apc.start_date then 1 end) as encounters
        -- Takes total spell cost & spreads it evenly across all days in the stay. If open spell, excludes the last 7 days from costing
        , sum(cost / datediff(day, apc.start_date, coalesce(dateadd(dd, 1, apc.end_date), dateadd(dd, -6, current_date))) ) as cost 
        --Duration (length of stay proxy) First day = 0, Every additional day = 1 so 3-day stay means duration = 2. This matches NHS LOS logic
        , sum(case when d.date = apc.start_date then 0 else 1 end) as duration 
        -- each row is 1 day. 0 day los would be 0, else 1. so 3 rows would give 2 day los - which is correct for admitted on first day and discharged 2 days later.
       FROM  {{ref('obt_encounter_apc') }} as apc
        --FROM  REPORTING.COMMISSIONING_REPORTING.OBT_ENCOUNTER_APC as apc
        INNER JOIN member_spine m on apc.sk_patient_id = m.patient_id
    -- join all days during the spell that are included in the activity date range
    --left join date_range as d on d.date <= coalesce(e.end_date, current_date) and d.date >= e.start_date
    --but this join includes admissions but no matching date was found in date_range and so we have nulls for activity_date_range.
    --testing alternative which includes: completed admissions if they overlap date_range, Ongoing admissions up to current_date and Non-overlapping admissions dropped (no NULL rows)
    INNER JOIN date_range d ON d.date >= apc.start_date  AND d.date <= COALESCE(apc.end_date, current_date)
    where 
--Removes invalid patients and Excludes very recent admissions (last 7 days)
        apc.sk_patient_id is not null and apc.sk_patient_id != '1'
        AND START_DATE <= dateadd(dd, -7, current_date)
    group by apc.sk_patient_id, m.hex_id, d.date, apc.start_date, apc.end_date, case when apc.spec_comm_flag = 'Y' then 'spec_comm' else apc.admission_method_group end
)

, combined as (
    select * from ae_encounter_summary
    union all
    select * from apc_encounter_summary
)
/*daily aggregation of healthcare activity per patient. converts raw events into 
ip_nel_emergency_encounters = number of emergency admissions
ip_nel_emergency_cost = cost of those admissions
ip_nel_emergency_duration = bed days (length of stay)
*/
, fct_person_activity_by_day_ral as (
    select 
        sk_patient_id
        , hex_id
        , activity_date_range
        -- A&E ()
        , sum(case when activity_type = 'A&E' then encounters else 0 end) as ae_encounters
        , sum(case when activity_type = 'A&E' then cost else 0 end) as ae_cost
        -- Inpatient NEL
        , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - emergency' then encounters else 0 end) as ip_nel_emergency_encounters
        , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - emergency' then cost else 0 end) as ip_nel_emergency_cost
        , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - emergency' then duration else 0 end) as ip_nel_emergency_duration
    from
        combined
    group by 
        all
)
/*This creates a daily time series per patient, even if they had no activity. Build a complete date spine, then attach costs/activity
Enables longitudinal analysis: Track cost over time, Align patients around intervention date, Compare pre vs post
Because of the date spine + left join: Patients with no activity still appear. Their cost = 0, avoids bias when calculating averages
 */
, member_daily_costs as (
    select
        d.date,
        m.hex_id ,
        m.myria_status,
        --Activity Status used for impact evaluation.
        case when d.date <= m.eligibility_date then 'Pre-intervention' else 'Post-intervention' end as activity_status,
        --Week 0 = first week after eligibility. Negative values = pre-period. Positive values = post-period
        FLOOR(datediff(DAY, dateadd(dd, 1, m.eligibility_date), d.date)/7) AS week_n,
        -- cost as total daily cost per person, across activity types.  No activity = 0 (via SUM)
        round (sum (ip_nel_emergency_cost), 10) as ip_nel_emergency_cost,
        round (sum (ae_cost), 10) as ae_cost,
        -- encounters as Number of events per day per patient
        round (sum (ip_nel_emergency_encounters), 10) as ip_nel_emergency_encounters,
        round (sum (ae_encounters), 10) as ae_encounters,
        -- bed days means total inpatient days used per day
        round (sum (ip_nel_emergency_duration), 10) as ip_nel_emergency_duration
    from
        member_spine as m
    left join date_range as d -- date spine between 1 year before eligibility date and date of death/current date - 7 days (to allow for incomplete data)
        on d.date between dateadd(dd, -365, eligibility_date) 
            and case when m.death_date < dateadd(dd, -7, current_date()) then m.death_date else dateadd(dd, -7, current_date()) end
    left join fct_person_activity_by_day_ral as c 
        on m.hex_id = c.hex_id
        and d.date = c.activity_date_range
     group by all
)

/*aggregates the daily patient-level data into weekly cohort-level metrics, grouped by:
week_n = time relative to intervention, myria_status = cohort/group (e.g. treatment vs control),activity_status = pre vs post intervention 
So each row = one cohort × one week × pre/post period
*/
select
    week_n,
    myria_status,
    activity_status,
   -- n_days = total patient-days in that group
    count(*) as n_days,
   -- n_patients = approximate number of patients. Each patient contributes ~7 days per week so divide by 7.
    COUNT(*)/7 as n_patients,
    -- costs as total weekly cost across all patients in that week
    sum (ifnull(ip_nel_emergency_cost,0)) as ip_nel_emergency_cost,
    sum (ifnull(ae_cost,0)) as ae_cost,
    -- encounters as Utilisation (activity volume) as total number of events across all patients in that week
    sum (ifnull(ip_nel_emergency_encounters,0)) as ip_nel_emergency_encounters,
    sum (ifnull(ae_encounters,0))  as ae_encounters,
    -- bed days. Total inpatient bed days used during that week
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

