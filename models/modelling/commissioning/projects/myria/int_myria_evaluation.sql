{{
    config(
        materialized='table',
        tags=['myria_eval'])
}}

/* EVALUATIon SCRIPT USING PROPENSITY MATCHED GROUPS WITH JESS AMENDMENTS 2.6.2026 person level member costs
THIS QUERY LOOKS AT JUST IP and UEC ACTIVITY but can be expanded to include OP activity and potentially GP apointments from OLIDS */

with date_range as (-- Generate days for 2 years
    select 
         dateadd('day', seq4() - 700, current_date) as date
    from 
        table(generator(rowcount => 700))
    where 
        date <= current_date()
)
--memberspine derived from matched group of patients rather than whole eligible population - select the latest PROPENSITY MATCH FILE
, member_spine as (
     select
        mp.patient_id,
        mp.hex_id,
        mp.local_authority,
        mp.first_file_date as eligibility_date,
        mp.onboarded_date,
        mp.death_date,
        case when mp.group_value = 1 then 'Onboarded 60+ days' else 'Not onboarded' end as myria_status
    from {{ ref('stg_myria_matched_patients') }} mp
   --from STAGING.MYRIA.STG_MYRIA_MATCHED_PATIENTS mp
   where file_date = (select max(file_date) from {{ ref('stg_myria_matched_patients') }})
   --WHERE file_date = (select max(file_date) from STAGING.MYRIA.STG_MYRIA_MATCHED_PATIENTS)
)
 /*Build activity data (A&E): ae_encounter_summary as Counts:encounters, cost,duration
limit to eligible group 66K rows by inner join with member spine. Added date range limits to reduce rows from 15 million!
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
    from {{ ref("obt_encounter_uec") }} uec
    --from REPORTING.COMMISSIonING_REPORTING.OBT_ENCOUNTER_UEC uec
        inner join member_spine m on uec.sk_patient_id = m.patient_id
        inner join date_range d on d.date >= uec.start_date  and d.date <= coalesce(uec.end_date, current_date)
    where 
        sk_patient_id is not null and sk_patient_id != '1' 
        and start_date <= dateadd(dd, -7, current_date)
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
limit by eligible patient activity by inner join with member spine.
testing alternative which includes: completed admissions if they overlap date_range, 
Ongoing admissions up to current_date and Non-overlapping admissions dropped (no NULL rows)*/
, apc_encounter_summary as(
   select
        apc.sk_patient_id
        ,m.hex_id
        , apc.start_date as activity_date
        , apc.end_date
        , d.date as activity_date_range
        , 'Inpatient' as activity_type
        , case when apc.spec_comm_flag = 'Y' then 'spec_comm' else apc.admission_method_group end as activity_subtype
        , sum(case when d.date = apc.start_date then 1 end) as encounters
        -- spreads costs against all days in spell, if open spell, all days except last 7
        , sum(cost / datediff(day, apc.start_date, coalesce(dateadd(dd, 1, apc.end_date), dateadd(dd, -6, current_date))) ) as cost 
        , sum(case when d.date = apc.start_date then 0 else 1 end) as duration 
        -- each row is 1 day. 0 day los would be 0, else 1. so 3 rows would give 2 day los - which is correct for admitted on first day and discharged 2 days later.
    from {{ ref("obt_encounter_apc") }} apc
    --from REPORTING.COMMISSIonING_REPORTING.obt_encounter_apc as apc
    inner join member_spine m on apc.sk_patient_id = m.patient_id
    -- join all days during the spell that are included in the activity date range
    --left join date_range as d on d.date <= coalesce(e.end_date, current_date) and d.date >= e.start_date
    --testing alternative which includes: completed admissions if they overlap date_range, Ongoing admissions up to current_date and Non-overlapping admissions dropped (no NULL rows)
    inner join date_range d on d.date >= apc.start_date  and d.date <= coalesce(apc.end_date, current_date)
    where 
        apc.sk_patient_id is not null and apc.sk_patient_id != '1'
        and start_date <= dateadd(dd, -7, current_date)
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
, fct_person_activity_by_day as (
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
    from combined
    group by 
        all
)
/*This creates a daily time series per patient, even if they had no activity. Build a complete date spine, then attach costs/activity
Enables longitudinal analysis: Track cost over time, Align patients around intervention date, Compare pre vs post
Because of the date spine + left join: Patients with no activity still appear. Their cost = 0, avoids bias when calculating averages.
Revised calculation person level n~1200
 */
, member_annual_costs as (
    select
        m.hex_id ,
        m.local_authority,
        m.myria_status,
        case when d.date <= m.eligibility_date then 'Pre-intervention' else 'Post-intervention' end as activity_status,
        count(d.date) as n_days,
        -- cost
        round (sum (ifnull(c.ip_nel_emergency_cost,0)), 10) as ip_nel_emergency_cost,
        round (sum (ifnull(c.ae_cost,0)), 10) as ae_cost,
        -- encounters
        round (sum (ifnull(c.ip_nel_emergency_encounters,0)), 10) as ip_nel_emergency_encounters,
        round (sum (ifnull(c.ae_encounters,0)), 10) as ae_encounters,
        -- bed days
        round (sum (ifnull(c.ip_nel_emergency_duration,0)), 10) as ip_nel_emergency_duration
    from member_spine as m
    left join date_range as d -- date spine between 1 year before eligibility date and date of death/current date - 7 days (to allow for incomplete data)
        on d.date between dateadd(dd, -365, eligibility_date) 
            and case when m.death_date < dateadd(dd, -7, current_date()) then m.death_date else dateadd(dd, -7, current_date()) end
    left join fct_person_activity_by_day as c 
        on m.hex_id = c.hex_id
        and d.date = c.activity_date_range
     group by all
)
/*aggregates the patient-level data into annualised cohort-level cost and activity metrics, grouped by:
myria_status = cohort/group (e.g. treatment vs control),activity_status = pre vs post intervention 
So each row = one cohort × pre/post period
*/
select
    myria_status,
    activity_status,
    count(*) as patients,
    -- averages
   avg (ip_nel_emergency_cost / (n_days / 365)) as avg_ip_nel_emergency_cost,
     avg (ae_cost / (n_days / 365)) as avg_ae_cost,
    avg (ip_nel_emergency_encounters / (n_days / 365)) as avg_ip_nel_emergency_encounters,
    avg (ae_encounters / (n_days / 365))  as avg_ae_encounters,
    avg (ip_nel_emergency_duration / (n_days / 365)) as avg_ip_nel_emergency_duration,
    -- -- standard deviations
    stddev (ip_nel_emergency_cost / (n_days / 365)) as stddev_ip_nel_emergency_cost,
    stddev (ae_cost / (n_days / 365)) as stddev_ae_cost,
    stddev (ip_nel_emergency_encounters / (n_days / 365)) as stddev_ip_nel_emergency_encounters,
    stddev (ae_encounters / (n_days / 365))  as stddev_ae_encounters,
    stddev (ip_nel_emergency_duration / (n_days / 365)) as stddev_ip_nel_emergency_duration
    
from member_annual_costs as m 
 where n_days <> 0
group by    all
order by 
    1, 2