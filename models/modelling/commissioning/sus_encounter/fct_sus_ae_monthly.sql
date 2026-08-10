{{
    config(materialized = 'table')
}}

--------------------------------------------------------------------------------
-- Migrated from [ETL].[postProcess_AE_Current].
--
-- The stored procedure ran a series of UPDATE statements against the loaded
-- fact table. Here each UPDATE becomes a column expression instead: the row is
-- built correct once rather than rewritten seven times.
--
-- The SP's step order turned out NOT to be load-bearing -- no step consumed a
-- column an earlier step had written -- so this is a single SELECT rather than
-- a chain of CTEs. If a future step does depend on an earlier one, it has to
-- become its own CTE layer.
--
-- SP steps implemented here:
--   1. z_arrival_datetime + 6 z_em_*_datetime columns  (macro ae_event_datetime)
--   2. z_commissioning_access = 2 for duplicate attendances
--   3. z_pod_level_4                                   (macro update_pod_ae)
--   4. z_provider_site_code
--   6. z_ccg_code                                      (macro assign_sus_commissioner)
--
-- SP steps NOT implemented (blocked, tagged [TODO] inline):
--   5. z_imd_2015_*                  -- source not yet in Snowflake (parked)
--   7. z_care_home                   -- source not yet in Snowflake (parked)
--
-- SP steps intentionally dropped: the blocks commented out in the original
-- (sensitive data category, NHS number pseudo, reason for access, zCCGCode via
-- udf_zCCGCodeFromFields, LSOA01). These were dead code in the SP -- the
-- "Sandpit does not contain..." notes explain why. They stay NULL, as they
-- were in the legacy table.
--------------------------------------------------------------------------------

with int_ae as (

    select * from {{ ref('int_sus_ae_monthly') }}

),

keyed as (

    -- SP step 2, part 1. The SP built this key into #temp1, numbered it in
    -- #temp2, then joined back on (SourceItemID, RowID). None of that is
    -- needed here: the numbering happens in the same pass as the rows it
    -- labels, so no row key and no temp tables.
    select
        *
        ,coalesce(sk_patient_id::varchar, local_patient_identifier, '0')
            || ' | ' || coalesce(organisation_code_code_of_provider, '0')
            || ' | ' || coalesce(to_char(to_date(arrival_date), 'DD-MM-YY'), '0')
            || ' | ' || coalesce(to_char(to_time(arrival_time), 'HH24:MI:SS'), '0')
            as dupe_id
    from int_ae

),

flagged as (

    -- SP step 2, part 2. Split from the key derivation above so the
    -- partition key is a column reference rather than the full concatenation
    -- repeated inline.
    select
        *
        ,case
            -- 20220908: sk_patient_id = 1 is excluded from dedup entirely.
            -- Those rows keep whatever z_commissioning_access they arrived with.
            when sk_patient_id = 1 then null
            -- [DETERMINISM] The SP ordered by DupeID, i.e. by the partition
            -- key itself, so which duplicate survived was arbitrary and could
            -- differ between runs. Ordered by sk_encounter_id here so the same
            -- row wins every time.
            else row_number() over (
                partition by dupe_id
                order by sk_encounter_id
            )
         end as dupe_row_number
    from keyed

)

select
    -- [STRUCTURE] Snowflake threw internal error 300002 when these expressions
    -- sat inside SELECT * REPLACE (...). Dropping REPLACE and excluding the
    -- overwritten columns instead avoids it. Consequence: the 12 derived
    -- columns below now appear at the END of the column list rather than in
    -- their original positions. Immaterial for anything selecting by name;
    -- matters only for positional / select * comparisons against legacy.
    * exclude (
        dupe_id
        ,dupe_row_number
        ,z_arrival_datetime
        ,z_em_assessment_waiting_datetime
        ,z_em_attendance_conclusion_datetime
        ,z_em_datetime_seen_for_treatment
        ,z_em_departure_datetime
        ,z_em_duration_datetime
        ,z_em_initial_assessment_datetime
        ,z_em_treatment_wait_datetime
        ,z_commissioning_access
        ,z_provider_site_code
        ,z_pod_level_4
        ,z_ccg_code
    )

        ------------------------------------------------------------------
        -- SP step 1: bare event times -> full timestamps
        ------------------------------------------------------------------
        ,timestamp_ntz_from_parts(
            to_date(arrival_date),
            to_time(arrival_time)
        ) as z_arrival_datetime

        -- NOTE: em_assessment_waiting_time is currently a NULL stub in the int
        -- model, so this resolves to NULL. Expression is live and will populate
        -- itself if that column is ever sourced.
        ,{{ ae_event_datetime('arrival_date', 'arrival_time', 'em_assessment_waiting_time') }}
            as z_em_assessment_waiting_datetime

        ,{{ ae_event_datetime('arrival_date', 'arrival_time', 'em_attendance_conclusion_time') }}
            as z_em_attendance_conclusion_datetime

        ,{{ ae_event_datetime('arrival_date', 'arrival_time', 'em_time_seen_for_treatment') }}
            as z_em_datetime_seen_for_treatment

        ,{{ ae_event_datetime('arrival_date', 'arrival_time', 'em_departure_time') }}
            as z_em_departure_datetime

        -- [PARITY BUG] em_duration_time is a DURATION IN MINUTES, not a clock
        -- time -- values cluster hard at 239/240, i.e. the four-hour A&E
        -- target. The SP converted it to a 1900-anchored datetime and used
        -- T-SQL's datetime + datetime, which adds the DAY count. So 240
        -- minutes became 240 days: arrival 2021-04-01 with a 87-minute
        -- duration produced 2021-06-27 in the legacy table.
        --
        -- Reproduced here so the migration matches legacy. The correct
        -- expression, once signed off, is:
        --     dateadd(minute, em_duration_time, <z_arrival_datetime expr>)
        -- Note nothing downstream can be relying on the real duration -- it
        -- has never been populated. [TODO] raise with SUS reporting owner.
        ,dateadd(day, em_duration_time, to_date(arrival_date))
            as z_em_duration_datetime

        ,{{ ae_event_datetime('arrival_date', 'arrival_time', 'em_initial_assessment_time') }}
            as z_em_initial_assessment_datetime

        -- NOTE: em_treatment_wait_time is also a NULL stub in the int model.
        ,{{ ae_event_datetime('arrival_date', 'arrival_time', 'em_treatment_wait_time') }}
            as z_em_treatment_wait_datetime

        -- z_em_conclusion_waiting_datetime is deliberately absent: the SP block
        -- for it was commented out (no EM Conclusion Waiting Time in Sandpit).

        ------------------------------------------------------------------
        -- SP step 2: flag duplicate attendances
        --   2 = duplicate (second and subsequent rows for the same
        --       patient / provider / arrival timestamp)
        --   1 = in-area commissioner, first occurrence
        --   0 = out-of-area
        -- Per the 20210924 note, only 2s flow through to consolidated.
        ------------------------------------------------------------------
        ,case
            when dupe_row_number > 1 then 2
            else z_commissioning_access
         end as z_commissioning_access

        ------------------------------------------------------------------
        -- SP step 4: resolve provider site code
        -- organisation_code_code_of_provider is ALREADY truncated to 3 chars
        -- in int_sus_ae_monthly, so the LEFT(...,3) wrapper the SP used on
        -- every branch is dropped here.
        -- [CASE SENSITIVITY] SQL Server's default collation is case
        -- insensitive, so 'pfh' matched 'PFH'. Snowflake's = is case
        -- sensitive, hence the upper() wrappers on the affected branches.
        ------------------------------------------------------------------
        ,case
            when organisation_code_code_of_provider = 'RYJ'
                then substr(unique_cds_identifier, 2, 5)
            when organisation_code_code_of_provider = 'RQN' then 'RYJ03'
            when organisation_code_code_of_provider = 'RJ5' then 'RYJ01'
            when organisation_code_code_of_provider = 'RQM'
                then substr(provider_reference_no, 1, 5)
            when organisation_code_code_of_provider = 'RAS'
                and em_department_type in ('01', '1') then 'RAS01'
            when organisation_code_code_of_provider = 'RAS'
                and em_department_type in ('03', '3') then 'RAS02'
            when organisation_code_code_of_provider = 'NTP'
                and em_department_type in ('03', '3')
                then left(organisation_code_code_of_provider, 5)
            when organisation_code_code_of_provider = 'RV8'
                and provider_site_code = 'RV846'
                and em_department_type in ('03', '3') then 'RC303'
            when organisation_code_code_of_provider = 'RV8'
                and provider_site_code = 'RV847'
                and em_department_type in ('03', '3') then 'NTP60'
            when organisation_code_code_of_provider = 'RC3'
                and em_department_type in ('01', '1') then 'RC368'
            when organisation_code_code_of_provider = 'RY9'
                and em_department_type in ('03', '3') then 'RY901'
            when organisation_code_code_of_provider = 'RFW'
                and em_department_type in ('01', '1') then 'RFW00'
            when organisation_code_code_of_provider in ('RV8', 'RAS', 'RFW')
                then left(provider_site_code, 5)
            when provider_site_code in ('RYX', 'RYX01') then 'RYX01'
            when provider_site_code = 'RYX02' then 'RYX02'
            when provider_site_code = 'RYX03' then 'RYX03'
            when provider_site_code = 'RYX23' then 'RYX23'
            when provider_site_code = 'RYX11' then 'RYX11'
            when upper(nhs_service_agreement_line_no) = 'PFH'
                and upper(provider_reference_no) = 'PFH' then 'PFH00'
            when upper(left(em_attendance_number, 2)) = 'NP'
                and organisation_code_code_of_provider = 'R1K' then 'R1K01'
            when upper(left(em_attendance_number, 2)) = 'ED'
                and organisation_code_code_of_provider = 'R1K' then 'R1K04'
            else left(provider_site_code, 5)
         end as z_provider_site_code

        ------------------------------------------------------------------
        -- SP step 3: point of delivery band
        ------------------------------------------------------------------
        ,{{ update_pod_ae('arrival_date', 'core_hrg', 'em_patient_group', 'em_department_type') }}
            as z_pod_level_4

        ------------------------------------------------------------------
        -- SP step 6: assign commissioner
        -- Argument order follows the UDF call in the SP:
        --   gp practice, lsoa, provider, activity date
        -- The SP wrapped the first three in ISNULL(...,''). Those coalesces
        -- are kept here; drop them if the macro handles nulls itself.
        -- Arrival date is passed bare: the SP's ISNULL([Arrival Date],'')
        -- relied on T-SQL coercing a date to an empty string, which will not
        -- work in Snowflake.
        ------------------------------------------------------------------
        ,{{ assign_sus_commissioner(
              "coalesce(gp_practice_code, '')",
              "coalesce(z_lsoa11, '')",
              "coalesce(organisation_code_code_of_provider, '')",
              "arrival_date"
           ) }} as z_ccg_code

        ------------------------------------------------------------------
        -- SP steps 5 and 7 go here once the sources land. Both need a dedup
        -- guard before joining -- IMD should be 1:1 on LSOA but is not
        -- guaranteed to be, and care home spells can overlap in time, which
        -- would multiply rows. Left as NULL for now (already NULL in int).
        --
        --   ,imd.z_imd_2015_london_quintile as z_imd_2015_london_quintile   -- [TODO]
        --   ,imd.z_imd_2015_national_decile as z_imd_2015_national_decile   -- [TODO]
        --   ,ch.ods_code                    as z_care_home                  -- [TODO]
        ------------------------------------------------------------------

from flagged

-- [TODO] SP step 5 -- once index_of_multiple_deprivation_2015 is a source:
-- left join (
--     select z_lsoa11, z_imd_2015_london_quintile, z_imd_2015_national_decile
--     from {{ '{{' }} source('dmic_reference', 'index_of_multiple_deprivation_2015') {{ '}}' }}
--     qualify row_number() over (partition by z_lsoa11 order by z_lsoa11) = 1
-- ) imd on flagged.z_lsoa11 = imd.z_lsoa11

-- [TODO] SP step 7 -- once care_home_extract is a source. The SP joined on
-- sk_patient_id AND arrival_date between start_date and coalesce(end_date,
-- '2050-12-31'). Overlapping spells for one patient WILL fan out; the qualify
-- picks the most recent spell that covers the attendance.
-- left join (
--     select sk_patient_id, ods_code, start_date,
--            coalesce(z_end_date, '2050-12-31'::date) as end_date
--     from {{ '{{' }} source('dmic_reference', 'care_home_extract') {{ '}}' }}
-- ) ch on flagged.sk_patient_id = ch.sk_patient_id
--     and to_date(flagged.arrival_date) between ch.start_date and ch.end_date
-- qualify row_number() over (
--     partition by flagged.sk_encounter_id order by ch.start_date desc
-- ) = 1
