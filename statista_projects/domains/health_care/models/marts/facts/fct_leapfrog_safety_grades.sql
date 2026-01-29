{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        event_time='created_at',
        begin='2025-01-01',
        lookback=1,
        batch_size='month'
    )
}}

/*
    Tables
*/

WITH leapfrog_safety_grades AS (

    SELECT * FROM {{ ref('snapshot_leapfrog_safety_grades') }}

),

/*
    Formatted
*/

formatted AS (

    select 
        _surrogate_key,
        facility_id,
        hospital_id,
        cms_provider_id,
    
        -- Details
        facility_name,
        address_line_1,
        state_code,
        city,
        zip_code,
        match_confidence,
        match_method,
        resolution_status,

        -- Measures
        safety_grade,
        infection_score,
        surgical_problems_score,
        medication_safety_score,
        hospital_acquired_infection_rate,
        fall_injury_rate,
        data_completeness_pct,

        -- Metadata
        created_at,
        measurement_period_start,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to

   FROM leapfrog_safety_grades
   QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _surrogate_key
        ORDER BY dbt_valid_from ASC
    ) = 1 --first_seen_record_per_facility 
)

SELECT * FROM formatted
