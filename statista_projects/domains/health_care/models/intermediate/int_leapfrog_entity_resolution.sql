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

WITH leapfrog_grade AS (

    SELECT * FROM {{ ref('stg_statista_leapfrog_hospital_safety') }}

),

statista_master_hospitals AS (

    SELECT * FROM {{ ref('stg_statista_global_hospitals') }}

),


/*
    Transformations
*/

resolved_candidates AS (

    ---------------------------------------------------------------------------
    -- (A) STRICT MATCH: CMS Provider ID (CCN)
    -- High-confidence match using government-assigned CCN ID
    ---------------------------------------------------------------------------
    SELECT
        leapfrog_grade.*,
        statista_master_hospitals.hospital_id,
        CASE WHEN statista_master_hospitals.hospital_id IS NOT NULL THEN 1.0 ELSE 0.0 END AS match_confidence,
        CASE WHEN statista_master_hospitals.hospital_id IS NOT NULL THEN 'cms_provider_id' END AS match_method,
        CASE WHEN statista_master_hospitals.hospital_id IS NOT NULL THEN 'matched_strict'  END AS resolution_status
    FROM leapfrog_grade
    LEFT JOIN MASTER statista_master_hospitals
        ON leapfrog_grade.cms_provider_number = statista_master_hospitals.cms_provider_id


    UNION ALL

    ---------------------------------------------------------------------------
    --FUZZY MATCH:
    -- Only applies if CMS ID missing (cms_provider_number IS NULL)
    -- Requires BOTH:
    --    • Jaro-Winkler name similarity >= 90
    --    • City + State exact match
    --
    -- This ensures conservative matching (minimize false positives)
    ---------------------------------------------------------------------------
    SELECT
        leapfrog_grade.*,
        statista_master_hospitals.hospital_id,
        0.6 as match_confidence, -- 0.6 signals “fallback match with moderate confidence,” 
        'jaro_winkler_city_state' as match_method,
        'matched_fuzzy' as resolution_status
    FROM leapfrog_grade
    JOIN statista_master_hospitals
        ON leapfrog_grade.cms_provider_number IS NULL                            -- CCN missing
       AND UPPER(leapfrog_grade.city) = UPPER(statista_master_hospitals.city)                            -- same city
       AND UPPER(leapfrog_grade.state) = UPPER(statista_master_hospitals.state_code)                     -- same state
       AND JAROWINKLER_SIMILARITY(UPPER(leapfrog_grade.facility_name),
                                  UPPER(statista_master_hospitals.hospital_name)) >= 90     -- strong name match
),

-- ---------------------------------------------------------------------------
-- Apply match priority:
--   strict (1) > fuzzy (2) > unmatched (3)
--
--For each Leapfrog hospital, pick the best match from strict match, fuzzy match, or unmatched. 
--Strict match always wins, fuzzy only if strict doesn’t exist, and unmatched only if no match is found at all.
-- Then keep only that one row.
-- ---------------------------------------------------------------------------
priority_match AS (
    SELECT *
    FROM resolved_candidates
   QUALIFY ROW_NUMBER() OVER (
            PARTITION BY _surrogate_key
            ORDER BY
                CASE resolution_status
                    WHEN 'matched_strict' THEN 1
                    WHEN 'matched_fuzzy'  THEN 2
                    ELSE 3
                END,
                match_confidence DESC
        ) = 1
),

/*
    Formatted
*/

formatted AS (

    SELECT 
        _surrogate_key,
        facility_id,
        hospital_id,
        cms_provider_number,
    
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
        measurement_period_start
   FROM priority_match
)

SELECT * FROM formatted
