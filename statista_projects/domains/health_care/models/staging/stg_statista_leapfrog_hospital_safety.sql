{{
    config(
        event_time='created_at',
    )
}}

/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('leapfrog_external', 'hospital_safety_grades') }}

),

/*
    Formatted
*/

formatted AS (

    SELECT
        -- FK
        {{ dbt_utils.generate_surrogate_key(['facility_id','cms_provider_number',grade_date']) }} AS _surrogate_key,
        CAST(facility_id AS STRING) AS facility_id,
        CAST(cms_provider_number AS STRING) AS cms_provider_number,
    
        -- Details
        CAST(facility_name AS STRING) AS facility_name,
        CAST(address_line_1 AS STRING) AS address_line_1,
        CAST(state AS STRING) AS state_code,
        CAST(city AS STRING) AS city,
        CAST(zip_code AS STRING) AS zip_code,

        -- Measures
        CAST(safety_grade AS DOUBLE) AS safety_grade,
        CAST(infection_score AS DOUBLE) AS infection_score,
        CAST(surgical_problems_score AS DOUBLE) AS surgical_problems_score ,
        CAST(medication_safety_score AS DOUBLE) AS medication_safety_score,
        CAST(hospital_acquired_infection_rate AS DOUBLE) AS hospital_acquired_infection_rate,
        CAST(fall_injury_rate AS DOUBLE) AS fall_injury_rate,
        CAST(data_completeness_pct AS DOUBLE) AS data_completeness_pct,

        -- Metadata
        CAST(loaded_at AS TIMESTAMP) AS created_at,
        CAST(grade_date AS TIMESTAMP) AS measurement_period_start
    FROM
        source_data

)

SELECT * FROM formatted
