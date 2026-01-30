{{
    config(
        event_time='created_at',
    )
}}

/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('statista_global', 'hospitals') }}

),

/*
    Formatted
*/

formatted AS (

    SELECT
        -- Pk/FK
        {{ dbt_utils.generate_surrogate_key(['hospital_id', 'cms_provider_id','created_at']) }} AS _surrogate_key,
        CAST(hospital_id AS STRING) AS hospital_id,
        CAST(cms_provider_id AS STRING) AS cms_provider_id,
    
        -- Details
        CAST(hospital_name AS STRING) AS hospital_name,
        CAST(npi_number AS STRING) AS npi_number,
        CAST(country_code AS STRING) AS country_code,
        CAST(state_code AS STRING) AS state_code,
        CAST(city AS STRING) AS city,
        CAST(hospital_type AS STRING) AS hospital_type,
        CAST(teaching_status AS STRING) AS teaching_status,

        -- Measures
        CAST(bed_count AS INTEGER) AS bed_count,

        -- Metadata
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at
    FROM
        source_data

)

SELECT * FROM formatted
