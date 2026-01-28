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

WITH hospital_data AS (

    SELECT * FROM {{ ref('stg_statista_global_hospitals') }}

),

/*
    Formatted
*/

formatted AS (

    SELECT
        -- FK
        _surrogate_key,
        hospital_id,
        cms_provider_id,
    
        -- Details
        hospital_name,
        npi_number,
        country_code,
        state_code,
        city,
        hospital_type,
        teaching_status,

        -- Measures
        bed_count,

        -- Metadata
        created_at,
        updated_at
    FROM
        source_data

)

SELECT * FROM formatted
