{{
    config(
        event_time='created_at',
    )
}}

/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('statista_global', 'hospital_quality_metrics') }}

),

/*
    Formatted
*/

formatted AS (

    SELECT
        -- FK
        {{ dbt_utils.generate_surrogate_key(['metric_id','hospital_id','created_at']) }} AS _surrogate_key,
        CAST(metric_id AS STRING) AS metric_id,
        CAST(hospital_id AS STRING) AS hospital_id,
    
        -- Details
        CAST(metric_source AS STRING) AS metric_source,
        CAST(metric_category AS STRING) AS metric_category,
        CAST(metric_name AS STRING) AS metric_name,
        CAST(data_quality_flag AS STRING) AS data_quality_flag,

        -- Measures
        CAST(metric_value AS DOUBLE) AS metric_value,

        -- Metadata
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(measurement_period_start AS TIMESTAMP) AS measurement_period_start,
        CAST(measurement_period_end AS TIMESTAMP) AS measurement_period_end
    FROM
        source_data

)

SELECT * FROM formatted
