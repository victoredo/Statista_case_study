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

WITH unpivoted AS (
    SELECT *
    FROM (
        {{
            dbt_utils.unpivot(
            relation=ref('int_leapfrog_entity_resolution'),
            exclude=[
                "_surrogate_key","facility_id", "cms_provider_number",
                "facility_name", " address_line_1", "hospital_id",
                "state_code", "city", "zip_code","match_confidence",
                "resolution_status", "created_at", "measurement_period_start"  
                ],
            field_name="metrics_name",
            value_name="metrics_value",
            cast_to="double"
        ) }}
    )
),


/*
    Formatted
*/
formatted AS (

    SELECT 
        {{ dbt_utils.generate_surrogate_key(['facility_id','cms_provider_number',grade_date']) }} AS _surrogate_key,
        {{ dbt_utils.generate_surrogate_key(['facility_id','cms_provider_number',grade_date','metric_name']) }} AS metric_id,
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
        -- Static values for vendor identity
        'LEAPFROG' as metric_source,
        'PATIENT_SAFETY' as metric_category,

        -- Measures
        metric_name,
        metric_value,

        -- Metadata
        created_at,
        measurement_period_start
    FROM priority_match
)

SELECT * FROM formatted