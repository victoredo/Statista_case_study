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

WITH all_vendor_data AS (
-- we add more vendors
        {{
            dbt_utils.union_relations(
                relations=[
                    ref('stg_statista_global_hospitals'),
                    ref('snapshot_leapfrog_metrics')
              
                ],
                source_column_name=None,
            )
        }}
),

/*
     Formatted
*/

 Formatted AS (

    SELECT
        _surrogate_key,
        metric_id,
        hospital_id,
        metric_source,
        metric_category,
        metric_name,
        metric_value,
        measurement_period_start,
        measurement_period_end,
        data_quality_flag,
        created_at
    FROM all_vendor_data
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _surrogate_key
        ORDER BY dbt_valid_from ASC
    ) = 1 --first_seen_record_per_facility 
)

SELECT * FROM formatted
