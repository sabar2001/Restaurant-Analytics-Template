{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['restaurant_id'], 'type': 'btree'},
      {'columns': ['city', 'state'], 'type': 'btree'},
      {'columns': ['rating_category'], 'type': 'btree'}
    ]
  )
}}

WITH staging AS (
    SELECT * FROM {{ ref('stg_restaurants') }}
),

restaurant_metrics AS (
    SELECT
        restaurant_id,
        restaurant_name,
        rating,
        rating_category,
        data_source,
        city,
        state,
        price_level,
        price_category,
        review_count,
        categories,
        latitude,
        longitude,
        ingestion_timestamp,
        processed_at,
        
        -- Calculate some business metrics
        CASE 
            WHEN review_count >= 100 THEN 'High Volume'
            WHEN review_count >= 50 THEN 'Medium Volume'
            WHEN review_count >= 10 THEN 'Low Volume'
            ELSE 'Very Low Volume'
        END AS review_volume_category,
        
        -- Create a composite location key
        CONCAT(COALESCE(city, 'Unknown'), ', ', COALESCE(state, 'Unknown')) AS full_location,
        
        -- Create a business tier based on rating and review count
        CASE 
            WHEN rating >= 4.5 AND review_count >= 100 THEN 'Premium'
            WHEN rating >= 4.0 AND review_count >= 50 THEN 'High Quality'
            WHEN rating >= 3.5 THEN 'Good Quality'
            WHEN rating >= 3.0 THEN 'Standard'
            ELSE 'Needs Improvement'
        END AS business_tier,
        
        -- Add data freshness indicator
        CASE 
            WHEN DATEDIFF('day', ingestion_timestamp, CURRENT_TIMESTAMP()) <= 1 THEN 'Very Fresh'
            WHEN DATEDIFF('day', ingestion_timestamp, CURRENT_TIMESTAMP()) <= 7 THEN 'Fresh'
            WHEN DATEDIFF('day', ingestion_timestamp, CURRENT_TIMESTAMP()) <= 30 THEN 'Recent'
            ELSE 'Stale'
        END AS data_freshness
        
    FROM staging
),

final AS (
    SELECT
        restaurant_id,
        restaurant_name,
        rating,
        rating_category,
        data_source,
        city,
        state,
        full_location,
        price_level,
        price_category,
        review_count,
        review_volume_category,
        categories,
        latitude,
        longitude,
        business_tier,
        data_freshness,
        ingestion_timestamp,
        processed_at,
        
        -- Add audit fields
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        '{{ invocation_id }}' AS dbt_run_id
        
    FROM restaurant_metrics
)

SELECT * FROM final
