{{ config(materialized='table') }}

-- Analytics-ready restaurant dimension table
WITH staging AS (
    SELECT * FROM {{ ref('stg_restaurants') }}
),

analytics_ready AS (
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
        review_volume_category,
        categories,
        latitude,
        longitude,
        formatted_address,
        business_tier,
        ingestion_timestamp,
        processed_at,
        
        -- Enhanced location information
        CONCAT(COALESCE(city, 'Unknown'), ', ', COALESCE(state, 'Unknown')) AS full_location,
        
        -- Additional analytics fields
        CASE 
            WHEN rating IS NOT NULL AND rating > 0 THEN 
                ROUND(rating * CAST(review_count AS FLOAT64) / 100, 2)
            ELSE 0
        END AS weighted_rating_score,
        
        -- Market positioning score
        CASE 
            WHEN rating >= 4.0 AND review_count >= 50 AND price_level <= 2 THEN 'High Value'
            WHEN rating >= 4.5 AND price_level >= 3 THEN 'Premium Experience'
            WHEN rating >= 3.5 AND review_count >= 100 THEN 'Popular Choice'
            WHEN rating >= 3.0 THEN 'Solid Option'
            ELSE 'Emerging/Risky'
        END AS market_positioning,
        
        -- Data quality score
        (
            CASE WHEN rating IS NOT NULL THEN 25 ELSE 0 END +
            CASE WHEN review_count IS NOT NULL AND review_count > 0 THEN 25 ELSE 0 END +
            CASE WHEN restaurant_name IS NOT NULL AND LENGTH(TRIM(restaurant_name)) > 0 THEN 25 ELSE 0 END +
            CASE WHEN formatted_address IS NOT NULL AND LENGTH(TRIM(formatted_address)) > 0 THEN 25 ELSE 0 END
        ) AS data_quality_score
        
    FROM staging
)

SELECT 
    *,
    CURRENT_DATETIME() AS dbt_updated_at
FROM analytics_ready
