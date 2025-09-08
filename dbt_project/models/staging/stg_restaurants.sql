{{ config(materialized='view') }}

-- Working staging model for our actual BigQuery data schema
WITH source AS (
    SELECT * FROM {{ source('restaurant_db', 'raw_restaurant_data') }}
),

cleaned AS (
    SELECT
        -- Use place_id as the primary identifier
        COALESCE(place_id, CONCAT('unknown_', ROW_NUMBER() OVER (ORDER BY ingestion_timestamp))) AS restaurant_id,
        
        -- Clean restaurant name
        TRIM(COALESCE(name, 'Unknown Restaurant')) AS restaurant_name,
        
        -- Validate rating
        CASE 
            WHEN rating IS NULL THEN NULL
            WHEN rating < 0 THEN 0
            WHEN rating > 5 THEN 5
            ELSE rating
        END AS rating,
        
        -- Data source
        LOWER(COALESCE(data_source, 'unknown')) AS data_source,
        
        -- Extract city and state from formatted_address using BigQuery regex
        TRIM(REGEXP_EXTRACT(formatted_address, r'^([^,]+)')) AS city,
        TRIM(REGEXP_EXTRACT(formatted_address, r'([A-Z]{2})\s+\d+')) AS state,
        
        -- Price level with validation
        CASE 
            WHEN price_level IS NULL THEN NULL
            WHEN price_level < 0 THEN 0
            WHEN price_level > 4 THEN 4
            ELSE CAST(price_level AS INT64)
        END AS price_level,
        
        -- Review count
        COALESCE(CAST(user_ratings_total AS INT64), 0) AS review_count,
        
        -- Coordinates
        CAST(geometry_location_lat AS FLOAT64) AS latitude,
        CAST(geometry_location_lng AS FLOAT64) AS longitude,
        
        -- Other fields
        COALESCE(formatted_address, '') AS formatted_address,
        COALESCE(types, '[]') AS categories,
        
        -- Timestamps
        ingestion_timestamp,
        CURRENT_DATETIME() AS processed_at
        
    FROM source
    WHERE name IS NOT NULL 
      AND rating IS NOT NULL
      AND geometry_location_lat IS NOT NULL 
      AND geometry_location_lng IS NOT NULL
),

enriched AS (
    SELECT
        *,
        
        -- Add derived fields using CASE statements (database-agnostic)
        CASE 
            WHEN rating IS NULL THEN 'No Rating'
            WHEN rating >= 4.5 THEN 'Excellent'
            WHEN rating >= 4.0 THEN 'Very Good'
            WHEN rating >= 3.5 THEN 'Good'
            WHEN rating >= 3.0 THEN 'Average'
            WHEN rating >= 2.0 THEN 'Below Average'
            ELSE 'Poor'
        END AS rating_category,
        
        CASE 
            WHEN price_level IS NULL THEN 'Unknown'
            WHEN price_level = 0 THEN 'Free'
            WHEN price_level = 1 THEN 'Budget'
            WHEN price_level = 2 THEN 'Mid-range'
            WHEN price_level = 3 THEN 'Expensive'
            WHEN price_level = 4 THEN 'Very Expensive'
            ELSE 'Unknown'
        END AS price_category,
        
        CASE 
            WHEN rating IS NULL OR review_count IS NULL THEN 'Unrated'
            WHEN rating >= 4.0 AND review_count >= 100 THEN 'Premium'
            WHEN rating >= 3.5 AND review_count >= 50 THEN 'Standard'
            WHEN rating >= 3.0 AND review_count >= 10 THEN 'Basic'
            ELSE 'New/Limited Reviews'
        END AS business_tier,
        
        CASE 
            WHEN review_count IS NULL THEN 'No Reviews'
            WHEN review_count = 0 THEN 'No Reviews'
            WHEN review_count <= 10 THEN 'Very Low Volume'
            WHEN review_count <= 50 THEN 'Low Volume'
            WHEN review_count <= 200 THEN 'Medium Volume'
            ELSE 'High Volume'
        END AS review_volume_category
        
    FROM cleaned
)

SELECT * FROM enriched
