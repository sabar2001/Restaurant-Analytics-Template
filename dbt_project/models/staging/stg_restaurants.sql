{{
  config(
    materialized='view'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'raw_restaurant_data') }}
),

cleaned AS (
    SELECT
        -- Standardize ID field
        COALESCE(id, CONCAT('unknown_', ROW_NUMBER() OVER (ORDER BY ingestion_timestamp))) AS restaurant_id,
        
        -- Clean and standardize name
        TRIM(name) AS restaurant_name,
        
        -- Standardize rating (ensure it's between 0 and 5)
        CASE 
            WHEN rating < 0 THEN 0
            WHEN rating > 5 THEN 5
            ELSE rating
        END AS rating,
        
        -- Standardize data source
        LOWER(data_source) AS data_source,
        
        -- Extract location information if available
        COALESCE(
            TRY_CAST(REGEXP_EXTRACT(formatted_address, '([^,]+),\\s*([^,]+),\\s*([A-Z]{2})', 1) AS STRING),
            'Unknown'
        ) AS city,
        
        COALESCE(
            TRY_CAST(REGEXP_EXTRACT(formatted_address, '([^,]+),\\s*([^,]+),\\s*([A-Z]{2})', 3) AS STRING),
            'Unknown'
        ) AS state,
        
        -- Extract price level if available
        COALESCE(price_level, 0) AS price_level,
        
        -- Extract review count if available
        COALESCE(user_ratings_total, 0) AS review_count,
        
        -- Extract categories/tags if available
        COALESCE(categories, '[]') AS categories,
        
        -- Standardize coordinates
        TRY_CAST(latitude AS FLOAT) AS latitude,
        TRY_CAST(longitude AS FLOAT) AS longitude,
        
        -- Timestamps
        ingestion_timestamp,
        CURRENT_TIMESTAMP() AS processed_at
        
    FROM source
    WHERE name IS NOT NULL  -- Filter out records without names
),

final AS (
    SELECT
        restaurant_id,
        restaurant_name,
        rating,
        data_source,
        city,
        state,
        price_level,
        review_count,
        categories,
        latitude,
        longitude,
        ingestion_timestamp,
        processed_at,
        
        -- Add some derived fields
        CASE 
            WHEN rating >= 4.5 THEN 'Excellent'
            WHEN rating >= 4.0 THEN 'Very Good'
            WHEN rating >= 3.5 THEN 'Good'
            WHEN rating >= 3.0 THEN 'Average'
            ELSE 'Below Average'
        END AS rating_category,
        
        CASE 
            WHEN price_level = 1 THEN '$'
            WHEN price_level = 2 THEN '$$'
            WHEN price_level = 3 THEN '$$$'
            WHEN price_level = 4 THEN '$$$$'
            ELSE 'Unknown'
        END AS price_category
        
    FROM cleaned
)

SELECT * FROM final
