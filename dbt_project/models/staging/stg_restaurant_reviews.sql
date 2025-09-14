{{ config(materialized='view') }}

-- Staging model for restaurant reviews data with rating buckets
WITH source AS (
    SELECT * FROM {{ source('restaurant_db', 'raw_restaurant_reviews') }}
),

cleaned AS (
    SELECT
        -- Core identifiers
        place_id,
        TRIM(COALESCE(restaurant_name, 'Unknown Restaurant')) AS restaurant_name,
        
        -- Overall restaurant metrics
        COALESCE(CAST(overall_rating AS FLOAT64), 0) AS overall_rating,
        COALESCE(CAST(user_ratings_total AS INT64), 0) AS user_ratings_total,
        COALESCE(CAST(total_reviews_available AS INT64), 0) AS total_reviews_available,
        
        -- Review counts per rating bucket
        COALESCE(CAST(reviews_1_2_count AS INT64), 0) AS reviews_1_2_count,
        COALESCE(CAST(reviews_2_3_count AS INT64), 0) AS reviews_2_3_count,
        COALESCE(CAST(reviews_3_4_count AS INT64), 0) AS reviews_3_4_count,
        COALESCE(CAST(reviews_4_5_count AS INT64), 0) AS reviews_4_5_count,
        
        -- JSON arrays of reviews (stored as STRING in BigQuery)
        COALESCE(reviews_1_2, '[]') AS reviews_1_2_json,
        COALESCE(reviews_2_3, '[]') AS reviews_2_3_json,
        COALESCE(reviews_3_4, '[]') AS reviews_3_4_json,
        COALESCE(reviews_4_5, '[]') AS reviews_4_5_json,
        
        -- Metadata
        COALESCE(data_source, 'google_places_reviews') AS data_source,
        collection_timestamp,
        CURRENT_DATETIME() AS processed_at
        
    FROM source
    WHERE place_id IS NOT NULL
),

enriched AS (
    SELECT
        *,
        
        -- Calculate total collected reviews
        (reviews_1_2_count + reviews_2_3_count + reviews_3_4_count + reviews_4_5_count) AS total_reviews_collected,
        
        -- Calculate collection completeness percentage
        CASE 
            WHEN total_reviews_available > 0 THEN
                ROUND(
                    ((reviews_1_2_count + reviews_2_3_count + reviews_3_4_count + reviews_4_5_count) / 
                    CAST(total_reviews_available AS FLOAT64)) * 100, 2
                )
            ELSE 0
        END AS collection_completeness_pct,
        
        -- Determine review collection quality
        CASE 
            WHEN (reviews_1_2_count + reviews_2_3_count + reviews_3_4_count + reviews_4_5_count) >= 30 THEN 'Excellent'
            WHEN (reviews_1_2_count + reviews_2_3_count + reviews_3_4_count + reviews_4_5_count) >= 20 THEN 'Good'
            WHEN (reviews_1_2_count + reviews_2_3_count + reviews_3_4_count + reviews_4_5_count) >= 10 THEN 'Fair'
            ELSE 'Limited'
        END AS review_collection_quality,
        
        -- Calculate rating distribution balance
        CASE 
            WHEN reviews_1_2_count > 0 AND reviews_2_3_count > 0 AND reviews_3_4_count > 0 AND reviews_4_5_count > 0 
            THEN 'Balanced'
            WHEN (reviews_1_2_count + reviews_2_3_count + reviews_3_4_count + reviews_4_5_count) > 0 
            THEN 'Partial'
            ELSE 'None'
        END AS rating_distribution_balance,
        
        -- Most represented rating bucket
        CASE 
            WHEN reviews_4_5_count >= GREATEST(reviews_1_2_count, reviews_2_3_count, reviews_3_4_count) THEN 'High Ratings (4-5)'
            WHEN reviews_3_4_count >= GREATEST(reviews_1_2_count, reviews_2_3_count, reviews_4_5_count) THEN 'Good Ratings (3-4)'
            WHEN reviews_2_3_count >= GREATEST(reviews_1_2_count, reviews_3_4_count, reviews_4_5_count) THEN 'Average Ratings (2-3)'
            WHEN reviews_1_2_count >= GREATEST(reviews_2_3_count, reviews_3_4_count, reviews_4_5_count) THEN 'Low Ratings (1-2)'
            ELSE 'Unknown'
        END AS dominant_rating_bucket
        
    FROM cleaned
)

SELECT * FROM enriched
