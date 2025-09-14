{{ config(materialized='table') }}

-- Comprehensive restaurant analytics combining basic info with reviews data
WITH restaurants AS (
    SELECT * FROM {{ ref('dim_restaurants') }}
),

reviews AS (
    SELECT * FROM {{ ref('stg_restaurant_reviews') }}
),

joined AS (
    SELECT
        -- Core restaurant information
        r.restaurant_id,
        r.restaurant_name,
        r.rating AS overall_rating,
        r.rating_category,
        r.review_count,
        r.review_volume_category,
        r.price_level,
        r.price_category,
        r.business_tier,
        r.market_positioning,
        r.city,
        r.state,
        r.full_location,
        r.formatted_address,
        r.latitude,
        r.longitude,
        r.data_source,
        r.data_quality_score,
        r.weighted_rating_score,
        r.categories,
        
        -- Reviews data (when available)
        rv.total_reviews_collected,
        rv.collection_completeness_pct,
        rv.review_collection_quality,
        rv.rating_distribution_balance,
        rv.dominant_rating_bucket,
        
        -- Individual rating bucket counts
        COALESCE(rv.reviews_1_2_count, 0) AS reviews_1_2_count,
        COALESCE(rv.reviews_2_3_count, 0) AS reviews_2_3_count,
        COALESCE(rv.reviews_3_4_count, 0) AS reviews_3_4_count,
        COALESCE(rv.reviews_4_5_count, 0) AS reviews_4_5_count,
        
        -- JSON reviews data (for AI training)
        rv.reviews_1_2_json,
        rv.reviews_2_3_json,
        rv.reviews_3_4_json,
        rv.reviews_4_5_json,
        
        -- Metadata
        r.ingestion_timestamp,
        r.processed_at,
        r.dbt_updated_at,
        rv.collection_timestamp AS reviews_collected_at
        
    FROM restaurants r
    LEFT JOIN reviews rv ON r.restaurant_id = rv.place_id
),

analytics_enhanced AS (
    SELECT
        *,
        
        -- Enhanced analytics combining restaurant and reviews data
        CASE 
            WHEN total_reviews_collected IS NOT NULL AND total_reviews_collected > 0 THEN 'Has Reviews'
            ELSE 'No Reviews'
        END AS reviews_availability,
        
        -- AI-readiness score (for future agentic system)
        (
            -- Base restaurant data quality (40 points)
            CASE WHEN data_quality_score >= 90 THEN 40
                 WHEN data_quality_score >= 75 THEN 30
                 WHEN data_quality_score >= 50 THEN 20
                 ELSE 10 END +
            
            -- Reviews availability (30 points)
            CASE WHEN total_reviews_collected >= 30 THEN 30
                 WHEN total_reviews_collected >= 20 THEN 25
                 WHEN total_reviews_collected >= 10 THEN 15
                 WHEN total_reviews_collected > 0 THEN 10
                 ELSE 0 END +
            
            -- Rating distribution balance (20 points)
            CASE WHEN rating_distribution_balance = 'Balanced' THEN 20
                 WHEN rating_distribution_balance = 'Partial' THEN 10
                 ELSE 0 END +
            
            -- Location data (10 points)
            CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 10
                 ELSE 0 END
        ) AS ai_readiness_score,
        
        -- Sentiment analysis readiness
        CASE 
            WHEN total_reviews_collected >= 20 AND rating_distribution_balance = 'Balanced' THEN 'Ready for Sentiment Analysis'
            WHEN total_reviews_collected >= 10 THEN 'Partial Sentiment Analysis'
            WHEN total_reviews_collected > 0 THEN 'Limited Sentiment Analysis'
            ELSE 'Insufficient Reviews'
        END AS sentiment_analysis_readiness,
        
        -- Recommendation engine readiness
        CASE 
            WHEN data_quality_score >= 75 AND total_reviews_collected >= 15 THEN 'Ready for Recommendations'
            WHEN data_quality_score >= 50 AND total_reviews_collected >= 5 THEN 'Basic Recommendations'
            ELSE 'Insufficient Data'
        END AS recommendation_readiness
        
    FROM joined
)

SELECT 
    *,
    CURRENT_DATETIME() AS analytics_updated_at
FROM analytics_enhanced
