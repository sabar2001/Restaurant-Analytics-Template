#!/usr/bin/env python3
"""
Scratch file for querying restaurant data directly from BigQuery
Run: python3 query_scratch.py
"""

from google.cloud import bigquery
from ingestion.utils import get_bigquery_config
import pandas as pd

# Initialize BigQuery client
config = get_bigquery_config()
client = bigquery.Client(project=config['project_id'])

def run_query(sql):
    """Execute a BigQuery SQL query and return results as DataFrame"""
    try:
        df = client.query(sql).to_dataframe()
        print(f"✅ Query returned {len(df)} rows")
        return df
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return None

def show_table_info():
    """Show basic table information"""
    sql = f"""
    SELECT 
        COUNT(*) as total_restaurants,
        COUNT(DISTINCT data_source) as data_sources,
        MIN(rating) as min_rating,
        MAX(rating) as max_rating,
        AVG(rating) as avg_rating,
        COUNT(DISTINCT SPLIT(formatted_address, ', ')[SAFE_OFFSET(2)]) as states
    FROM `{config['project_id']}.{config['dataset_id']}.raw_restaurant_data`
    """
    
    result = run_query(sql)
    if result is not None:
        print("\n📊 DATASET OVERVIEW:")
        print(result.to_string(index=False))

def top_restaurants_by_city():
    """Show top restaurants by city"""
    sql = f"""
    SELECT 
        SPLIT(formatted_address, ', ')[SAFE_OFFSET(1)] as city,
        name,
        rating,
        user_ratings_total as reviews
    FROM `{config['project_id']}.{config['dataset_id']}.raw_restaurant_data`
    WHERE rating >= 4.5 
    ORDER BY rating DESC, user_ratings_total DESC
    LIMIT 20
    """
    
    result = run_query(sql)
    if result is not None:
        print("\n🏆 TOP RATED RESTAURANTS:")
        print(result.to_string(index=False))

def city_stats():
    """Show restaurant stats by city"""
    sql = f"""
    SELECT 
        SPLIT(formatted_address, ', ')[SAFE_OFFSET(1)] as city,
        COUNT(*) as restaurant_count,
        ROUND(AVG(rating), 2) as avg_rating,
        SUM(user_ratings_total) as total_reviews
    FROM `{config['project_id']}.{config['dataset_id']}.raw_restaurant_data`
    GROUP BY 1
    ORDER BY restaurant_count DESC
    """
    
    result = run_query(sql)
    if result is not None:
        print("\n🏙️ CITY STATISTICS:")
        print(result.to_string(index=False))

def search_restaurants(search_term="pizza"):
    """Search for restaurants by name or type"""
    sql = f"""
    SELECT 
        name,
        SPLIT(formatted_address, ', ')[SAFE_OFFSET(1)] as city,
        rating,
        user_ratings_total as reviews,
        types
    FROM `{config['project_id']}.{config['dataset_id']}.raw_restaurant_data`
    WHERE LOWER(name) LIKE LOWER('%{search_term}%') 
       OR LOWER(types) LIKE LOWER('%{search_term}%')
    ORDER BY rating DESC
    LIMIT 15
    """
    
    result = run_query(sql)
    if result is not None:
        print(f"\n🔍 SEARCH RESULTS FOR '{search_term}':")
        print(result.to_string(index=False))

# ==================== MAIN EXECUTION ====================
if __name__ == "__main__":
    print("🍽️  RESTAURANT DATA EXPLORER")
    print("=" * 50)
    
    # Basic overview
    show_table_info()
    
    # City statistics
    city_stats()
    
    # Top restaurants
    top_restaurants_by_city()
    
    # Search example
    search_restaurants("pizza")
    
    print("\n" + "=" * 50)
    print("💡 CUSTOM QUERIES:")
    print("Edit this file to add your own SQL queries!")
    print("Available table: raw_restaurant_data")
    print("Available columns: place_id, name, rating, user_ratings_total, price_level, types, formatted_address, geometry_location_lat, geometry_location_lng, ingestion_timestamp, data_source")
