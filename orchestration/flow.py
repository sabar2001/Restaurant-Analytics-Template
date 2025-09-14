import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd
from typing import List, Dict, Any
import logging

from ingestion.yelp_ingest import YelpIngestion
from ingestion.google_places_ingest import GooglePlacesIngestion
from ingestion.utils import get_bigquery_config, get_available_apis
from ingestion.location_manager import LocationManager, get_enabled_locations
from ingestion.spark_processor import SparkRestaurantProcessor
from ingestion.spark_streaming import SparkStreamingProcessor

logger = logging.getLogger(__name__)

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def ingest_yelp_data(location: str = "San Francisco, CA", limit: int = 50) -> pd.DataFrame:
    """Ingest data from Yelp API."""
    try:
        from ingestion.utils import get_api_key
        
        # Check if Yelp API key is available
        if not get_api_key('yelp'):
            logger.warning("Yelp API key not configured - skipping Yelp data")
            return pd.DataFrame()
        
        yelp = YelpIngestion()
        businesses = yelp.search_businesses(location, "restaurant", limit)
        
        if businesses:
            df = yelp.process_business_data(businesses)
            logger.info(f"Successfully ingested {len(df)} Yelp records")
            return df
        else:
            logger.warning("No Yelp data retrieved")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Error in Yelp ingestion: {e}")
        return pd.DataFrame()

@task  # Temporarily disable cache to force fresh API calls
def ingest_google_places_data(location: str = "San Francisco, CA", radius: int = 10000, target_count: int = 500) -> pd.DataFrame:
    """Ingest large-scale data from Google Places API."""
    try:
        from ingestion.utils import get_api_key
        
        # Check if Google Places API key is available
        if not get_api_key('google'):
            logger.warning("Google Places API key not configured - skipping Google Places data")
            return pd.DataFrame()
        
        logger.info(f"🚀 Starting LARGE-SCALE Google Places ingestion for {location}")
        logger.info(f"Target: {target_count} restaurants with expanded search strategies")
        
        google = GooglePlacesIngestion()
        places = google.search_nearby_places(location, radius, "restaurant", target_count)
        
        if places:
            df = google.process_places_data(places)
            logger.info(f"✅ Successfully ingested {len(df)} Google Places records for {location}")
            return df
        else:
            logger.warning("No Google Places data retrieved")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Error in Google Places ingestion: {e}")
        return pd.DataFrame()

@task
def collect_restaurant_reviews(restaurant_df: pd.DataFrame, reviews_per_bucket: int = 10) -> pd.DataFrame:
    """Collect detailed reviews for restaurants."""
    try:
        from ingestion.utils import get_api_key
        
        if not get_api_key('google'):
            logger.warning("Google Places API key not configured - skipping reviews collection")
            return pd.DataFrame()
        
        if restaurant_df.empty or 'place_id' not in restaurant_df.columns:
            logger.warning("No restaurant data or place_id column available for reviews collection")
            return pd.DataFrame()
        
        logger.info(f"🔍 Starting reviews collection for {len(restaurant_df)} restaurants")
        logger.info(f"Target: {reviews_per_bucket} reviews per rating bucket")
        
        google = GooglePlacesIngestion()
        reviews_df = google.collect_reviews_for_restaurants(restaurant_df, reviews_per_bucket)
        
        if not reviews_df.empty:
            logger.info(f"✅ Successfully collected reviews for {len(reviews_df)} restaurants")
            return reviews_df
        else:
            logger.warning("No reviews data collected")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Error in reviews collection: {e}")
        return pd.DataFrame()

@task
def combine_data_sources_with_spark(yelp_dataframes: List[pd.DataFrame], google_dataframes: List[pd.DataFrame], config: Dict[str, Any] = None) -> Any:
    """Combine data from multiple sources using Spark or pandas."""
    try:
        # Initialize Spark processor
        spark_processor = SparkRestaurantProcessor(config)
        
        # Process each DataFrame
        processed_dataframes = []
        
        for yelp_df in yelp_dataframes:
            if not yelp_df.empty:
                processed_df = spark_processor.process_restaurant_data(yelp_df)
                processed_dataframes.append(processed_df)
        
        for google_df in google_dataframes:
            if not google_df.empty:
                processed_df = spark_processor.process_restaurant_data(google_df)
                processed_dataframes.append(processed_df)
        
        if not processed_dataframes:
            logger.warning("No data from any source")
            return spark_processor._process_with_pandas([])
        
        # Combine all processed DataFrames
        combined_data = spark_processor.combine_data_sources(processed_dataframes)
        
        logger.info(f"Combined data using {'Spark' if spark_processor.use_spark else 'pandas'}")
        return combined_data
        
    except Exception as e:
        logger.error(f"Error combining data sources: {e}")
        return pd.DataFrame()

@task
def load_to_bigquery_with_spark(data: Any, table_name: str = "raw_restaurant_data", config: Dict[str, Any] = None) -> bool:
    """Load data to BigQuery using Spark or pandas."""
    try:
        # Initialize Spark processor
        spark_processor = SparkRestaurantProcessor(config)
        
        # Write to BigQuery
        success = spark_processor.write_to_bigquery(data, table_name)
        
        if success:
            logger.info(f"Successfully loaded data to BigQuery using {'Spark' if spark_processor.use_spark else 'pandas'}")
        
        return success
        
    except Exception as e:
        logger.error(f"Error loading to BigQuery: {e}")
        return False

@task
def validate_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """Validate data quality and return metrics."""
    try:
        if df.empty:
            return {"status": "error", "message": "No data to validate"}
        
        metrics = {
            "total_records": len(df),
            "total_columns": len(df.columns),
            "missing_values": df.isnull().sum().to_dict(),
            "duplicate_records": df.duplicated().sum(),
            "data_types": df.dtypes.to_dict()
        }
        
        # Check for required columns
        required_columns = ['name', 'rating', 'data_source']
        missing_required = [col for col in required_columns if col not in df.columns]
        metrics["missing_required_columns"] = missing_required
        
        logger.info(f"Data quality validation completed: {metrics}")
        return metrics
        
    except Exception as e:
        logger.error(f"Error in data quality validation: {e}")
        return {"status": "error", "message": str(e)}

@flow(name="restaurant-data-pipeline")
def restaurant_data_pipeline(
    locations: List[str] = None,
    yelp_limit: int = 50,
    google_radius: int = 5000,
    use_all_locations: bool = True,
    use_spark: bool = None
):
    """Main orchestration flow for restaurant data pipeline."""
    
    # Load configuration
    location_manager = LocationManager()
    config = location_manager.get_settings()
    
    # Override Spark setting if specified
    if use_spark is not None:
        config['use_spark'] = use_spark
    
    logger.info(f"Using Spark: {config.get('use_spark', False)}")
    
    # Check available APIs
    available_apis = get_available_apis()
    if not any(available_apis.values()):
        logger.error("No API keys available! Please configure at least one API key in .env file")
        return {"status": "error", "step": "api_configuration", "message": "No API keys configured"}
    
    # Get locations to process
    if locations is None:
        if use_all_locations:
            locations = get_enabled_locations()
            logger.info(f"Using all enabled locations: {locations}")
        else:
            locations = ["San Francisco, CA"]  # Default fallback
            logger.info(f"Using default location: {locations}")
    else:
        logger.info(f"Using specified locations: {locations}")
    
    if not locations:
        logger.error("No locations specified for data ingestion")
        return {"status": "error", "step": "location_configuration"}
    
    logger.info(f"Starting restaurant data pipeline for {len(locations)} locations")
    
    # Process each location
    all_yelp_data = []
    all_google_data = []
    location_results = {}
    
    for location in locations:
        logger.info(f"Processing location: {location}")
        
        try:
            # Ingest data from available sources for this location
            yelp_data = pd.DataFrame()
            google_data = pd.DataFrame()
            
            if available_apis['yelp']:
                yelp_data = ingest_yelp_data(location, yelp_limit)
            
            if available_apis['google_places']:
                google_data = ingest_google_places_data(location, google_radius)
            
            # Store results
            all_yelp_data.append(yelp_data)
            all_google_data.append(google_data)
            
            location_results[location] = {
                "yelp_records": len(yelp_data),
                "google_records": len(google_data),
                "status": "success",
                "apis_used": [api for api, available in available_apis.items() if available]
            }
            
            logger.info(f"Location {location}: Yelp={len(yelp_data)}, Google={len(google_data)}")
            
        except Exception as e:
            logger.error(f"Error processing location {location}: {e}")
            location_results[location] = {
                "yelp_records": 0,
                "google_records": 0,
                "status": "error",
                "error": str(e),
                "apis_used": []
            }
    
    # Combine data sources using Spark or pandas
    combined_data = combine_data_sources_with_spark(all_yelp_data, all_google_data, config)
    logger.info(f"Returned from combine_data_sources_with_spark: type={type(combined_data)}, hasattr count={hasattr(combined_data, 'count') if combined_data is not None else False}")
    
    # Check if we have valid data - ultra-defensive approach
    has_data = False
    try:
        if combined_data is not None:
            logger.info(f"Combined data type: {type(combined_data)}")
            
            # Simple approach: try to get length, handle all edge cases
            # Better Spark detection - check for specific Spark DataFrame type
            if hasattr(combined_data, 'sql') or str(type(combined_data)).find('pyspark') != -1:  # Spark DataFrame
                try:
                    row_count = int(combined_data.count())
                    has_data = row_count > 0
                    logger.info(f"Spark DataFrame has {row_count} rows")
                except Exception as e:
                    logger.error(f"Failed to count Spark DataFrame rows: {e}")
                    has_data = False
                    
            elif hasattr(combined_data, '__len__'):  # pandas DataFrame/Series or list
                try:
                    row_count = len(combined_data)
                    # Additional check for pandas objects
                    if hasattr(combined_data, 'empty'):
                        # Safely check if empty without triggering Series ambiguity
                        try:
                            is_empty_check = combined_data.shape[0] == 0  # Use shape instead of .empty
                        except:
                            is_empty_check = row_count == 0
                        has_data = row_count > 0 and not is_empty_check
                    else:
                        has_data = row_count > 0
                    logger.info(f"Data object has {row_count} rows")
                except Exception as e:
                    logger.error(f"Error checking data length: {e}")
                    has_data = False
            else:
                logger.warning(f"Unknown data type: {type(combined_data)}")
                has_data = False
        else:
            logger.warning("combined_data is None")
            has_data = False
            
    except Exception as e:
        logger.error(f"Critical error in data checking: {e}")
        has_data = False
    
    # Ensure has_data is always a boolean
    has_data = bool(has_data)
    logger.info(f"Final has_data = {has_data}")
    
    if has_data:
        # Validate data quality
        if config.get('use_spark', False):
            # For Spark DataFrames, convert to pandas for validation
            try:
                validation_df = combined_data.toPandas()
            except:
                validation_df = pd.DataFrame()
        else:
            validation_df = combined_data
        
        quality_metrics = validate_data_quality(validation_df)
        
        # Load to BigQuery using Spark or pandas
        load_success = load_to_bigquery_with_spark(combined_data, "raw_restaurant_data", config)
        
        if load_success:
            logger.info("✅ Restaurant data loaded successfully")
            
            # Phase 2: Collect reviews for the restaurants we just ingested
            reviews_df = pd.DataFrame()
            try:
                logger.info("🔍 Starting Phase 2: Reviews collection...")
                
                # Use the validation_df (pandas) for reviews collection
                if not validation_df.empty and 'place_id' in validation_df.columns:
                    # Limit to top restaurants for reviews collection (API efficiency)
                    top_restaurants = validation_df.head(50)  # Collect reviews for top 50 restaurants
                    reviews_df = collect_restaurant_reviews(top_restaurants, reviews_per_bucket=10)
                    
                    if not reviews_df.empty:
                        # Load reviews to BigQuery
                        reviews_load_success = load_to_bigquery_with_spark(reviews_df, "raw_restaurant_reviews", config)
                        if reviews_load_success:
                            logger.info(f"✅ Reviews data loaded successfully: {len(reviews_df)} restaurants")
                        else:
                            logger.warning("Reviews collection succeeded but BigQuery loading failed")
                    else:
                        logger.warning("No reviews data collected")
                else:
                    logger.warning("Cannot collect reviews: missing place_id column or no restaurant data")
                    
            except Exception as e:
                logger.error(f"Error in reviews collection phase: {e}")
            
            logger.info("🎉 Complete pipeline finished successfully")
            return {
                "status": "success",
                "records_processed": len(validation_df) if not validation_df.empty else 0,
                "reviews_restaurants_processed": len(reviews_df) if not reviews_df.empty else 0,
                "locations_processed": len(locations),
                "location_results": location_results,
                "quality_metrics": quality_metrics,
                "processing_engine": "Spark" if config.get('use_spark', False) else "Pandas",
                "apis_used": [api for api, available in available_apis.items() if available],
                "total_api_sources": sum(available_apis.values()),
                "reviews_collection": "success" if not reviews_df.empty else "no_data"
            }
        else:
            logger.error("Pipeline failed at BigQuery loading step")
            return {"status": "error", "step": "bigquery_loading"}
    else:
        logger.error("Pipeline failed - no data to process")
        return {"status": "error", "step": "data_ingestion"}

@flow(name="streaming-restaurant-pipeline")
def streaming_restaurant_pipeline(
    table_name: str = "raw_restaurant_data",
    use_spark_streaming: bool = None
):
    """Real-time streaming pipeline for restaurant data."""
    
    # Load configuration
    location_manager = LocationManager()
    config = location_manager.get_settings()
    
    # Override streaming setting if specified
    if use_spark_streaming is not None:
        config['spark_streaming_enabled'] = use_spark_streaming
    
    if not config.get('spark_streaming_enabled', False):
        logger.warning("Spark streaming not enabled in configuration")
        return {"status": "error", "message": "Streaming not enabled"}
    
    try:
        # Initialize streaming processor
        streaming_processor = SparkStreamingProcessor(config)
        
        # Start streaming pipeline
        query = streaming_processor.start_streaming_pipeline(table_name)
        
        if query:
            logger.info("Streaming pipeline started successfully")
            return {
                "status": "success",
                "message": "Streaming pipeline started",
                "table_name": table_name,
                "query_id": query.id if hasattr(query, 'id') else "unknown"
            }
        else:
            logger.error("Failed to start streaming pipeline")
            return {"status": "error", "message": "Failed to start streaming"}
            
    except Exception as e:
        logger.error(f"Error in streaming pipeline: {e}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    # Run the pipeline
    result = restaurant_data_pipeline()
    print(f"Pipeline result: {result}")
