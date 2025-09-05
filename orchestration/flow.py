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
from ingestion.utils import get_bigquery_config

logger = logging.getLogger(__name__)

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def ingest_yelp_data(location: str = "San Francisco, CA", limit: int = 50) -> pd.DataFrame:
    """Ingest data from Yelp API."""
    try:
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

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def ingest_google_places_data(location: str = "San Francisco, CA", radius: int = 5000) -> pd.DataFrame:
    """Ingest data from Google Places API."""
    try:
        google = GooglePlacesIngestion()
        places = google.search_nearby_places(location, radius, "restaurant")
        
        if places:
            df = google.process_places_data(places)
            logger.info(f"Successfully ingested {len(df)} Google Places records")
            return df
        else:
            logger.warning("No Google Places data retrieved")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Error in Google Places ingestion: {e}")
        return pd.DataFrame()

@task
def combine_data_sources(yelp_df: pd.DataFrame, google_df: pd.DataFrame) -> pd.DataFrame:
    """Combine data from multiple sources."""
    try:
        if yelp_df.empty and google_df.empty:
            logger.warning("No data from any source")
            return pd.DataFrame()
        
        # Add source identifier if not already present
        if not yelp_df.empty:
            yelp_df['source'] = 'yelp'
        if not google_df.empty:
            google_df['source'] = 'google_places'
        
        # Combine the dataframes
        combined_df = pd.concat([yelp_df, google_df], ignore_index=True)
        
        logger.info(f"Combined {len(combined_df)} total records")
        return combined_df
        
    except Exception as e:
        logger.error(f"Error combining data sources: {e}")
        return pd.DataFrame()

@task
def load_to_bigquery(df: pd.DataFrame, table_name: str = "raw_restaurant_data") -> bool:
    """Load data to BigQuery data warehouse."""
    try:
        bigquery_config = get_bigquery_config()
        if not bigquery_config:
            logger.error("BigQuery configuration not available")
            return False
        
        # For now, we'll just log the data that would be loaded
        # In a real implementation, you would use google-cloud-bigquery
        logger.info(f"Would load {len(df)} records to BigQuery table: {table_name}")
        logger.info(f"Columns: {list(df.columns)}")
        logger.info(f"Project: {bigquery_config['project_id']}, Dataset: {bigquery_config['dataset_id']}")
        
        # TODO: Implement actual BigQuery loading
        # from google.cloud import bigquery
        # from google.oauth2 import service_account
        # 
        # # Initialize BigQuery client
        # credentials = service_account.Credentials.from_service_account_file(
        #     bigquery_config['credentials_path']
        # )
        # client = bigquery.Client(
        #     credentials=credentials,
        #     project=bigquery_config['project_id']
        # )
        # 
        # # Define table reference
        # table_id = f"{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.{table_name}"
        # 
        # # Configure load job
        # job_config = bigquery.LoadJobConfig(
        #     write_disposition="WRITE_TRUNCATE",  # or WRITE_APPEND
        #     source_format=bigquery.SourceFormat.CSV,
        #     autodetect=True
        # )
        # 
        # # Load data
        # job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        # job.result()  # Wait for job to complete
        
        return True
        
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
    location: str = "San Francisco, CA",
    yelp_limit: int = 50,
    google_radius: int = 5000
):
    """Main orchestration flow for restaurant data pipeline."""
    
    logger.info(f"Starting restaurant data pipeline for location: {location}")
    
    # Ingest data from multiple sources
    yelp_data = ingest_yelp_data(location, yelp_limit)
    google_data = ingest_google_places_data(location, google_radius)
    
    # Combine data sources
    combined_data = combine_data_sources(yelp_data, google_data)
    
    if not combined_data.empty:
        # Validate data quality
        quality_metrics = validate_data_quality(combined_data)
        
        # Load to BigQuery
        load_success = load_to_bigquery(combined_data)
        
        if load_success:
            logger.info("Pipeline completed successfully")
            return {
                "status": "success",
                "records_processed": len(combined_data),
                "quality_metrics": quality_metrics
            }
        else:
            logger.error("Pipeline failed at BigQuery loading step")
            return {"status": "error", "step": "bigquery_loading"}
    else:
        logger.error("Pipeline failed - no data to process")
        return {"status": "error", "step": "data_ingestion"}

if __name__ == "__main__":
    # Run the pipeline
    result = restaurant_data_pipeline()
    print(f"Pipeline result: {result}")
