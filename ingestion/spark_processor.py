import os
import logging
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

class SparkRestaurantProcessor:
    """Spark-based data processor for restaurant analytics."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.spark = None
        self.use_spark = self.config.get('use_spark', False)
        
        if self.use_spark:
            self._initialize_spark()
    
    def _initialize_spark(self):
        """Initialize Spark session."""
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.functions import col, explode, regexp_extract, when, current_timestamp
            from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
            
            # Create Spark session builder
            builder = SparkSession.builder \
                .appName(self.config.get('spark_app_name', 'RestaurantAnalytics')) \
                .master(self.config.get('spark_master', 'local[*]')) \
                .config('spark.executor.memory', self.config.get('spark_memory', '2g')) \
                .config('spark.executor.cores', self.config.get('spark_cores', 4)) \
                .config('spark.sql.adaptive.enabled', 'true') \
                .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
            
            # Create Spark session
            self.spark = builder.getOrCreate()
            
            # Import functions for use in methods
            self.col = col
            self.explode = explode
            self.regexp_extract = regexp_extract
            self.when = when
            self.current_timestamp = current_timestamp
            
            logger.info("Spark session initialized successfully")
            
        except ImportError:
            logger.error("PySpark not available. Falling back to pandas processing.")
            self.use_spark = False
        except Exception as e:
            logger.error(f"Error initializing Spark: {e}")
            self.use_spark = False
    
    def process_restaurant_data(self, data: Union[List[Dict], pd.DataFrame]) -> Union[pd.DataFrame, Any]:
        """Process restaurant data using Spark or pandas."""
        if self.use_spark and self.spark:
            return self._process_with_spark(data)
        else:
            return self._process_with_pandas(data)
    
    def _process_with_spark(self, data: Union[List[Dict], pd.DataFrame]) -> Any:
        """Process data using Spark."""
        try:
            # Convert data to Spark DataFrame
            if isinstance(data, pd.DataFrame):
                spark_df = self.spark.createDataFrame(data)
            else:
                spark_df = self.spark.createDataFrame(data)
            
            # Apply transformations
            processed_df = self._apply_spark_transformations(spark_df)
            
            logger.info(f"Processed {processed_df.count()} records using Spark")
            return processed_df
            
        except Exception as e:
            logger.error(f"Error in Spark processing: {e}")
            # Fallback to pandas
            return self._process_with_pandas(data)
    
    def _apply_spark_transformations(self, df):
        """Apply data transformations using Spark SQL."""
        from pyspark.sql.functions import col, regexp_extract, when, current_timestamp, lit
        
        # Clean and standardize data
        processed_df = df \
            .withColumn("restaurant_name", col("name")) \
            .withColumn("rating", 
                when(col("rating") < 0, 0)
                .when(col("rating") > 5, 5)
                .otherwise(col("rating"))
            ) \
            .withColumn("data_source", col("data_source").cast("string")) \
            .withColumn("city", 
                regexp_extract(col("formatted_address"), r"([^,]+),\s*([^,]+),\s*([A-Z]{2})", 1)
            ) \
            .withColumn("state", 
                regexp_extract(col("formatted_address"), r"([^,]+),\s*([^,]+),\s*([A-Z]{2})", 3)
            ) \
            .withColumn("price_level", col("price_level").cast("integer")) \
            .withColumn("review_count", col("user_ratings_total").cast("integer")) \
            .withColumn("latitude", col("latitude").cast("float")) \
            .withColumn("longitude", col("longitude").cast("float")) \
            .withColumn("ingestion_timestamp", current_timestamp())
        
        return processed_df
    
    def _process_with_pandas(self, data: Union[List[Dict], pd.DataFrame]) -> pd.DataFrame:
        """Fallback processing using pandas."""
        try:
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = data.copy()
            
            # Apply pandas transformations
            df = self._apply_pandas_transformations(df)
            
            logger.info(f"Processed {len(df)} records using pandas")
            return df
            
        except Exception as e:
            logger.error(f"Error in pandas processing: {e}")
            return pd.DataFrame()
    
    def _apply_pandas_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply minimal transformations to pandas DataFrame - most logic moved to dbt."""
        logger.info(f"Applying minimal pandas transformations to {len(df)} records")
        
        # Only ensure required columns exist for raw data storage - dbt handles the rest
        required_columns = {
            'name': 'Unknown Restaurant',
            'rating': None,
            'data_source': 'unknown',
            'formatted_address': '',
            'price_level': None,
            'user_ratings_total': None,
            'latitude': None,
            'longitude': None,
            'place_id': None,
            'city': None,
            'state': None,
            'categories': None
        }
        
        for col, default_val in required_columns.items():
            if col not in df.columns:
                df[col] = default_val
                logger.debug(f"Missing column '{col}', added with default value")
        
        # Minimal processing - just ensure data types won't break BigQuery
        # Convert obvious numeric columns to avoid type inference issues
        numeric_columns = ['rating', 'price_level', 'user_ratings_total', 'latitude', 'longitude']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Add basic audit fields - everything else handled by dbt
        df['ingestion_timestamp'] = pd.Timestamp.now()
        
        # Remove any columns with all null values to keep data clean
        df = df.dropna(axis=1, how='all')
        
        logger.info(f"Processed {len(df)} records with {len(df.columns)} columns for raw storage")
        logger.info("✨ Business logic and transformations now handled by dbt models")
        return df
    
    def combine_data_sources(self, dataframes: List[Union[pd.DataFrame, Any]]) -> Union[pd.DataFrame, Any]:
        """Combine multiple data sources."""
        if self.use_spark and self.spark:
            return self._combine_with_spark(dataframes)
        else:
            return self._combine_with_pandas(dataframes)
    
    def _combine_with_spark(self, dataframes: List[Any]) -> Any:
        """Combine DataFrames using Spark."""
        try:
            if not dataframes:
                return self.spark.createDataFrame([], self.spark.sql.types.StructType())
            
            # Union all DataFrames
            combined_df = dataframes[0]
            for df in dataframes[1:]:
                combined_df = combined_df.union(df)
            
            logger.info(f"Combined {combined_df.count()} records using Spark")
            return combined_df
            
        except Exception as e:
            logger.error(f"Error combining DataFrames with Spark: {e}")
            return self._combine_with_pandas(dataframes)
    
    def _combine_with_pandas(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        """Combine DataFrames using pandas."""
        try:
            if not dataframes:
                return pd.DataFrame()
            
            # Filter out empty DataFrames
            non_empty_dfs = [df for df in dataframes if not df.empty]
            
            if not non_empty_dfs:
                return pd.DataFrame()
            
            # Ensure we always get a DataFrame, not a Series
            if len(non_empty_dfs) == 1:
                combined_df = non_empty_dfs[0].copy()
            else:
                combined_df = pd.concat(non_empty_dfs, ignore_index=True, sort=False)
            
            # Ensure it's a DataFrame
            if isinstance(combined_df, pd.Series):
                combined_df = combined_df.to_frame()
            
            logger.info(f"Combined {len(combined_df)} records using pandas")
            return combined_df
            
        except Exception as e:
            logger.error(f"Error combining DataFrames with pandas: {e}")
            return pd.DataFrame()
    
    def write_to_bigquery(self, df: Union[pd.DataFrame, Any], table_name: str) -> bool:
        """Write data to BigQuery."""
        try:
            if self.use_spark and self.spark:
                return self._write_to_bigquery_spark(df, table_name)
            else:
                return self._write_to_bigquery_pandas(df, table_name)
                
        except Exception as e:
            logger.error(f"Error writing to BigQuery: {e}")
            return False
    
    def _write_to_bigquery_spark(self, df: Any, table_name: str) -> bool:
        """Write Spark DataFrame to BigQuery."""
        try:
            # Configure BigQuery connection
            project_id = os.getenv('BIGQUERY_PROJECT_ID')
            dataset_id = os.getenv('BIGQUERY_DATASET_ID')
            
            if not project_id or not dataset_id:
                logger.error("BigQuery configuration not available")
                return False
            
            table_id = f"{project_id}.{dataset_id}.{table_name}"
            
            # Write to BigQuery
            df.write \
                .format("bigquery") \
                .option("table", table_id) \
                .option("writeDisposition", "WRITE_TRUNCATE") \
                .mode("overwrite") \
                .save()
            
            logger.info(f"Successfully wrote {df.count()} records to BigQuery using Spark")
            return True
            
        except Exception as e:
            logger.error(f"Error writing to BigQuery with Spark: {e}")
            return False
    
    def _write_to_bigquery_pandas(self, df: pd.DataFrame, table_name: str) -> bool:
        """Write pandas DataFrame to BigQuery."""
        try:
            from google.cloud import bigquery
            
            project_id = os.getenv('BIGQUERY_PROJECT_ID')
            dataset_id = os.getenv('BIGQUERY_DATASET_ID')
            
            if not project_id or not dataset_id:
                logger.error("BigQuery configuration not available")
                return False
            
            client = bigquery.Client(project=project_id)
            table_id = f"{project_id}.{dataset_id}.{table_name}"
            
            # Write DataFrame to BigQuery
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                autodetect=True
            )
            
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            
            logger.info(f"Successfully wrote {len(df)} records to BigQuery using pandas")
            return True
            
        except Exception as e:
            logger.error(f"Error writing to BigQuery with pandas: {e}")
            return False
    
    def stop_spark(self):
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")

# Convenience function
def create_spark_processor(config: Dict[str, Any] = None) -> SparkRestaurantProcessor:
    """Create a Spark restaurant processor."""
    return SparkRestaurantProcessor(config)
