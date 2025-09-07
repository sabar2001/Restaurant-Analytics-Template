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
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("rating_category",
                when(col("rating") >= 4.5, "Excellent")
                .when(col("rating") >= 4.0, "Very Good")
                .when(col("rating") >= 3.5, "Good")
                .when(col("rating") >= 3.0, "Average")
                .otherwise("Below Average")
            ) \
            .withColumn("price_category",
                when(col("price_level") == 1, "$")
                .when(col("price_level") == 2, "$$")
                .when(col("price_level") == 3, "$$$")
                .when(col("price_level") == 4, "$$$$")
                .otherwise("Unknown")
            ) \
            .withColumn("business_tier",
                when((col("rating") >= 4.5) & (col("review_count") >= 100), "Premium")
                .when((col("rating") >= 4.0) & (col("review_count") >= 50), "High Quality")
                .when(col("rating") >= 3.5, "Good Quality")
                .when(col("rating") >= 3.0, "Standard")
                .otherwise("Needs Improvement")
            )
        
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
        """Apply data transformations using pandas."""
        # Clean and standardize data
        df['restaurant_name'] = df['name']
        df['rating'] = df['rating'].clip(0, 5)
        df['data_source'] = df['data_source'].astype(str)
        
        # Extract location information
        df['city'] = df['formatted_address'].str.extract(r'([^,]+),\s*([^,]+),\s*([A-Z]{2})')[0]
        df['state'] = df['formatted_address'].str.extract(r'([^,]+),\s*([^,]+),\s*([A-Z]{2})')[2]
        
        # Convert data types safely
        df['price_level'] = pd.to_numeric(df.get('price_level', 0), errors='coerce').fillna(0).astype(int)
        df['review_count'] = pd.to_numeric(df.get('user_ratings_total', 0), errors='coerce').fillna(0).astype(int)
        df['latitude'] = pd.to_numeric(df.get('latitude', 0), errors='coerce').fillna(0).astype(float)
        df['longitude'] = pd.to_numeric(df.get('longitude', 0), errors='coerce').fillna(0).astype(float)
        
        # Add derived fields
        df['processed_at'] = pd.Timestamp.now()
        df['rating_category'] = pd.cut(df['rating'], 
            bins=[0, 3, 3.5, 4, 4.5, 5], 
            labels=['Below Average', 'Average', 'Good', 'Very Good', 'Excellent']
        )
        df['price_category'] = df['price_level'].map({1: '$', 2: '$$', 3: '$$$', 4: '$$$$'})
        
        # Business tier logic
        df['business_tier'] = 'Standard'
        df.loc[(df['rating'] >= 4.5) & (df['review_count'] >= 100), 'business_tier'] = 'Premium'
        df.loc[(df['rating'] >= 4.0) & (df['review_count'] >= 50), 'business_tier'] = 'High Quality'
        df.loc[df['rating'] >= 3.5, 'business_tier'] = 'Good Quality'
        df.loc[df['rating'] < 3.0, 'business_tier'] = 'Needs Improvement'
        
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
            
            combined_df = pd.concat(dataframes, ignore_index=True)
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
