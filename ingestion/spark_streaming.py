import os
import logging
from typing import Dict, Any, Optional
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class SparkStreamingProcessor:
    """Spark Streaming processor for real-time restaurant data."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.spark = None
        self.streaming_enabled = self.config.get('spark_streaming_enabled', False)
        
        if self.streaming_enabled:
            self._initialize_spark_streaming()
    
    def _initialize_spark_streaming(self):
        """Initialize Spark Streaming session."""
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.functions import col, from_json, current_timestamp, window
            from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
            
            # Spark configuration for streaming
            spark_config = {
                'spark.app.name': f"{self.config.get('spark_app_name', 'RestaurantAnalytics')}-Streaming",
                'spark.master': self.config.get('spark_master', 'local[*]'),
                'spark.executor.memory': self.config.get('spark_memory', '2g'),
                'spark.executor.cores': self.config.get('spark_cores', 4),
                'spark.sql.streaming.checkpointLocation': '/tmp/restaurant_streaming_checkpoint',
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.streaming.schemaInference': 'true'
            }
            
            # Create Spark session
            self.spark = SparkSession.builder \
                .config(**spark_config) \
                .getOrCreate()
            
            # Import streaming functions
            self.col = col
            self.from_json = from_json
            self.current_timestamp = current_timestamp
            self.window = window
            
            logger.info("Spark Streaming session initialized successfully")
            
        except ImportError:
            logger.error("PySpark not available for streaming. Streaming disabled.")
            self.streaming_enabled = False
        except Exception as e:
            logger.error(f"Error initializing Spark Streaming: {e}")
            self.streaming_enabled = False
    
    def create_kafka_stream(self, topic: str = None) -> Optional[Any]:
        """Create Kafka streaming source."""
        if not self.streaming_enabled or not self.spark:
            logger.warning("Spark Streaming not enabled")
            return None
        
        try:
            from pyspark.sql.functions import col
            
            topic = topic or self.config.get('kafka_topic', 'restaurant_data')
            bootstrap_servers = self.config.get('kafka_bootstrap_servers', 'localhost:9092')
            
            # Define schema for restaurant data
            restaurant_schema = self._get_restaurant_schema()
            
            # Create streaming DataFrame from Kafka
            kafka_stream = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", bootstrap_servers) \
                .option("subscribe", topic) \
                .option("startingOffsets", "latest") \
                .load()
            
            # Parse JSON data
            parsed_stream = kafka_stream \
                .select(
                    col("key").cast("string"),
                    col("value").cast("string"),
                    col("timestamp")
                ) \
                .select(
                    col("key"),
                    self.from_json(col("value"), restaurant_schema).alias("data"),
                    col("timestamp")
                ) \
                .select("key", "data.*", "timestamp")
            
            logger.info(f"Kafka stream created for topic: {topic}")
            return parsed_stream
            
        except Exception as e:
            logger.error(f"Error creating Kafka stream: {e}")
            return None
    
    def _get_restaurant_schema(self):
        """Define schema for restaurant data."""
        from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
        
        return StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("rating", FloatType(), True),
            StructField("data_source", StringType(), True),
            StructField("formatted_address", StringType(), True),
            StructField("price_level", IntegerType(), True),
            StructField("user_ratings_total", IntegerType(), True),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
            StructField("categories", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), True)
        ])
    
    def process_streaming_data(self, stream: Any) -> Any:
        """Process streaming restaurant data."""
        if not stream:
            return None
        
        try:
            from pyspark.sql.functions import col, regexp_extract, when, current_timestamp, lit
            
            # Apply transformations to streaming data
            processed_stream = stream \
                .withColumn("restaurant_name", col("name")) \
                .withColumn("rating", 
                    when(col("rating") < 0, 0)
                    .when(col("rating") > 5, 5)
                    .otherwise(col("rating"))
                ) \
                .withColumn("city", 
                    regexp_extract(col("formatted_address"), r"([^,]+),\s*([^,]+),\s*([A-Z]{2})", 1)
                ) \
                .withColumn("state", 
                    regexp_extract(col("formatted_address"), r"([^,]+),\s*([^,]+),\s*([A-Z]{2})", 3)
                ) \
                .withColumn("processed_at", current_timestamp()) \
                .withColumn("rating_category",
                    when(col("rating") >= 4.5, "Excellent")
                    .when(col("rating") >= 4.0, "Very Good")
                    .when(col("rating") >= 3.5, "Good")
                    .when(col("rating") >= 3.0, "Average")
                    .otherwise("Below Average")
                ) \
                .withColumn("business_tier",
                    when((col("rating") >= 4.5) & (col("user_ratings_total") >= 100), "Premium")
                    .when((col("rating") >= 4.0) & (col("user_ratings_total") >= 50), "High Quality")
                    .when(col("rating") >= 3.5, "Good Quality")
                    .when(col("rating") >= 3.0, "Standard")
                    .otherwise("Needs Improvement")
                )
            
            logger.info("Streaming data processing configured")
            return processed_stream
            
        except Exception as e:
            logger.error(f"Error processing streaming data: {e}")
            return None
    
    def write_stream_to_bigquery(self, stream: Any, table_name: str) -> Optional[Any]:
        """Write streaming data to BigQuery."""
        if not stream:
            return None
        
        try:
            project_id = os.getenv('BIGQUERY_PROJECT_ID')
            dataset_id = os.getenv('BIGQUERY_DATASET_ID')
            
            if not project_id or not dataset_id:
                logger.error("BigQuery configuration not available")
                return None
            
            table_id = f"{project_id}.{dataset_id}.{table_name}"
            
            # Write stream to BigQuery
            query = stream \
                .writeStream \
                .format("bigquery") \
                .option("table", table_id) \
                .option("writeDisposition", "WRITE_APPEND") \
                .option("checkpointLocation", f"/tmp/restaurant_streaming_checkpoint_{table_name}") \
                .trigger(processingTime=f"{self.config.get('streaming_batch_interval', 30)} seconds") \
                .start()
            
            logger.info(f"Streaming query started for table: {table_name}")
            return query
            
        except Exception as e:
            logger.error(f"Error writing stream to BigQuery: {e}")
            return None
    
    def create_aggregated_stream(self, stream: Any) -> Optional[Any]:
        """Create aggregated streaming analytics."""
        if not stream:
            return None
        
        try:
            from pyspark.sql.functions import col, count, avg, window
            
            # Create windowed aggregations
            windowed_stream = stream \
                .withWatermark("processed_at", "10 minutes") \
                .groupBy(
                    window(col("processed_at"), "5 minutes"),
                    col("city"),
                    col("data_source")
                ) \
                .agg(
                    count("*").alias("restaurant_count"),
                    avg("rating").alias("avg_rating")
                ) \
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("city"),
                    col("data_source"),
                    col("restaurant_count"),
                    col("avg_rating")
                )
            
            logger.info("Aggregated streaming analytics configured")
            return windowed_stream
            
        except Exception as e:
            logger.error(f"Error creating aggregated stream: {e}")
            return None
    
    def start_streaming_pipeline(self, table_name: str = "raw_restaurant_data") -> Optional[Any]:
        """Start the complete streaming pipeline."""
        if not self.streaming_enabled:
            logger.warning("Streaming not enabled")
            return None
        
        try:
            # Create Kafka stream
            kafka_stream = self.create_kafka_stream()
            if not kafka_stream:
                return None
            
            # Process streaming data
            processed_stream = self.process_streaming_data(kafka_stream)
            if not processed_stream:
                return None
            
            # Write to BigQuery
            query = self.write_stream_to_bigquery(processed_stream, table_name)
            
            # Create aggregated analytics
            aggregated_stream = self.create_aggregated_stream(processed_stream)
            if aggregated_stream:
                analytics_query = self.write_stream_to_bigquery(
                    aggregated_stream, 
                    f"{table_name}_analytics"
                )
            
            logger.info("Streaming pipeline started successfully")
            return query
            
        except Exception as e:
            logger.error(f"Error starting streaming pipeline: {e}")
            return None
    
    def stop_streaming(self, query: Any = None):
        """Stop streaming query."""
        if query:
            query.stop()
            logger.info("Streaming query stopped")
    
    def stop_spark(self):
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark Streaming session stopped")

# Convenience function
def create_streaming_processor(config: Dict[str, Any] = None) -> SparkStreamingProcessor:
    """Create a Spark streaming processor."""
    return SparkStreamingProcessor(config)
