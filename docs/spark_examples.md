# Spark Examples and Use Cases

This document provides practical examples of how to use Apache Spark in the Restaurant Analytics platform.

## Table of Contents
1. [Basic Spark Processing](#basic-spark-processing)
2. [Streaming Processing](#streaming-processing)
3. [Large-Scale Data Processing](#large-scale-data-processing)
4. [Performance Optimization](#performance-optimization)
5. [Real-World Use Cases](#real-world-use-cases)
6. [Troubleshooting](#troubleshooting)

## Basic Spark Processing

### Example 1: Processing Restaurant Data with Spark

```python
from ingestion.spark_processor import SparkRestaurantProcessor
import pandas as pd

# Initialize Spark processor
config = {
    'use_spark': True,
    'spark_master': 'local[*]',
    'spark_app_name': 'RestaurantAnalytics',
    'spark_memory': '2g'
}

processor = SparkRestaurantProcessor(config)

# Process restaurant data
restaurant_data = [
    {
        'name': 'Joe\'s Pizza',
        'rating': 4.5,
        'formatted_address': '123 Main St, New York, NY',
        'price_level': 2,
        'user_ratings_total': 150,
        'latitude': 40.7128,
        'longitude': -74.0060,
        'data_source': 'yelp'
    },
    # ... more restaurant data
]

# Convert to DataFrame and process
df = pd.DataFrame(restaurant_data)
processed_data = processor.process_restaurant_data(df)

# Write to BigQuery
success = processor.write_to_bigquery(processed_data, "raw_restaurant_data")
print(f"Processing successful: {success}")
```

### Example 2: Combining Multiple Data Sources

```python
from ingestion.spark_processor import SparkRestaurantProcessor

# Initialize processor
processor = SparkRestaurantProcessor({'use_spark': True})

# Multiple data sources
yelp_data = [/* Yelp restaurant data */]
google_data = [/* Google Places data */]

# Process and combine
processed_yelp = processor.process_restaurant_data(yelp_data)
processed_google = processor.process_restaurant_data(google_data)

# Combine all sources
combined_data = processor.combine_data_sources([processed_yelp, processed_google])

# Write to BigQuery
processor.write_to_bigquery(combined_data, "combined_restaurant_data")
```

## Streaming Processing

### Example 3: Real-time Restaurant Data Ingestion

```python
from ingestion.spark_streaming import SparkStreamingProcessor

# Initialize streaming processor
config = {
    'spark_streaming_enabled': True,
    'kafka_bootstrap_servers': 'localhost:9092',
    'kafka_topic': 'restaurant_data',
    'streaming_batch_interval': 30
}

streaming_processor = SparkStreamingProcessor(config)

# Start streaming pipeline
query = streaming_processor.start_streaming_pipeline("raw_restaurant_data")

# Monitor the streaming query
if query:
    print(f"Streaming query started: {query.id}")
    
    # Wait for user input to stop
    input("Press Enter to stop streaming...")
    streaming_processor.stop_streaming(query)
```

### Example 4: Kafka Producer for Restaurant Data

```python
from kafka import KafkaProducer
import json
import time

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Simulate restaurant data stream
restaurant_data = {
    'id': 'rest_001',
    'name': 'Mario\'s Italian',
    'rating': 4.2,
    'data_source': 'yelp',
    'formatted_address': '456 Oak Ave, San Francisco, CA',
    'price_level': 3,
    'user_ratings_total': 89,
    'latitude': 37.7749,
    'longitude': -122.4194,
    'ingestion_timestamp': time.time()
}

# Send to Kafka topic
producer.send('restaurant_data', restaurant_data)
producer.flush()
```

## Large-Scale Data Processing

### Example 5: Processing Multiple Cities with Spark

```python
from ingestion.spark_processor import SparkRestaurantProcessor
from ingestion.location_manager import get_enabled_locations

# Get all enabled locations
locations = get_enabled_locations()
print(f"Processing {len(locations)} locations")

# Initialize Spark processor for large-scale processing
config = {
    'use_spark': True,
    'spark_master': 'yarn',  # Use YARN cluster
    'spark_app_name': 'MultiCityRestaurantAnalytics',
    'spark_memory': '4g',
    'spark_cores': 8
}

processor = SparkRestaurantProcessor(config)

# Process each location
all_data = []
for location in locations:
    print(f"Processing {location}...")
    
    # Simulate data for each location
    location_data = generate_restaurant_data_for_location(location)
    
    # Process with Spark
    processed_data = processor.process_restaurant_data(location_data)
    all_data.append(processed_data)

# Combine all location data
combined_data = processor.combine_data_sources(all_data)

# Write to BigQuery
success = processor.write_to_bigquery(combined_data, "multi_city_restaurant_data")
print(f"Processed {len(locations)} locations successfully: {success}")
```

### Example 6: Complex Data Transformations

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract, current_timestamp

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RestaurantDataTransformations") \
    .getOrCreate()

# Load data from BigQuery
df = spark.read.format("bigquery") \
    .option("table", "project.dataset.raw_restaurant_data") \
    .load()

# Complex transformations
transformed_df = df \
    .withColumn("city", regexp_extract(col("formatted_address"), r"([^,]+),\s*([^,]+),\s*([A-Z]{2})", 1)) \
    .withColumn("state", regexp_extract(col("formatted_address"), r"([^,]+),\s*([^,]+),\s*([A-Z]{2})", 3)) \
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
    ) \
    .withColumn("processed_at", current_timestamp())

# Write transformed data back to BigQuery
transformed_df.write \
    .format("bigquery") \
    .option("table", "project.dataset.transformed_restaurant_data") \
    .mode("overwrite") \
    .save()
```

## Performance Optimization

### Example 7: Optimizing Spark Configuration

```python
from ingestion.spark_processor import SparkRestaurantProcessor

# Optimized configuration for large datasets
config = {
    'use_spark': True,
    'spark_master': 'yarn',
    'spark_app_name': 'OptimizedRestaurantAnalytics',
    
    # Memory optimization
    'spark_memory': '8g',
    'spark_executor_memory': '4g',
    'spark_driver_memory': '2g',
    
    # Core optimization
    'spark_cores': 16,
    'spark_executor_cores': 4,
    
    # Performance tuning
    'spark_sql_adaptive_enabled': True,
    'spark_sql_adaptive_coalescePartitions_enabled': True,
    'spark_sql_adaptive_skewJoin_enabled': True,
    
    # Serialization
    'spark_serializer': 'org.apache.spark.serializer.KryoSerializer',
    'spark_kryo_registrationRequired': False
}

processor = SparkRestaurantProcessor(config)
```

### Example 8: Partitioning and Caching

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PartitionedRestaurantAnalytics") \
    .getOrCreate()

# Load and partition data by city
df = spark.read.format("bigquery") \
    .option("table", "project.dataset.raw_restaurant_data") \
    .load()

# Partition by city for better performance
partitioned_df = df.repartition(10, col("city"))

# Cache frequently accessed data
partitioned_df.cache()

# Perform multiple operations on cached data
city_stats = partitioned_df.groupBy("city").agg({
    "rating": "avg",
    "user_ratings_total": "sum"
})

price_stats = partitioned_df.groupBy("price_level").agg({
    "rating": "avg",
    "user_ratings_total": "count"
})

# Write results
city_stats.write.mode("overwrite").saveAsTable("city_statistics")
price_stats.write.mode("overwrite").saveAsTable("price_statistics")
```

## Real-World Use Cases

### Use Case 1: National Restaurant Chain Analytics

**Scenario**: A national restaurant chain wants to analyze competitor data across 50+ cities.

**Solution**:
```python
# Process data for 50+ cities using Spark cluster
config = {
    'use_spark': True,
    'spark_master': 'yarn',
    'spark_memory': '16g',
    'spark_cores': 32
}

processor = SparkRestaurantProcessor(config)

# Process all major cities
major_cities = [
    'New York, NY', 'Los Angeles, CA', 'Chicago, IL',
    'Houston, TX', 'Phoenix, AZ', 'Philadelphia, PA',
    # ... 44 more cities
]

all_data = []
for city in major_cities:
    city_data = ingest_restaurant_data(city, limit=1000)
    processed_data = processor.process_restaurant_data(city_data)
    all_data.append(processed_data)

# Combine and analyze
combined_data = processor.combine_data_sources(all_data)
processor.write_to_bigquery(combined_data, "national_competitor_analysis")
```

### Use Case 2: Real-time Restaurant Monitoring

**Scenario**: A food delivery company needs real-time monitoring of restaurant availability and ratings.

**Solution**:
```python
# Set up streaming pipeline
streaming_config = {
    'spark_streaming_enabled': True,
    'kafka_bootstrap_servers': 'kafka-cluster:9092',
    'kafka_topic': 'restaurant_updates',
    'streaming_batch_interval': 10  # 10-second batches
}

streaming_processor = SparkStreamingProcessor(streaming_config)

# Start real-time monitoring
query = streaming_processor.start_streaming_pipeline("real_time_restaurant_data")

# Create real-time alerts
def check_rating_drops():
    # Monitor for significant rating drops
    pass

def check_availability_changes():
    # Monitor for restaurant availability changes
    pass
```

### Use Case 3: Machine Learning Feature Engineering

**Scenario**: Building ML models to predict restaurant success based on location and characteristics.

**Solution**:
```python
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline

# Load restaurant data
df = spark.read.format("bigquery") \
    .option("table", "project.dataset.restaurant_data") \
    .load()

# Feature engineering
feature_assembler = VectorAssembler(
    inputCols=["rating", "price_level", "user_ratings_total", "latitude", "longitude"],
    outputCol="features"
)

# Create ML pipeline
ml_pipeline = Pipeline(stages=[feature_assembler])

# Transform data
transformed_df = ml_pipeline.fit(df).transform(df)

# Train ML model
rf = RandomForestRegressor(featuresCol="features", labelCol="rating")
model = rf.fit(transformed_df)

# Make predictions
predictions = model.transform(transformed_df)
```

## Troubleshooting

### Common Issues and Solutions

#### Issue 1: Spark Session Not Starting
```python
# Problem: PySpark not installed or Java not available
# Solution: Install dependencies
pip install pyspark
# Ensure Java 8+ is installed
```

#### Issue 2: Memory Issues
```python
# Problem: OutOfMemoryError
# Solution: Increase memory allocation
config = {
    'spark_memory': '8g',  # Increase from 2g
    'spark_executor_memory': '4g',
    'spark_driver_memory': '2g'
}
```

#### Issue 3: Slow Performance
```python
# Problem: Slow processing
# Solution: Optimize configuration
config = {
    'spark_sql_adaptive_enabled': True,
    'spark_sql_adaptive_coalescePartitions_enabled': True,
    'spark_serializer': 'org.apache.spark.serializer.KryoSerializer'
}
```

#### Issue 4: BigQuery Connection Issues
```python
# Problem: Cannot connect to BigQuery
# Solution: Check credentials and configuration
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/path/to/service-account.json'
os.environ['BIGQUERY_PROJECT_ID'] = 'your-project-id'
```

### Performance Monitoring

```python
# Monitor Spark application performance
def monitor_spark_performance():
    spark_context = spark.sparkContext
    
    # Get application info
    app_id = spark_context.applicationId
    app_name = spark_context.appName
    
    print(f"Application ID: {app_id}")
    print(f"Application Name: {app_name}")
    
    # Monitor memory usage
    status = spark_context.statusTracker()
    print(f"Executor count: {status.getExecutorInfos()}")
    
    # Monitor job progress
    for job_id in status.getJobIdsForGroup():
        job_info = status.getJobInfo(job_id)
        print(f"Job {job_id}: {job_info.status}")
```

## Best Practices

### 1. Configuration Management
- Use environment-specific configurations
- Set appropriate memory and core allocations
- Enable adaptive query execution

### 2. Data Partitioning
- Partition large datasets by relevant columns
- Use appropriate number of partitions
- Consider data skew when partitioning

### 3. Caching Strategy
- Cache frequently accessed DataFrames
- Use appropriate storage levels
- Monitor cache hit rates

### 4. Error Handling
- Implement proper exception handling
- Use try-catch blocks for Spark operations
- Log errors and performance metrics

### 5. Resource Management
- Monitor Spark UI for performance
- Clean up unused DataFrames
- Stop Spark sessions when done
