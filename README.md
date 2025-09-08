# 🍽️ Restaurant Analytics Template

An open-source **data engineering + analytics engineering** framework for restaurant data APIs.

This project demonstrates:
- **Ingestion & preprocessing** with Spark
- **Cloud data warehouse** storage in Google BigQuery
- **Transformations** with dbt
- **Pipeline orchestration** with Prefect
- **Interactive dashboard** with Streamlit

---

## 🚀 Architecture

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                                DATA SOURCES                                   │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────────┤
│   Yelp API      │ Google Places   │   Kafka Stream  │    Future Sources       │
│   (REST)        │     API         │   (Real-time)   │    (OpenTable, etc.)    │
└─────────────────┴─────────────────┴─────────────────┴─────────────────────────┘
         │                   │                   │                   │
         ▼                   ▼                   ▼                   ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                              INGESTION LAYER                                  │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────────┤
│   Python        │   Data          │   Location      │    Streaming            │
│   Requests      │   Cleaning      │   Management    │    Processing           │
│   & Parsing     │   & Validation  │   System        │    (Spark)              │
└─────────────────┴─────────────────┴─────────────────┴─────────────────────────┘
         │                   │                   │                   │
         ▼                   ▼                   ▼                   ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                            PROCESSING ENGINE                                  │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────────┤
│   Pandas        │   Apache Spark  │   Hybrid         │    Real-time           │
│   (Small Data)  │   (Large Data)  │   Processing    │    Streaming            │
│   Fallback      │   Distributed   │   Auto-fallback │    (Spark Streaming)    │
└─────────────────┴─────────────────┴─────────────────┴─────────────────────────┘
         │                   │                   │                   │
         ▼                   ▼                   ▼                   ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                           ORCHESTRATION LAYER                                 │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────────┤
│   Prefect       │   Workflow      │   Scheduling    │    Monitoring           │
│   Flows         │   Management    │   & Triggers    │    & Logging            │
└─────────────────┴─────────────────┴─────────────────┴─────────────────────────┘
         │                   │                   │                   │
         ▼                   ▼                   ▼                   ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                            DATA WAREHOUSE                                     │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────────┤
│   BigQuery      │   Raw Tables    │   Analytics     │    Streaming            │
│   (Cloud)       │   (Staging)     │   Schema        │    Tables               │
└─────────────────┴─────────────────┴─────────────────┴─────────────────────────┘
         │                   │                   │                   │
         ▼                   ▼                   ▼                   ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                            TRANSFORMATION LAYER                               │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────────┤
│   dbt Models    │   SQL           │   Data          │    ML Features          │
│   (Staging)     │   Transformation│   Quality       │    Engineering          │
│                 │   (Marts)       │   Checks        │    (Future)             │
└─────────────────┴─────────────────┴─────────────────┴─────────────────────────┘
         │                   │                   │                   │
         ▼                   ▼                   ▼                   ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                            VISUALIZATION LAYER                                │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────────┤
│   Streamlit     │   Interactive   │   Real-time     │    Reports              │
│   Dashboard     │   Analytics     │   Dashboards    │    & Exports            │
└─────────────────┴─────────────────┴─────────────────┴─────────────────────────┘
```

**Flow**:  
APIs (Yelp, Google Places, Kafka Stream) → **Spark/Pandas ingestion & processing** → **BigQuery (raw)** → **dbt models** → **BigQuery (analytics schema)** → **Streamlit dashboard**

### **Processing Modes**
- **Batch Processing**: Traditional ETL with pandas or Spark
- **Streaming Processing**: Real-time data ingestion with Spark Streaming
- **Hybrid Processing**: Automatic fallback from Spark to pandas

---
## ⚡ TLDR - Quick Setup

**Get your restaurant analytics platform running in 10 minutes:**

### **📋 Prerequisites**
- Python 3.9+
- Google Cloud account
- Git

### **🚀 Setup Steps**

**1. Clone & Install**
```bash
git clone https://github.com/your-username/restaurant-analytics-template.git
cd restaurant-analytics-template
pip3 install -r requirements.txt
```

**2. Get API Keys**
- **Google Cloud Console** → APIs & Services → Credentials
  - Create **API Key** for Places API
  - Create **Service Account** + download JSON key for BigQuery
  - Enable: **Places API (New)**, **Geocoding API**, **BigQuery API**

**3. Create `.env` File**
```bash
cp config/sample.env .env
```

**Edit `.env` with your credentials:**
```env
# Required - Google Places API
GOOGLE_PLACES_API_KEY=your_google_places_api_key_here

# Required - BigQuery (download JSON file from service account)
GOOGLE_APPLICATION_CREDENTIALS=/full/path/to/your/service-account-key.json
BIGQUERY_PROJECT_ID=your_google_cloud_project_id
BIGQUERY_DATASET_ID=restaurant_db  # Create this dataset in BigQuery
BIGQUERY_LOCATION=US

# Optional - Yelp API (for additional data)
YELP_API_KEY=your_yelp_api_key_here
```

**4. Create BigQuery Dataset**
- Go to **BigQuery Console** → Create Dataset named `restaurant_db`

**5. Run Pipeline**
```bash
# Process restaurants for San Francisco
python3 orchestration/flow.py --locations "San Francisco, CA"
```

**6. Launch Dashboard**
```bash
# Start the Streamlit dashboard
python3 -m streamlit run dashboards/app.py
```

**🎉 Done! Open http://localhost:8501 to see your restaurant analytics dashboard with real data!**

### **🔧 Service Account Permissions**
Your service account needs these BigQuery roles:
- `BigQuery Data Editor`
- `BigQuery Job User` 
- `BigQuery User`

### **📍 Adding More Cities**
```bash
# Add new locations easily
python3 scripts/manage_locations.py add "Austin, TX"
python3 scripts/manage_locations.py add "Seattle, WA"

# Run pipeline for all enabled locations
python3 orchestration/flow.py
```

### **⚠️ Common Issues**
- **"REQUEST_DENIED"**: Enable Geocoding API in Google Cloud Console
- **"Permission denied"**: Add BigQuery roles to your service account
- **"Module not found"**: Run `pip3 install -r requirements.txt`

---

## 🛠️ Tech Stack

- **Apache Spark** – distributed processing & real-time streaming  
- **Pandas** – fallback processing for smaller datasets  
- **Google BigQuery** – cloud warehouse for structured data  
- **dbt** – ✨ **DATABASE-AGNOSTIC** SQL transformations & business logic  
- **Prefect** – orchestration of the full pipeline  
- **Streamlit** – visualization/dashboard  
- **Kafka** – streaming data source (optional)

> 🎯 **Super Database-Agnostic Architecture**: All business logic in dbt macros works on **ANY** database (BigQuery, Snowflake, PostgreSQL, etc.) - just change one config file to migrate!  

---

## ⚡ Quickstart

### 1. Clone the repo
```bash
git clone https://github.com/yourname/restaurant-analytics-template.git
cd restaurant-analytics-template
```

### 2. Install dependencies
```bash
pip3 install -r requirements.txt
```

### 3. Setup environment
Copy `config/sample.env` → `.env` and add your credentials:
```bash
cp config/sample.env .env
```

### 4. Configure Spark (Optional)
Edit `config/locations.yml` to enable Spark processing:
```yaml
settings:
  use_spark: true  # Enable Spark for large-scale processing
  spark_streaming_enabled: false  # Enable real-time streaming
  spark_master: "local[*]"  # Spark cluster configuration
```

Edit `.env` with your actual values:
```env
# API Keys
YELP_API_KEY=your_actual_yelp_key
GOOGLE_PLACES_API_KEY=your_actual_google_key

# Google BigQuery Configuration
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
BIGQUERY_PROJECT_ID=your_project_id
BIGQUERY_DATASET_ID=restaurant_db
BIGQUERY_LOCATION=US
```

### 4. Test the setup
Run the test suite to ensure everything is working:
```bash
python3 -m pytest tests/ -v
```

### 5. Manage locations (Optional)
```bash
# View all configured locations
python3 scripts/manage_locations.py status

# Add a new location
python3 scripts/manage_locations.py add "Phoenix, AZ" --priority high

# List all locations
python3 scripts/manage_locations.py list

# Enable/disable locations
python3 scripts/manage_locations.py enable "Phoenix, AZ"
python3 scripts/manage_locations.py disable "Portland, OR"
```

### 6. Run the pipeline
```bash
# Run with pandas (default)
python3 orchestration/flow.py

# Run with Spark processing
python3 orchestration/flow.py --use-spark true

# Run streaming pipeline (requires Kafka)
python3 orchestration/flow.py --streaming true

# Run for specific locations
python3 orchestration/flow.py --locations "San Francisco, CA" "New York, NY"

# Run for single location
python3 orchestration/flow.py --locations "Miami, FL"

# Run with custom limits
python3 orchestration/flow.py --yelp-limit 100 --google-radius 10000
```

### 7. Run dbt transformations
```bash
cd dbt_project
dbt run
```

### 8. Launch dashboard
```bash
streamlit run dashboards/app.py
```

---

## 📁 Project Structure

```
restaurant-analytics-template/
│
├── ingestion/                    # Data ingestion modules
│   ├── yelp_ingest.py          # Yelp API ingestion
│   ├── google_places_ingest.py # Google Places API ingestion
│   └── utils.py                # Common utilities
│
├── orchestration/               # Pipeline orchestration
│   └── flow.py                 # Prefect workflow
│
├── dbt_project/                 # dbt transformations
│   ├── models/
│   │   ├── staging/            # Data cleaning & standardization
│   │   │   └── stg_restaurants.sql
│   │   ├── marts/              # Business-ready models
│   │   │   └── dim_restaurants.sql
│   │   └── sources.yml         # Source definitions
│   ├── dbt_project.yml         # dbt configuration
│   └── profiles.yml            # Connection profiles
│
├── dashboards/                  # Visualization
│   └── app.py                  # Streamlit dashboard
│
├── config/                      # Configuration files
│   ├── sample.env              # Environment template
│   └── dbt_profiles.yml        # dbt connection profiles
│
├── tests/                       # Test suite
│   └── test_ingestion.py       # Ingestion tests
│
├── docs/                        # Documentation
│   └── architecture.txt         # Architecture documentation
│
├── .gitignore
├── requirements.txt
└── README.md
```

---

## ⚡ Spark Processing

The platform supports both **Apache Spark** and **pandas** processing, with automatic fallback capabilities.

### **When to Use Spark**
- **Large datasets**: 10,000+ restaurants per day
- **Multiple locations**: Processing 10+ cities simultaneously  
- **Real-time streaming**: Live data ingestion from Kafka
- **Complex transformations**: Heavy data processing requirements
- **Distributed processing**: Multi-node cluster environments

### **When to Use Pandas**
- **Small datasets**: <5,000 restaurants per day
- **Single location**: Processing one city at a time
- **Batch processing**: Traditional ETL workflows
- **Simple transformations**: Basic data cleaning
- **Development/testing**: Quick iteration and debugging

### **Spark Configuration**
```yaml
# config/locations.yml
settings:
  use_spark: true  # Enable Spark processing
  spark_master: "local[*]"  # or "yarn" for cluster
  spark_app_name: "RestaurantAnalytics"
  spark_memory: "2g"  # Executor memory
  spark_cores: 4  # Number of cores
  
  # Streaming configuration
  spark_streaming_enabled: false
  kafka_bootstrap_servers: "localhost:9092"
  kafka_topic: "restaurant_data"
  streaming_batch_interval: 30  # seconds
```

### **Processing Examples**
```python
# Batch processing with Spark
from ingestion.spark_processor import SparkRestaurantProcessor

processor = SparkRestaurantProcessor({'use_spark': True})
processed_data = processor.process_restaurant_data(restaurant_data)
processor.write_to_bigquery(processed_data, "raw_restaurant_data")

# Streaming processing
from ingestion.spark_streaming import SparkStreamingProcessor

streaming = SparkStreamingProcessor({'spark_streaming_enabled': True})
query = streaming.start_streaming_pipeline("raw_restaurant_data")
```

### **Performance Comparison**
| Metric | Pandas | Spark |
|--------|--------|-------|
| **Small Data** (<1K records) | ⚡ Fast | 🐌 Slower |
| **Medium Data** (1K-10K records) | ✅ Good | ✅ Good |
| **Large Data** (>10K records) | ❌ Memory issues | ⚡ Excellent |
| **Real-time** | ❌ Not supported | ✅ Native streaming |
| **Distributed** | ❌ Single machine | ✅ Multi-node |

### **Advanced Examples**
For detailed Spark examples and use cases, see [Spark Examples Documentation](docs/spark_examples.md).

---

## 🗺️ Location Management

The platform includes a powerful location management system that makes it easy to add new cities and regions for restaurant data collection.

### **Easy Location Addition**
```bash
# Add a new location with just one command
python3 scripts/manage_locations.py add "Phoenix, AZ" --priority high

# Add multiple locations
python3 scripts/manage_locations.py add "Las Vegas, NV" --priority medium
python3 scripts/manage_locations.py add "Nashville, TN" --priority low
```

### **Location Management Commands**
```bash
# View all locations and their status
python3 scripts/manage_locations.py status

# List locations by priority
python3 scripts/manage_locations.py list --priority high
python3 scripts/manage_locations.py list --priority medium

# Enable/disable locations
python3 scripts/manage_locations.py enable "Phoenix, AZ"
python3 scripts/manage_locations.py disable "Portland, OR"

# Remove a location
python3 scripts/manage_locations.py remove "Portland, OR"
```

### **Location Configuration**
Locations are stored in `config/locations.yml` and can be managed through:
- **CLI commands** (recommended)
- **Direct YAML editing**
- **Dashboard interface** (shows configured locations)

### **Priority Levels**
- **High**: Major cities, tech hubs, food capitals
- **Medium**: Regional centers, growing cities
- **Low**: Smaller cities, experimental locations

### **How Priority Works**

The priority system controls how locations are processed and resources are allocated:

#### **Processing Order**
Locations are processed in priority order: High → Medium → Low
```bash
# Process only high priority locations first
python3 scripts/manage_locations.py list --priority high

# Process medium priority locations
python3 scripts/manage_locations.py list --priority medium
```

#### **Resource Allocation**
Priority affects API limits and processing parameters:
- **High Priority**: Full API limits (50 restaurants, 5km radius)
- **Medium Priority**: Reduced limits (30 restaurants, 3km radius)  
- **Low Priority**: Minimal limits (20 restaurants, 2km radius)

#### **Batch Processing**
The pipeline processes locations in batches by priority:
1. **First**: All high priority locations
2. **Then**: All medium priority locations
3. **Finally**: All low priority locations

#### **Priority Best Practices**

**High Priority Locations:**
- Major metropolitan areas (NYC, LA, SF, Chicago)
- Tech hubs (San Francisco, Seattle, Austin)
- Food capitals (New York, Los Angeles, San Francisco)
- High restaurant density areas

**Medium Priority Locations:**
- Regional centers (Miami, Denver, Boston)
- Growing cities (Austin, Seattle)
- Secondary markets
- Good restaurant scenes

**Low Priority Locations:**
- Smaller cities (Portland, Nashville)
- Experimental locations
- Testing new markets
- Limited restaurant data

#### **Dynamic Priority Management**
```bash
# Promote a location to high priority
python3 scripts/manage_locations.py add "Las Vegas, NV" --priority high

# Change priority of existing location
python3 scripts/manage_locations.py disable "Portland, OR"  # Then re-add with new priority

# Focus on specific priority levels
python3 orchestration/flow.py --locations $(python3 scripts/manage_locations.py list --priority high)
```

#### **Priority Impact on Pipeline**
Priority affects:
- **Processing order** (high → medium → low)
- **Resource allocation** (API limits, search radius)
- **Error handling** (retry attempts per priority)
- **Monitoring** (which locations to watch closely)
- **Cost control** (disable low priority to save API costs)

### Priority Quick Reference

| Priority | Use Case | API Limits | Processing Order |
|----------|----------|------------|------------------|
| **High** | Major cities, tech hubs | 50 restaurants, 5km radius | First |
| **Medium** | Regional centers | 30 restaurants, 3km radius | Second |
| **Low** | Smaller cities, testing | 20 restaurants, 2km radius | Last |

**Quick Commands:**
```bash
# Add high priority location
python3 scripts/manage_locations.py add "Las Vegas, NV" --priority high

# List by priority
python3 scripts/manage_locations.py list --priority high

# Process only high priority
python3 orchestration/flow.py --locations $(python3 scripts/manage_locations.py list --priority high)
```

---

### API Keys

You'll need to obtain API keys for:

1. **Yelp API**: 
   - Visit [Yelp Developers](https://www.yelp.com/developers)
   - Create an app and get your API key

2. **Google Places API**:
   - Visit [Google Cloud Console](https://console.cloud.google.com/)
   - Enable Places API and get your API key

### Google BigQuery Setup

1. Create a Google Cloud Project if you don't have one
2. Enable the BigQuery API
3. Create a service account and download the JSON key file
4. Create a dataset: `restaurant_db`
5. Grant the service account BigQuery Data Editor and Job User roles
6. Update your `.env` file with the credentials

---

## 🚀 Running the Pipeline

### Manual Execution

1. **Test individual components**:
   ```bash
   # Test Yelp ingestion
   python3 ingestion/yelp_ingest.py
   
   # Test Google Places ingestion
   python3 ingestion/google_places_ingest.py
   ```

2. **Run the full pipeline**:
   ```bash
   python3 orchestration/flow.py
   ```

3. **Transform data with dbt**:
   ```bash
   cd dbt_project
   dbt run
   dbt test
   ```

4. **Launch the dashboard**:
   ```bash
   streamlit run dashboards/app.py
   ```

### Scheduled Execution

For production, you can schedule the pipeline using Prefect:

```bash
# Deploy the flow
prefect deployment build orchestration/flow.py:restaurant_data_pipeline -n "restaurant-pipeline"

# Apply the deployment
prefect deployment apply restaurant_data_pipeline-deployment.yaml

# Run the deployment
prefect deployment run restaurant-data-pipeline/restaurant-pipeline
```

---

## 📊 Dashboard Features

The Streamlit dashboard includes:

- **Real-time metrics**: Total restaurants, average ratings, review counts
- **Interactive filters**: By location, rating, price, data source
- **Visualizations**: Rating distributions, city comparisons, price analysis
- **Data exploration**: Searchable restaurant database
- **Business insights**: Restaurant tiers, quality metrics

---

## 🧪 Testing

Run the test suite to ensure everything works correctly:

```bash
# Run all tests
python3 -m pytest tests/ -v

# Run specific test file
python3 -m pytest tests/test_ingestion.py -v

# Run with coverage
python3 -m pytest tests/ --cov=ingestion --cov-report=html
```

---

## 🗄️ Database Migration Guide

This project is designed to be database-agnostic. To migrate from BigQuery to another database:

### Quick Migration Checklist
```bash
# 1. Update dependencies in requirements.txt
# 2. Modify database config in ingestion/utils.py  
# 3. Update connection logic in ingestion/spark_processor.py
# 4. Change dbt profile in dbt_project/config/dbt_profiles.yml
# 5. Update dashboard client in dashboards/app.py
# 6. Modify environment variables in .env
# 7. Test pipeline: python3 orchestration/flow.py
# 8. Update this README
```

### Supported Database Types
- **Cloud Warehouses**: Snowflake, Redshift, Azure Synapse *(Easy - 2-4 hours)*
- **Traditional SQL**: PostgreSQL, MySQL, SQL Server *(Medium - 1-2 days)*  
- **NoSQL**: MongoDB, Cassandra, DynamoDB *(Hard - 1-2 weeks)*

### Making It More Database-Agnostic ✨
- **✅ Completed**: All business logic moved to dbt macros
- **✅ Completed**: Database-specific SQL handled automatically  
- **✅ Completed**: Python simplified to data loading only
- **✅ Completed**: Comprehensive data quality and validation in dbt
- **Result**: Change 1 config file to migrate databases! 🎯

---

## 🔍 Troubleshooting

### Common Issues

1. **API Key Errors**: Ensure your API keys are correctly set in `.env`
2. **BigQuery Connection**: Verify your BigQuery credentials and service account permissions
3. **Missing Dependencies**: Run `pip3 install -r requirements.txt`
4. **Path Issues**: Ensure you're running commands from the project root
5. **Location Priority**: Use priority levels to control resource usage and processing order
6. **API Rate Limits**: Disable low priority locations if hitting rate limits
7. **Spark Issues**: Check Java installation and PySpark dependencies
8. **Streaming Issues**: Verify Kafka installation and configuration

### **BigQuery Connection**
```bash
# Check credentials
echo $GOOGLE_APPLICATION_CREDENTIALS
echo $BIGQUERY_PROJECT_ID

# Test connection
python3 -c "from google.cloud import bigquery; print('Connection OK')"
```

### **Spark Issues**
```bash
# Check Java installation
java -version

# Check PySpark installation
python3 -c "import pyspark; print('PySpark OK')"

# Memory issues - increase allocation
# Edit config/locations.yml:
# spark_memory: "8g"  # Increase from 2g
```

### **Streaming Issues**
```bash
# Check Kafka installation
kafka-topics.sh --list --bootstrap-server localhost:9092

# Check streaming configuration
# Edit config/locations.yml:
# spark_streaming_enabled: true
# kafka_bootstrap_servers: "localhost:9092"
```

### Debug Mode

Enable debug logging by setting the log level:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

---

## 📈 Next Steps

### Enhancements

1. **Add more data sources**: TripAdvisor, OpenTable, etc.
2. **Implement real-time streaming**: Use Kafka or similar
3. **Add ML models**: Sentiment analysis, recommendation engine
4. **Expand analytics**: Customer behavior, market trends
5. **Add monitoring**: Data quality alerts, pipeline health

### Production Considerations

1. **Security**: Use secrets management for API keys
2. **Scalability**: Implement proper error handling and retries
3. **Monitoring**: Add comprehensive logging and alerting
4. **Testing**: Expand test coverage and add integration tests
5. **Documentation**: Add API documentation and user guides

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- Yelp for providing restaurant data API
- Google for Places API
- The open-source community for the amazing tools used in this project

---

## 📞 Support

If you encounter any issues or have questions:

1. Check the troubleshooting section above
2. Review the test suite for examples
3. Open an issue on GitHub
4. Check the documentation in the `docs/` folder

---

**Happy Data Engineering! 🚀**



