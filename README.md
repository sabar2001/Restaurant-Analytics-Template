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

![Architecture Diagram](docs/architecture.png)

**Flow**:  
APIs (Yelp, Google Places, others) → **Spark ingestion & flattening** → **BigQuery (raw)** → **dbt models** → **BigQuery (analytics schema)** → **Streamlit dashboard**

---

## 🛠️ Tech Stack

- **Apache Spark** – scale ingestion & JSON preprocessing  
- **Google BigQuery** – cloud warehouse for structured data  
- **dbt** – SQL transformations into analytics-ready models  
- **Prefect** – orchestration of the full pipeline  
- **Streamlit** – visualization/dashboard  

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
# Run for all configured locations
python3 orchestration/flow.py

# Run for specific locations
python3 orchestration/flow.py --locations "San Francisco, CA" "New York, NY"

# Run for single location
python3 orchestration/flow.py --locations "Miami, FL"
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
│   └── architecture.png         # Architecture diagram
│
├── .gitignore
├── requirements.txt
└── README.md
```

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

---

## 🔧 Configuration

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

## 🔍 Troubleshooting

### Common Issues

1. **API Key Errors**: Ensure your API keys are correctly set in `.env`
2. **BigQuery Connection**: Verify your BigQuery credentials and service account permissions
3. **Missing Dependencies**: Run `pip3 install -r requirements.txt`
4. **Path Issues**: Ensure you're running commands from the project root

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



