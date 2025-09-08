# Database Migration Guide

This project is designed to be database-agnostic through its modular architecture and dbt integration. Here's how to migrate from BigQuery to other database systems.

## 🎯 Why dbt Makes This Project Database-Agnostic

**dbt (data build tool)** acts as a translation layer between your SQL transformations and different databases:

1. **SQL Dialect Translation**: dbt automatically converts SQL to database-specific syntax
2. **Type Mapping**: Handles data type differences between databases  
3. **Macro System**: Write once, run anywhere with database-specific implementations
4. **Adapter Architecture**: Just change the dbt adapter to switch databases

## 📋 Migration Steps by Database Type

### Cloud Data Warehouses (Easy: 2-4 hours)

#### Snowflake
```bash
# 1. Update requirements.txt
sed -i 's/dbt-bigquery/dbt-snowflake/' requirements.txt
sed -i 's/google-cloud-bigquery/snowflake-connector-python/' requirements.txt

# 2. Update dbt profile
# Edit dbt_project/config/dbt_profiles.yml:
# type: snowflake
# account: your_account
# user: your_user
# password: your_password
# warehouse: your_warehouse
# database: your_database
# schema: your_schema

# 3. Update Python config
# Edit ingestion/utils.py - replace get_bigquery_config with get_snowflake_config
```

#### Amazon Redshift  
```bash
# 1. Update requirements.txt
sed -i 's/dbt-bigquery/dbt-redshift/' requirements.txt
sed -i 's/google-cloud-bigquery/redshift-connector/' requirements.txt

# 2. Update dbt profile for Redshift connection
# 3. Update Python config for Redshift
```

### Traditional Databases (Medium: 1-2 days)

#### PostgreSQL
```bash
# 1. Update requirements.txt
sed -i 's/dbt-bigquery/dbt-postgres/' requirements.txt
sed -i 's/google-cloud-bigquery/psycopg2-binary/' requirements.txt

# 2. Design schema (BigQuery is schemaless, PostgreSQL needs structure)
# 3. Update dbt models for PostgreSQL syntax differences
# 4. Consider indexing strategy for performance
```

#### MySQL
```bash
# 1. Update requirements.txt  
sed -i 's/dbt-bigquery/dbt-mysql/' requirements.txt
sed -i 's/google-cloud-bigquery/mysql-connector-python/' requirements.txt

# 2. Handle JSON column differences (MySQL 5.7+ supports JSON)
# 3. Update dbt models for MySQL syntax
```

### NoSQL Databases (Hard: 1-2 weeks)

#### MongoDB
```bash
# 1. Complete rewrite of data models
# 2. Replace dbt with custom transformation logic
# 3. Redesign dashboard queries for document structure
# 4. Handle relationships with embedded documents or references
```

## 🛠️ File-by-File Changes

### 1. `requirements.txt`
```txt
# Replace BigQuery dependencies
google-cloud-bigquery -> your_db_connector
dbt-bigquery -> dbt-your_adapter
```

### 2. `ingestion/utils.py`
```python
# Replace function name and logic
def get_bigquery_config() -> Dict[str, str]:
    # Change to get_your_db_config()
    # Update environment variables
    # Update connection parameters
```

### 3. `ingestion/spark_processor.py`  
```python
# Update write_to_database method
def write_to_bigquery(self, df, table_name):
    # Change to write_to_your_db()
    # Update connection logic
    # Handle database-specific data types
```

### 4. `dbt_project/config/dbt_profiles.yml`
```yaml
# Replace entire profile
your_project:
  target: dev
  outputs:
    dev:
      type: your_adapter  # postgres, snowflake, redshift, etc.
      # Add database-specific connection parameters
```

### 5. `dashboards/app.py`
```python
# Replace BigQuery client
from google.cloud import bigquery
# Change to your database client

# Update query logic in load_real_data()
```

### 6. `.env` file
```bash
# Replace BigQuery variables
GOOGLE_APPLICATION_CREDENTIALS=
BIGQUERY_PROJECT_ID=  
BIGQUERY_DATASET_ID=

# With your database variables
YOUR_DB_HOST=
YOUR_DB_USER=
YOUR_DB_PASSWORD=
```

## 🔄 Making the Project Even More Database-Agnostic

### Move More Logic to dbt

Currently, some data transformations happen in Python. Moving these to dbt makes switching databases easier:

1. **Price categorization** → dbt macro
2. **Rating calculations** → dbt model  
3. **Geospatial processing** → dbt with PostGIS (for PostgreSQL)

### Use dbt Macros for Database-Specific Logic

```sql
-- macros/price_category.sql
{% macro price_category(price_level) %}
  {% if target.type == 'bigquery' %}
    CASE 
      WHEN {{ price_level }} IS NULL THEN 'Unknown'
      WHEN {{ price_level }} <= 1 THEN 'Budget'
      WHEN {{ price_level }} <= 2 THEN 'Mid-range'  
      ELSE 'Expensive'
    END
  {% elif target.type == 'snowflake' %}
    CASE 
      WHEN {{ price_level }} IS NULL THEN 'Unknown'
      WHEN {{ price_level }} <= 1 THEN 'Budget'
      WHEN {{ price_level }} <= 2 THEN 'Mid-range'
      ELSE 'Expensive'  
    END
  {% endif %}
{% endmacro %}
```

### Abstract Raw Data Storage

Keep API responses in their original JSON format, then use dbt to create database-specific views:

```sql
-- models/raw/raw_restaurant_responses.sql  
-- Store original API JSON responses

-- models/staging/stg_restaurants.sql
-- Extract and transform for specific database
SELECT 
  {{ extract_json_field('api_response', 'place_id') }} as place_id,
  {{ extract_json_field('api_response', 'name') }} as restaurant_name,
  -- Use database-specific JSON extraction
FROM {{ ref('raw_restaurant_responses') }}
```

## ✅ Testing Your Migration

```bash
# 1. Test database connection
python3 -c "from ingestion.utils import get_your_db_config; print('✅ Config OK')"

# 2. Test dbt connection  
cd dbt_project
dbt debug

# 3. Run a small test
python3 orchestration/flow.py --locations "Test City"

# 4. Check dashboard
python3 -m streamlit run dashboards/app.py
```

## 🎯 Database Selection Guide

| Database | Best For | Migration Effort | dbt Support |
|----------|----------|------------------|-------------|
| **Snowflake** | Large-scale analytics | ⭐ Easy | ✅ Excellent |
| **Redshift** | AWS ecosystem | ⭐ Easy | ✅ Excellent |  
| **BigQuery** | Google Cloud, ML | ⭐ Current | ✅ Excellent |
| **PostgreSQL** | Cost-effective, flexible | ⭐⭐ Medium | ✅ Excellent |
| **MySQL** | Web applications | ⭐⭐ Medium | ✅ Good |
| **MongoDB** | Document-heavy workloads | ⭐⭐⭐ Hard | ❌ No native support |

## 💡 Pro Tips

1. **Start with dbt migration** - Get your transformations working first
2. **Test with small data** - Don't migrate everything at once  
3. **Keep BigQuery as backup** - Until you're confident in the new setup
4. **Document custom changes** - For future migrations
5. **Consider multi-database setup** - Use different databases for different purposes

---

**The goal is to make database migration as simple as changing a configuration file! 🎯**
