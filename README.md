# 🍽️ Restaurant Analytics Template

An open-source **data engineering + analytics engineering** framework for restaurant data APIs.

This project demonstrates:
- **Ingestion & preprocessing** with Spark
- **Cloud data warehouse** storage in Snowflake
- **Transformations** with dbt
- **Pipeline orchestration** with Prefect
- **Interactive dashboard** with Streamlit

---

## 🚀 Architecture

![Architecture Diagram](docs/architecture.png)

**Flow**:  
APIs (Yelp, Google Places, others) → **Spark ingestion & flattening** → **Snowflake (raw)** → **dbt models** → **Snowflake (analytics schema)** → **Streamlit dashboard**

---

## 🛠️ Tech Stack

- **Apache Spark** – scale ingestion & JSON preprocessing  
- **Snowflake** – cloud warehouse for structured data  
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

### 2.Install dependancies
```bash
pip install -r requirements.txt
```

### 3. Setup environment
Copy  ```config/sample.env``` → .env
Add:
API keys (Yelp, Google, etc.)
Snowflake credentials


