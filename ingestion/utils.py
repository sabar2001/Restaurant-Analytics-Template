import os
import json
import logging
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_api_key(service: str) -> Optional[str]:
    """Get API key for a specific service from environment variables."""
    key_map = {
        'yelp': 'YELP_API_KEY',
        'google': 'GOOGLE_PLACES_API_KEY'
    }
    
    key_name = key_map.get(service.lower())
    if not key_name:
        logger.error(f"Unknown service: {service}")
        return None
    
    api_key = os.getenv(key_name)
    if not api_key:
        logger.error(f"API key not found for {service}")
        return None
    
    return api_key

def get_bigquery_config() -> Dict[str, str]:
    """Get BigQuery connection configuration from environment variables."""
    config = {
        'project_id': os.getenv('BIGQUERY_PROJECT_ID'),
        'dataset_id': os.getenv('BIGQUERY_DATASET_ID'),
        'location': os.getenv('BIGQUERY_LOCATION', 'US'),
        'credentials_path': os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    }
    
    # Check if all required config is present
    missing = [k for k, v in config.items() if not v and k != 'location']  # location has default
    if missing:
        logger.error(f"Missing BigQuery configuration: {missing}")
        return {}
    
    return config

def flatten_json(data: Dict[str, Any], prefix: str = '') -> Dict[str, Any]:
    """Flatten nested JSON structure for easier database storage."""
    flattened = {}
    
    for key, value in data.items():
        new_key = f"{prefix}.{key}" if prefix else key
        
        if isinstance(value, dict):
            flattened.update(flatten_json(value, new_key))
        elif isinstance(value, list):
            # Handle lists by converting to JSON string for now
            flattened[new_key] = json.dumps(value)
        else:
            flattened[new_key] = value
    
    return flattened

def validate_response(response, service: str) -> bool:
    """Validate API response and log any errors."""
    if response.status_code == 200:
        return True
    
    logger.error(f"{service} API error: {response.status_code} - {response.text}")
    return False
