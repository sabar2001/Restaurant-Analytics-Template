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
    if not api_key or api_key.startswith('your_'):
        logger.warning(f"API key not found or not configured for {service}")
        return None
    
    return api_key

def get_available_apis() -> Dict[str, bool]:
    """Check which API keys are available and valid."""
    apis = {
        'yelp': get_api_key('yelp') is not None,
        'google_places': get_api_key('google') is not None
    }
    
    available_count = sum(apis.values())
    available_names = [api for api, available in apis.items() if available]
    
    if available_count == 0:
        logger.error("❌ No API keys configured! Please add at least one API key to .env file")
    else:
        logger.info(f"✅ Available APIs: {available_names} ({available_count}/2)")
    
    return apis

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
