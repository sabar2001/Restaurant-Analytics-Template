import requests
import pandas as pd
from typing import List, Dict, Any
from utils import get_api_key, validate_response, flatten_json
import logging

logger = logging.getLogger(__name__)

class YelpIngestion:
    """Yelp API data ingestion class."""
    
    def __init__(self):
        self.api_key = get_api_key('yelp')
        self.base_url = "https://api.yelp.com/v3"
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
    
    def search_businesses(self, location: str, term: str = "restaurant", limit: int = 50) -> List[Dict[str, Any]]:
        """Search for businesses using Yelp API."""
        if not self.api_key:
            logger.error("Yelp API key not available")
            return []
        
        endpoint = f"{self.base_url}/businesses/search"
        params = {
            'location': location,
            'term': term,
            'limit': limit,
            'sort_by': 'rating'
        }
        
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            
            if not validate_response(response, 'Yelp'):
                return []
            
            data = response.json()
            businesses = data.get('businesses', [])
            
            logger.info(f"Retrieved {len(businesses)} businesses from Yelp")
            return businesses
            
        except Exception as e:
            logger.error(f"Error fetching Yelp data: {e}")
            return []
    
    def get_business_details(self, business_id: str) -> Dict[str, Any]:
        """Get detailed information for a specific business."""
        if not self.api_key:
            return {}
        
        endpoint = f"{self.base_url}/businesses/{business_id}"
        
        try:
            response = requests.get(endpoint, headers=self.headers)
            
            if not validate_response(response, 'Yelp'):
                return {}
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Error fetching business details for {business_id}: {e}")
            return {}
    
    def get_reviews(self, business_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Get reviews for a specific business."""
        if not self.api_key:
            return []
        
        endpoint = f"{self.base_url}/businesses/{business_id}/reviews"
        params = {'limit': limit}
        
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            
            if not validate_response(response, 'Yelp'):
                return []
            
            data = response.json()
            reviews = data.get('reviews', [])
            
            # Add business_id to each review for reference
            for review in reviews:
                review['business_id'] = business_id
            
            return reviews
            
        except Exception as e:
            logger.error(f"Error fetching reviews for {business_id}: {e}")
            return []
    
    def process_business_data(self, businesses: List[Dict[str, Any]]) -> pd.DataFrame:
        """Process and flatten business data into a DataFrame."""
        processed_data = []
        
        for business in businesses:
            # Flatten the business data
            flattened = flatten_json(business)
            
            # Add some derived fields
            flattened['ingestion_timestamp'] = pd.Timestamp.now()
            flattened['data_source'] = 'yelp'
            
            processed_data.append(flattened)
        
        df = pd.DataFrame(processed_data)
        logger.info(f"Processed {len(df)} business records")
        
        return df
    
    def process_reviews_data(self, reviews: List[Dict[str, Any]]) -> pd.DataFrame:
        """Process and flatten reviews data into a DataFrame."""
        processed_data = []
        
        for review in reviews:
            flattened = flatten_json(review)
            flattened['ingestion_timestamp'] = pd.Timestamp.now()
            flattened['data_source'] = 'yelp'
            
            processed_data.append(flattened)
        
        df = pd.DataFrame(processed_data)
        logger.info(f"Processed {len(df)} review records")
        
        return df

def main():
    """Main function to demonstrate Yelp ingestion."""
    yelp = YelpIngestion()
    
    # Example: Search for restaurants in San Francisco
    businesses = yelp.search_businesses("San Francisco, CA", "restaurant", 10)
    
    if businesses:
        # Process business data
        business_df = yelp.process_business_data(businesses)
        print(f"Business data shape: {business_df.shape}")
        
        # Get reviews for first business
        if len(businesses) > 0:
            first_business_id = businesses[0]['id']
            reviews = yelp.get_reviews(first_business_id, 5)
            
            if reviews:
                reviews_df = yelp.process_reviews_data(reviews)
                print(f"Reviews data shape: {reviews_df.shape}")
    
    return yelp

if __name__ == "__main__":
    main()
