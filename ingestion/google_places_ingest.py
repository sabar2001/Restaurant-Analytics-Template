import requests
import pandas as pd
from typing import List, Dict, Any
from .utils import get_api_key, validate_response, flatten_json
import logging

logger = logging.getLogger(__name__)

class GooglePlacesIngestion:
    """Google Places API data ingestion class."""
    
    def __init__(self):
        self.api_key = get_api_key('google')
        self.base_url = "https://maps.googleapis.com/maps/api/place"
        self.new_base_url = "https://places.googleapis.com/v1"
        self.headers = {
            'Content-Type': 'application/json'
        }
    
    def search_nearby_places(self, location: str, radius: int = 5000, type: str = "restaurant") -> List[Dict[str, Any]]:
        """Search for nearby places using Google Places API (New)."""
        if not self.api_key:
            logger.error("Google Places API key not available")
            return []
        
        # First, get coordinates for the location
        coords = self._get_coordinates(location)
        if not coords:
            return []
        
        # Try new Places API (New) first
        new_results = self._search_nearby_new_api(coords, radius, type)
        if new_results:
            return new_results
        
        # Fallback to legacy API if new API fails
        logger.warning("New Places API failed, trying legacy API")
        return self._search_nearby_legacy_api(coords, radius, type)
    
    def _search_nearby_new_api(self, coords: Dict[str, float], radius: int, type: str) -> List[Dict[str, Any]]:
        """Search using the new Places API (v1)."""
        endpoint = f"{self.new_base_url}/places:searchNearby"
        
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': self.api_key,
            'X-Goog-FieldMask': 'places.id,places.displayName,places.rating,places.location,places.types,places.priceLevel,places.userRatingCount,places.formattedAddress'
        }
        
        body = {
            "locationRestriction": {
                "circle": {
                    "center": {
                        "latitude": coords['lat'],
                        "longitude": coords['lng']
                    },
                    "radius": float(radius)
                }
            },
            "includedTypes": [type],
            "maxResultCount": 20
        }
        
        try:
            response = requests.post(endpoint, json=body, headers=headers)
            
            if response.status_code != 200:
                logger.error(f"New Places API HTTP error: {response.status_code} - {response.text}")
                return []
            
            data = response.json()
            places = data.get('places', [])
            
            # Convert new API format to legacy format for compatibility
            converted_places = []
            for place in places:
                converted_place = {
                    'place_id': place.get('id', ''),
                    'name': place.get('displayName', {}).get('text', 'Unknown'),
                    'rating': place.get('rating', 0),
                    'user_ratings_total': place.get('userRatingCount', 0),
                    'price_level': place.get('priceLevel', 0),
                    'types': place.get('types', []),
                    'formatted_address': place.get('formattedAddress', ''),
                    'geometry': {
                        'location': {
                            'lat': place.get('location', {}).get('latitude', 0),
                            'lng': place.get('location', {}).get('longitude', 0)
                        }
                    }
                }
                converted_places.append(converted_place)
            
            logger.info(f"Retrieved {len(converted_places)} places from new Places API")
            return converted_places
            
        except Exception as e:
            logger.error(f"Error with new Places API: {e}")
            return []
    
    def _search_nearby_legacy_api(self, coords: Dict[str, float], radius: int, type: str) -> List[Dict[str, Any]]:
        """Search using the legacy Places API."""
        endpoint = f"{self.base_url}/nearbysearch/json"
        params = {
            'location': f"{coords['lat']},{coords['lng']}",
            'radius': radius,
            'type': type,
            'key': self.api_key
        }
        
        try:
            response = requests.get(endpoint, params=params)
            
            if not validate_response(response, 'Google Places'):
                return []
            
            data = response.json()
            
            if data.get('status') != 'OK':
                logger.error(f"Legacy Places API error: {data.get('status')}")
                return []
            
            places = data.get('results', [])
            logger.info(f"Retrieved {len(places)} places from legacy Places API")
            
            return places
            
        except Exception as e:
            logger.error(f"Error fetching legacy Places data: {e}")
            return []
    
    def get_place_details(self, place_id: str) -> Dict[str, Any]:
        """Get detailed information for a specific place."""
        if not self.api_key:
            return {}
        
        endpoint = f"{self.base_url}/details/json"
        params = {
            'place_id': place_id,
            'fields': 'name,formatted_address,geometry,rating,user_ratings_total,types,price_level,opening_hours,website,formatted_phone_number,reviews',
            'key': self.api_key
        }
        
        try:
            response = requests.get(endpoint, params=params)
            
            if not validate_response(response, 'Google Places'):
                return {}
            
            data = response.json()
            
            if data.get('status') != 'OK':
                logger.error(f"Google Places API error: {data.get('status')}")
                return {}
            
            return data.get('result', {})
            
        except Exception as e:
            logger.error(f"Error fetching place details for {place_id}: {e}")
            return {}
    
    def _get_coordinates(self, location: str) -> Dict[str, float]:
        """Get coordinates for a location using Geocoding API."""
        if not self.api_key:
            return {}
        
        endpoint = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            'address': location,
            'key': self.api_key
        }
        
        try:
            response = requests.get(endpoint, params=params)
            
            if not validate_response(response, 'Google Geocoding'):
                return {}
            
            data = response.json()
            
            if data.get('status') != 'OK':
                logger.error(f"Geocoding API error: {data.get('status')}")
                return {}
            
            results = data.get('results', [])
            if results:
                location_data = results[0]['geometry']['location']
                return {
                    'lat': location_data['lat'],
                    'lng': location_data['lng']
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting coordinates for {location}: {e}")
            return {}
    
    def process_places_data(self, places: List[Dict[str, Any]]) -> pd.DataFrame:
        """Process and flatten places data into a DataFrame."""
        processed_data = []
        
        for place in places:
            # Flatten the place data
            flattened = flatten_json(place)
            
            # Add some derived fields
            flattened['ingestion_timestamp'] = pd.Timestamp.now()
            flattened['data_source'] = 'google_places'
            
            processed_data.append(flattened)
        
        df = pd.DataFrame(processed_data)
        logger.info(f"Processed {len(df)} place records")
        
        return df
    
    def process_reviews_data(self, place_details: Dict[str, Any]) -> pd.DataFrame:
        """Process and flatten reviews data from place details."""
        reviews = place_details.get('reviews', [])
        
        if not reviews:
            return pd.DataFrame()
        
        processed_data = []
        
        for review in reviews:
            flattened = flatten_json(review)
            flattened['place_id'] = place_details.get('place_id', '')
            flattened['ingestion_timestamp'] = pd.Timestamp.now()
            flattened['data_source'] = 'google_places'
            
            processed_data.append(flattened)
        
        df = pd.DataFrame(processed_data)
        logger.info(f"Processed {len(df)} review records")
        
        return df

def main():
    """Main function to demonstrate Google Places ingestion."""
    google = GooglePlacesIngestion()
    
    # Example: Search for restaurants in San Francisco
    places = google.search_nearby_places("San Francisco, CA", 5000, "restaurant")
    
    if places:
        # Process places data
        places_df = google.process_places_data(places)
        print(f"Places data shape: {places_df.shape}")
        
        # Get details for first place
        if len(places) > 0:
            first_place_id = places[0]['place_id']
            place_details = google.get_place_details(first_place_id)
            
            if place_details:
                reviews_df = google.process_reviews_data(place_details)
                if not reviews_df.empty:
                    print(f"Reviews data shape: {reviews_df.shape}")
    
    return google

if __name__ == "__main__":
    main()
