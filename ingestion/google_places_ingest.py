import requests
import pandas as pd
from typing import List, Dict, Any
from .utils import get_api_key, validate_response, flatten_json
import logging

logger = logging.getLogger(__name__)

class GooglePlacesIngestion:
    """Google Places API data ingestion class with reviews collection."""
    
    def __init__(self):
        self.api_key = get_api_key('google')
        self.base_url = "https://maps.googleapis.com/maps/api/place"
        self.new_base_url = "https://places.googleapis.com/v1"
        self.headers = {
            'Content-Type': 'application/json'
        }
    
    def search_nearby_places(self, location: str, radius: int = 5000, type: str = "restaurant", target_count: int = 500) -> List[Dict[str, Any]]:
        """Search for nearby places using Google Places API (New) with large-scale data collection."""
        if not self.api_key:
            logger.error("Google Places API key not available")
            return []
        
        # First, get coordinates for the location
        coords = self._get_coordinates(location)
        if not coords:
            return []
        
        logger.info(f"🚀 Starting large-scale data collection for {location}")
        logger.info(f"Target: {target_count} restaurants using Spark-enabled pipeline")
        
        all_places = []
        
        # Strategy 1: Multiple radius searches (concentric circles)
        radii = [2000, 5000, 10000, 15000, 20000]  # Expanding search radius
        for search_radius in radii:
            if len(all_places) >= target_count:
                break
                
            logger.info(f"🔍 Searching radius {search_radius}m around {location}")
            places = self._search_with_radius(coords, search_radius, type)
            new_places = self._deduplicate_places(all_places, places)
            all_places.extend(new_places)
            logger.info(f"Found {len(new_places)} new places (total: {len(all_places)})")
        
        # Strategy 2: Different restaurant types for diversity
        restaurant_types = ["restaurant", "meal_takeaway", "meal_delivery", "cafe", "bar", "bakery"]
        for search_type in restaurant_types:
            if len(all_places) >= target_count:
                break
                
            logger.info(f"🍽️ Searching for {search_type} in {location}")
            places = self._search_with_radius(coords, radius, search_type)
            new_places = self._deduplicate_places(all_places, places)
            all_places.extend(new_places)
            logger.info(f"Found {len(new_places)} new {search_type} places (total: {len(all_places)})")
        
        # Strategy 3: Nearby areas search (offset coordinates)
        if len(all_places) < target_count:
            logger.info(f"🗺️ Expanding search to nearby areas around {location}")
            offsets = [
                (0.01, 0.01), (-0.01, -0.01), (0.01, -0.01), (-0.01, 0.01),  # Diagonal offsets
                (0.02, 0), (-0.02, 0), (0, 0.02), (0, -0.02)  # Cardinal offsets
            ]
            
            for lat_offset, lng_offset in offsets:
                if len(all_places) >= target_count:
                    break
                    
                offset_coords = {
                    'lat': coords['lat'] + lat_offset,
                    'lng': coords['lng'] + lng_offset
                }
                places = self._search_with_radius(offset_coords, radius, type)
                new_places = self._deduplicate_places(all_places, places)
                all_places.extend(new_places)
                logger.info(f"Found {len(new_places)} new places from offset search (total: {len(all_places)})")
        
        logger.info(f"✅ Completed large-scale collection: {len(all_places)} restaurants for {location}")
        return all_places[:target_count]  # Limit to target count
    
    def _search_with_radius(self, coords: Dict[str, float], radius: int, type: str) -> List[Dict[str, Any]]:
        """Helper method to search with specific coordinates and radius."""
        # Try new Places API (New) first
        new_results = self._search_nearby_new_api(coords, radius, type)
        if new_results:
            return new_results
        
        # Fallback to legacy API if new API fails
        return self._search_nearby_legacy_api(coords, radius, type)
    
    def _deduplicate_places(self, existing_places: List[Dict], new_places: List[Dict]) -> List[Dict]:
        """Remove duplicate places based on place_id or name+address."""
        existing_ids = {place.get('place_id', '') for place in existing_places}
        existing_signatures = {
            f"{place.get('name', '')}-{place.get('formatted_address', '')}" 
            for place in existing_places
        }
        
        unique_places = []
        for place in new_places:
            place_id = place.get('place_id', '')
            signature = f"{place.get('name', '')}-{place.get('formatted_address', '')}"
            
            if place_id not in existing_ids and signature not in existing_signatures:
                unique_places.append(place)
                existing_ids.add(place_id)
                existing_signatures.add(signature)
        
        return unique_places
    
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
        """Get detailed information for a specific place using Places API (New)."""
        if not self.api_key:
            return {}
        
        # Use the new Places API (v1) format
        endpoint = f"{self.new_base_url}/places/{place_id}"
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': self.api_key,
            'X-Goog-FieldMask': 'id,displayName,rating,userRatingCount,reviews,formattedAddress,location,types,priceLevel'
        }
        
        try:
            response = requests.get(endpoint, headers=headers)
            
            if not validate_response(response, 'Google Places (New)'):
                return {}
            
            data = response.json()
            
            # Convert new API format to legacy format for compatibility
            result = {}
            if 'id' in data:
                result['place_id'] = data['id']
            if 'displayName' in data:
                result['name'] = data['displayName'].get('text', '')
            if 'rating' in data:
                result['rating'] = data['rating']
            if 'userRatingCount' in data:
                result['user_ratings_total'] = data['userRatingCount']
            if 'formattedAddress' in data:
                result['formatted_address'] = data['formattedAddress']
            if 'location' in data:
                result['geometry'] = {
                    'location': {
                        'lat': data['location']['latitude'],
                        'lng': data['location']['longitude']
                    }
                }
            if 'types' in data:
                result['types'] = data['types']
            if 'priceLevel' in data:
                result['price_level'] = data['priceLevel']
            
            # Convert reviews to legacy format
            if 'reviews' in data:
                legacy_reviews = []
                for review in data['reviews']:
                    legacy_review = {
                        'author_name': review.get('authorAttribution', {}).get('displayName', 'Anonymous'),
                        'rating': review.get('rating', 0),
                        'text': review.get('text', {}).get('text', ''),
                        'time': review.get('publishTime', ''),
                        'relative_time_description': review.get('relativePublishTimeDescription', ''),
                        'language': review.get('originalText', {}).get('languageCode', 'en')
                    }
                    legacy_reviews.append(legacy_review)
                result['reviews'] = legacy_reviews
            
            return result
            
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

    def get_restaurant_reviews_by_rating(self, place_id: str, target_reviews_per_bucket: int = 10) -> Dict[str, Any]:
        """
        Collect reviews for a restaurant, targeting specific reviews per rating bucket.
        
        Args:
            place_id: Google Places place_id for the restaurant
            target_reviews_per_bucket: Number of reviews to collect per rating range (1-2, 2-3, 3-4, 4-5)
            
        Returns:
            Dictionary with reviews organized by rating buckets
        """
        if not self.api_key:
            logger.error("Google Places API key not available")
            return {}
        
        logger.info(f"🔍 Collecting reviews for place_id: {place_id}")
        
        # Get place details with reviews
        place_details = self.get_place_details(place_id)
        
        if not place_details or 'reviews' not in place_details:
            logger.warning(f"No reviews found for place_id: {place_id}")
            return {}
        
        reviews = place_details['reviews']
        total_reviews = len(reviews)
        restaurant_name = place_details.get('name', 'Unknown')
        
        logger.info(f"📊 Found {total_reviews} reviews for {restaurant_name}")
        
        # Initialize rating buckets
        rating_buckets = {
            '1-2': {'reviews': [], 'target': target_reviews_per_bucket},
            '2-3': {'reviews': [], 'target': target_reviews_per_bucket}, 
            '3-4': {'reviews': [], 'target': target_reviews_per_bucket},
            '4-5': {'reviews': [], 'target': target_reviews_per_bucket}
        }
        
        # Categorize reviews by rating
        for review in reviews:
            rating = review.get('rating', 0)
            
            # Determine bucket
            bucket = None
            if 1 <= rating < 2:
                bucket = '1-2'
            elif 2 <= rating < 3:
                bucket = '2-3'
            elif 3 <= rating < 4:
                bucket = '3-4'
            elif 4 <= rating <= 5:
                bucket = '4-5'
            
            if bucket and len(rating_buckets[bucket]['reviews']) < target_reviews_per_bucket:
                # Clean and structure the review
                clean_review = {
                    'author_name': review.get('author_name', 'Anonymous'),
                    'rating': rating,
                    'text': review.get('text', ''),
                    'time': review.get('time', 0),
                    'relative_time_description': review.get('relative_time_description', ''),
                    'language': review.get('language', 'en')
                }
                rating_buckets[bucket]['reviews'].append(clean_review)
        
        # Log collection results
        collected_counts = {bucket: len(data['reviews']) for bucket, data in rating_buckets.items()}
        logger.info(f"📈 Reviews collected by rating: {collected_counts}")
        
        return {
            'place_id': place_id,
            'restaurant_name': restaurant_name,
            'total_reviews_available': total_reviews,
            'overall_rating': place_details.get('rating', 0),
            'user_ratings_total': place_details.get('user_ratings_total', 0),
            'reviews_by_rating': rating_buckets,
            'collection_timestamp': pd.Timestamp.now().isoformat()
        }

    def process_reviews_data_for_bigquery(self, reviews_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Convert reviews data to BigQuery-ready DataFrame format.
        
        Args:
            reviews_data: Output from get_restaurant_reviews_by_rating()
            
        Returns:
            DataFrame ready for BigQuery ingestion
        """
        if not reviews_data or 'reviews_by_rating' not in reviews_data:
            return pd.DataFrame()
        
        # Create single row with JSON arrays for each rating bucket
        row_data = {
            'place_id': reviews_data['place_id'],
            'restaurant_name': reviews_data['restaurant_name'],
            'overall_rating': reviews_data['overall_rating'],
            'user_ratings_total': reviews_data['user_ratings_total'],
            'total_reviews_available': reviews_data['total_reviews_available'],
            
            # JSON arrays for each rating bucket
            'reviews_1_2': reviews_data['reviews_by_rating']['1-2']['reviews'],
            'reviews_2_3': reviews_data['reviews_by_rating']['2-3']['reviews'],
            'reviews_3_4': reviews_data['reviews_by_rating']['3-4']['reviews'],
            'reviews_4_5': reviews_data['reviews_by_rating']['4-5']['reviews'],
            
            # Summary counts
            'reviews_1_2_count': len(reviews_data['reviews_by_rating']['1-2']['reviews']),
            'reviews_2_3_count': len(reviews_data['reviews_by_rating']['2-3']['reviews']),
            'reviews_3_4_count': len(reviews_data['reviews_by_rating']['3-4']['reviews']),
            'reviews_4_5_count': len(reviews_data['reviews_by_rating']['4-5']['reviews']),
            
            'collection_timestamp': reviews_data['collection_timestamp'],
            'data_source': 'google_places_reviews'
        }
        
        return pd.DataFrame([row_data])

    def collect_reviews_for_restaurants(self, restaurant_df: pd.DataFrame, reviews_per_bucket: int = 10) -> pd.DataFrame:
        """
        Collect reviews for multiple restaurants from a DataFrame.
        
        Args:
            restaurant_df: DataFrame with restaurant data (must have 'place_id' column)
            reviews_per_bucket: Number of reviews to collect per rating bucket
            
        Returns:
            DataFrame with reviews data for all restaurants
        """
        if 'place_id' not in restaurant_df.columns:
            logger.error("restaurant_df must have 'place_id' column")
            return pd.DataFrame()
        
        logger.info(f"🚀 Starting reviews collection for {len(restaurant_df)} restaurants")
        logger.info(f"Target: {reviews_per_bucket} reviews per rating bucket (40 total per restaurant)")
        
        all_reviews_data = []
        
        for idx, row in restaurant_df.iterrows():
            place_id = row['place_id']
            restaurant_name = row.get('name', row.get('restaurant_name', 'Unknown'))
            
            try:
                logger.info(f"📝 Collecting reviews for: {restaurant_name} ({idx+1}/{len(restaurant_df)})")
                
                reviews_data = self.get_restaurant_reviews_by_rating(place_id, reviews_per_bucket)
                
                if reviews_data:
                    reviews_df = self.process_reviews_data_for_bigquery(reviews_data)
                    if not reviews_df.empty:
                        all_reviews_data.append(reviews_df)
                        
                        # Log progress
                        total_collected = sum([
                            len(reviews_data['reviews_by_rating'][bucket]['reviews'])
                            for bucket in ['1-2', '2-3', '3-4', '4-5']
                        ])
                        logger.info(f"✅ Collected {total_collected} reviews for {restaurant_name}")
                
            except Exception as e:
                logger.error(f"❌ Error collecting reviews for {restaurant_name}: {e}")
                continue
        
        if all_reviews_data:
            final_df = pd.concat(all_reviews_data, ignore_index=True)
            logger.info(f"🎉 Reviews collection complete! {len(final_df)} restaurants processed")
            return final_df
        else:
            logger.warning("No reviews data collected")
            return pd.DataFrame()

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
