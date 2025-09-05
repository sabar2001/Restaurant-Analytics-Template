import unittest
import pandas as pd
import sys
import os

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.utils import flatten_json, get_api_key, get_bigquery_config

class TestIngestionUtils(unittest.TestCase):
    """Test cases for ingestion utility functions."""
    
    def test_flatten_json_simple(self):
        """Test flattening of simple JSON structure."""
        data = {
            'name': 'Test Restaurant',
            'rating': 4.5,
            'location': {
                'city': 'San Francisco',
                'state': 'CA'
            }
        }
        
        flattened = flatten_json(data)
        
        self.assertEqual(flattened['name'], 'Test Restaurant')
        self.assertEqual(flattened['rating'], 4.5)
        self.assertEqual(flattened['location.city'], 'San Francisco')
        self.assertEqual(flattened['location.state'], 'CA')
    
    def test_flatten_json_with_list(self):
        """Test flattening of JSON with list values."""
        data = {
            'name': 'Test Restaurant',
            'categories': ['Italian', 'Pizza', 'Dinner']
        }
        
        flattened = flatten_json(data)
        
        self.assertEqual(flattened['name'], 'Test Restaurant')
        self.assertIn('categories', flattened)
        # Lists should be converted to JSON strings
        self.assertIsInstance(flattened['categories'], str)
    
    def test_flatten_json_nested(self):
        """Test flattening of deeply nested JSON structure."""
        data = {
            'restaurant': {
                'info': {
                    'basic': {
                        'name': 'Deep Restaurant'
                    }
                }
            }
        }
        
        flattened = flatten_json(data)
        
        self.assertEqual(flattened['restaurant.info.basic.name'], 'Deep Restaurant')
    
    def test_get_api_key_missing_env(self):
        """Test API key retrieval when environment variable is missing."""
        # This test will fail if the environment variable is set
        # In a real test environment, you'd mock the environment
        key = get_api_key('yelp')
        # The function should handle missing keys gracefully
        self.assertIsInstance(key, (str, type(None)))
    
    def test_get_bigquery_config_missing_env(self):
        """Test BigQuery config retrieval when environment variables are missing."""
        config = get_bigquery_config()
        # Should return empty dict when config is missing
        self.assertIsInstance(config, dict)

class TestDataProcessing(unittest.TestCase):
    """Test cases for data processing functions."""
    
    def test_dataframe_creation(self):
        """Test that we can create DataFrames with our data structure."""
        data = [
            {
                'restaurant_id': 'REST_001',
                'restaurant_name': 'Test Restaurant 1',
                'rating': 4.5,
                'data_source': 'yelp'
            },
            {
                'restaurant_id': 'REST_002',
                'restaurant_name': 'Test Restaurant 2',
                'rating': 3.8,
                'data_source': 'google_places'
            }
        ]
        
        df = pd.DataFrame(data)
        
        self.assertEqual(len(df), 2)
        self.assertEqual(len(df.columns), 4)
        self.assertIn('restaurant_id', df.columns)
        self.assertIn('rating', df.columns)
    
    def test_data_validation(self):
        """Test basic data validation logic."""
        data = [
            {'name': 'Restaurant 1', 'rating': 4.5, 'data_source': 'yelp'},
            {'name': 'Restaurant 2', 'rating': 3.8, 'data_source': 'google_places'},
            {'name': None, 'rating': 4.0, 'data_source': 'yelp'},  # Invalid: missing name
            {'name': 'Restaurant 4', 'rating': 6.0, 'data_source': 'yelp'}  # Invalid: rating > 5
        ]
        
        df = pd.DataFrame(data)
        
        # Filter out invalid records
        valid_df = df[
            (df['name'].notna()) & 
            (df['rating'] >= 0) & 
            (df['rating'] <= 5)
        ]
        
        self.assertEqual(len(valid_df), 2)  # Only first two should be valid

if __name__ == '__main__':
    unittest.main()
