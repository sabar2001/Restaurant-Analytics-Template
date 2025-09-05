import yaml
import os
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

@dataclass
class Location:
    """Data class for location information."""
    name: str
    country: str
    priority: str
    enabled: bool
    coordinates: Optional[Dict[str, float]] = None
    last_updated: Optional[str] = None

class LocationManager:
    """Manages restaurant data collection locations."""
    
    def __init__(self, config_path: str = "config/locations.yml"):
        self.config_path = config_path
        self.locations = []
        self.settings = {}
        self.categories = {}
        self.load_config()
    
    def load_config(self) -> None:
        """Load location configuration from YAML file."""
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                logger.error(f"Location config file not found: {self.config_path}")
                return
            
            with open(config_file, 'r') as file:
                config = yaml.safe_load(file)
            
            # Load locations
            self.locations = []
            for loc_data in config.get('locations', []):
                location = Location(
                    name=loc_data['name'],
                    country=loc_data['country'],
                    priority=loc_data['priority'],
                    enabled=loc_data['enabled']
                )
                self.locations.append(location)
            
            # Load settings
            self.settings = config.get('settings', {})
            
            # Load categories
            self.categories = config.get('categories', {})
            
            logger.info(f"Loaded {len(self.locations)} locations from config")
            
        except Exception as e:
            logger.error(f"Error loading location config: {e}")
    
    def get_enabled_locations(self) -> List[Location]:
        """Get all enabled locations."""
        return [loc for loc in self.locations if loc.enabled]
    
    def get_locations_by_priority(self, priority: str) -> List[Location]:
        """Get locations filtered by priority."""
        return [loc for loc in self.locations if loc.priority == priority and loc.enabled]
    
    def get_locations_by_category(self, category: str) -> List[Location]:
        """Get locations by category."""
        if category not in self.categories:
            logger.warning(f"Category '{category}' not found")
            return []
        
        category_locations = self.categories[category]
        return [loc for loc in self.locations if loc.name in category_locations and loc.enabled]
    
    def add_location(self, name: str, country: str = "US", priority: str = "medium", enabled: bool = True) -> bool:
        """Add a new location to the configuration."""
        try:
            # Check if location already exists
            if any(loc.name == name for loc in self.locations):
                logger.warning(f"Location '{name}' already exists")
                return False
            
            # Create new location
            new_location = Location(
                name=name,
                country=country,
                priority=priority,
                enabled=enabled
            )
            
            self.locations.append(new_location)
            logger.info(f"Added new location: {name}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding location '{name}': {e}")
            return False
    
    def remove_location(self, name: str) -> bool:
        """Remove a location from the configuration."""
        try:
            original_count = len(self.locations)
            self.locations = [loc for loc in self.locations if loc.name != name]
            
            if len(self.locations) < original_count:
                logger.info(f"Removed location: {name}")
                return True
            else:
                logger.warning(f"Location '{name}' not found")
                return False
                
        except Exception as e:
            logger.error(f"Error removing location '{name}': {e}")
            return False
    
    def enable_location(self, name: str) -> bool:
        """Enable a location."""
        for loc in self.locations:
            if loc.name == name:
                loc.enabled = True
                logger.info(f"Enabled location: {name}")
                return True
        
        logger.warning(f"Location '{name}' not found")
        return False
    
    def disable_location(self, name: str) -> bool:
        """Disable a location."""
        for loc in self.locations:
            if loc.name == name:
                loc.enabled = False
                logger.info(f"Disabled location: {name}")
                return True
        
        logger.warning(f"Location '{name}' not found")
        return False
    
    def get_location_names(self) -> List[str]:
        """Get list of all location names."""
        return [loc.name for loc in self.locations]
    
    def get_enabled_location_names(self) -> List[str]:
        """Get list of enabled location names."""
        return [loc.name for loc in self.locations if loc.enabled]
    
    def validate_location(self, name: str) -> bool:
        """Validate if a location exists and is enabled."""
        return any(loc.name == name and loc.enabled for loc in self.locations)
    
    def get_settings(self) -> Dict[str, Any]:
        """Get configuration settings."""
        return self.settings
    
    def get_categories(self) -> Dict[str, List[str]]:
        """Get location categories."""
        return self.categories
    
    def save_config(self) -> bool:
        """Save current configuration back to file."""
        try:
            config = {
                'locations': [
                    {
                        'name': loc.name,
                        'country': loc.country,
                        'priority': loc.priority,
                        'enabled': loc.enabled
                    }
                    for loc in self.locations
                ],
                'settings': self.settings,
                'categories': self.categories
            }
            
            with open(self.config_path, 'w') as file:
                yaml.dump(config, file, default_flow_style=False, sort_keys=False)
            
            logger.info("Configuration saved successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
            return False
    
    def print_status(self) -> None:
        """Print current location status."""
        print(f"\n📍 Location Manager Status")
        print(f"Total locations: {len(self.locations)}")
        print(f"Enabled locations: {len(self.get_enabled_locations())}")
        print(f"\nEnabled locations:")
        
        for loc in self.get_enabled_locations():
            print(f"  • {loc.name} ({loc.priority})")
        
        print(f"\nCategories:")
        for category, locations in self.categories.items():
            print(f"  • {category}: {len(locations)} locations")

# Convenience functions for easy access
def get_enabled_locations() -> List[str]:
    """Get list of enabled location names."""
    manager = LocationManager()
    return manager.get_enabled_location_names()

def add_location(name: str, country: str = "US", priority: str = "medium") -> bool:
    """Add a new location easily."""
    manager = LocationManager()
    success = manager.add_location(name, country, priority)
    if success:
        manager.save_config()
    return success

def remove_location(name: str) -> bool:
    """Remove a location easily."""
    manager = LocationManager()
    success = manager.remove_location(name)
    if success:
        manager.save_config()
    return success

if __name__ == "__main__":
    # Example usage
    manager = LocationManager()
    manager.print_status()
    
    # Add a new location
    print(f"\nAdding new location...")
    add_location("Phoenix, AZ", "US", "medium")
    
    # Print updated status
    manager.load_config()
    manager.print_status()
