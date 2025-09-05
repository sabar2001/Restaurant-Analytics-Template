#!/usr/bin/env python3
"""
Location Management CLI Tool
Easy way to add, remove, and manage restaurant data collection locations.
"""

import sys
import os
import argparse
from pathlib import Path

# Add the parent directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from ingestion.location_manager import LocationManager, add_location, remove_location

def main():
    parser = argparse.ArgumentParser(description="Manage restaurant data collection locations")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Add location command
    add_parser = subparsers.add_parser('add', help='Add a new location')
    add_parser.add_argument('name', help='Location name (e.g., "Phoenix, AZ")')
    add_parser.add_argument('--country', default='US', help='Country code (default: US)')
    add_parser.add_argument('--priority', choices=['high', 'medium', 'low'], 
                           default='medium', help='Priority level (default: medium)')
    
    # Remove location command
    remove_parser = subparsers.add_parser('remove', help='Remove a location')
    remove_parser.add_argument('name', help='Location name to remove')
    
    # List locations command
    list_parser = subparsers.add_parser('list', help='List all locations')
    list_parser.add_argument('--enabled-only', action='store_true', 
                            help='Show only enabled locations')
    list_parser.add_argument('--priority', choices=['high', 'medium', 'low'],
                            help='Filter by priority')
    
    # Enable/disable commands
    enable_parser = subparsers.add_parser('enable', help='Enable a location')
    enable_parser.add_argument('name', help='Location name to enable')
    
    disable_parser = subparsers.add_parser('disable', help='Disable a location')
    disable_parser.add_argument('name', help='Location name to disable')
    
    # Status command
    subparsers.add_parser('status', help='Show location manager status')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    manager = LocationManager()
    
    if args.command == 'add':
        success = add_location(args.name, args.country, args.priority)
        if success:
            print(f"✅ Successfully added location: {args.name}")
        else:
            print(f"❌ Failed to add location: {args.name}")
    
    elif args.command == 'remove':
        success = remove_location(args.name)
        if success:
            print(f"✅ Successfully removed location: {args.name}")
        else:
            print(f"❌ Failed to remove location: {args.name}")
    
    elif args.command == 'list':
        locations = manager.locations
        
        if args.priority:
            locations = [loc for loc in locations if loc.priority == args.priority]
        
        if args.enabled_only:
            locations = [loc for loc in locations if loc.enabled]
        
        if not locations:
            print("No locations found matching criteria.")
            return
        
        print(f"\n📍 Locations ({len(locations)} total):")
        print("-" * 50)
        
        for loc in locations:
            status = "✅" if loc.enabled else "❌"
            print(f"{status} {loc.name} ({loc.priority}) - {loc.country}")
    
    elif args.command == 'enable':
        success = manager.enable_location(args.name)
        if success:
            manager.save_config()
            print(f"✅ Successfully enabled location: {args.name}")
        else:
            print(f"❌ Failed to enable location: {args.name}")
    
    elif args.command == 'disable':
        success = manager.disable_location(args.name)
        if success:
            manager.save_config()
            print(f"✅ Successfully disabled location: {args.name}")
        else:
            print(f"❌ Failed to disable location: {args.name}")
    
    elif args.command == 'status':
        manager.print_status()

if __name__ == "__main__":
    main()
