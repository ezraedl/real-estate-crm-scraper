#!/usr/bin/env python3
"""
Test homeharvest limitations vs Zillow data availability
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from homeharvest import scrape_property
import pandas as pd

def test_homeharvest_vs_zillow():
    """Test homeharvest limitations compared to Zillow data"""
    
    print("🔍 Testing homeharvest vs Zillow data availability...")
    print("📊 Zillow reports: 8,036 sold properties in Indianapolis (last 6 months)")
    print("📊 Homeharvest results:")
    
    # Test different time periods to see the limitation
    test_cases = [
        {"past_days": 30, "limit": 1000, "description": "Last 30 days"},
        {"past_days": 90, "limit": 1000, "description": "Last 90 days"},
        {"past_days": 180, "limit": 1000, "description": "Last 6 months"},
        {"past_days": 365, "limit": 1000, "description": "Last year"},
    ]
    
    for test_case in test_cases:
        print(f"\n🧪 Testing {test_case['description']} (limit: {test_case['limit']})...")
        try:
            df = scrape_property(
                location="Indianapolis, IN",
                listing_type="sold",
                limit=test_case['limit'],
                past_days=test_case['past_days']
            )
            print(f"   ✅ Got {len(df)} properties")
            
            if len(df) > 0:
                # Check date range
                sold_dates = pd.to_datetime(df['last_sold_date'], errors='coerce')
                min_date = sold_dates.min()
                max_date = sold_dates.max()
                print(f"   📅 Date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
                
                # Check if we hit the limit
                if len(df) == test_case['limit']:
                    print(f"   ⚠️  Hit limit of {test_case['limit']} - there may be more data available")
                else:
                    print(f"   ℹ️  Got {len(df)} out of {test_case['limit']} requested")
                    
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    # Test with different locations to see if it's location-specific
    print(f"\n🧪 Testing different location formats...")
    locations = [
        "Indianapolis, IN",
        "Marion County, IN", 
        "46201, Indianapolis, IN",
        "46202, Indianapolis, IN"
    ]
    
    for location in locations:
        try:
            df = scrape_property(
                location=location,
                listing_type="sold",
                limit=100,
                past_days=90
            )
            print(f"   📍 {location}: {len(df)} properties")
        except Exception as e:
            print(f"   📍 {location}: Error - {e}")
    
    # Test with different parameters
    print(f"\n🧪 Testing different scraping parameters...")
    param_tests = [
        {"mls_only": True, "description": "MLS only"},
        {"mls_only": False, "description": "All sources"},
        {"foreclosure": False, "description": "Exclude foreclosures"},
        {"foreclosure": True, "description": "Include foreclosures"},
    ]
    
    for param_test in param_tests:
        try:
            df = scrape_property(
                location="Indianapolis, IN",
                listing_type="sold",
                limit=100,
                past_days=90,
                **{k: v for k, v in param_test.items() if k != 'description'}
            )
            print(f"   🔧 {param_test['description']}: {len(df)} properties")
        except Exception as e:
            print(f"   🔧 {param_test['description']}: Error - {e}")

if __name__ == "__main__":
    test_homeharvest_vs_zillow()
