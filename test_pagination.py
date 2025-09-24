#!/usr/bin/env python3
"""
Test pagination and higher limits with homeharvest
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from homeharvest import scrape_property
import pandas as pd

def test_pagination():
    """Test pagination and higher limits"""
    
    print("🔍 Testing pagination and higher limits...")
    
    # Test 1: Try higher limits
    print("\n1. Testing higher limits...")
    limits_to_test = [1000, 2000, 5000, 10000]
    
    for limit in limits_to_test:
        print(f"\n   Testing limit: {limit}")
        try:
            df = scrape_property(
                location="Indianapolis, IN",
                listing_type="sold",
                limit=limit,
                past_days=180  # 6 months
            )
            print(f"   ✅ Got {len(df)} properties (requested: {limit})")
            
            if len(df) == limit:
                print(f"   ⚠️  Hit the limit - there may be more data")
            else:
                print(f"   ℹ️  Got {len(df)} out of {limit} requested")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    # Test 2: Try different date ranges to see if it's time-based
    print(f"\n2. Testing different date ranges...")
    date_ranges = [
        {"past_days": 30, "description": "Last 30 days"},
        {"past_days": 90, "description": "Last 90 days"},
        {"past_days": 180, "description": "Last 6 months"},
        {"past_days": 365, "description": "Last year"},
    ]
    
    for date_range in date_ranges:
        print(f"\n   Testing {date_range['description']}...")
        try:
            df = scrape_property(
                location="Indianapolis, IN",
                listing_type="sold",
                limit=5000,  # High limit
                past_days=date_range['past_days']
            )
            print(f"   ✅ Got {len(df)} properties")
            
            if len(df) > 0:
                sold_dates = pd.to_datetime(df['last_sold_date'], errors='coerce')
                min_date = sold_dates.min()
                max_date = sold_dates.max()
                print(f"   📅 Date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    # Test 3: Try with different parameters
    print(f"\n3. Testing different parameter combinations...")
    param_combinations = [
        {"mls_only": False, "description": "All sources"},
        {"mls_only": True, "description": "MLS only"},
        {"foreclosure": False, "description": "Exclude foreclosures"},
        {"exclude_pending": True, "description": "Exclude pending"},
    ]
    
    for params in param_combinations:
        print(f"\n   Testing {params['description']}...")
        try:
            df = scrape_property(
                location="Indianapolis, IN",
                listing_type="sold",
                limit=2000,
                past_days=180,
                **{k: v for k, v in params.items() if k != 'description'}
            )
            print(f"   ✅ Got {len(df)} properties")
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    # Test 4: Try with date_from and date_to instead of past_days
    print(f"\n4. Testing date_from and date_to parameters...")
    try:
        # Last 6 months using specific dates
        six_months_ago = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
        today = datetime.now().strftime('%Y-%m-%d')
        
        df = scrape_property(
            location="Indianapolis, IN",
            listing_type="sold",
            limit=5000,
            date_from=six_months_ago,
            date_to=today
        )
        print(f"   ✅ Got {len(df)} properties using date_from/date_to")
        
        if len(df) > 0:
            sold_dates = pd.to_datetime(df['last_sold_date'], errors='coerce')
            min_date = sold_dates.min()
            max_date = sold_dates.max()
            print(f"   📅 Date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")

if __name__ == "__main__":
    test_pagination()
