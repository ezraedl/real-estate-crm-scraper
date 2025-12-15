"""
Test script to debug single address scraping returning 0 results.
"""
import asyncio
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from homeharvest import scrape_property
import pandas as pd

def test_address_scrape(address: str):
    """Test scraping a single address with different parameters"""
    print(f"\n{'='*80}")
    print(f"Testing address scraping for: {address}")
    print(f"{'='*80}\n")
    
    # Test 1: Basic scrape without listing_type
    print("[TEST 1] Basic scrape without listing_type filter...")
    try:
        df1 = scrape_property(location=address, limit=10)
        print(f"  Result: {len(df1)} properties found")
        if not df1.empty:
            print(f"  Columns: {list(df1.columns)}")
            print(f"  First property address: {df1.iloc[0].get('address', 'N/A') if len(df1) > 0 else 'N/A'}")
    except Exception as e:
        print(f"  ERROR: {e}")
    
    # Test 2: With listing_type=None explicitly
    print("\n[TEST 2] With listing_type=None explicitly...")
    try:
        df2 = scrape_property(location=address, listing_type=None, limit=10)
        print(f"  Result: {len(df2)} properties found")
        if not df2.empty:
            print(f"  Columns: {list(df2.columns)}")
    except Exception as e:
        print(f"  ERROR: {e}")
    
    # Test 3: Try each listing type individually
    print("\n[TEST 3] Testing each listing type individually...")
    listing_types = ["for_sale", "sold", "pending", "for_rent"]
    for listing_type in listing_types:
        try:
            df = scrape_property(location=address, listing_type=listing_type, limit=10)
            print(f"  {listing_type}: {len(df)} properties found")
            if not df.empty:
                print(f"    First address: {df.iloc[0].get('address', 'N/A')}")
        except Exception as e:
            print(f"  {listing_type}: ERROR - {e}")
    
    # Test 4: Try with all listing types as a list
    print("\n[TEST 4] With listing_type as list of all types...")
    try:
        df4 = scrape_property(location=address, listing_type=listing_types, limit=10)
        print(f"  Result: {len(df4)} properties found")
        if not df4.empty:
            print(f"  Columns: {list(df4.columns)}")
            if 'listing_type' in df4.columns:
                print(f"  Listing types found: {df4['listing_type'].unique().tolist()}")
    except Exception as e:
        print(f"  ERROR: {e}")
    
    # Test 5: Try with just zip code if address contains one
    import re
    zip_match = re.search(r'\b(\d{5})\b', address)
    if zip_match:
        zip_code = zip_match.group(1)
        print(f"\n[TEST 5] Trying with just zip code: {zip_code}...")
        try:
            df5 = scrape_property(location=zip_code, limit=10)
            print(f"  Result: {len(df5)} properties found")
            if not df5.empty:
                print(f"  First property address: {df5.iloc[0].get('address', 'N/A')}")
        except Exception as e:
            print(f"  ERROR: {e}")
    
    # Test 6: Try with city, state if address contains it
    city_state_match = re.search(r'^(.+?),\s*([A-Z]{2})', address)
    if city_state_match:
        city = city_state_match.group(1).strip()
        state = city_state_match.group(2)
        city_state = f"{city}, {state}"
        print(f"\n[TEST 6] Trying with city, state: {city_state}...")
        try:
            df6 = scrape_property(location=city_state, limit=10)
            print(f"  Result: {len(df6)} properties found")
            if not df6.empty:
                print(f"  First property address: {df6.iloc[0].get('address', 'N/A')}")
        except Exception as e:
            print(f"  ERROR: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        address = sys.argv[1]
    else:
        # Default test address
        address = input("Enter address to test (or press Enter for default): ").strip()
        if not address:
            address = "123 Main St, Indianapolis, IN 46201"
            print(f"Using default address: {address}")
    
    test_address_scrape(address)



