"""
Test script to analyze what statuses HomeHarvest returns for properties.

This script:
1. Queries HomeHarvest for properties with different listing_types
2. Analyzes all status values returned
3. Checks if off-market properties still exist in HomeHarvest
4. Tests querying specific properties by address
"""

import sys
import os
from collections import Counter
import pandas as pd

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from homeharvest import scrape_property

# Test location - use a location you know has properties
TEST_LOCATION = "Indianapolis, IN"
TEST_ZIP = "46201"  # You can change this to a specific area

def analyze_statuses(df, listing_type):
    """Analyze status values in a DataFrame"""
    if df is None or df.empty:
        print(f"   No properties found for {listing_type}")
        return
    
    print(f"\n   Found {len(df)} properties for listing_type='{listing_type}'")
    
    # Check what status columns exist
    status_cols = [col for col in df.columns if 'status' in col.lower()]
    print(f"   Status-related columns: {status_cols}")
    
    # Analyze each status column
    for col in status_cols:
        if col in df.columns:
            non_null = df[col].dropna()
            if len(non_null) > 0:
                unique_statuses = non_null.unique()
                status_counts = Counter(non_null)
                print(f"\n   Column '{col}':")
                print(f"      Unique values ({len(unique_statuses)}): {sorted(unique_statuses)}")
                print(f"      Top 10 most common:")
                for status, count in status_counts.most_common(10):
                    print(f"         {status}: {count}")
    
    # Check for off-market indicators
    print(f"\n   Checking for off-market indicators:")
    off_market_keywords = ['withdrawn', 'expired', 'cancelled', 'off_market', 'off-market', 'inactive', 'delisted']
    
    for col in status_cols:
        if col in df.columns:
            for keyword in off_market_keywords:
                matches = df[col].astype(str).str.contains(keyword, case=False, na=False)
                if matches.any():
                    count = matches.sum()
                    print(f"      Found '{keyword}' in {col}: {count} properties")
                    # Show examples
                    examples = df[matches].head(3)
                    for idx, row in examples.iterrows():
                        address = row.get('formatted_address', row.get('street', 'N/A'))
                        status_val = row.get(col, 'N/A')
                        print(f"         Example: {address} - {col}={status_val}")

def test_listing_types():
    """Test different listing types and analyze statuses"""
    print("="*80)
    print("TEST 1: Analyzing statuses for different listing_types")
    print("="*80)
    
    listing_types = ["for_sale", "sold", "pending", "for_rent"]
    
    for listing_type in listing_types:
        print(f"\n{'='*80}")
        print(f"Testing listing_type='{listing_type}'")
        print(f"{'='*80}")
        
        try:
            df = scrape_property(
                location=TEST_LOCATION,
                listing_type=listing_type,
                limit=100  # Get a sample
            )
            analyze_statuses(df, listing_type)
        except Exception as e:
            print(f"   ERROR: {e}")
            import traceback
            traceback.print_exc()

def test_specific_property():
    """Test querying a specific property by address"""
    print("\n" + "="*80)
    print("TEST 2: Querying specific property by address")
    print("="*80)
    
    # Try a specific address - you can modify this
    test_addresses = [
        "4509 E 10th St, Indianapolis, IN 46201",
        "2916 E North St, Indianapolis, IN, 46201",
        # Add more addresses you know exist
    ]
    
    for address in test_addresses:
        print(f"\nTesting address: {address}")
        
        # Try different listing types
        for listing_type in ["for_sale", "sold", "pending", None]:
            try:
                params = {"location": address}
                if listing_type:
                    params["listing_type"] = listing_type
                
                print(f"   Querying with listing_type={listing_type}...")
                df = scrape_property(**params, limit=10)
                
                if df is not None and not df.empty:
                    print(f"   [OK] Found {len(df)} properties")
                    for idx, row in df.iterrows():
                        found_address = row.get('formatted_address', row.get('street', 'N/A'))
                        status = row.get('status', 'N/A')
                        mls_status = row.get('mls_status', 'N/A')
                        listing_type_found = row.get('listing_type', 'N/A')
                        print(f"      Address: {found_address}")
                        print(f"      status: {status}")
                        print(f"      mls_status: {mls_status}")
                        print(f"      listing_type: {listing_type_found}")
                else:
                    print(f"   [NO] No properties found")
            except Exception as e:
                print(f"   ERROR: {e}")

def test_all_statuses_in_location():
    """Get all properties from a location and analyze all statuses"""
    print("\n" + "="*80)
    print("TEST 3: Comprehensive status analysis for a location")
    print("="*80)
    
    print(f"\nQuerying {TEST_LOCATION} without listing_type filter...")
    try:
        # Try without listing_type to see if we get all properties
        df = scrape_property(
            location=TEST_LOCATION,
            limit=200
        )
        
        if df is not None and not df.empty:
            print(f"[OK] Found {len(df)} properties")
            analyze_statuses(df, "all_types")
        else:
            print("[NO] No properties found without listing_type filter")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

def check_status_field_details():
    """Detailed analysis of status field values"""
    print("\n" + "="*80)
    print("TEST 4: Detailed status field analysis")
    print("="*80)
    
    print(f"\nQuerying {TEST_LOCATION} with listing_type='for_sale'...")
    try:
        df = scrape_property(
            location=TEST_LOCATION,
            listing_type="for_sale",
            limit=500  # Get more to see variety
        )
        
        if df is not None and not df.empty:
            print(f"[OK] Found {len(df)} properties")
            
            # Check all columns
            print(f"\nAll columns in DataFrame:")
            for col in df.columns:
                print(f"   - {col}")
            
            # Detailed status analysis
            if 'status' in df.columns:
                print(f"\nDetailed 'status' field analysis:")
                status_series = df['status'].dropna()
                if len(status_series) > 0:
                    print(f"   Total non-null values: {len(status_series)}")
                    print(f"   Unique statuses: {sorted(status_series.unique())}")
                    print(f"   Value counts:")
                    for status, count in status_series.value_counts().items():
                        print(f"      {status}: {count}")
            
            if 'mls_status' in df.columns:
                print(f"\nDetailed 'mls_status' field analysis:")
                mls_status_series = df['mls_status'].dropna()
                if len(mls_status_series) > 0:
                    print(f"   Total non-null values: {len(mls_status_series)}")
                    print(f"   Unique mls_statuses: {sorted(mls_status_series.unique())}")
                    print(f"   Value counts:")
                    for status, count in mls_status_series.value_counts().items():
                        print(f"      {status}: {count}")
            
            # Check for properties that might be off-market
            print(f"\nChecking for potential off-market properties:")
            if 'status' in df.columns:
                off_market_patterns = ['withdrawn', 'expired', 'cancelled', 'off', 'inactive']
                for pattern in off_market_patterns:
                    matches = df['status'].astype(str).str.contains(pattern, case=False, na=False)
                    if matches.any():
                        count = matches.sum()
                        print(f"   Found '{pattern}' in status: {count} properties")
                        examples = df[matches][['formatted_address', 'status', 'mls_status']].head(5)
                        print(f"   Examples:")
                        for idx, row in examples.iterrows():
                            print(f"      {row.get('formatted_address', 'N/A')}: status={row.get('status', 'N/A')}, mls_status={row.get('mls_status', 'N/A')}")
        else:
            print("[NO] No properties found")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("="*80)
    print("HomeHarvest Status Analysis Test")
    print("="*80)
    print(f"\nTest Location: {TEST_LOCATION}")
    print(f"Test ZIP: {TEST_ZIP}")
    
    # Run all tests
    test_listing_types()
    test_specific_property()
    test_all_statuses_in_location()
    check_status_field_details()
    
    print("\n" + "="*80)
    print("Test Complete")
    print("="*80)

