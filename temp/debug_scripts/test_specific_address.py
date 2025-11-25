"""
Test scraping a specific address without any filters
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from homeharvest import scrape_property
import pandas as pd

ADDRESS = "4509 E 10th St, Indianapolis, IN 46201"

print("="*80)
print(f"Testing address: {ADDRESS}")
print("="*80)

# Test 1: Without listing_type filter
print("\n[TEST 1] Querying WITHOUT listing_type filter...")
try:
    df = scrape_property(location=ADDRESS, limit=10)
    if df is not None and not df.empty:
        print(f"[OK] Found {len(df)} properties")
        for idx, row in df.iterrows():
            print(f"\nProperty {idx + 1}:")
            print(f"  Address: {row.get('formatted_address', row.get('street', 'N/A'))}")
            print(f"  Status: {row.get('status', 'N/A')}")
            print(f"  MLS Status: {row.get('mls_status', 'N/A')}")
            print(f"  List Price: ${row.get('list_price', 'N/A'):,.0f}" if pd.notna(row.get('list_price')) else f"  List Price: {row.get('list_price', 'N/A')}")
            print(f"  Sold Price: ${row.get('sold_price', 'N/A'):,.0f}" if pd.notna(row.get('sold_price')) else f"  Sold Price: {row.get('sold_price', 'N/A')}")
            print(f"  MLS ID: {row.get('mls_id', 'N/A')}")
            print(f"  Property URL: {row.get('property_url', 'N/A')}")
    else:
        print("[NO] No properties found")
except Exception as e:
    print(f"[ERROR] {e}")
    import traceback
    traceback.print_exc()

# Test 2: With listing_type=None explicitly
print("\n[TEST 2] Querying with listing_type=None explicitly...")
try:
    df = scrape_property(location=ADDRESS, listing_type=None, limit=10)
    if df is not None and not df.empty:
        print(f"[OK] Found {len(df)} properties")
        for idx, row in df.iterrows():
            print(f"\nProperty {idx + 1}:")
            print(f"  Address: {row.get('formatted_address', row.get('street', 'N/A'))}")
            print(f"  Status: {row.get('status', 'N/A')}")
            print(f"  MLS Status: {row.get('mls_status', 'N/A')}")
    else:
        print("[NO] No properties found")
except Exception as e:
    print(f"[ERROR] {e}")

# Test 3: Try each listing_type
print("\n[TEST 3] Trying each listing_type...")
for listing_type in ["for_sale", "sold", "pending", "for_rent"]:
    print(f"\n  Testing listing_type='{listing_type}'...")
    try:
        df = scrape_property(location=ADDRESS, listing_type=listing_type, limit=10)
        if df is not None and not df.empty:
            print(f"    [OK] Found {len(df)} properties")
            for idx, row in df.head(1).iterrows():
                print(f"      Status: {row.get('status', 'N/A')}")
                print(f"      MLS Status: {row.get('mls_status', 'N/A')}")
        else:
            print(f"    [NO] No properties found")
    except Exception as e:
        print(f"    [ERROR] {e}")

print("\n" + "="*80)
print("Test Complete")
print("="*80)

