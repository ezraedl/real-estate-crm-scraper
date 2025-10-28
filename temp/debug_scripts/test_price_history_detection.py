"""
Debug script to test price history detection for a specific property.

This script:
1. Fetches the existing property from the database
2. Manually scrapes the property using homeharvest to get raw data
3. Shows the raw scraped data before any processing
4. Tests the change detection logic
5. Identifies if the issue is in scraping or change detection
"""

import asyncio
import sys
import os
from datetime import datetime
import json
from bson import ObjectId

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from homeharvest import scrape_property
import pandas as pd
from database import db
from models import Property
from services.property_differ import PropertyDiffer
from services.history_tracker import HistoryTracker

PROPERTY_ADDRESS = "2916 E North St, Indianapolis, IN, 46201"
PROPERTY_ID = "4915688430"
MLS_PROPERTY_ID = "68cbbf8023d999782e70023b"  # MongoDB _id

async def fetch_existing_property():
    """Fetch the existing property from database"""
    print("\n" + "="*80)
    print("STEP 1: Fetching existing property from database")
    print("="*80)
    
    # Try fetching by property_id first
    existing = await db.properties_collection.find_one({"property_id": PROPERTY_ID})
    
    # If not found, try by MongoDB _id
    if not existing:
        try:
            existing = await db.properties_collection.find_one({"_id": ObjectId(MLS_PROPERTY_ID)})
        except:
            pass
    
    # If still not found, try by address
    if not existing:
        existing = await db.properties_collection.find_one({
            "address.formatted_address": {"$regex": PROPERTY_ADDRESS, "$options": "i"}
        })
    
    if not existing:
        print(f"[ERROR] Property not found in database")
        return None
    
    print(f"[OK] Found property in database:")
    print(f"   Property ID: {existing.get('property_id')}")
    print(f"   MLS ID: {existing.get('mls_id')}")
    print(f"   Address: {existing.get('address', {}).get('formatted_address')}")
    print(f"   Current List Price: ${existing.get('financial', {}).get('list_price', 'N/A'):,}")
    print(f"   Content Hash: {existing.get('content_hash', 'N/A')[:32]}...")
    print(f"   Last Scraped: {existing.get('scraped_at', 'N/A')}")
    print(f"   Last Content Updated: {existing.get('last_content_updated', 'N/A')}")
    
    # Check enrichment data
    enrichment = existing.get('enrichment', {})
    if enrichment:
        print(f"\n   Enrichment Data:")
        print(f"   - Has Price Reduction: {existing.get('has_price_reduction', False)}")
        print(f"   - Price Changes Count: {enrichment.get('change_summary', {}).get('price_changes_count', 0)}")
        print(f"   - Motivated Seller Score: {enrichment.get('motivated_seller', {}).get('score', 0)}")
    
    # Check price history
    history_tracker = HistoryTracker(db.db)
    price_history = await history_tracker.get_price_history(existing.get('property_id'), limit=10)
    print(f"\n   Price History Entries: {len(price_history)}")
    for entry in price_history[:5]:
        data = entry.get('data', {})
        print(f"   - {entry.get('timestamp')}: ${data.get('old_price', 'N/A'):,} -> ${data.get('new_price', 'N/A'):,} ({data.get('percent_change', 0):.2f}%)")
    
    return existing

def scrape_property_raw(location: str):
    """Manually scrape property using homeharvest and return raw data"""
    print("\n" + "="*80)
    print("STEP 2: Scraping property using homeharvest (raw data)")
    print("="*80)
    
    try:
        print(f"Scraping location: {location}")
        print(f"Listing type: for_sale")
        
        # Scrape using homeharvest
        properties_df = scrape_property(
            location=location,
            listing_type="for_sale",
            mls_only=False,
            limit=100
        )
        
        if properties_df is None or len(properties_df) == 0:
            print("[ERROR] No properties found in scraped data")
            return None
        
        print(f"[OK] Found {len(properties_df)} properties")
        
        # Find the specific property
        target_property = None
        for idx, row in properties_df.iterrows():
            address = str(row.get('formatted_address', '')).lower()
            property_id = str(row.get('property_id', ''))
            
            if PROPERTY_ADDRESS.lower() in address or property_id == PROPERTY_ID:
                target_property = row
                print(f"\n[OK] Found target property at index {idx}")
                break
        
        if target_property is None:
            print(f"[ERROR] Target property not found in scraped data")
            print(f"\nAvailable property IDs:")
            for idx, row in properties_df.iterrows()[:10]:
                print(f"  - {row.get('property_id')}: {row.get('formatted_address')}")
            return None
        
        print("\n[RAW DATA] RAW SCRAPED DATA (before any processing):")
        print("-" * 80)
        
        # Show key fields
        key_fields = [
            'property_id', 'mls_id', 'list_price', 'status', 'mls_status',
            'formatted_address', 'days_on_mls', 'list_date'
        ]
        
        raw_data = {}
        for field in key_fields:
            value = target_property.get(field)
            raw_data[field] = value
            print(f"  {field:25s}: {value}")
        
        # Show all available fields
        print(f"\n[ALL FIELDS] ALL AVAILABLE FIELDS IN RAW DATA:")
        print("-" * 80)
        for field in target_property.index:
            value = target_property.get(field)
            # Only show non-null values and limit length
            try:
                is_notna = pd.notna(value) if hasattr(value, '__iter__') and not isinstance(value, str) else value is not None
                if is_notna and value is not None:
                    value_str = str(value)
                    if len(value_str) > 100:
                        value_str = value_str[:100] + "..."
                    print(f"  {field:30s}: {value_str}")
            except:
                pass
        
        # Convert to dict for easier handling
        property_dict = target_property.to_dict()
        return property_dict
        
    except Exception as e:
        print(f"[ERROR] Error scraping property: {e}")
        import traceback
        traceback.print_exc()
        return None

async def test_change_detection(existing_property, scraped_data):
    """Test the change detection logic"""
    print("\n" + "="*80)
    print("STEP 3: Testing change detection logic")
    print("="*80)
    
    if not existing_property or not scraped_data:
        print("[ERROR] Cannot test change detection - missing data")
        return
    
    # Convert scraped data to Property model
    print("\n[CONVERTING] Converting scraped data to Property model...")
    from scraper import MLSScraper
    scraper = MLSScraper()
    
    try:
        # Convert pandas Series to Property model
        property_obj = scraper.convert_to_property_model(
            pd.Series(scraped_data),
            job_id="debug_test",
            listing_type="for_sale"
        )
        
        print(f"[OK] Converted to Property model")
        print(f"   New List Price: ${property_obj.financial.list_price:,}")
        print(f"   New Content Hash: {property_obj.content_hash[:32]}...")
        
        # Get old price and hash
        old_price = existing_property.get('financial', {}).get('list_price')
        new_price = property_obj.financial.list_price
        old_hash = existing_property.get('content_hash')
        new_hash = property_obj.content_hash
        
        print(f"\n[PRICE COMPARISON] PRICE COMPARISON:")
        print(f"   Old Price: ${old_price:,}")
        print(f"   New Price: ${new_price:,}")
        print(f"   Difference: ${new_price - old_price:,}" if (old_price and new_price) else "   Difference: N/A")
        print(f"   Price Changed: {old_price != new_price if (old_price and new_price) else 'Unknown'}")
        
        print(f"\n[HASH COMPARISON] CONTENT HASH COMPARISON:")
        print(f"   Old Hash: {old_hash[:32] if old_hash else 'N/A'}...")
        print(f"   New Hash: {new_hash[:32] if new_hash else 'N/A'}...")
        print(f"   Hash Changed: {old_hash != new_hash if (old_hash and new_hash) else 'Unknown'}")
        
        # Test PropertyDiffer
        print(f"\n[TESTING] TESTING PROPERTY DIFFER:")
        property_differ = PropertyDiffer()
        property_dict = property_obj.dict(by_alias=True, exclude={"id"})
        
        changes = property_differ.detect_changes(existing_property, property_dict)
        
        print(f"   Has Changes: {changes.get('has_changes', False)}")
        print(f"   Total Changes: {changes.get('summary', {}).get('total_changes', 0)}")
        print(f"   Price Changes Count: {changes.get('summary', {}).get('price_changes_count', 0)}")
        
        if changes.get('price_changes'):
            print(f"\n   [PRICE CHANGES] PRICE CHANGES DETECTED:")
            for change in changes.get('price_changes', []):
                print(f"      Field: {change.get('field')}")
                print(f"      Old Value: ${change.get('old_value'):,}" if isinstance(change.get('old_value'), (int, float)) else f"      Old Value: {change.get('old_value')}")
                print(f"      New Value: ${change.get('new_value'):,}" if isinstance(change.get('new_value'), (int, float)) else f"      New Value: {change.get('new_value')}")
                print(f"      Change Type: {change.get('change_type')}")
                print()
            
            # Get price change summary
            price_summary = property_differ.get_price_change_summary(changes)
            if price_summary:
                print(f"   [PRICE SUMMARY] PRICE CHANGE SUMMARY:")
                print(f"      Old Price: ${price_summary.get('old_price'):,}")
                print(f"      New Price: ${price_summary.get('new_price'):,}")
                print(f"      Difference: ${price_summary.get('price_difference'):,}")
                print(f"      Percent Change: {price_summary.get('percent_change', 0):.2f}%")
                print(f"      Change Type: {price_summary.get('change_type')}")
        else:
            print(f"   [WARNING] No price changes detected!")
        
        # Check what fields changed
        if changes.get('field_changes'):
            print(f"\n   [ALL CHANGES] ALL FIELD CHANGES:")
            for change in changes.get('field_changes', []):
                old_val = change.get('old_value')
                new_val = change.get('new_value')
                if isinstance(old_val, (int, float)) and isinstance(new_val, (int, float)):
                    old_val_str = f"${old_val:,}"
                    new_val_str = f"${new_val:,}"
                else:
                    old_val_str = str(old_val)[:50] if old_val else "None"
                    new_val_str = str(new_val)[:50] if new_val else "None"
                print(f"      {change.get('field')}: {old_val_str} -> {new_val_str}")
        
        return changes
        
    except Exception as e:
        print(f"[ERROR] Error testing change detection: {e}")
        import traceback
        traceback.print_exc()
        return None

async def main():
    """Main function"""
    print("\n" + "="*80)
    print("PRICE HISTORY DETECTION DEBUG SCRIPT")
    print("="*80)
    print(f"Property: {PROPERTY_ADDRESS}")
    print(f"Property ID: {PROPERTY_ID}")
    print(f"MongoDB ID: {MLS_PROPERTY_ID}")
    print("="*80)
    
    # Connect to database
    print("\nConnecting to database...")
    connected = await db.connect()
    if not connected:
        print("[ERROR] Failed to connect to database")
        return
    
    print("[OK] Connected to database")
    
    # Step 1: Fetch existing property
    existing_property = await fetch_existing_property()
    
    # Step 2: Scrape property raw
    scraped_data = scrape_property_raw(PROPERTY_ADDRESS)
    
    # Step 3: Test change detection
    if existing_property and scraped_data:
        changes = await test_change_detection(existing_property, scraped_data)
        
        # Summary
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        
        if changes and changes.get('price_changes'):
            print("[OK] Price change WAS detected by PropertyDiffer")
            print("   -> Issue is likely NOT in change detection logic")
            print("   -> Issue might be in:")
            print("     1. Content hash not changing (scraped price same as stored)")
            print("     2. Enrichment not running")
            print("     3. History not being recorded")
        else:
            print("[ERROR] Price change was NOT detected by PropertyDiffer")
            print("   -> Possible issues:")
            print("     1. Scraped price is same as stored price (no actual change)")
            print("     2. Field name mismatch (financial.list_price vs something else)")
            print("     3. Data type mismatch (int vs float)")
            print("     4. PropertyDiffer not comparing the right fields")
        
        old_price = existing_property.get('financial', {}).get('list_price')
        new_price = scraped_data.get('list_price')
        
        if old_price and new_price and old_price != new_price:
            print(f"\n[IMPORTANT] Price difference exists in raw data!")
            print(f"   Old: ${old_price:,}")
            print(f"   New: ${new_price:,}")
            print(f"   -> This means the scraper IS getting the updated price")
            print(f"   -> But change detection or history recording is failing")
        elif old_price and new_price and old_price == new_price:
            print(f"\n[IMPORTANT] Price is the same in scraped data!")
            print(f"   Both: ${old_price:,}")
            print(f"   -> This means the scraper is NOT getting the updated price")
            print(f"   -> Issue is in the scraping process, not change detection")
    
    await db.disconnect()
    print("\n[OK] Done")

if __name__ == "__main__":
    asyncio.run(main())

