"""
Script to inspect a specific property and show all price-related data.
"""

import asyncio
import os
import sys
import json
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import settings


async def inspect_property(property_id: str):
    """Inspect a specific property and show all price-related data"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        print("=" * 80)
        print(f"Inspecting property: {property_id}")
        print("=" * 80)
        print("")
        
        # Get the full property document
        property_doc = await db['properties'].find_one(
            {"property_id": property_id}
        )
        
        if not property_doc:
            print(f"Property {property_id} not found")
            return
        
        print("PROPERTY BASIC INFO:")
        print("-" * 80)
        print(f"Property ID: {property_doc.get('property_id')}")
        print(f"MLS ID: {property_doc.get('mls_id')}")
        print(f"Address: {property_doc.get('address', {}).get('formatted_address', 'N/A')}")
        print(f"Status: {property_doc.get('status')}")
        print("")
        
        # Financial info
        financial = property_doc.get("financial", {})
        print("FINANCIAL DATA:")
        print("-" * 80)
        print(f"Current List Price: ${financial.get('list_price', 'N/A')}")
        print(f"Original List Price: ${financial.get('original_list_price', 'N/A')}")
        print(f"Price Per Sqft: ${financial.get('price_per_sqft', 'N/A')}")
        
        if financial.get('original_list_price') and financial.get('list_price'):
            orig = financial.get('original_list_price')
            current = financial.get('list_price')
            if orig != current:
                reduction = orig - current
                reduction_pct = (reduction / orig * 100) if orig > 0 else 0
                print(f"Price Difference: ${reduction:,.0f} ({reduction_pct:.2f}%)")
            else:
                print("Price Difference: No difference between original and current")
        print("")
        
        # change_logs
        change_logs = property_doc.get("change_logs", [])
        print(f"CHANGE_LOGS ARRAY:")
        print("-" * 80)
        print(f"Number of entries: {len(change_logs)}")
        if change_logs:
            print("")
            for i, log in enumerate(change_logs, 1):
                print(f"Entry #{i}:")
                field = log.get("field")
                print(f"  Field: {field}")
                print(f"  Change Type: {log.get('change_type', 'N/A')}")
                print(f"  Old Value: {log.get('old_value', 'N/A')}")
                print(f"  New Value: {log.get('new_value', 'N/A')}")
                timestamp = log.get("timestamp")
                if isinstance(timestamp, datetime):
                    timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                print(f"  Timestamp: {timestamp}")
                
                # Check for consolidated format
                if field is None and isinstance(log.get("field_changes"), dict):
                    print(f"  Format: CONSOLIDATED (multiple fields)")
                    field_changes = log.get("field_changes", {})
                    print(f"  Fields in this entry: {list(field_changes.keys())}")
                    if "financial.list_price" in field_changes:
                        change = field_changes["financial.list_price"]
                        print(f"    financial.list_price:")
                        print(f"      Old Value: {change.get('old_value', 'N/A')}")
                        print(f"      New Value: {change.get('new_value', 'N/A')}")
                        print(f"      Change Type: {change.get('change_type', 'N/A')}")
                else:
                    print(f"  Format: SIMPLE (single field)")
                print("")
        else:
            print("  No change_logs entries found")
            print("")
        
        # Check for old price_history_summary
        enrichment = property_doc.get("enrichment", {})
        price_history_summary = enrichment.get("price_history_summary")
        if price_history_summary:
            print("OLD FORMAT: enrichment.price_history_summary (should be removed):")
            print("-" * 80)
            print(json.dumps(price_history_summary, indent=2, default=str))
            print("")
        
        # Flags
        print("FLAGS:")
        print("-" * 80)
        print(f"has_price_reduction: {property_doc.get('has_price_reduction', False)}")
        print(f"is_motivated_seller: {property_doc.get('is_motivated_seller', False)}")
        print(f"has_distress_signals: {property_doc.get('has_distress_signals', False)}")
        print("")
        
        # Enrichment version
        enrichment_version = enrichment.get("enrichment_version", "N/A")
        print(f"Enrichment Version: {enrichment_version}")
        print("")
        
        # Check for old property_history collection (if exists)
        history_entry = await db['property_history'].find_one(
            {"property_id": property_id, "change_type": "price_change"}
        )
        if history_entry:
            print("OLD COLLECTION: property_history entry found:")
            print("-" * 80)
            print(json.dumps(history_entry, indent=2, default=str))
            print("")
        
        # Check for old property_change_logs collection (if exists)
        change_log_entry = await db['property_change_logs'].find_one(
            {"property_id": property_id, "field": "financial.list_price"}
        )
        if change_log_entry:
            print("OLD COLLECTION: property_change_logs entry found:")
            print("-" * 80)
            print(json.dumps(change_log_entry, indent=2, default=str))
            print("")
        
        print("=" * 80)
        print("SUMMARY:")
        print("=" * 80)
        if not change_logs:
            print("No change_logs entries found in the embedded array")
            if price_history_summary:
                print("OLD FORMAT: price_history_summary exists (should be migrated)")
            if history_entry:
                print("OLD COLLECTION: property_history has entries (should be migrated)")
            if change_log_entry:
                print("OLD COLLECTION: property_change_logs has entries (should be migrated)")
            print("")
            print("Possible reasons:")
            print("1. Property was enriched before change_logs tracking was implemented")
            print("2. Migration script hasn't run yet")
            print("3. New enrichments haven't detected price changes yet")
        else:
            price_changes_found = any(
                log.get("field") == "financial.list_price" or 
                (log.get("field") is None and "financial.list_price" in log.get("field_changes", {}))
                for log in change_logs
            )
            if price_changes_found:
                print("✅ Price changes found in change_logs")
            else:
                print("⚠️  change_logs exists but no price changes found")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()


if __name__ == "__main__":
    import sys
    property_id = sys.argv[1] if len(sys.argv) > 1 else "4034412049"
    asyncio.run(inspect_property(property_id))

