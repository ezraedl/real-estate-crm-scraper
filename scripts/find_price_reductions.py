"""
Script to find properties with actual price reductions in change_logs.
"""

import asyncio
import os
import sys
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import settings


async def find_price_reductions():
    """Find properties with actual price reductions in change_logs"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        print("=" * 80)
        print("Finding properties with price reductions in change_logs...")
        print("=" * 80)
        print("")
        
        # Get all properties with change_logs
        properties_cursor = db['properties'].find(
            {
                "change_logs": {"$exists": True, "$ne": []}
            },
            {
                "property_id": 1,
                "mls_id": 1,
                "address": 1,
                "financial.list_price": 1,
                "financial.original_list_price": 1,
                "status": 1,
                "change_logs": 1,
                "enrichment.motivated_seller.score": 1
            }
        ).limit(50)
        
        properties_with_reductions = []
        
        async for prop in properties_cursor:
            change_logs = prop.get("change_logs", [])
            
            # Check for price reductions
            has_price_reduction = False
            price_reductions = []
            
            for log in change_logs:
                # Simple format
                if log.get("field") == "financial.list_price":
                    change_type = log.get("change_type")
                    if change_type == "decreased":
                        old_val = log.get("old_value")
                        new_val = log.get("new_value")
                        if old_val and new_val and old_val > new_val:
                            has_price_reduction = True
                            reduction = old_val - new_val
                            reduction_pct = (reduction / old_val * 100) if old_val > 0 else 0
                            timestamp = log.get("timestamp", "N/A")
                            price_reductions.append({
                                "old_value": old_val,
                                "new_value": new_val,
                                "reduction": reduction,
                                "reduction_pct": reduction_pct,
                                "timestamp": timestamp
                            })
                
                # Consolidated format
                elif log.get("field") is None and isinstance(log.get("field_changes"), dict):
                    field_changes = log.get("field_changes", {})
                    if "financial.list_price" in field_changes:
                        change = field_changes["financial.list_price"]
                        change_type = change.get("change_type")
                        if change_type == "decreased":
                            old_val = change.get("old_value")
                            new_val = change.get("new_value")
                            if old_val and new_val and old_val > new_val:
                                has_price_reduction = True
                                reduction = old_val - new_val
                                reduction_pct = (reduction / old_val * 100) if old_val > 0 else 0
                                timestamp = log.get("timestamp", "N/A")
                                price_reductions.append({
                                    "old_value": old_val,
                                    "new_value": new_val,
                                    "reduction": reduction,
                                    "reduction_pct": reduction_pct,
                                    "timestamp": timestamp
                                })
            
            if has_price_reduction and price_reductions:
                prop["_found_reductions"] = price_reductions
                properties_with_reductions.append(prop)
                
                if len(properties_with_reductions) >= 5:
                    break
        
        if not properties_with_reductions:
            print("No properties found with price reductions in change_logs")
            print("")
            print("This might mean:")
            print("1. Change logs haven't been populated yet")
            print("2. Properties were enriched before change_logs tracking was implemented")
            print("3. The has_price_reduction flag was set from old price_history_summary data")
            return
        
        print(f"Found {len(properties_with_reductions)} properties with price reductions")
        print("")
        print("=" * 80)
        print("PROPERTIES WITH PRICE REDUCTIONS:")
        print("=" * 80)
        print("")
        
        for i, prop in enumerate(properties_with_reductions, 1):
            property_id = prop.get("property_id", "N/A")
            mls_id = prop.get("mls_id", "N/A")
            address = prop.get("address", {})
            formatted_address = address.get("formatted_address", "N/A")
            list_price = prop.get("financial", {}).get("list_price", "N/A")
            original_price = prop.get("financial", {}).get("original_list_price")
            status = prop.get("status", "N/A")
            price_reductions = prop.get("_found_reductions", [])
            motivated_score = prop.get("enrichment", {}).get("motivated_seller", {}).get("score", 0)
            
            total_reduction = sum(r["reduction"] for r in price_reductions)
            
            print(f"Property #{i}:")
            print(f"  Property ID: {property_id}")
            print(f"  MLS ID: {mls_id}")
            print(f"  Address: {formatted_address}")
            print(f"  Status: {status}")
            if isinstance(list_price, (int, float)):
                print(f"  Current List Price: ${list_price:,.0f}")
            else:
                print(f"  Current List Price: {list_price}")
            
            if original_price:
                print(f"  Original List Price: ${original_price:,.0f}")
                if original_price != list_price:
                    reduction_amt = original_price - list_price
                    reduction_pct = (reduction_amt / original_price * 100) if original_price > 0 else 0
                    print(f"  Total Reduction: ${reduction_amt:,.0f} ({reduction_pct:.2f}%)")
            
            print(f"  Number of Price Reductions in change_logs: {len(price_reductions)}")
            print(f"  Total Reduction from change_logs: ${total_reduction:,.0f}")
            print(f"  Motivated Seller Score: {motivated_score}/100")
            
            if price_reductions:
                print(f"  Price Reduction Details:")
                for j, reduction in enumerate(price_reductions, 1):
                    timestamp_str = reduction["timestamp"]
                    if isinstance(timestamp_str, datetime):
                        timestamp_str = timestamp_str.strftime("%Y-%m-%d")
                    print(f"    {j}. ${reduction['old_value']:,.0f} -> ${reduction['new_value']:,.0f} (${reduction['reduction']:,.0f}, {reduction['reduction_pct']:.2f}%) on {timestamp_str}")
            
            print("")
            print("-" * 80)
            print("")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(find_price_reductions())

