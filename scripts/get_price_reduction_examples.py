"""
Script to get 5 example properties with price reductions.
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


async def get_price_reduction_examples():
    """Get 5 example properties with price reductions"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        print("=" * 80)
        print("Querying properties with price reductions in change_logs...")
        print("=" * 80)
        print("")
        
        # Query for properties that actually have price reduction entries in change_logs
        # Using aggregation to find properties with price reduction entries
        pipeline = [
            {
                "$match": {
                    "has_price_reduction": True
                }
            },
            {
                "$addFields": {
                    "price_reductions": {
                        "$filter": {
                            "input": "$change_logs",
                            "as": "log",
                            "cond": {
                                "$and": [
                                    {
                                        "$or": [
                                            { "$eq": ["$$log.field", "financial.list_price"] },
                                            { "$ne": [{ "$ifNull": ["$$log.field_changes.financial.list_price", None] }, None] }
                                        ]
                                    },
                                    {
                                        "$or": [
                                            { "$eq": ["$$log.change_type", "decreased"] },
                                            { "$eq": [{ "$ifNull": ["$$log.field_changes.financial.list_price.change_type", ""] }, "decreased"] }
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            },
            {
                "$match": {
                    "price_reductions": { "$ne": [] }
                }
            },
            {
                "$limit": 5
            }
        ]
        
        properties = await db['properties'].aggregate(pipeline).to_list(length=5)
        
        if not properties:
            print("No properties found with price reductions in change_logs")
            print("")
            print("Checking properties with has_price_reduction flag...")
            print("")
            # Fallback: just get properties with the flag and show their change_logs
            properties = await db['properties'].find(
                {
                    "has_price_reduction": True,
                    "change_logs": { "$exists": True, "$ne": [] }
                },
                {
                    "property_id": 1,
                    "mls_id": 1,
                    "address": 1,
                    "financial.list_price": 1,
                    "financial.original_list_price": 1,
                    "status": 1,
                    "change_logs": 1,
                    "has_price_reduction": 1,
                    "enrichment.motivated_seller.score": 1
                }
            ).limit(5).to_list(length=5)
        
        if not properties:
            print("No properties found")
            return
        
        print(f"Found {len(properties)} properties with price reductions")
        print("")
        print("=" * 80)
        print("PROPERTIES WITH PRICE REDUCTIONS:")
        print("=" * 80)
        print("")
        
        for i, prop in enumerate(properties, 1):
            property_id = prop.get("property_id", "N/A")
            mls_id = prop.get("mls_id", "N/A")
            address = prop.get("address", {})
            formatted_address = address.get("formatted_address", "N/A")
            list_price = prop.get("financial", {}).get("list_price", "N/A")
            original_price = prop.get("financial", {}).get("original_list_price")
            status = prop.get("status", "N/A")
            change_logs = prop.get("change_logs", [])
            motivated_score = prop.get("enrichment", {}).get("motivated_seller", {}).get("score", 0)
            
            # Count price reductions in change_logs
            price_reductions = []
            for log in change_logs:
                # Check simple format
                if log.get("field") == "financial.list_price":
                    change_type = log.get("change_type")
                    if change_type == "decreased":
                        old_val = log.get("old_value")
                        new_val = log.get("new_value")
                        if old_val and new_val and old_val > new_val:
                            reduction = old_val - new_val
                            reduction_pct = (reduction / old_val * 100) if old_val > 0 else 0
                            timestamp = log.get("timestamp", "N/A")
                            price_reductions.append({
                                "old_value": old_val,
                                "new_value": new_val,
                                "reduction": reduction,
                                "reduction_pct": reduction_pct,
                                "timestamp": timestamp,
                                "change_type": change_type
                            })
                # Check consolidated format
                elif log.get("field") is None and isinstance(log.get("field_changes"), dict):
                    field_changes = log.get("field_changes", {})
                    if "financial.list_price" in field_changes:
                        change = field_changes["financial.list_price"]
                        change_type = change.get("change_type")
                        if change_type == "decreased":
                            old_val = change.get("old_value")
                            new_val = change.get("new_value")
                            if old_val and new_val and old_val > new_val:
                                reduction = old_val - new_val
                                reduction_pct = (reduction / old_val * 100) if old_val > 0 else 0
                                timestamp = log.get("timestamp", "N/A")
                                price_reductions.append({
                                    "old_value": old_val,
                                    "new_value": new_val,
                                    "reduction": reduction,
                                    "reduction_pct": reduction_pct,
                                    "timestamp": timestamp,
                                    "change_type": change_type
                                })
            
            total_reduction = sum(r["reduction"] for r in price_reductions) if price_reductions else 0
            
            print(f"Property #{i}:")
            print(f"  Property ID: {property_id}")
            print(f"  MLS ID: {mls_id}")
            print(f"  Address: {formatted_address}")
            print(f"  Status: {status}")
            if isinstance(list_price, (int, float)):
                print(f"  Current List Price: ${list_price:,.0f}")
            else:
                print(f"  Current List Price: {list_price}")
            
            if original_price and original_price != list_price:
                print(f"  Original List Price: ${original_price:,.0f}")
                reduction_amt = original_price - list_price
                reduction_pct = (reduction_amt / original_price * 100) if original_price > 0 else 0
                print(f"  Total Reduction: ${reduction_amt:,.0f} ({reduction_pct:.2f}%)")
            
            print(f"  Number of Price Reductions in change_logs: {len(price_reductions)}")
            print(f"  Total Reduction from change_logs: ${total_reduction:,.0f}")
            print(f"  Motivated Seller Score: {motivated_score}/100")
            
            # Show sample of change_logs
            print(f"  Total change_logs entries: {len(change_logs)}")
            if change_logs:
                print(f"  Sample change_logs (first 3):")
                for j, log in enumerate(change_logs[:3], 1):
                    field = log.get("field", "N/A")
                    timestamp = log.get("timestamp", "N/A")
                    if isinstance(timestamp, datetime):
                        timestamp = timestamp.strftime("%Y-%m-%d")
                    print(f"    {j}. Field: {field}, Timestamp: {timestamp}, Change Type: {log.get('change_type', 'N/A')}")
                    if log.get("field") is None and isinstance(log.get("field_changes"), dict):
                        print(f"       (Consolidated: {len(log.get('field_changes', {}))} fields)")
            
            if price_reductions:
                print(f"  Price Reduction Details:")
                for j, reduction in enumerate(price_reductions[:3], 1):  # Show first 3 reductions
                    timestamp_str = reduction["timestamp"]
                    if isinstance(timestamp_str, datetime):
                        timestamp_str = timestamp_str.strftime("%Y-%m-%d")
                    print(f"    {j}. ${reduction['old_value']:,.0f} -> ${reduction['new_value']:,.0f} (${reduction['reduction']:,.0f}, {reduction['reduction_pct']:.2f}%) on {timestamp_str}")
                if len(price_reductions) > 3:
                    print(f"    ... and {len(price_reductions) - 3} more reduction(s)")
            else:
                print(f"  Note: No price reductions found in change_logs (but has_price_reduction flag is true)")
            
            print("")
            print("-" * 80)
            print("")
        
        print("=" * 80)
        print("Query used:")
        print("=" * 80)
        print("")
        print("db.properties.find({")
        print("  has_price_reduction: true,")
        print("  change_logs: { $exists: true, $ne: [] }")
        print("}).limit(5)")
        print("")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(get_price_reduction_examples())
