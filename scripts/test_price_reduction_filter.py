"""
Script to test why backend filter still returns only 7 properties.
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


async def test_price_reduction_filter():
    """Test the price reduction filter logic"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        print("=" * 80)
        print("TESTING PRICE REDUCTION FILTER")
        print("=" * 80)
        print("")
        
        # Query 1: Properties with has_price_reduction flag
        count1 = await db['properties'].count_documents({
            "listing_type": "for_sale",
            "has_price_reduction": True
        })
        print(f"Query 1: Properties with has_price_reduction=true")
        print(f"  Result: {count1} properties")
        print("")
        
        # Query 2: Backend filter logic (exactly as backend does it)
        # This matches what the backend PriceReductionFilter does
        query2 = {
            "listing_type": "for_sale",
            "has_price_reduction": True,
            "$expr": {
                "$gte": [
                    {
                        "$sum": {
                            "$map": {
                                "input": {
                                    "$filter": {
                                        "input": "$change_logs",
                                        "as": "log",
                                        "cond": {
                                            "$and": [
                                                {"$eq": ["$$log.field", "financial.list_price"]},
                                                {"$eq": ["$$log.change_type", "decreased"]}
                                            ]
                                        }
                                    }
                                },
                                "as": "reduction",
                                "in": {
                                    "$subtract": [
                                        {"$ifNull": ["$$reduction.old_value", 0]},
                                        {"$ifNull": ["$$reduction.new_value", 0]}
                                    ]
                                }
                            }
                        }
                    },
                    1  # Minimum $1 reduction
                ]
            }
        }
        
        # Test the query
        properties = await db['properties'].find(query2).limit(10).to_list(length=10)
        count2 = len(properties)
        
        # Get exact count
        count2_exact = await db['properties'].count_documents(query2)
        
        print(f"Query 2: Backend filter (has_price_reduction + $expr aggregation >= $1)")
        print(f"  Result: {count2_exact} properties")
        print("")
        
        # Query 3: Without has_price_reduction pre-filter (test if aggregation works)
        query3 = {
            "listing_type": "for_sale",
            "$expr": {
                "$gte": [
                    {
                        "$sum": {
                            "$map": {
                                "input": {
                                    "$filter": {
                                        "input": "$change_logs",
                                        "as": "log",
                                        "cond": {
                                            "$and": [
                                                {"$eq": ["$$log.field", "financial.list_price"]},
                                                {"$eq": ["$$log.change_type", "decreased"]}
                                            ]
                                        }
                                    }
                                },
                                "as": "reduction",
                                "in": {
                                    "$subtract": [
                                        {"$ifNull": ["$$reduction.old_value", 0]},
                                        {"$ifNull": ["$$reduction.new_value", 0]}
                                    ]
                                }
                            }
                        }
                    },
                    1
                ]
            }
        }
        
        count3 = await db['properties'].count_documents(query3)
        print(f"Query 3: Without has_price_reduction pre-filter (aggregation only)")
        print(f"  Result: {count3} properties")
        print("")
        
        # Debug: Check properties with has_price_reduction=true but aggregation returns 0
        print("=" * 80)
        print("DEBUGGING: Properties with flag=true but aggregation fails")
        print("=" * 80)
        print("")
        
        # Get sample properties with has_price_reduction=true
        sample_props = await db['properties'].find(
            {
                "listing_type": "for_sale",
                "has_price_reduction": True
            },
            {
                "property_id": 1,
                "change_logs": 1
            }
        ).limit(10).to_list(length=10)
        
        print(f"Analyzing {len(sample_props)} properties with has_price_reduction=true...")
        print("")
        
        for prop in sample_props:
            property_id = prop.get("property_id")
            change_logs = prop.get("change_logs", [])
            
            # Calculate reduction using same logic as backend
            total_reduction = 0
            found_reductions = []
            
            for log in change_logs:
                # Simple format
                if log.get("field") == "financial.list_price" and log.get("change_type") == "decreased":
                    old_val = log.get("old_value")
                    new_val = log.get("new_value")
                    if old_val is not None and new_val is not None:
                        reduction = old_val - new_val
                        total_reduction += reduction
                        found_reductions.append({
                            "format": "simple",
                            "old_value": old_val,
                            "new_value": new_val,
                            "reduction": reduction
                        })
                
                # Consolidated format
                elif log.get("field") is None and isinstance(log.get("field_changes"), dict):
                    field_changes = log.get("field_changes", {})
                    if "financial.list_price" in field_changes:
                        change = field_changes["financial.list_price"]
                        if change.get("change_type") == "decreased":
                            old_val = change.get("old_value")
                            new_val = change.get("new_value")
                            if old_val is not None and new_val is not None:
                                reduction = old_val - new_val
                                total_reduction += reduction
                                found_reductions.append({
                                    "format": "consolidated",
                                    "old_value": old_val,
                                    "new_value": new_val,
                                    "reduction": reduction
                                })
            
            if total_reduction >= 1:
                print(f"Property {property_id}: Total reduction = ${total_reduction:,.2f} (PASS)")
            else:
                print(f"Property {property_id}: Total reduction = ${total_reduction:,.2f} (FAIL - why?)")
                print(f"  Change logs count: {len(change_logs)}")
                if found_reductions:
                    print(f"  Found {len(found_reductions)} reductions:")
                    for r in found_reductions:
                        print(f"    {r['format']}: ${r['old_value']} -> ${r['new_value']} (${r['reduction']})")
                else:
                    print(f"  No reductions found - checking why...")
                    for i, log in enumerate(change_logs[:3], 1):
                        field = log.get("field")
                        change_type = log.get("change_type")
                        print(f"    Log #{i}: field={field}, change_type={change_type}")
            print("")
        
        print("=" * 80)
        print("SUMMARY:")
        print("=" * 80)
        print(f"Properties with has_price_reduction=true: {count1}")
        print(f"Properties passing backend filter (>= $1): {count2_exact}")
        print(f"Properties with reduction >= $1 (without flag filter): {count3}")
        print("")
        
        if count2_exact < count1:
            print("ISSUE: Backend filter is excluding properties even with has_price_reduction=true")
            print("Possible reasons:")
            print("1. Aggregation only checks simple format (field='financial.list_price')")
            print("2. Aggregation doesn't check consolidated format (field=None, field_changes)")
            print("3. Some properties have change_type != 'decreased'")
            print("4. Some properties have null old_value or new_value")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(test_price_reduction_filter())

