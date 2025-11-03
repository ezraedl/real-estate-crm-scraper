"""
Script to analyze why there's a difference between properties with any decreased change
vs properties with actual price reductions >= $1.
"""

import asyncio
import os
import sys
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse
from collections import defaultdict

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import settings


async def analyze_decreased_changes():
    """Analyze decreased changes to understand the discrepancy"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        print("=" * 80)
        print("ANALYZING DECREASED CHANGES")
        print("=" * 80)
        print("")
        
        # Query 1: Properties with any decreased change (like user's query)
        query1 = {
            "listing_type": "for_sale",
            "change_logs.change_type": "decreased"
        }
        
        count1 = await db['properties'].count_documents(query1)
        print(f"Query 1: Properties with any decreased change_log")
        print(f"  Query: {{listing_type: 'for_sale', 'change_logs.change_type': 'decreased'}}")
        print(f"  Result: {count1} properties")
        print("")
        
        # Query 2: Properties with financial.list_price decreased changes
        query2 = {
            "listing_type": "for_sale",
            "change_logs": {
                "$elemMatch": {
                    "field": "financial.list_price",
                    "change_type": "decreased"
                }
            }
        }
        
        count2 = await db['properties'].count_documents(query2)
        print(f"Query 2: Properties with financial.list_price decreased")
        print(f"  Query: {{listing_type: 'for_sale', change_logs: {{$elemMatch: {{field: 'financial.list_price', change_type: 'decreased'}}}}}}")
        print(f"  Result: {count2} properties")
        print("")
        
        # Query 3: Properties with actual price reduction >= $1 (like backend filter)
        # This is what the backend does
        properties_cursor = db['properties'].find(
            {
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
        ).limit(10)
        
        properties_with_reductions = await properties_cursor.to_list(length=10)
        count3 = len(properties_with_reductions)
        
        # Get exact count
        pipeline = [
            {
                "$match": {
                    "listing_type": "for_sale",
                    "has_price_reduction": True
                }
            },
            {
                "$addFields": {
                    "total_reduction": {
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
                    }
                }
            },
            {
                "$match": {
                    "total_reduction": {"$gte": 1}
                }
            },
            {
                "$count": "count"
            }
        ]
        
        count_result = await db['properties'].aggregate(pipeline).to_list(length=1)
        count3_exact = count_result[0]["count"] if count_result else 0
        
        print(f"Query 3: Properties with financial.list_price reduction >= $1 (backend filter)")
        print(f"  Uses: has_price_reduction flag + $expr aggregation")
        print(f"  Result: {count3_exact} properties")
        print("")
        
        # Analyze what types of decreased changes exist
        print("=" * 80)
        print("ANALYZING WHAT TYPES OF DECREASED CHANGES EXIST:")
        print("=" * 80)
        print("")
        
        # Get sample of properties with any decreased change
        sample_properties = await db['properties'].find(
            query1,
            {"property_id": 1, "change_logs": 1}
        ).limit(50).to_list(length=50)
        
        field_counts = defaultdict(int)
        total_decreased_entries = 0
        list_price_decreased_count = 0
        list_price_reduction_sum = 0
        
        for prop in sample_properties:
            change_logs = prop.get("change_logs", [])
            for log in change_logs:
                if log.get("change_type") == "decreased":
                    total_decreased_entries += 1
                    field = log.get("field")
                    
                    if field:
                        field_counts[field] += 1
                        
                        # Check if it's list_price and calculate reduction
                        if field == "financial.list_price":
                            list_price_decreased_count += 1
                            old_val = log.get("old_value")
                            new_val = log.get("new_value")
                            if old_val and new_val:
                                reduction = old_val - new_val
                                list_price_reduction_sum += reduction
                    
                    # Check consolidated format
                    elif log.get("field") is None and isinstance(log.get("field_changes"), dict):
                        field_changes = log.get("field_changes", {})
                        for field_name, change in field_changes.items():
                            if change.get("change_type") == "decreased":
                                field_counts[field_name] += 1
                                
                                if field_name == "financial.list_price":
                                    list_price_decreased_count += 1
                                    old_val = change.get("old_value")
                                    new_val = change.get("new_value")
                                    if old_val and new_val:
                                        reduction = old_val - new_val
                                        list_price_reduction_sum += reduction
        
        print(f"Sample analysis (from {len(sample_properties)} properties):")
        print(f"  Total decreased entries found: {total_decreased_entries}")
        print(f"  financial.list_price decreased entries: {list_price_decreased_count}")
        if list_price_decreased_count > 0:
            avg_reduction = list_price_reduction_sum / list_price_decreased_count
            print(f"  Average list_price reduction: ${avg_reduction:,.2f}")
        print("")
        print("Breakdown by field:")
        for field, count in sorted(field_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {field}: {count} decreased entries")
        print("")
        
        # Show examples
        print("=" * 80)
        print("EXAMPLES OF NON-LIST-PRICE DECREASED CHANGES:")
        print("=" * 80)
        print("")
        
        examples_shown = 0
        for prop in sample_properties:
            if examples_shown >= 5:
                break
            
            change_logs = prop.get("change_logs", [])
            for log in change_logs:
                if log.get("change_type") == "decreased":
                    field = log.get("field")
                    if field and field != "financial.list_price":
                        examples_shown += 1
                        print(f"Example {examples_shown}:")
                        print(f"  Property ID: {prop.get('property_id')}")
                        print(f"  Field: {field}")
                        print(f"  Old Value: {log.get('old_value')}")
                        print(f"  New Value: {log.get('new_value')}")
                        print(f"  Change Type: {log.get('change_type')}")
                        print("")
                        break
        
        print("=" * 80)
        print("SUMMARY:")
        print("=" * 80)
        print("")
        print(f"Total properties with ANY decreased change: {count1}")
        print(f"Properties with financial.list_price decreased: {count2}")
        print(f"Properties with list_price reduction >= $1: {count3_exact}")
        print("")
        print("REASON FOR DISCREPANCY:")
        print("=" * 80)
        print(f"1. Query 1 matches ANY field with change_type='decreased'")
        print(f"   This includes: financial.price_per_sqft, status, mls_status, listing_type")
        print(f"   Example: price_per_sqft going from 92 to 90 is 'decreased' but not a dollar reduction")
        print("")
        print(f"2. Query 2 matches only financial.list_price decreased changes")
        print(f"   This still includes cases where old_value might equal new_value (shouldn't happen but could)")
        print("")
        print(f"3. Query 3 (backend filter) calculates actual dollar reduction:")
        print(f"   - Only looks at financial.list_price")
        print(f"   - Calculates: old_value - new_value")
        print(f"   - Requires: reduction >= $1")
        print(f"   - This is the correct filter for 'price reduction'")
        print("")
        print("RECOMMENDATION:")
        print("If you want to match Query 1 but get Query 3 results, use:")
        print("  Query: {listing_type: 'for_sale', has_price_reduction: true}")
        print("  Then filter by minimum reduction amount using $expr (like Query 3)")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(analyze_decreased_changes())

