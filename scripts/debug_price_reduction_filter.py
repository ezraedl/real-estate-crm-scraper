"""
Script to debug why backend filter returns only 7 properties vs 342.
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


async def debug_price_reduction_filter():
    """Debug the price reduction filter discrepancy"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        print("=" * 80)
        print("DEBUGGING PRICE REDUCTION FILTER")
        print("=" * 80)
        print("")
        
        # Get properties with financial.list_price decreased
        properties_cursor = db['properties'].find(
            {
                "listing_type": "for_sale",
                "change_logs": {
                    "$elemMatch": {
                        "field": "financial.list_price",
                        "change_type": "decreased"
                    }
                }
            },
            {
                "property_id": 1,
                "has_price_reduction": 1,
                "change_logs": 1
            }
        ).limit(50)
        
        properties = await properties_cursor.to_list(length=50)
        
        print(f"Analyzing {len(properties)} properties with financial.list_price decreased...")
        print("")
        
        issues = {
            "no_actual_reduction": 0,  # old_value <= new_value
            "missing_values": 0,  # null values
            "has_flag_false": 0,  # has_price_reduction is False
            "has_flag_true": 0,  # has_price_reduction is True
            "valid_reductions": 0  # Actually has reduction >= $1
        }
        
        examples = {
            "no_actual_reduction": [],
            "missing_values": [],
            "has_flag_false": []
        }
        
        for prop in properties:
            property_id = prop.get("property_id")
            has_price_reduction = prop.get("has_price_reduction", False)
            change_logs = prop.get("change_logs", [])
            
            found_valid_reduction = False
            found_issue = None
            
            for log in change_logs:
                if log.get("field") == "financial.list_price" and log.get("change_type") == "decreased":
                    old_val = log.get("old_value")
                    new_val = log.get("new_value")
                    
                    # Check for missing values
                    if old_val is None or new_val is None:
                        issues["missing_values"] += 1
                        if len(examples["missing_values"]) < 3:
                            examples["missing_values"].append({
                                "property_id": property_id,
                                "old_value": old_val,
                                "new_value": new_val
                            })
                        found_issue = "missing_values"
                        break
                    
                    # Check if actual reduction
                    reduction = old_val - new_val
                    if reduction >= 1:
                        found_valid_reduction = True
                        issues["valid_reductions"] += 1
                        break
                    else:
                        # No actual reduction or even increase
                        if len(examples["no_actual_reduction"]) < 3:
                            examples["no_actual_reduction"].append({
                                "property_id": property_id,
                                "old_value": old_val,
                                "new_value": new_val,
                                "reduction": reduction
                            })
                        found_issue = "no_actual_reduction"
            
            if not found_valid_reduction:
                if found_issue:
                    issues[found_issue] += 1
                else:
                    issues["no_actual_reduction"] += 1
            
            if has_price_reduction:
                issues["has_flag_true"] += 1
            else:
                issues["has_flag_false"] += 1
                if len(examples["has_flag_false"]) < 3 and found_issue:
                    examples["has_flag_false"].append({
                        "property_id": property_id,
                        "issue": found_issue
                    })
        
        print("=" * 80)
        print("ISSUES FOUND:")
        print("=" * 80)
        print(f"Properties with NO actual reduction (old_value <= new_value): {issues['no_actual_reduction']}")
        print(f"Properties with MISSING values (null old_value or new_value): {issues['missing_values']}")
        print(f"Properties with has_price_reduction=FALSE: {issues['has_flag_false']}")
        print(f"Properties with has_price_reduction=TRUE: {issues['has_flag_true']}")
        print(f"Properties with VALID reductions >= $1: {issues['valid_reductions']}")
        print("")
        
        if examples["no_actual_reduction"]:
            print("=" * 80)
            print("EXAMPLES: No Actual Reduction")
            print("=" * 80)
            for ex in examples["no_actual_reduction"]:
                print(f"Property {ex['property_id']}:")
                print(f"  old_value: {ex['old_value']}")
                print(f"  new_value: {ex['new_value']}")
                print(f"  Calculated reduction: ${ex['reduction']}")
                if ex['reduction'] <= 0:
                    print(f"  Problem: Not actually a reduction! (old <= new)")
                print("")
        
        if examples["missing_values"]:
            print("=" * 80)
            print("EXAMPLES: Missing Values")
            print("=" * 80)
            for ex in examples["missing_values"]:
                print(f"Property {ex['property_id']}:")
                print(f"  old_value: {ex['old_value']}")
                print(f"  new_value: {ex['new_value']}")
                print(f"  Problem: Missing old_value or new_value")
                print("")
        
        if examples["has_flag_false"]:
            print("=" * 80)
            print("EXAMPLES: has_price_reduction Flag is False")
            print("=" * 80)
            for ex in examples["has_flag_false"]:
                print(f"Property {ex['property_id']}:")
                print(f"  has_price_reduction: False")
                print(f"  Issue: {ex['issue']}")
                print("")
        
        print("=" * 80)
        print("ROOT CAUSE:")
        print("=" * 80)
        print("")
        print("The backend filter requires BOTH:")
        print("1. has_price_reduction: true (pre-filter)")
        print("2. Calculated reduction >= $1 (using $expr aggregation)")
        print("")
        print("Most properties with 'decreased' changes either:")
        print("1. Have has_price_reduction=false (flag not set correctly)")
        print("2. Have old_value <= new_value (incorrectly marked as 'decreased')")
        print("3. Have missing/null values")
        print("")
        print("SOLUTION:")
        print("1. Fix has_price_reduction flag calculation during enrichment")
        print("2. Fix change_type logic to only mark as 'decreased' when old_value > new_value")
        print("3. Ensure old_value and new_value are always present")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(debug_price_reduction_filter())

