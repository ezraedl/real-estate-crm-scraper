"""
Script to show example of change_logs structure in property JSON.

This shows where price, status, and listing_type changes are stored.
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


async def show_change_logs_example():
    """Show example of change_logs in property JSON"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        print("=" * 80)
        print("Finding property with change_logs...")
        print("=" * 80)
        print("")
        
        # Find a property with change_logs
        property_doc = await db['properties'].find_one(
            {"change_logs": {"$exists": True, "$ne": []}},
            {"property_id": 1, "mls_id": 1, "address": 1, "change_logs": 1}
        )
        
        if not property_doc:
            print("No properties with change_logs found")
            print("")
            print("Note: change_logs are stored at the ROOT level of the property document:")
            print("")
            print("```json")
            print("{")
            print('  "property_id": "...",')
            print('  "mls_id": "...",')
            print('  "status": "...",')
            print('  "financial": { ... },')
            print('  "change_logs": [  <-- HERE')
            print('    {')
            print('      "field": "financial.list_price",')
            print('      "old_value": 100000,')
            print('      "new_value": 90000,')
            print('      "change_type": "decreased",')
            print('      "timestamp": "2025-01-15T00:00:00"')
            print('    },')
            print('    {')
            print('      "field": None,  <-- Consolidated format (multiple fields same date)')
            print('      "field_changes": {')
            print('        "financial.list_price": {')
            print('          "old_value": 90000,')
            print('          "new_value": 85000,')
            print('          "change_type": "decreased"')
            print('        },')
            print('        "status": {')
            print('          "old_value": "FOR_SALE",')
            print('          "new_value": "PENDING",')
            print('          "change_type": "modified"')
            print('        }')
            print('      },')
            print('      "timestamp": "2025-02-01T00:00:00",')
            print('      "change_count": 2')
            print('    }')
            print('  ],')
            print('  "enrichment": { ... },')
            print('  "has_price_reduction": true,  <-- Flag at root level')
            print('  "is_motivated_seller": false,  <-- Flag at root level')
            print('  "has_distress_signals": false  <-- Flag at root level')
            print("}")
            print("```")
            return
        
        property_id = property_doc.get("property_id")
        mls_id = property_doc.get("mls_id")
        address = property_doc.get("address", {})
        change_logs = property_doc.get("change_logs", [])
        
        print(f"Found property: {property_id}")
        print(f"   MLS ID: {mls_id}")
        print(f"   Address: {address.get('formatted_address', 'N/A')}")
        print(f"   Change logs count: {len(change_logs)}")
        print("")
        print("=" * 80)
        print("LOCATION IN JSON:")
        print("=" * 80)
        print("")
        print("The change_logs array is stored at the ROOT level of the property document:")
        print("")
        print("```json")
        print("{")
        print(f'  "_id": "...",')
        print(f'  "property_id": "{property_id}",')
        print(f'  "mls_id": "{mls_id}",')
        print(f'  "status": "...",')
        print(f'  "listing_type": "...",')
        print(f'  "financial": {{')
        print(f'    "list_price": ...,')
        print(f'    "original_list_price": ...')
        print(f'  }},')
        print(f'  // ... other property fields ...')
        print(f'  ')
        print(f'  "change_logs": [  <-- HERE - Array at root level')
        print(f'    // Price, status, and listing_type changes')
        print(f'  ],')
        print(f'  ')
        print(f'  "enrichment": {{ ... }},')
        print(f'  "has_price_reduction": true,  // Root level flag')
        print(f'  "is_motivated_seller": false,  // Root level flag')
        print(f'  "has_distress_signals": false  // Root level flag')
        print("}")
        print("```")
        print("")
        print("=" * 80)
        print(f"CHANGE_LOGS STRUCTURE (showing first {min(3, len(change_logs))} entries):")
        print("=" * 80)
        print("")
        
        for i, log in enumerate(change_logs[:3], 1):
            print(f"Entry #{i}:")
            print("```json")
            
            # Check if consolidated format or simple format
            if log.get("field") is None and isinstance(log.get("field_changes"), dict):
                # Consolidated format
                print(json.dumps({
                    "field": None,
                    "field_changes": log.get("field_changes"),
                    "timestamp": log.get("timestamp").isoformat() if isinstance(log.get("timestamp"), datetime) else str(log.get("timestamp")),
                    "change_count": log.get("change_count"),
                    "job_id": log.get("job_id"),
                    "created_at": log.get("created_at").isoformat() if isinstance(log.get("created_at"), datetime) else str(log.get("created_at"))
                }, indent=2))
            else:
                # Simple format (single field)
                print(json.dumps({
                    "field": log.get("field"),
                    "old_value": log.get("old_value"),
                    "new_value": log.get("new_value"),
                    "change_type": log.get("change_type"),
                    "timestamp": log.get("timestamp").isoformat() if isinstance(log.get("timestamp"), datetime) else str(log.get("timestamp")),
                    "job_id": log.get("job_id"),
                    "created_at": log.get("created_at").isoformat() if isinstance(log.get("created_at"), datetime) else str(log.get("created_at"))
                }, indent=2))
            
            print("```")
            print("")
        
        if len(change_logs) > 3:
            print(f"... and {len(change_logs) - 3} more entries")
            print("")
        
        print("=" * 80)
        print("HOW TO QUERY CHANGE_LOGS:")
        print("=" * 80)
        print("")
        print("1. Get all change_logs for a property:")
        print("   db.properties.findOne({property_id: '...'}, {change_logs: 1})")
        print("")
        print("2. Find properties with price reductions:")
        print("   db.properties.find({")
        print('     change_logs: {')
        print('       $elemMatch: {')
        print('         field: "financial.list_price",')
        print('         change_type: "decreased"')
        print("       }")
        print("     }")
        print("   })")
        print("")
        print("3. Find properties with status changes:")
        print("   db.properties.find({")
        print('     change_logs: {')
        print('       $elemMatch: {')
        print('         $or: [')
        print('           {field: "status"},')
        print('           {"field_changes.status": {$exists: true}}')
        print("         ]")
        print("       }")
        print("     }")
        print("   })")
        print("")
        print("4. Using MongoDB aggregation to calculate reductions:")
        print("   db.properties.find({")
        print('     $expr: {')
        print('       $gt: [{')
        print('         $size: {')
        print('           $filter: {')
        print('             input: "$change_logs",')
        print('             cond: {')
        print('               $and: [')
        print('                 {$eq: ["$$this.field", "financial.list_price"]},')
        print('                 {$eq: ["$$this.change_type", "decreased"]}')  
        print("               ]")
        print("             }")
        print("           }")
        print("         }")
        print("       }, 0]")
        print("     }")
        print("   })")
        print("")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(show_change_logs_example())

