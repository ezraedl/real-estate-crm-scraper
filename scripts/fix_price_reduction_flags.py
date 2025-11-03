"""
Script to fix has_price_reduction flags based on embedded change_logs.
"""

import asyncio
import os
import sys
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse
import logging

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def fix_price_reduction_flags(dry_run: bool = True):
    """Fix has_price_reduction flags based on embedded change_logs"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        print("=" * 80)
        if dry_run:
            print("DRY RUN: Fixing has_price_reduction flags")
        else:
            print("Fixing has_price_reduction flags")
        print("=" * 80)
        print("")
        
        # Find properties with change_logs that have price reductions
        properties_cursor = db['properties'].find(
            {
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
        )
        
        fixed_count = 0
        skipped_count = 0
        
        async for prop in properties_cursor:
            property_id = prop.get("property_id")
            current_flag = prop.get("has_price_reduction", False)
            change_logs = prop.get("change_logs", [])
            
            # Check if there's an actual price reduction
            has_actual_reduction = False
            
            for log in change_logs:
                if log.get("field") == "financial.list_price" and log.get("change_type") == "decreased":
                    old_val = log.get("old_value")
                    new_val = log.get("new_value")
                    
                    if old_val is not None and new_val is not None:
                        reduction = old_val - new_val
                        if reduction >= 1:
                            has_actual_reduction = True
                            break
            
            # Update flag if needed
            if has_actual_reduction and not current_flag:
                if dry_run:
                    logger.info(f"Would fix property {property_id}: has_price_reduction: false -> true")
                else:
                    await db['properties'].update_one(
                        {"property_id": property_id},
                        {"$set": {"has_price_reduction": True}}
                    )
                    logger.info(f"Fixed property {property_id}: has_price_reduction: false -> true")
                fixed_count += 1
            elif not has_actual_reduction and current_flag:
                if dry_run:
                    logger.info(f"Would fix property {property_id}: has_price_reduction: true -> false")
                else:
                    await db['properties'].update_one(
                        {"property_id": property_id},
                        {"$set": {"has_price_reduction": False}}
                    )
                    logger.info(f"Fixed property {property_id}: has_price_reduction: true -> false")
                fixed_count += 1
            else:
                skipped_count += 1
        
        print("")
        print("=" * 80)
        print("SUMMARY:")
        print("=" * 80)
        if dry_run:
            print(f"Would fix: {fixed_count} properties")
            print(f"Would skip: {skipped_count} properties")
            print("")
            print("Run without --dry-run to apply changes")
        else:
            print(f"Fixed: {fixed_count} properties")
            print(f"Skipped: {skipped_count} properties")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()


if __name__ == "__main__":
    import sys
    if "--dry-run" in sys.argv:
        dry_run = True
    else:
        dry_run = False
    
    asyncio.run(fix_price_reduction_flags(dry_run=dry_run))

