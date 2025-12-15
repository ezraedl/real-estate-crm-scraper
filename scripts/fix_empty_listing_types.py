"""
Fix existing scheduled jobs that have empty listing_types.
This script updates scheduled jobs to have default listing types if they're empty.
"""
import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import db
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fix_empty_listing_types():
    """Fix scheduled jobs with empty listing_types"""
    try:
        await db.connect()
        
        print("\n" + "="*80)
        print("FIXING EMPTY LISTING_TYPES IN SCHEDULED JOBS")
        print("="*80 + "\n")
        
        # Get all active scheduled jobs
        scheduled_jobs = await db.get_active_scheduled_jobs()
        
        fixed_count = 0
        for sj in scheduled_jobs:
            listing_types = sj.listing_types
            listing_type = sj.listing_type
            
            # Check if listing_types is empty (handle both None and empty list)
            # Also check if it's stored as empty list in database
            is_empty = (listing_types is None) or (isinstance(listing_types, list) and len(listing_types) == 0)
            
            if is_empty:
                print(f"\nFound scheduled job with empty listing_types:")
                print(f"  - ID: {sj.scheduled_job_id}")
                print(f"  - Name: {sj.name}")
                print(f"  - listing_types: {listing_types}")
                print(f"  - listing_type: {listing_type}")
                
                # Determine what to set
                if listing_type:
                    new_listing_types = [listing_type]
                    print(f"  - Fixing: Using listing_type '{listing_type}'")
                else:
                    new_listing_types = ["for_sale", "sold", "for_rent", "pending"]
                    print(f"  - Fixing: Setting default listing_types: {new_listing_types}")
                
                # Update the scheduled job
                await db.update_scheduled_job(sj.scheduled_job_id, {
                    "listing_types": new_listing_types
                })
                
                print(f"  - ✅ Fixed!")
                fixed_count += 1
        
        print(f"\n{'='*80}")
        print(f"SUMMARY")
        print(f"{'='*80}")
        print(f"Total scheduled jobs checked: {len(scheduled_jobs)}")
        print(f"Scheduled jobs fixed: {fixed_count}")
        
        if fixed_count == 0:
            print("\n✅ No scheduled jobs with empty listing_types found!")
        else:
            print(f"\n✅ Fixed {fixed_count} scheduled job(s)!")
            print("   New job instances created from these scheduled jobs will now have proper listing_types.")
        
        await db.disconnect()
        
    except Exception as e:
        logger.error(f"Error fixing empty listing_types: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(fix_empty_listing_types())

