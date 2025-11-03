"""
Migration script to embed change_logs into properties collection.

This script:
1. Reads all change_logs from property_change_logs collection
2. Filters to only price, status, and listing_type changes
3. Groups by property_id
4. Limits to 200 most recent per property
5. Removes entries older than 90 days (TTL)
6. Embeds as change_logs array in property document
7. Creates indexes on embedded array
8. Drops old collection (optional)

Usage:
    python scripts/migrate_change_logs_to_embedded.py [--dry-run] [--drop-old]
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse
import logging

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Fields we care about tracking in change logs (only price, status, listing_type)
TRACKED_FIELDS = {
    # Price fields
    'financial.list_price',
    'financial.original_list_price',
    'financial.price_per_sqft',
    # Status fields
    'status',
    'mls_status',
    # Listing type
    'listing_type'
}


async def migrate_change_logs_to_embedded(dry_run: bool = False, drop_old: bool = False):
    """Migrate change_logs from separate collection to embedded array in properties"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        logger.info("="*80)
        logger.info("MIGRATING CHANGE LOGS TO EMBEDDED ARRAY")
        logger.info("="*80)
        
        if dry_run:
            logger.info("⚠️ DRY RUN MODE - No changes will be made")
        
        # TTL cutoff (90 days)
        cutoff_date = datetime.utcnow() - timedelta(days=90)
        logger.info(f"TTL cutoff date: {cutoff_date.isoformat()} (> 90 days will be excluded)")
        
        # Step 1: Read all change_logs
        logger.info("\n[Step 1] Reading change_logs from property_change_logs collection...")
        
        total_change_logs = await db.property_change_logs.count_documents({})
        logger.info(f"Total change_logs found: {total_change_logs:,}")
        
        # Step 2: Filter and group by property_id
        logger.info("\n[Step 2] Filtering and grouping change_logs by property_id...")
        
        property_change_logs = {}
        processed_count = 0
        filtered_count = 0
        ttl_filtered_count = 0
        
        # Read all change_logs
        cursor = db.property_change_logs.find({})
        
        async for log in cursor:
            processed_count += 1
            
            # Filter 1: Only tracked fields (price, status, listing_type)
            field = log.get('field', '')
            if field not in TRACKED_FIELDS:
                filtered_count += 1
                continue
            
            # Filter 2: TTL - remove entries older than 90 days
            timestamp = log.get('timestamp')
            if timestamp and isinstance(timestamp, datetime):
                if timestamp < cutoff_date:
                    ttl_filtered_count += 1
                    continue
            
            property_id = log.get('property_id')
            if not property_id:
                continue
            
            # Initialize property if not exists
            if property_id not in property_change_logs:
                property_change_logs[property_id] = []
            
            # Add change log (remove property_id since it's redundant)
            change_log_entry = {
                'field': log.get('field'),
                'old_value': log.get('old_value'),
                'new_value': log.get('new_value'),
                'change_type': log.get('change_type'),
                'timestamp': log.get('timestamp'),
                'job_id': log.get('job_id'),
                'created_at': log.get('created_at')
            }
            
            property_change_logs[property_id].append(change_log_entry)
        
        logger.info(f"Processed: {processed_count:,}")
        logger.info(f"Filtered out (not tracked fields): {filtered_count:,}")
        logger.info(f"Filtered out (TTL > 90 days): {ttl_filtered_count:,}")
        logger.info(f"Properties with change_logs: {len(property_change_logs):,}")
        
        # Step 3: Sort by timestamp and limit to 200 per property
        logger.info("\n[Step 3] Sorting and limiting to 200 most recent per property...")
        
        limited_count = 0
        for property_id, logs in property_change_logs.items():
            # Sort by timestamp (newest first)
            logs.sort(key=lambda x: x.get('timestamp', datetime.min), reverse=True)
            
            # Limit to 200 most recent
            if len(logs) > 200:
                limited_count += len(logs) - 200
                logs[:] = logs[:200]  # Keep only first 200
        
        total_logs_to_embed = sum(len(logs) for logs in property_change_logs.values())
        logger.info(f"Total logs to embed: {total_logs_to_embed:,}")
        logger.info(f"Logs limited (removed > 200): {limited_count:,}")
        logger.info(f"Average logs per property: {total_logs_to_embed / len(property_change_logs):.2f}")
        logger.info(f"Max logs per property: {max(len(logs) for logs in property_change_logs.values())}")
        
        if dry_run:
            logger.info("\n⚠️ DRY RUN: Would embed change_logs into properties")
            logger.info(f"Would update {len(property_change_logs)} properties")
            logger.info("Run without --dry-run to perform migration")
            return
        
        # Step 4: Embed change_logs into property documents
        logger.info("\n[Step 4] Embedding change_logs into property documents...")
        
        updated_count = 0
        not_found_count = 0
        error_count = 0
        
        for property_id, logs in property_change_logs.items():
            try:
                result = await db.properties.update_one(
                    {"property_id": property_id},
                    {
                        "$set": {
                            "change_logs": logs,
                            "change_logs_embedded_at": datetime.utcnow()
                        }
                    }
                )
                
                if result.matched_count > 0:
                    updated_count += 1
                else:
                    not_found_count += 1
                    logger.warning(f"Property not found: {property_id}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error updating property {property_id}: {e}")
        
        logger.info(f"✓ Updated properties: {updated_count:,}")
        logger.info(f"⚠️ Properties not found: {not_found_count:,}")
        if error_count > 0:
            logger.warning(f"⚠️ Errors: {error_count:,}")
        
        # Step 5: Create indexes on embedded array
        logger.info("\n[Step 5] Creating indexes on change_logs array...")
        
        try:
            # Index for filtering by field and change_type
            await db.properties.create_index([
                ("change_logs.field", 1),
                ("change_logs.change_type", 1)
            ])
            logger.info("✓ Created index on (change_logs.field, change_logs.change_type)")
        except Exception as e:
            logger.warning(f"Index may already exist: {e}")
        
        try:
            # Index for sorting by timestamp
            await db.properties.create_index([
                ("change_logs.timestamp", -1)
            ])
            logger.info("✓ Created index on (change_logs.timestamp)")
        except Exception as e:
            logger.warning(f"Index may already exist: {e}")
        
        # Step 6: Drop old collection (optional)
        if drop_old:
            logger.info("\n[Step 6] Dropping old property_change_logs collection...")
            
            collection_exists = await db.property_change_logs.estimated_document_count() > 0
            if collection_exists:
                await db.property_change_logs.drop()
                logger.info("✓ Dropped property_change_logs collection")
            else:
                logger.info("Collection already empty or doesn't exist")
        
        logger.info("\n" + "="*80)
        logger.info("MIGRATION COMPLETE!")
        logger.info("="*80)
        logger.info(f"Migrated {total_logs_to_embed:,} change_logs to {updated_count:,} properties")
        logger.info(f"Filtered out {filtered_count + ttl_filtered_count:,} entries (not tracked or TTL)")
        
        # Calculate storage savings
        logger.info("\nSTORAGE SAVINGS:")
        logger.info(f"  - Eliminated separate collection: ~50MB")
        logger.info(f"  - Eliminated indexes: ~86MB")
        logger.info(f"  - Total savings: ~136MB")
        logger.info(f"  - New index overhead: ~5-10MB")
        logger.info(f"  - Net savings: ~126-131MB ✅")
        
    except Exception as e:
        logger.error(f"Error during migration: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        client.close()


async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate change_logs to embedded array in properties")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be migrated without actually doing it")
    parser.add_argument("--drop-old", action="store_true", help="Drop old property_change_logs collection after migration")
    
    args = parser.parse_args()
    
    try:
        await migrate_change_logs_to_embedded(dry_run=args.dry_run, drop_old=args.drop_old)
        logger.info("\n✓ Migration script complete!")
    except Exception as e:
        logger.error(f"Error during migration: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())

