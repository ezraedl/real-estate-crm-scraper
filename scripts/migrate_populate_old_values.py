"""
Migration script to populate _old values for existing properties.

This script:
1. Finds properties with current values but missing _old values
2. Copies current tracked values to _old fields
3. Sets _old_values_scraped_at from scraped_at or last_scraped timestamp
4. Enables immediate comparison on next scrape

Tracked fields:
- financial.list_price → financial.list_price_old
- financial.original_list_price → financial.original_list_price_old
- status → status_old
- mls_status → mls_status_old
- listing_type → listing_type_old

Usage:
    python scripts/migrate_populate_old_values.py [--dry-run] [--batch-size 100]
"""

import asyncio
import os
import sys
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse
import logging
import argparse

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_timestamp(timestamp):
    """Parse timestamp from various formats"""
    if not timestamp:
        return None
    
    if isinstance(timestamp, datetime):
        return timestamp
    
    if isinstance(timestamp, str):
        try:
            # Try parsing ISO format string
            if 'T' in timestamp or ' ' in timestamp:
                return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            pass
    
    return None


async def migrate_properties(dry_run: bool = False, batch_size: int = 100, limit: int = None):
    """
    Migrate properties to populate _old values
    
    Args:
        dry_run: If True, only report what would be changed without making changes
        batch_size: Number of properties to process in each batch
        limit: Maximum number of properties to process (None for all)
    """
    try:
        # Parse MongoDB URI
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        # Connect to MongoDB
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        properties_collection = db.properties
        
        logger.info(f"Connected to MongoDB: {database_name}")
        
        # Find properties that need migration:
        # 1. Have current values for tracked fields
        # 2. Missing _old values (or missing _old_values_scraped_at)
        query = {
            "$or": [
                # Has current values but missing _old values
                {
                    "financial.list_price": {"$exists": True, "$ne": None},
                    "financial.list_price_old": {"$exists": False}
                },
                {
                    "status": {"$exists": True, "$ne": None},
                    "status_old": {"$exists": False}
                },
                {
                    "_old_values_scraped_at": {"$exists": False}
                }
            ]
        }
        
        total_count = await properties_collection.count_documents(query)
        logger.info(f"Found {total_count} properties that need _old values migration")
        
        if total_count == 0:
            logger.info("No properties need migration. Exiting.")
            return
        
        # Apply limit if specified
        if limit:
            total_count = min(total_count, limit)
            logger.info(f"Processing limited to {limit} properties")
        
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be made")
        
        # Process in batches
        processed = 0
        updated = 0
        errors = 0
        batch_number = 0
        
        cursor = properties_collection.find(query).batch_size(batch_size)
        if limit:
            cursor = cursor.limit(limit)
        
        async for property_doc in cursor:
            try:
                property_id = property_doc.get("property_id")
                if not property_id:
                    continue
                
                # Extract current values for tracked fields
                financial = property_doc.get("financial", {})
                current_list_price = financial.get("list_price")
                current_original_list_price = financial.get("original_list_price")
                current_status = property_doc.get("status")
                current_mls_status = property_doc.get("mls_status")
                current_listing_type = property_doc.get("listing_type")
                
                # Get scraped_at timestamp
                scraped_at = property_doc.get("scraped_at") or property_doc.get("last_scraped")
                scraped_at_dt = parse_timestamp(scraped_at) or datetime.utcnow()
                
                # Check if we need to update (at least one _old value is missing)
                needs_update = (
                    (current_list_price is not None and not financial.get("list_price_old")) or
                    (current_status is not None and not property_doc.get("status_old")) or
                    not property_doc.get("_old_values_scraped_at")
                )
                
                if not needs_update:
                    processed += 1
                    continue
                
                # Prepare update fields
                update_fields = {}
                
                # Copy current to _old (only if current value exists and _old doesn't)
                if current_list_price is not None and not financial.get("list_price_old"):
                    update_fields["financial.list_price_old"] = current_list_price
                
                if current_original_list_price is not None and not financial.get("original_list_price_old"):
                    update_fields["financial.original_list_price_old"] = current_original_list_price
                
                if current_status is not None and not property_doc.get("status_old"):
                    update_fields["status_old"] = current_status
                
                if current_mls_status is not None and not property_doc.get("mls_status_old"):
                    update_fields["mls_status_old"] = current_mls_status
                
                if current_listing_type is not None and not property_doc.get("listing_type_old"):
                    update_fields["listing_type_old"] = current_listing_type
                
                # Set timestamp if missing
                if not property_doc.get("_old_values_scraped_at"):
                    update_fields["_old_values_scraped_at"] = scraped_at_dt
                
                if update_fields:
                    if dry_run:
                        logger.info(
                            f"[DRY RUN] Would update property {property_id}: "
                            f"{len(update_fields)} fields"
                        )
                    else:
                        # Update property
                        result = await properties_collection.update_one(
                            {"property_id": property_id},
                            {"$set": update_fields}
                        )
                        
                        if result.modified_count > 0:
                            updated += 1
                            logger.debug(f"Updated property {property_id}: {list(update_fields.keys())}")
                
                processed += 1
                
                # Log progress every batch
                if processed % batch_size == 0:
                    batch_number += 1
                    logger.info(
                        f"Processed {processed}/{total_count} properties, "
                        f"updated {updated}, errors {errors}"
                    )
                
            except Exception as e:
                errors += 1
                logger.error(f"Error processing property {property_doc.get('property_id', 'unknown')}: {e}")
        
        # Final summary
        logger.info("=" * 80)
        logger.info("Migration Summary:")
        logger.info(f"  Total properties found: {total_count}")
        logger.info(f"  Properties processed: {processed}")
        logger.info(f"  Properties updated: {updated}")
        logger.info(f"  Errors: {errors}")
        if dry_run:
            logger.info("  Mode: DRY RUN (no changes made)")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        if 'client' in locals():
            client.close()
            logger.info("Disconnected from MongoDB")


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Migrate properties to populate _old values")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no changes will be made)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of properties to process in each batch (default: 100)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of properties to process (default: all)"
    )
    
    args = parser.parse_args()
    
    logger.info("Starting _old values migration...")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info(f"Batch size: {args.batch_size}")
    if args.limit:
        logger.info(f"Limit: {args.limit} properties")
    
    await migrate_properties(dry_run=args.dry_run, batch_size=args.batch_size, limit=args.limit)
    
    logger.info("Migration completed")


if __name__ == "__main__":
    asyncio.run(main())

