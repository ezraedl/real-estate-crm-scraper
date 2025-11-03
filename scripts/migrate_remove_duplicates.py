"""
Migration script to remove duplicate enrichment data.

This script:
1. Removes price_history_summary from enrichment (now calculated dynamically)
2. Removes quick_access_flags from enrichment (using root-level flags only)
3. Updates enrichment_version to 2.0
4. Recalculates root-level flags from change_logs

Usage:
    python scripts/migrate_remove_duplicates.py [--dry-run] [--batch-size 100]
"""

import asyncio
import os
import sys
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse
import logging

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def recalculate_price_reduction_flag(property_doc):
    """Recalculate has_price_reduction flag from change_logs"""
    if not property_doc or "change_logs" not in property_doc:
        return False
    
    change_logs = property_doc.get("change_logs", [])
    price_fields = {'financial.list_price', 'financial.original_list_price'}
    
    for log in change_logs:
        if log.get("field") in price_fields:
            old_val = log.get("old_value")
            new_val = log.get("new_value")
            if old_val is not None and new_val is not None and new_val < old_val:
                return True
    
    return False


async def migrate_properties(dry_run: bool = False, batch_size: int = 100):
    """Migrate properties to remove duplicate enrichment data"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        logger.info("=" * 60)
        logger.info("Migration: Remove Duplicate Enrichment Data")
        logger.info("=" * 60)
        logger.info(f"Database: {database_name}")
        logger.info(f"Dry run: {dry_run}")
        logger.info(f"Batch size: {batch_size}")
        logger.info("")
        
        # Find properties with enrichment
        query = {"enrichment": {"$exists": True}}
        
        total_count = await db.properties.count_documents(query)
        logger.info(f"Found {total_count} properties with enrichment")
        
        if total_count == 0:
            logger.info("No properties to migrate")
            return
        
        # Count properties that need migration
        needs_migration_query = {
            "$or": [
                {"enrichment.price_history_summary": {"$exists": True}},
                {"enrichment.quick_access_flags": {"$exists": True}},
                {"enrichment.enrichment_version": {"$ne": "2.0"}}
            ]
        }
        
        needs_migration_count = await db.properties.count_documents({
            **query,
            **needs_migration_query
        })
        
        logger.info(f"Properties needing migration: {needs_migration_count}")
        logger.info("")
        
        if needs_migration_count == 0:
            logger.info("All properties already migrated!")
            return
        
        # Process using skip/limit to avoid cursor timeouts
        total_to_process = needs_migration_count
        processed = 0
        updated = 0
        skipped = 0
        skip = 0
        
        while processed < total_to_process:
            # Get batch
            batch = await db.properties.find({
                **query,
                **needs_migration_query
            }).skip(skip).limit(batch_size).to_list(length=batch_size)
            
            if not batch:
                break
            
            for property_doc in batch:
                property_id = property_doc.get("property_id")
                enrichment = property_doc.get("enrichment", {})
                
                # Check if migration needed
                has_price_summary = "price_history_summary" in enrichment
                has_quick_flags = "quick_access_flags" in enrichment
                version = enrichment.get("enrichment_version", "1.0")
                
                needs_update = has_price_summary or has_quick_flags or version != "2.0"
                
                if not needs_update:
                    skipped += 1
                    processed += 1
                    continue
                
                processed += 1
                
                # Build update document
                update_doc = {
                    "$unset": {},
                    "$set": {}
                }
                
                # Remove price_history_summary
                if has_price_summary:
                    update_doc["$unset"]["enrichment.price_history_summary"] = ""
                    logger.debug(f"  Property {property_id}: Removing price_history_summary")
                
                # Remove quick_access_flags
                if has_quick_flags:
                    update_doc["$unset"]["enrichment.quick_access_flags"] = ""
                    logger.debug(f"  Property {property_id}: Removing quick_access_flags")
                
                # Update enrichment_version
                if version != "2.0":
                    update_doc["$set"]["enrichment.enrichment_version"] = "2.0"
                    logger.debug(f"  Property {property_id}: Updating enrichment_version to 2.0")
                
                # Recalculate has_price_reduction flag from change_logs
                has_price_reduction = await recalculate_price_reduction_flag(property_doc)
                update_doc["$set"]["has_price_reduction"] = has_price_reduction
                
                # Recalculate is_motivated_seller flag
                motivated_score = enrichment.get("motivated_seller", {}).get("score", 0)
                is_motivated_seller = motivated_score >= 40
                update_doc["$set"]["is_motivated_seller"] = is_motivated_seller
                
                # Recalculate has_distress_signals flag
                distress_signals_list = enrichment.get("distress_signals", {}).get("signals_found", [])
                text_analysis = enrichment.get("text_analysis", {})
                has_distress_signals = len(distress_signals_list) > 0 or text_analysis.get("summary", {}).get("has_distress_signals", False)
                update_doc["$set"]["has_distress_signals"] = has_distress_signals
                
                # Remove empty $unset if needed
                if not update_doc["$unset"]:
                    del update_doc["$unset"]
                
                if dry_run:
                    logger.info(f"  DRY RUN: Would update property {property_id}")
                    if has_price_summary:
                        logger.info(f"    - Remove price_history_summary")
                    if has_quick_flags:
                        logger.info(f"    - Remove quick_access_flags")
                    if version != "2.0":
                        logger.info(f"    - Update enrichment_version to 2.0")
                    logger.info(f"    - has_price_reduction: {has_price_reduction}")
                    logger.info(f"    - is_motivated_seller: {is_motivated_seller}")
                    logger.info(f"    - has_distress_signals: {has_distress_signals}")
                else:
                    # Update property
                    await db.properties.update_one(
                        {"property_id": property_id},
                        update_doc
                    )
                    updated += 1
                
                if processed % batch_size == 0:
                    logger.info(f"Processed {processed} properties ({updated} updated, {skipped} skipped)...")
            
            skip += batch_size
            
            # Check if we should continue
            remaining = await db.properties.count_documents({
                **query,
                **needs_migration_query,
                "_id": {"$gt": batch[-1]["_id"]}
            })
            
            if remaining == 0:
                break
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("Migration Summary")
        logger.info("=" * 60)
        logger.info(f"Total properties processed: {processed}")
        logger.info(f"Properties updated: {updated}")
        logger.info(f"Properties skipped (already migrated): {skipped}")
        
        if dry_run:
            logger.info("")
            logger.info("DRY RUN: No changes were made")
        else:
            logger.info("")
            logger.info("✅ Migration completed successfully!")
        
        return updated
        
    except Exception as e:
        logger.error(f"Error during migration: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        client.close()


async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate properties to remove duplicate enrichment data")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode (no changes)")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing")
    
    args = parser.parse_args()
    
    try:
        await migrate_properties(dry_run=args.dry_run, batch_size=args.batch_size)
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

