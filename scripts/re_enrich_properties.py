"""
Re-enrich existing properties to use the new combined price/status history format.

This script:
1. Re-enriches properties to ensure _old values are properly set
2. Ensures change logs are properly structured
3. Works with the new get_property_history format that combines price and status changes

The new format:
- Combines price and status changes by timestamp
- Prioritizes list_price over price_per_sqft
- Each history entry contains both price_change and status_change (if both occurred)

Usage:
    python scripts/re_enrich_properties.py [--dry-run] [--batch-size 50] [--limit 1000]
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
from database import Database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def re_enrich_properties(dry_run: bool = False, batch_size: int = 50, limit: int = None):
    """
    Re-enrich properties to ensure they work with the new combined history format
    
    Args:
        dry_run: If True, only report what would be changed without making changes
        batch_size: Number of properties to process in each batch
        limit: Maximum number of properties to process (None for all)
    """
    try:
        # Initialize database connection
        db = Database()
        await db.connect()
        
        logger.info("Connected to MongoDB")
        
        # Find properties that need re-enrichment:
        # Properties that have been scraped but might be missing _old values or have outdated enrichment
        query = {
            "$or": [
                # Missing _old values
                {
                    "financial.list_price": {"$exists": True, "$ne": None},
                    "financial.list_price_old": {"$exists": False}
                },
                {
                    "status": {"$exists": True, "$ne": None},
                    "status_old": {"$exists": False}
                },
                # Missing price_per_sqft_old (new field)
                {
                    "financial.price_per_sqft": {"$exists": True, "$ne": None},
                    "financial.price_per_sqft_old": {"$exists": False}
                },
                # Missing _old_values_scraped_at
                {
                    "_old_values_scraped_at": {"$exists": False}
                }
            ]
        }
        
        total_count = await db.properties_collection.count_documents(query)
        logger.info(f"Found {total_count} properties that may need re-enrichment")
        
        if total_count == 0:
            logger.info("No properties need re-enrichment. Exiting.")
            return
        
        # Apply limit if specified
        if limit:
            total_count = min(total_count, limit)
            logger.info(f"Processing limited to {limit} properties")
        
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be made")
        
        # Process in batches
        processed = 0
        enriched = 0
        errors = 0
        batch_number = 0
        batch = []
        
        cursor = db.properties_collection.find(query).batch_size(batch_size)
        if limit:
            cursor = cursor.limit(limit)
        
        async for property_doc in cursor:
            try:
                property_id = property_doc.get("property_id")
                if not property_id:
                    continue
                
                batch.append(property_doc)
                
                # Process batch when it reaches batch_size
                if len(batch) >= batch_size:
                    if not dry_run:
                        # Re-enrich each property in the batch
                        for prop in batch:
                            try:
                                property_id = prop["property_id"]
                                # Get property as dict for enrichment
                                prop_dict = await db.enrichment_pipeline._get_property_dict(property_id)
                                if prop_dict:
                                    # Re-enrich the property (this will set _old values and update enrichment)
                                    await db.enrichment_pipeline.enrich_property(
                                        property_id=property_id,
                                        property_dict=prop_dict,
                                        existing_property=None,
                                        job_id=None
                                    )
                                    enriched += 1
                                    if enriched % 10 == 0:
                                        logger.info(f"Re-enriched {enriched} properties so far...")
                            except Exception as e:
                                errors += 1
                                logger.error(f"Error re-enriching property {prop.get('property_id', 'unknown')}: {e}")
                    
                    processed += len(batch)
                    batch_number += 1
                    logger.info(
                        f"Processed {processed}/{total_count} properties, "
                        f"enriched {enriched}, errors {errors}"
                    )
                    batch = []
                
            except Exception as e:
                errors += 1
                logger.error(f"Error processing property {property_doc.get('property_id', 'unknown')}: {e}")
        
        # Process remaining batch
        if batch:
            if not dry_run:
                for prop in batch:
                    try:
                        property_id = prop["property_id"]
                        prop_dict = await db.enrichment_pipeline._get_property_dict(property_id)
                        if prop_dict:
                            await db.enrichment_pipeline.enrich_property(
                                property_id=property_id,
                                property_dict=prop_dict,
                                existing_property=None,
                                job_id=None
                            )
                            enriched += 1
                    except Exception as e:
                        errors += 1
                        logger.error(f"Error re-enriching property {prop.get('property_id', 'unknown')}: {e}")
            else:
                # Dry run - just count
                enriched = len(batch)
            
            processed += len(batch)
        
        # Final summary
        logger.info("=" * 80)
        logger.info("Re-enrichment Summary:")
        logger.info(f"  Total properties found: {total_count}")
        logger.info(f"  Properties processed: {processed}")
        logger.info(f"  Properties enriched: {enriched}")
        logger.info(f"  Errors: {errors}")
        if dry_run:
            logger.info("  Mode: DRY RUN (no changes made)")
        logger.info("=" * 80)
        logger.info("")
        logger.info("Note: The new get_property_history() method will automatically combine")
        logger.info("price and status changes by timestamp. No additional migration needed.")
        
    except Exception as e:
        logger.error(f"Re-enrichment failed: {e}")
        raise
    finally:
        if 'db' in locals() and db.client:
            db.client.close()
            logger.info("Disconnected from MongoDB")


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Re-enrich existing properties to use new combined price/status history format"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no changes will be made)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of properties to process in each batch (default: 50)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of properties to process (default: all)"
    )
    
    args = parser.parse_args()
    
    logger.info("Starting property re-enrichment...")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info(f"Batch size: {args.batch_size}")
    if args.limit:
        logger.info(f"Limit: {args.limit} properties")
    logger.info("")
    
    await re_enrich_properties(dry_run=args.dry_run, batch_size=args.batch_size, limit=args.limit)
    
    logger.info("Re-enrichment completed")


if __name__ == "__main__":
    asyncio.run(main())

