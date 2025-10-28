"""
Migration Script: Convert all existing enrichments to v2 scoring system

This script backfills the 'findings' field for all existing properties
by re-running detection on their current data and calculating new scores.

Run this immediately after deploying the new scoring system.
"""

import asyncio
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from database import db
from services.property_enrichment import PropertyEnrichmentPipeline
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def migrate_all_properties():
    """Migrate all properties to v2 scoring system"""
    try:
        # Connect to database
        connected = await db.connect()
        if not connected:
            logger.error("Failed to connect to database")
            return
        
        logger.info("Starting migration to v2 scoring system...")
        start_time = datetime.utcnow()
        
        # Count total properties
        total_count = await db.properties_collection.count_documents({})
        logger.info(f"Total properties to migrate: {total_count}")
        
        # Process properties
        total_processed = 0
        total_migrated = 0
        total_errors = 0
        errors = []
        
        batch_size = 100
        property_ids = []
        
        # Get all properties
        cursor = db.properties_collection.find({}, {"property_id": 1})
        
        async for doc in cursor:
            property_ids.append(doc["property_id"])
            total_processed += 1
            
            if len(property_ids) >= batch_size:
                # Process batch
                for prop_id in property_ids:
                    try:
                        # Get property data
                        property_dict = await db.enrichment_pipeline._get_property_dict(prop_id)
                        
                        if not property_dict:
                            logger.warning(f"Property {prop_id} not found, skipping")
                            continue
                        
                        # Check if already has findings (already migrated)
                        enrichment = property_dict.get('enrichment', {})
                        motivated_seller = enrichment.get('motivated_seller', {})
                        if motivated_seller.get('findings'):
                            logger.debug(f"Property {prop_id} already has findings, skipping")
                            continue
                        
                        # Run full enrichment to generate findings
                        await db.enrichment_pipeline.enrich_property(
                            property_id=prop_id,
                            property_dict=property_dict,
                            existing_property=None,
                            job_id=None
                        )
                        
                        total_migrated += 1
                        
                        if total_migrated % 100 == 0:
                            logger.info(f"Migrated {total_migrated} properties (processed {total_processed}/{total_count})")
                    
                    except Exception as e:
                        total_errors += 1
                        error_msg = f"Property {prop_id}: {str(e)}"
                        errors.append(error_msg)
                        logger.error(error_msg)
                
                property_ids = []
        
        # Process remaining
        for prop_id in property_ids:
            try:
                property_dict = await db.enrichment_pipeline._get_property_dict(prop_id)
                
                if not property_dict:
                    continue
                
                enrichment = property_dict.get('enrichment', {})
                motivated_seller = enrichment.get('motivated_seller', {})
                if motivated_seller.get('findings'):
                    continue
                
                await db.enrichment_pipeline.enrich_property(
                    property_id=prop_id,
                    property_dict=property_dict,
                    existing_property=None,
                    job_id=None
                )
                
                total_migrated += 1
            except Exception as e:
                total_errors += 1
                error_msg = f"Property {prop_id}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
        
        completed_at = datetime.utcnow()
        duration = (completed_at - start_time).total_seconds()
        
        logger.info("=" * 60)
        logger.info("Migration Complete!")
        logger.info(f"Total processed: {total_processed}")
        logger.info(f"Total migrated: {total_migrated}")
        logger.info(f"Total errors: {total_errors}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("=" * 60)
        
        if errors:
            logger.warning(f"First 10 errors:")
            for error in errors[:10]:
                logger.warning(f"  - {error}")
        
        return {
            "total_processed": total_processed,
            "total_migrated": total_migrated,
            "total_errors": total_errors,
            "duration_seconds": duration,
            "errors": errors[:50]
        }
        
    except Exception as e:
        logger.error(f"Fatal error in migration: {e}")
        raise
    finally:
        await db.disconnect()

if __name__ == "__main__":
    result = asyncio.run(migrate_all_properties())
    print("\nMigration result:", result)

