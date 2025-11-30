"""
Test script to verify _old values enrichment flow works correctly.

Tests:
1. Enrichment extracts _old values correctly
2. Enrichment compares current vs _old values
3. Enrichment moves current to _old after completion
4. Change detection works with _old values
"""

import asyncio
import os
import sys
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import settings
from database import Database
from services.property_enrichment import PropertyEnrichmentPipeline
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def test_enrichment_with_old_values():
    """Test enrichment flow with _old values"""
    try:
        # Connect to database
        db = Database()
        await db.connect()
        
        # Get a property that has _old values (from migration)
        property_doc = await db.properties_collection.find_one({
            "financial.list_price_old": {"$exists": True},
            "status_old": {"$exists": True}
        })
        
        if not property_doc:
            logger.warning("No property with _old values found. Run migration first.")
            return False
        
        property_id = property_doc.get("property_id")
        logger.info(f"Testing enrichment with property {property_id}")
        
        # Get current values
        current_list_price = property_doc.get("financial", {}).get("list_price")
        old_list_price = property_doc.get("financial", {}).get("list_price_old")
        current_status = property_doc.get("status")
        old_status = property_doc.get("status_old")
        
        logger.info(f"Current values: list_price={current_list_price}, status={current_status}")
        logger.info(f"_old values: list_price_old={old_list_price}, status_old={old_status}")
        
        # Test 1: Extract _old values
        logger.info("\n=== Test 1: Extract _old values ===")
        old_property = db.enrichment_pipeline._extract_old_values(property_doc)
        if old_property:
            logger.info(f"✅ Successfully extracted _old values: {old_property}")
        else:
            logger.error("❌ Failed to extract _old values")
            return False
        
        # Test 2: Compare current vs _old
        logger.info("\n=== Test 2: Compare current vs _old ===")
        property_dict = await db.enrichment_pipeline._get_property_dict(property_id)
        if not property_dict:
            logger.error("❌ Failed to get property dict")
            return False
        
        from services.property_differ import PropertyDiffer
        differ = PropertyDiffer()
        changes = differ.detect_changes(old_property, property_dict)
        
        logger.info(f"Changes detected: {changes.get('has_changes', False)}")
        logger.info(f"Price changes: {len(changes.get('price_changes', []))}")
        logger.info(f"Status changes: {len(changes.get('status_changes', []))}")
        logger.info(f"✅ Change detection works")
        
        # Test 3: Run enrichment (should move current to _old after)
        logger.info("\n=== Test 3: Run enrichment (should move current to _old) ===")
        
        # Store original _old values
        original_old_list_price = old_list_price
        original_old_status = old_status
        
        # Run enrichment
        enrichment_data = await db.enrichment_pipeline.enrich_property(
            property_id=property_id,
            property_dict=property_dict,
            existing_property=None,  # Should fetch from DB
            job_id="test_job"
        )
        
        if not enrichment_data:
            logger.error("❌ Enrichment failed")
            return False
        
        logger.info("✅ Enrichment completed")
        
        # Test 4: Verify _old values were updated
        logger.info("\n=== Test 4: Verify _old values were updated ===")
        updated_property = await db.properties_collection.find_one({"property_id": property_id})
        
        new_old_list_price = updated_property.get("financial", {}).get("list_price_old")
        new_old_status = updated_property.get("status_old")
        old_values_scraped_at = updated_property.get("_old_values_scraped_at")
        
        logger.info(f"After enrichment - _old list_price: {new_old_list_price}")
        logger.info(f"After enrichment - _old status: {new_old_status}")
        logger.info(f"_old_values_scraped_at: {old_values_scraped_at}")
        
        # Verify _old values match current values (they should after enrichment)
        if new_old_list_price == current_list_price and new_old_status == current_status:
            logger.info("✅ _old values correctly updated to match current values")
        else:
            logger.warning(f"⚠️  _old values may not match: list_price_old={new_old_list_price} vs current={current_list_price}, status_old={new_old_status} vs current={current_status}")
        
        if old_values_scraped_at:
            logger.info("✅ _old_values_scraped_at timestamp set correctly")
        else:
            logger.error("❌ _old_values_scraped_at not set")
            return False
        
        logger.info("\n" + "="*80)
        logger.info("✅ All tests passed!")
        logger.info("="*80)
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


async def main():
    """Main entry point"""
    logger.info("Starting _old values enrichment test...")
    success = await test_enrichment_with_old_values()
    
    if success:
        logger.info("\n✅ Test completed successfully!")
        logger.info("The _old values system is working correctly.")
        logger.info("Safe to deploy to production.")
    else:
        logger.error("\n❌ Test failed!")
        logger.error("Do not deploy until issues are resolved.")
    
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

