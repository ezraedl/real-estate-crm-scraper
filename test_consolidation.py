"""
Test script for consolidation changes:
1. Test enrichment API endpoints
2. Test history retrieval from change_logs
3. Test price reduction filtering
"""

import asyncio
import os
import sys
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse
from datetime import datetime, timedelta
import json
import logging

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from config import settings
from database import db
from services.property_enrichment import PropertyEnrichmentPipeline
from services.history_tracker import HistoryTracker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_enrichment_pipeline(db_instance):
    """Test that enrichment pipeline works without property_history"""
    logger.info("=" * 60)
    logger.info("TEST 1: Enrichment Pipeline")
    logger.info("=" * 60)
    
    try:
        pipeline = PropertyEnrichmentPipeline(db_instance)
        await pipeline.initialize()
        
        # Find a property with enrichment
        property_doc = await db_instance['properties'].find_one(
            {"enrichment": {"$exists": True}},
            {"property_id": 1, "enrichment": 1, "change_logs": 1}
        )
        
        if not property_doc:
            logger.warning("No property with enrichment found - skipping test")
            return True
        
        property_id = property_doc.get("property_id")
        logger.info(f"Testing with property: {property_id}")
        
        # Check enrichment structure
        enrichment = property_doc.get("enrichment", {})
        
        # Should NOT have price_history_summary
        if "price_history_summary" in enrichment:
            logger.error(f"❌ FAIL: price_history_summary still exists in enrichment!")
            return False
        else:
            logger.info(f"✅ PASS: price_history_summary removed from enrichment")
        
        # Should NOT have quick_access_flags (check enrichment_version 2.0)
        # Note: Old enrichments may still have it, but new ones (v2.0) should not
        enrichment_version = enrichment.get("enrichment_version", "1.0")
        if "quick_access_flags" in enrichment:
            if enrichment_version == "2.0":
                logger.error(f"❌ FAIL: quick_access_flags exists in v2.0 enrichment!")
                return False
            else:
                logger.warning(f"⚠️  WARN: quick_access_flags exists in old enrichment (v{enrichment_version}) - this is expected for old data")
        else:
            logger.info(f"✅ PASS: quick_access_flags removed from enrichment")
        
        # Should have enrichment_version 2.0
        if enrichment.get("enrichment_version") == "2.0":
            logger.info(f"✅ PASS: enrichment_version is 2.0")
        else:
            logger.warning(f"⚠️  WARN: enrichment_version is {enrichment.get('enrichment_version')}, expected 2.0")
        
        # Check root-level flags exist
        if "is_motivated_seller" in property_doc:
            logger.info(f"✅ PASS: is_motivated_seller flag exists at root level")
        if "has_price_reduction" in property_doc:
            logger.info(f"✅ PASS: has_price_reduction flag exists at root level")
        if "has_distress_signals" in property_doc:
            logger.info(f"✅ PASS: has_distress_signals flag exists at root level")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ ERROR in test_enrichment_pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_history_retrieval(db_instance):
    """Test that history retrieval works from change_logs"""
    logger.info("=" * 60)
    logger.info("TEST 2: History Retrieval from change_logs")
    logger.info("=" * 60)
    
    try:
        tracker = HistoryTracker(db_instance)
        
        # Find a property with change_logs
        property_doc = await db_instance['properties'].find_one(
            {"change_logs": {"$exists": True, "$ne": []}},
            {"property_id": 1, "change_logs": 1}
        )
        
        if not property_doc:
            logger.warning("No property with change_logs found - skipping test")
            return True
        
        property_id = property_doc.get("property_id")
        logger.info(f"Testing with property: {property_id}")
        
        # Test get_price_history
        price_history = await tracker.get_price_history(property_id, limit=10)
        logger.info(f"✅ PASS: get_price_history returned {len(price_history)} entries")
        
        # Test get_status_history
        status_history = await tracker.get_status_history(property_id, limit=10)
        logger.info(f"✅ PASS: get_status_history returned {len(status_history)} entries")
        
        # Test get_change_logs
        change_logs = await tracker.get_property_change_logs(property_id, limit=10)
        logger.info(f"✅ PASS: get_property_change_logs returned {len(change_logs)} entries")
        
        # Verify price_history format is correct
        if price_history:
            entry = price_history[0]
            if "data" in entry and "old_price" in entry.get("data", {}):
                logger.info(f"✅ PASS: price_history format is correct")
            else:
                logger.error(f"❌ FAIL: price_history format is incorrect")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ ERROR in test_history_retrieval: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_price_reduction_flag(db_instance):
    """Test that has_price_reduction flag is calculated correctly"""
    logger.info("=" * 60)
    logger.info("TEST 3: Price Reduction Flag Calculation")
    logger.info("=" * 60)
    
    try:
        # Find properties with price reductions in change_logs
        cursor = db_instance['properties'].find(
            {
                "change_logs": {
                    "$elemMatch": {
                        "field": "financial.list_price",
                        "change_type": "decreased"
                    }
                }
            },
            {"property_id": 1, "has_price_reduction": 1, "change_logs": 1}
        ).limit(5)
        
        test_count = 0
        passed_count = 0
        
        async for property_doc in cursor:
            property_id = property_doc.get("property_id")
            has_flag = property_doc.get("has_price_reduction", False)
            
            # Check if change_logs actually has price reduction
            change_logs = property_doc.get("change_logs", [])
            has_reduction = False
            for log in change_logs:
                if log.get("field") == "financial.list_price" and log.get("change_type") == "decreased":
                    old_val = log.get("old_value")
                    new_val = log.get("new_value")
                    if old_val is not None and new_val is not None and new_val < old_val:
                        has_reduction = True
                        break
            
            test_count += 1
            if has_reduction == has_flag:
                passed_count += 1
                logger.info(f"✅ PASS: Property {property_id} - flag matches change_logs")
            else:
                logger.warning(f"⚠️  WARN: Property {property_id} - flag={has_flag}, has_reduction={has_reduction}")
        
        if test_count > 0:
            logger.info(f"Price reduction flag test: {passed_count}/{test_count} passed")
            return passed_count == test_count
        else:
            logger.warning("No properties with price reductions found")
            return True
        
    except Exception as e:
        logger.error(f"❌ ERROR in test_price_reduction_flag: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run all tests"""
    logger.info("Starting Consolidation Tests...")
    logger.info("")
    
    # Connect to MongoDB
    parsed_uri = urlparse(settings.MONGODB_URI)
    database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
    
    client = AsyncIOMotorClient(settings.MONGODB_URI)
    db_test = client[database_name]
    
    # Update functions to use db_test
    async def test_enrichment_with_db():
        return await test_enrichment_pipeline(db_test)
    
    async def test_history_with_db():
        return await test_history_retrieval(db_test)
    
    async def test_flag_with_db():
        return await test_price_reduction_flag(db_test)
    
    results = []
    
    try:
        results.append(("Enrichment Pipeline", await test_enrichment_with_db()))
        results.append(("History Retrieval", await test_history_with_db()))
        results.append(("Price Reduction Flag", await test_flag_with_db()))
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("TEST SUMMARY")
        logger.info("=" * 60)
        
        for test_name, passed in results:
            status = "✅ PASS" if passed else "❌ FAIL"
            logger.info(f"{status}: {test_name}")
        
        total_passed = sum(1 for _, passed in results if passed)
        total_tests = len(results)
        logger.info("")
        logger.info(f"Total: {total_passed}/{total_tests} tests passed")
        
        if total_passed == total_tests:
            logger.info("🎉 All tests passed!")
            return 0
        else:
            logger.error("❌ Some tests failed")
            return 1
            
    finally:
        client.close()

if __name__ == "__main__":
    import logging
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

