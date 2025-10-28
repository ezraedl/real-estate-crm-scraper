"""
Test V2 Scoring System - Enrich a single property
Tests the new two-phase scoring system on a specific property.
"""

import asyncio
import sys
import os
import json
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from database import db
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_property_enrichment(address: str):
    """Test enrichment on a specific property by address"""
    try:
        # Connect to database
        connected = await db.connect()
        if not connected:
            logger.error("Failed to connect to database")
            return
        
        logger.info(f"Searching for property: {address}")
        
        # Find property by address
        # Try formatted_address first
        property_doc = await db.properties_collection.find_one({
            "address.formatted_address": {"$regex": address, "$options": "i"}
        })
        
        if not property_doc:
            # Try searching in street field
            property_doc = await db.properties_collection.find_one({
                "address.street": {"$regex": address, "$options": "i"}
            })
        
        if not property_doc:
            logger.error(f"Property not found for address: {address}")
            logger.info("Available search options:")
            logger.info("  1. Check if property exists in database")
            logger.info("  2. Try a different address format")
            return
        
        property_id = property_doc.get("property_id")
        logger.info(f"Found property: {property_id}")
        logger.info(f"  Address: {property_doc.get('address', {}).get('formatted_address', 'N/A')}")
        logger.info(f"  MLS ID: {property_doc.get('mls_id', 'N/A')}")
        logger.info(f"  Status: {property_doc.get('status', 'N/A')}")
        
        # Check existing enrichment
        existing_enrichment = property_doc.get('enrichment', {})
        if existing_enrichment:
            logger.info("\nExisting enrichment found:")
            motivated_seller = existing_enrichment.get('motivated_seller', {})
            if motivated_seller:
                logger.info(f"  Current score: {motivated_seller.get('score', 'N/A')}")
                logger.info(f"  Score version: {motivated_seller.get('score_version', 'v1 (old)')}")
                logger.info(f"  Has findings: {bool(motivated_seller.get('findings'))}")
        
        # Run enrichment
        logger.info("\n" + "="*60)
        logger.info("Running V2 enrichment...")
        logger.info("="*60)
        
        enrichment_data = await db.enrichment_pipeline.enrich_property(
            property_id=property_id,
            property_dict=property_doc,
            existing_property=None,
            job_id=None
        )
        
        # Display results
        logger.info("\n" + "="*60)
        logger.info("ENRICHMENT RESULTS")
        logger.info("="*60)
        
        motivated_seller = enrichment_data.get('motivated_seller', {})
        
        logger.info(f"\nScore (capped): {motivated_seller.get('score', 'N/A')}")
        logger.info(f"Score (uncapped): {motivated_seller.get('score_uncapped', 'N/A')}")
        logger.info(f"Score version: {motivated_seller.get('score_version', 'N/A')}")
        logger.info(f"Config hash: {motivated_seller.get('config_hash', 'N/A')}")
        logger.info(f"Confidence: {motivated_seller.get('confidence', 'N/A')}")
        logger.info(f"Last calculated: {motivated_seller.get('last_calculated', 'N/A')}")
        
        # Show findings
        findings = motivated_seller.get('findings', {})
        if findings:
            logger.info("\n" + "-"*60)
            logger.info("RAW FINDINGS (Phase 1 Detection)")
            logger.info("-"*60)
            
            # Explicit keywords
            explicit = findings.get('explicit_keywords', {})
            if explicit.get('detected', False):
                logger.info(f"\n✓ Explicit Keywords: DETECTED")
                logger.info(f"  Keywords found: {', '.join(explicit.get('keywords_found', []))}")
                logger.info(f"  Count: {explicit.get('count', 0)}")
                logger.info(f"  Raw score: {explicit.get('raw_score', 0)}")
            else:
                logger.info("\n✗ Explicit Keywords: Not detected")
            
            # Days on market
            dom = findings.get('days_on_market', {})
            logger.info(f"\nDays on Market:")
            logger.info(f"  Value: {dom.get('value', 0)} days")
            logger.info(f"  Threshold: {dom.get('threshold_met', 'N/A')}")
            logger.info(f"  Raw score: {dom.get('raw_score', 0)}")
            
            # Price reduction
            price = findings.get('price_reduction', {})
            if price.get('detected', False):
                logger.info(f"\n✓ Price Reduction: DETECTED")
                logger.info(f"  Max reduction: {price.get('max_percent', 0):.1f}%")
                logger.info(f"  Number of reductions: {len(price.get('reductions', []))}")
                logger.info(f"  Raw score: {price.get('raw_score', 0)}")
            else:
                logger.info("\n✗ Price Reduction: Not detected")
            
            # Distress keywords
            distress = findings.get('distress_keywords', {})
            keywords_found = distress.get('keywords_found', [])
            if keywords_found:
                logger.info(f"\n✓ Distress Keywords: DETECTED")
                logger.info(f"  Total keywords: {len(keywords_found)}")
                count_by_level = distress.get('count_by_level', {})
                logger.info(f"  By level: {count_by_level}")
                logger.info(f"  Raw score: {distress.get('raw_score', 0)}")
                logger.info(f"  Keywords: {', '.join(keywords_found[:10])}")  # Show first 10
                if len(keywords_found) > 10:
                    logger.info(f"  ... and {len(keywords_found) - 10} more")
            else:
                logger.info("\n✗ Distress Keywords: Not detected")
            
            # Special sale types
            special = findings.get('special_sale_types', {})
            types_found = special.get('types_found', [])
            if types_found:
                logger.info(f"\n✓ Special Sale Types: DETECTED")
                logger.info(f"  Types: {', '.join(types_found)}")
                logger.info(f"  Raw score: {special.get('raw_score', 0)}")
            else:
                logger.info("\n✗ Special Sale Types: Not detected")
            
            # Recent activity
            activity = findings.get('recent_activity', {})
            if activity.get('detected', False):
                logger.info(f"\n✓ Recent Activity: DETECTED")
                logger.info(f"  Days since update: {activity.get('days_since_update', 'N/A')}")
                logger.info(f"  Raw score: {activity.get('raw_score', 0)}")
            else:
                logger.info("\n✗ Recent Activity: Not detected")
        else:
            logger.warning("\nNo findings found - this property may not have been enriched with v2 system yet")
        
        # Show reasoning
        reasoning = motivated_seller.get('reasoning', [])
        if reasoning:
            logger.info("\n" + "-"*60)
            logger.info("SCORING REASONING")
            logger.info("-"*60)
            for reason in reasoning:
                logger.info(f"  • {reason}")
        
        # Show breakdown by component (if available)
        components = motivated_seller.get('components', {})
        if components:
            logger.info("\n" + "-"*60)
            logger.info("SCORE BREAKDOWN (Legacy Components)")
            logger.info("-"*60)
            for component_name, component_data in components.items():
                score = component_data.get('score', 0)
                max_score = component_data.get('max_possible', 0)
                if score > 0 or component_name == 'explicit_keywords':
                    logger.info(f"  {component_name}: {score}/{max_score} points")
                    details = component_data.get('details', [])
                    if details:
                        for detail in details[:2]:  # Show first 2 details
                            logger.info(f"    - {detail}")
        
        logger.info("\n" + "="*60)
        logger.info("Enrichment test completed!")
        logger.info("="*60)
        
        # Save detailed results to file
        output_file = f"enrichment_test_{property_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        output_path = os.path.join(os.path.dirname(__file__), output_file)
        with open(output_path, 'w') as f:
            json.dump({
                'property_id': property_id,
                'address': address,
                'enrichment': enrichment_data
            }, f, indent=2, default=str)
        
        logger.info(f"\nDetailed results saved to: {output_file}")
        
        return enrichment_data
        
    except Exception as e:
        logger.error(f"Error during enrichment test: {e}", exc_info=True)
        raise
    finally:
        await db.disconnect()

if __name__ == "__main__":
    test_address = "2916 E North St, Indianapolis, IN, 46201"
    result = asyncio.run(test_property_enrichment(test_address))

