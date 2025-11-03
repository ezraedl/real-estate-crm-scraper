"""
Utility script to query properties filtered by change_logs.

Examples:
- Properties with 3+ price reductions
- Properties with status changes
- Properties with listing_type changes

Usage:
    python scripts/query_properties_by_changes.py --field financial.list_price --change-type decreased --min-count 3
"""

import asyncio
import os
import sys
import argparse
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse
from config import settings

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def find_properties_with_changes(field: str, change_type: str = None, min_count: int = 1, limit: int = 100):
    """
    Find properties with specific change_logs criteria
    
    Args:
        field: Field name (e.g., "financial.list_price", "status", "listing_type")
        change_type: Change type filter (e.g., "decreased", "increased", "modified")
        min_count: Minimum number of matching changes
        limit: Max properties to return
    """
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        # Build filter condition
        filter_cond = {"$eq": ["$$log.field", field]}
        
        if change_type:
            filter_cond = {
                "$and": [
                    {"$eq": ["$$log.field", field]},
                    {"$eq": ["$$log.change_type", change_type]}
                ]
            }
        
        # Query properties with embedded change_logs
        query = {
            "$expr": {
                "$gte": [
                    {"$size": {
                        "$filter": {
                            "input": "$change_logs",
                            "as": "log",
                            "cond": filter_cond
                        }
                    }},
                    min_count
                ]
            }
        }
        
        logger.info(f"Querying properties with:")
        logger.info(f"  Field: {field}")
        if change_type:
            logger.info(f"  Change type: {change_type}")
        logger.info(f"  Min count: {min_count}")
        
        cursor = db.properties.find(query).limit(limit)
        
        properties = []
        async for prop in cursor:
            property_id = prop.get("property_id")
            change_logs = prop.get("change_logs", [])
            
            # Filter matching change_logs
            matching_logs = [
                log for log in change_logs
                if log.get("field") == field and (not change_type or log.get("change_type") == change_type)
            ]
            
            properties.append({
                "property_id": property_id,
                "address": prop.get("address", {}).get("formatted_address"),
                "mls_id": prop.get("mls_id"),
                "status": prop.get("status"),
                "list_price": prop.get("financial", {}).get("list_price"),
                "matching_changes_count": len(matching_logs),
                "matching_changes": matching_logs[:5]  # Show first 5
            })
        
        logger.info(f"\nFound {len(properties)} properties matching criteria")
        
        # Show summary
        for prop in properties[:10]:  # Show first 10
            logger.info(f"\n  Property: {prop['address']}")
            logger.info(f"    MLS ID: {prop['mls_id']}")
            logger.info(f"    Status: {prop['status']}")
            logger.info(f"    List Price: ${prop['list_price']:,.0f}" if prop['list_price'] else "    List Price: N/A")
            logger.info(f"    Matching changes: {prop['matching_changes_count']}")
        
        if len(properties) > 10:
            logger.info(f"\n  ... and {len(properties) - 10} more properties")
        
        return properties
        
    except Exception as e:
        logger.error(f"Error querying properties: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        client.close()


async def find_properties_with_price_reductions(min_reductions: int = 3, limit: int = 100):
    """Find properties with N+ price reductions"""
    return await find_properties_with_changes(
        field="financial.list_price",
        change_type="decreased",
        min_count=min_reductions,
        limit=limit
    )


async def main():
    parser = argparse.ArgumentParser(description="Query properties filtered by change_logs")
    parser.add_argument("--field", type=str, help="Field name (e.g., financial.list_price, status, listing_type)")
    parser.add_argument("--change-type", type=str, help="Change type (decreased, increased, modified)")
    parser.add_argument("--min-count", type=int, default=1, help="Minimum number of matching changes")
    parser.add_argument("--price-reductions", type=int, help="Find properties with N+ price reductions (shortcut)")
    parser.add_argument("--limit", type=int, default=100, help="Max properties to return")
    
    args = parser.parse_args()
    
    try:
        if args.price_reductions:
            logger.info(f"Finding properties with {args.price_reductions}+ price reductions...")
            await find_properties_with_price_reductions(min_reductions=args.price_reductions, limit=args.limit)
        elif args.field:
            await find_properties_with_changes(
                field=args.field,
                change_type=args.change_type,
                min_count=args.min_count,
                limit=args.limit
            )
        else:
            parser.print_help()
            logger.info("\nExample: python scripts/query_properties_by_changes.py --price-reductions 3")
            
    except Exception as e:
        logger.error(f"Error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())

