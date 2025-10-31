"""
Cleanup script to remove old and duplicate history entries to reduce MongoDB storage.

This script helps reduce storage bloat caused by:
1. Old history entries (older than specified days)
2. Duplicate history entries
3. Excess entries beyond per-property limits

Usage:
    python scripts/cleanup_history.py [--days-old 90] [--dry-run]
"""

import asyncio
import argparse
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

async def cleanup_old_history(days_old: int = 90, dry_run: bool = False):
    """Remove history entries older than specified days"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        cutoff = datetime.utcnow() - timedelta(days=days_old)
        
        logger.info(f"Finding history entries older than {days_old} days (before {cutoff.isoformat()})...")
        
        # Count old history entries
        history_count = await db.property_history.count_documents({
            "timestamp": {"$lt": cutoff}
        })
        
        # Count old change logs
        logs_count = await db.property_change_logs.count_documents({
            "timestamp": {"$lt": cutoff}
        })
        
        logger.info(f"Found {history_count} old history entries and {logs_count} old change log entries")
        
        if dry_run:
            logger.info("DRY RUN: Would delete these entries")
            return
        
        # Delete old history
        result1 = await db.property_history.delete_many({
            "timestamp": {"$lt": cutoff}
        })
        
        # Delete old change logs
        result2 = await db.property_change_logs.delete_many({
            "timestamp": {"$lt": cutoff}
        })
        
        logger.info(f"✓ Deleted {result1.deleted_count} history entries")
        logger.info(f"✓ Deleted {result2.deleted_count} change log entries")
        
        return result1.deleted_count + result2.deleted_count
        
    except Exception as e:
        logger.error(f"Error cleaning up old history: {e}")
        raise

async def cleanup_duplicates(dry_run: bool = False):
    """Remove duplicate history entries"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        logger.info("Finding duplicate history entries...")
        
        # Find duplicate price changes
        pipeline_price = [
            {
                "$match": {"change_type": "price_change"}
            },
            {
                "$group": {
                    "_id": {
                        "property_id": "$property_id",
                        "old_price": "$data.old_price",
                        "new_price": "$data.new_price",
                        "timestamp": "$timestamp"
                    },
                    "count": {"$sum": 1},
                    "ids": {"$push": "$_id"}
                }
            },
            {
                "$match": {"count": {"$gt": 1}}
            }
        ]
        
        duplicates_deleted = 0
        
        async for group in db.property_history.aggregate(pipeline_price):
            # Keep the first one, delete the rest
            ids_to_delete = group["ids"][1:]  # Skip first one
            count = len(ids_to_delete)
            
            logger.debug(f"Found {count} duplicates for property {group['_id']['property_id']} price change")
            
            if not dry_run and ids_to_delete:
                result = await db.property_history.delete_many({"_id": {"$in": ids_to_delete}})
                duplicates_deleted += result.deleted_count
        
        # Find duplicate status changes
        pipeline_status = [
            {
                "$match": {"change_type": "status_change"}
            },
            {
                "$group": {
                    "_id": {
                        "property_id": "$property_id",
                        "old_status": "$data.old_status",
                        "new_status": "$data.new_status",
                        "timestamp": "$timestamp"
                    },
                    "count": {"$sum": 1},
                    "ids": {"$push": "$_id"}
                }
            },
            {
                "$match": {"count": {"$gt": 1}}
            }
        ]
        
        async for group in db.property_history.aggregate(pipeline_status):
            ids_to_delete = group["ids"][1:]
            count = len(ids_to_delete)
            
            logger.debug(f"Found {count} duplicates for property {group['_id']['property_id']} status change")
            
            if not dry_run and ids_to_delete:
                result = await db.property_history.delete_many({"_id": {"$in": ids_to_delete}})
                duplicates_deleted += result.deleted_count
        
        # Find duplicate change logs
        pipeline_logs = [
            {
                "$group": {
                    "_id": {
                        "property_id": "$property_id",
                        "field": "$field",
                        "old_value": "$old_value",
                        "new_value": "$new_value",
                        "timestamp": "$timestamp"
                    },
                    "count": {"$sum": 1},
                    "ids": {"$push": "$_id"}
                }
            },
            {
                "$match": {"count": {"$gt": 1}}
            }
        ]
        
        async for group in db.property_change_logs.aggregate(pipeline_logs):
            ids_to_delete = group["ids"][1:]
            count = len(ids_to_delete)
            
            logger.debug(f"Found {count} duplicates for property {group['_id']['property_id']} change log")
            
            if not dry_run and ids_to_delete:
                result = await db.property_change_logs.delete_many({"_id": {"$in": ids_to_delete}})
                duplicates_deleted += result.deleted_count
        
        if dry_run:
            logger.info("DRY RUN: Would delete duplicate entries")
        else:
            logger.info(f"✓ Deleted {duplicates_deleted} duplicate entries")
        
        return duplicates_deleted
        
    except Exception as e:
        logger.error(f"Error cleaning up duplicates: {e}")
        raise

async def limit_entries_per_property(max_price_changes: int = 100, max_status_changes: int = 50, max_change_logs: int = 200, dry_run: bool = False):
    """Limit history entries per property to prevent unbounded growth"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        logger.info("Limiting entries per property...")
        
        total_deleted = 0
        
        # Get unique property IDs
        property_ids = await db.property_history.distinct("property_id")
        logger.info(f"Processing {len(property_ids)} properties...")
        
        for property_id in property_ids:
            # Limit price changes
            count = await db.property_history.count_documents({
                "property_id": property_id,
                "change_type": "price_change"
            })
            
            if count > max_price_changes:
                to_delete = count - max_price_changes
                oldest_cursor = db.property_history.find(
                    {"property_id": property_id, "change_type": "price_change"}
                ).sort("timestamp", 1).limit(to_delete)
                
                ids_to_delete = []
                async for entry in oldest_cursor:
                    ids_to_delete.append(entry["_id"])
                
                if not dry_run and ids_to_delete:
                    result = await db.property_history.delete_many({"_id": {"$in": ids_to_delete}})
                    total_deleted += result.deleted_count
                    logger.debug(f"Deleted {result.deleted_count} old price changes for property {property_id}")
            
            # Limit status changes
            count = await db.property_history.count_documents({
                "property_id": property_id,
                "change_type": "status_change"
            })
            
            if count > max_status_changes:
                to_delete = count - max_status_changes
                oldest_cursor = db.property_history.find(
                    {"property_id": property_id, "change_type": "status_change"}
                ).sort("timestamp", 1).limit(to_delete)
                
                ids_to_delete = []
                async for entry in oldest_cursor:
                    ids_to_delete.append(entry["_id"])
                
                if not dry_run and ids_to_delete:
                    result = await db.property_history.delete_many({"_id": {"$in": ids_to_delete}})
                    total_deleted += result.deleted_count
                    logger.debug(f"Deleted {result.deleted_count} old status changes for property {property_id}")
            
            # Limit change logs
            count = await db.property_change_logs.count_documents({
                "property_id": property_id
            })
            
            if count > max_change_logs:
                to_delete = count - max_change_logs
                oldest_cursor = db.property_change_logs.find(
                    {"property_id": property_id}
                ).sort("timestamp", 1).limit(to_delete)
                
                ids_to_delete = []
                async for entry in oldest_cursor:
                    ids_to_delete.append(entry["_id"])
                
                if not dry_run and ids_to_delete:
                    result = await db.property_change_logs.delete_many({"_id": {"$in": ids_to_delete}})
                    total_deleted += result.deleted_count
                    logger.debug(f"Deleted {result.deleted_count} old change logs for property {property_id}")
        
        if dry_run:
            logger.info("DRY RUN: Would limit entries per property")
        else:
            logger.info(f"✓ Limited entries per property, deleted {total_deleted} excess entries")
        
        return total_deleted
        
    except Exception as e:
        logger.error(f"Error limiting entries: {e}")
        raise

async def get_storage_stats():
    """Get storage statistics for history collections"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        # Get collection stats
        history_stats = await db.property_history.count_documents({})
        logs_stats = await db.property_change_logs.count_documents({})
        
        # Get database size
        db_stats = await db.command("dbStats")
        total_size_mb = db_stats.get("dataSize", 0) / (1024 * 1024)
        
        logger.info("\n" + "="*60)
        logger.info("STORAGE STATISTICS")
        logger.info("="*60)
        logger.info(f"Database: {database_name}")
        logger.info(f"Total database size: {total_size_mb:.2f} MB")
        logger.info(f"property_history entries: {history_stats:,}")
        logger.info(f"property_change_logs entries: {logs_stats:,}")
        logger.info("="*60 + "\n")
        
    except Exception as e:
        logger.error(f"Error getting storage stats: {e}")

async def main():
    parser = argparse.ArgumentParser(description="Cleanup MongoDB history collections to reduce storage")
    parser.add_argument("--days-old", type=int, default=90, help="Delete entries older than N days (default: 90)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without actually deleting")
    parser.add_argument("--skip-old", action="store_true", help="Skip cleaning old entries")
    parser.add_argument("--skip-duplicates", action="store_true", help="Skip cleaning duplicates")
    parser.add_argument("--skip-limits", action="store_true", help="Skip limiting entries per property")
    
    args = parser.parse_args()
    
    try:
        logger.info("Starting history cleanup...")
        
        # Show stats before
        await get_storage_stats()
        
        if not args.skip_old:
            logger.info("\n[1/3] Cleaning up old entries...")
            await cleanup_old_history(args.days_old, args.dry_run)
        
        if not args.skip_duplicates:
            logger.info("\n[2/3] Cleaning up duplicates...")
            await cleanup_duplicates(args.dry_run)
        
        if not args.skip_limits:
            logger.info("\n[3/3] Limiting entries per property...")
            await limit_entries_per_property(dry_run=args.dry_run)
        
        # Show stats after
        if not args.dry_run:
            logger.info("\nCleaning up complete! Final stats:")
            await get_storage_stats()
        
        logger.info("Done!")
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        raise
    finally:
        # Close connection
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    asyncio.run(main())

