"""
Migration script to convert existing change_logs to consolidated format.

This script converts change_logs from old format (separate entries per field) 
to new format (grouped by date - multiple fields on same date = one entry).

Old format:
  [
    {"field": "financial.list_price", "old_value": 100, "new_value": 90, "timestamp": "2025-01-01"},
    {"field": "status", "old_value": "FOR_SALE", "new_value": "PENDING", "timestamp": "2025-01-01"},
    {"field": "listing_type", "old_value": "for_sale", "new_value": "for_rent", "timestamp": "2025-01-01"}
  ]

New format:
  [
    {
      "field": None,
      "field_changes": {
        "financial.list_price": {"old_value": 100, "new_value": 90, "change_type": "decreased"},
        "status": {"old_value": "FOR_SALE", "new_value": "PENDING", "change_type": "modified"},
        "listing_type": {"old_value": "for_sale", "new_value": "for_rent", "change_type": "modified"}
      },
      "timestamp": "2025-01-01T00:00:00",
      "change_count": 3
    }
  ]

Usage:
    python scripts/migrate_change_logs_to_consolidated.py [--dry-run] [--batch-size 100]
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse
import logging

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Fields we track in change logs
TRACKED_FIELDS = {
    'financial.list_price',
    'financial.original_list_price',
    'financial.price_per_sqft',
    'status',
    'mls_status',
    'listing_type'
}


def normalize_timestamp(ts):
    """Normalize timestamp to date (remove time component)"""
    if ts is None:
        return datetime.utcnow().date()
    if isinstance(ts, datetime):
        return ts.date()
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            if dt.tzinfo:
                dt = dt.replace(tzinfo=None)
            return dt.date()
        except:
            return datetime.utcnow().date()
    return datetime.utcnow().date()


def convert_to_consolidated_format(change_logs):
    """
    Convert change_logs from old format to new consolidated format.
    
    Groups changes by date and consolidates multiple fields on same date.
    """
    if not change_logs:
        return []
    
    # Group changes by normalized date
    changes_by_date = defaultdict(list)
    
    for log in change_logs:
        # Skip if already in consolidated format
        if log.get("field") is None and isinstance(log.get("field_changes"), dict):
            # Already consolidated - keep as is
            changes_by_date[normalize_timestamp(log.get("timestamp"))].append(log)
            continue
        
        # Old format - group by date
        field = log.get("field")
        if field and field in TRACKED_FIELDS:
            timestamp = log.get("timestamp")
            normalized_date = normalize_timestamp(timestamp)
            changes_by_date[normalized_date].append(log)
    
    # Build consolidated entries
    consolidated_logs = []
    cutoff_date = datetime.utcnow() - timedelta(days=90)  # TTL: 90 days
    
    for change_date, date_changes in changes_by_date.items():
        change_datetime = datetime.combine(change_date, datetime.min.time())
        
        # Check TTL
        if change_datetime < cutoff_date:
            continue
        
        # Separate already-consolidated vs old format
        already_consolidated = [log for log in date_changes if log.get("field") is None]
        old_format = [log for log in date_changes if log.get("field") is not None]
        
        if old_format:
            # Group old format changes by field
            fields_by_field = defaultdict(list)
            for log in old_format:
                field = log.get("field")
                if field:
                    fields_by_field[field].append(log)
            
            # Get the most recent change per field (in case of duplicates)
            unique_field_changes = {}
            for field, field_logs in fields_by_field.items():
                # Sort by timestamp (newest first) and take first
                field_logs.sort(key=lambda x: x.get("timestamp", datetime.min), reverse=True)
                latest_log = field_logs[0]
                unique_field_changes[field] = {
                    "old_value": latest_log.get("old_value"),
                    "new_value": latest_log.get("new_value"),
                    "change_type": latest_log.get("change_type", "modified")
                }
            
            # Create consolidated entry
            if len(unique_field_changes) == 1:
                # Single field - use simple format (backward compatible)
                field = list(unique_field_changes.keys())[0]
                change_data = unique_field_changes[field]
                consolidated_entry = {
                    "field": field,
                    "old_value": change_data.get("old_value"),
                    "new_value": change_data.get("new_value"),
                    "change_type": change_data.get("change_type"),
                    "timestamp": change_datetime,
                    "job_id": latest_log.get("job_id"),
                    "created_at": latest_log.get("created_at") or datetime.utcnow()
                }
            else:
                # Multiple fields - use consolidated format
                # Get metadata from most recent log
                latest_log = max(old_format, key=lambda x: x.get("timestamp", datetime.min))
                consolidated_entry = {
                    "field": None,
                    "field_changes": unique_field_changes,
                    "timestamp": change_datetime,
                    "job_id": latest_log.get("job_id"),
                    "created_at": latest_log.get("created_at") or datetime.utcnow(),
                    "change_count": len(unique_field_changes)
                }
            
            consolidated_logs.append(consolidated_entry)
        
        # Add already consolidated entries (preserve as-is)
        consolidated_logs.extend(already_consolidated)
    
    # Sort by timestamp (newest first) and limit to 200
    consolidated_logs.sort(key=lambda x: x.get("timestamp", datetime.min), reverse=True)
    consolidated_logs = consolidated_logs[:200]
    
    # Filter out entries older than 90 days
    consolidated_logs = [
        log for log in consolidated_logs
        if not log.get("timestamp") or (isinstance(log.get("timestamp"), datetime) and log.get("timestamp") >= cutoff_date)
    ]
    
    return consolidated_logs


async def migrate_change_logs(dry_run: bool = False, batch_size: int = 100):
    """Migrate all change_logs to consolidated format"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        logger.info("=" * 60)
        logger.info("Migration: Convert change_logs to Consolidated Format")
        logger.info("=" * 60)
        logger.info(f"Database: {database_name}")
        logger.info(f"Dry run: {dry_run}")
        logger.info(f"Batch size: {batch_size}")
        logger.info("")
        
        # Find properties with change_logs
        query = {"change_logs": {"$exists": True, "$ne": []}}
        
        total_count = await db['properties'].count_documents(query)
        logger.info(f"Found {total_count} properties with change_logs")
        
        if total_count == 0:
            logger.info("No properties to migrate")
            return
        
        # Process in batches
        skip = 0
        processed = 0
        updated = 0
        skipped = 0
        total_changes_consolidated = 0
        
        while skip < total_count:
            # Get batch
            batch = await db['properties'].find(query).skip(skip).limit(batch_size).to_list(length=batch_size)
            
            if not batch:
                break
            
            for property_doc in batch:
                property_id = property_doc.get("property_id")
                change_logs = property_doc.get("change_logs", [])
                
                if not change_logs:
                    skipped += 1
                    processed += 1
                    continue
                
                # Check if already in new format (has consolidated entries)
                has_consolidated = any(
                    log.get("field") is None and isinstance(log.get("field_changes"), dict)
                    for log in change_logs
                )
                
                # Convert to consolidated format
                consolidated_logs = convert_to_consolidated_format(change_logs)
                
                # Check if conversion resulted in changes
                original_count = len(change_logs)
                new_count = len(consolidated_logs)
                
                # Compare if structure changed
                structure_changed = False
                if original_count != new_count:
                    structure_changed = True
                elif not has_consolidated and any(
                    log.get("field") is None for log in consolidated_logs
                ):
                    structure_changed = True
                else:
                    # Check if entries were consolidated
                    dates_old = {normalize_timestamp(log.get("timestamp")) for log in change_logs}
                    dates_new = {normalize_timestamp(log.get("timestamp")) for log in consolidated_logs}
                    if dates_old != dates_new:
                        structure_changed = True
                
                if not structure_changed:
                    skipped += 1
                    processed += 1
                    continue
                
                processed += 1
                
                # Count consolidated changes
                changes_count = sum(
                    entry.get("change_count", 1) for entry in consolidated_logs
                    if entry.get("field") is None
                )
                if changes_count > 0:
                    total_changes_consolidated += changes_count
                
                if dry_run:
                    logger.info(f"  DRY RUN: Would update property {property_id}")
                    logger.info(f"    - Old format: {original_count} entries")
                    logger.info(f"    - New format: {new_count} entries")
                    if new_count < original_count:
                        logger.info(f"    - Consolidated: {original_count - new_count} entries merged")
                else:
                    # Update property with consolidated change_logs
                    await db['properties'].update_one(
                        {"property_id": property_id},
                        {
                            "$set": {
                                "change_logs": consolidated_logs,
                                "change_logs_updated_at": datetime.utcnow(),
                                "change_logs_format": "consolidated"  # Mark as migrated
                            }
                        }
                    )
                    updated += 1
                    
                    if processed % batch_size == 0:
                        logger.info(f"Processed {processed} properties ({updated} updated, {skipped} skipped)...")
            
            skip += batch_size
            
            # Check if we should continue
            remaining = await db['properties'].count_documents({
                **query,
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
        logger.info(f"Properties skipped (already migrated or no changes): {skipped}")
        logger.info(f"Total changes consolidated: {total_changes_consolidated}")
        
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
    
    parser = argparse.ArgumentParser(description="Migrate change_logs to consolidated format")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode (no changes)")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing")
    
    args = parser.parse_args()
    
    try:
        await migrate_change_logs(dry_run=args.dry_run, batch_size=args.batch_size)
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

