"""
History Tracker Service

Records property history (price changes, status changes) and detailed change logs
in dedicated MongoDB collections for auditing and analytics.

Change logs are now embedded in the properties collection for better efficiency.
Only tracks price, status, and listing_type changes.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging
from motor.motor_asyncio import AsyncIOMotorDatabase

logger = logging.getLogger(__name__)

# Fields we track in change logs (only price, status, listing_type)
TRACKED_CHANGE_LOG_FIELDS = {
    'financial.list_price',
    'financial.original_list_price',
    'financial.price_per_sqft',
    'status',
    'mls_status',
    'listing_type'
}

class HistoryTracker:
    """Manages property history and change logging"""
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        # Note: property_history collection still exists but is no longer actively used
        # We now use embedded change_logs in properties collection as single source of truth
        # Keeping reference for backward compatibility and potential future cleanup
        # Access collection using bracket notation for Motor compatibility
        self.history_collection = db['property_history']
        self.properties_collection = db['properties']
    
    async def initialize(self):
        """Create necessary indexes for history collections"""
        try:
            # Property history indexes
            await self.history_collection.create_index([
                ("property_id", 1),
                ("timestamp", -1)
            ])
            await self.history_collection.create_index([
                ("property_id", 1),
                ("change_type", 1)
            ])
            
            # TTL index for automatic cleanup - delete entries older than 90 days
            # Note: MongoDB TTL cleanup runs every 60 seconds
            try:
                await self.history_collection.create_index(
                    [("timestamp", 1)],
                    expireAfterSeconds=7776000  # 90 days in seconds (90 * 24 * 60 * 60)
                )
                logger.info("Created TTL index on property_history.timestamp (90 days)")
            except Exception as e:
                logger.warning(f"TTL index may already exist: {e}")
            
            # Change logs are now embedded in properties collection
            # Create indexes on embedded change_logs array
            try:
                await self.properties_collection.create_index([
                    ("change_logs.field", 1),
                    ("change_logs.change_type", 1)
                ])
                logger.info("Created index on properties.change_logs.field and change_type")
            except Exception as e:
                logger.warning(f"Index may already exist: {e}")
            
            try:
                await self.properties_collection.create_index([
                    ("change_logs.timestamp", -1)
                ])
                logger.info("Created index on properties.change_logs.timestamp")
            except Exception as e:
                logger.warning(f"Index may already exist: {e}")
            
            logger.info("History tracker indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating history tracker indexes: {e}")
    
    async def record_price_change(self, property_id: str, price_data: Dict[str, Any], job_id: Optional[str] = None) -> bool:
        """Record a price change in the history"""
        try:
            # Check for duplicate entry to prevent storage bloat
            existing = await self.history_collection.find_one({
                "property_id": property_id,
                "change_type": "price_change",
                "data.old_price": price_data["old_price"],
                "data.new_price": price_data["new_price"],
                "timestamp": price_data["timestamp"]
            })
            
            if existing:
                logger.debug(f"Price change already recorded for property {property_id}, skipping duplicate")
                return True
            
            history_entry = {
                "property_id": property_id,
                "change_type": "price_change",
                "timestamp": price_data["timestamp"],
                "data": {
                    "old_price": price_data["old_price"],
                    "new_price": price_data["new_price"],
                    "price_difference": price_data["price_difference"],
                    "percent_change": price_data["percent_change"],
                    "change_type": price_data["change_type"],
                    # Store status/listing_type for filtering
                    "old_status": price_data.get("old_status"),
                    "new_status": price_data.get("new_status"),
                    "old_listing_type": price_data.get("old_listing_type"),
                    "new_listing_type": price_data.get("new_listing_type")
                },
                "job_id": job_id,
                "created_at": datetime.utcnow()
            }
            
            await self.history_collection.insert_one(history_entry)
            
            # Limit to last 100 price changes per property to prevent unbounded growth
            count = await self.history_collection.count_documents({
                "property_id": property_id,
                "change_type": "price_change"
            })
            
            if count > 100:
                # Delete oldest entries beyond limit
                oldest_cursor = self.history_collection.find(
                    {"property_id": property_id, "change_type": "price_change"}
                ).sort("timestamp", 1).limit(count - 100)
                
                ids_to_delete = []
                async for entry in oldest_cursor:
                    ids_to_delete.append(entry["_id"])
                
                if ids_to_delete:
                    await self.history_collection.delete_many({"_id": {"$in": ids_to_delete}})
                    logger.debug(f"Deleted {len(ids_to_delete)} old price change entries for property {property_id}")
            
            logger.info(f"Recorded price change for property {property_id}: {price_data['old_price']} -> {price_data['new_price']}")
            return True
            
        except Exception as e:
            logger.error(f"Error recording price change for property {property_id}: {e}")
            return False
    
    async def record_status_change(self, property_id: str, status_data: Dict[str, Any], job_id: Optional[str] = None) -> bool:
        """Record a status change in the history"""
        try:
            # Check for duplicate entry to prevent storage bloat
            existing = await self.history_collection.find_one({
                "property_id": property_id,
                "change_type": "status_change",
                "data.old_status": status_data["old_status"],
                "data.new_status": status_data["new_status"],
                "timestamp": status_data["timestamp"]
            })
            
            if existing:
                logger.debug(f"Status change already recorded for property {property_id}, skipping duplicate")
                return True
            
            history_entry = {
                "property_id": property_id,
                "change_type": "status_change",
                "timestamp": status_data["timestamp"],
                "data": {
                    "old_status": status_data["old_status"],
                    "new_status": status_data["new_status"]
                },
                "job_id": job_id,
                "created_at": datetime.utcnow()
            }
            
            await self.history_collection.insert_one(history_entry)
            
            # Limit to last 50 status changes per property
            count = await self.history_collection.count_documents({
                "property_id": property_id,
                "change_type": "status_change"
            })
            
            if count > 50:
                # Delete oldest entries beyond limit
                oldest_cursor = self.history_collection.find(
                    {"property_id": property_id, "change_type": "status_change"}
                ).sort("timestamp", 1).limit(count - 50)
                
                ids_to_delete = []
                async for entry in oldest_cursor:
                    ids_to_delete.append(entry["_id"])
                
                if ids_to_delete:
                    await self.history_collection.delete_many({"_id": {"$in": ids_to_delete}})
                    logger.debug(f"Deleted {len(ids_to_delete)} old status change entries for property {property_id}")
            
            logger.info(f"Recorded status change for property {property_id}: {status_data['old_status']} -> {status_data['new_status']}")
            return True
            
        except Exception as e:
            logger.error(f"Error recording status change for property {property_id}: {e}")
            return False
    
    async def record_change_logs(self, property_id: str, changes: List[Dict[str, Any]], job_id: Optional[str] = None) -> bool:
        """
        Record detailed field-level changes - ONLY price, status, and listing_type
        
        Change logs are embedded in the properties collection as an array.
        Groups changes by timestamp (same date) into single entries for efficiency.
        Only tracks: financial.list_price, financial.original_list_price, financial.price_per_sqft,
                     status, mls_status, listing_type
        """
        try:
            if not changes:
                return True
            
            # Filter to only tracked fields (should already be filtered upstream, but double-check)
            filtered_changes = [
                change for change in changes
                if change.get("field") in TRACKED_CHANGE_LOG_FIELDS
            ]
            
            if not filtered_changes:
                return True
            
            # Get existing property to check for duplicates
            property_doc = await self.properties_collection.find_one(
                {"property_id": property_id},
                {"change_logs": 1}
            )
            
            existing_change_logs = property_doc.get("change_logs", []) if property_doc else []
            
            # Normalize timestamps to date (group changes on same day)
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
            
            # Group changes by normalized date (same date = same entry)
            from collections import defaultdict
            changes_by_date = defaultdict(list)
            
            for change in filtered_changes:
                timestamp = change.get("timestamp")
                normalized_date = normalize_timestamp(timestamp)
                changes_by_date[normalized_date].append(change)
            
            # Create new consolidated change log entries
            cutoff_date = datetime.utcnow() - timedelta(days=90)  # TTL: 90 days
            new_entries = []
            
            for change_date, date_changes in changes_by_date.items():
                # Create a datetime from the date for timestamp
                change_datetime = datetime.combine(change_date, datetime.min.time())
                
                # Check TTL
                if change_datetime < cutoff_date:
                    logger.debug(f"Change log older than 90 days, skipping TTL")
                    continue
                
                # Build consolidated entry with all fields that changed on this date
                # Check for duplicates - see if we already have changes with same fields/values on this date
                existing_for_date = [
                    log for log in existing_change_logs
                    if log.get("timestamp") and normalize_timestamp(log.get("timestamp")) == change_date
                ]
                
                # Check each field change for duplicates
                field_changes_to_record = []
                for change in date_changes:
                    field = change.get("field")
                    old_val = change.get("old_value")
                    new_val = change.get("new_value")
                    
                    # Check if this exact change already exists for this date
                    is_duplicate = any(
                        (log.get("field") == field or 
                         (isinstance(log.get("field_changes"), dict) and field in log.get("field_changes", {}))) and
                        self._get_log_old_value(log, field) == old_val and
                        self._get_log_new_value(log, field) == new_val
                        for log in existing_for_date
                    )
                    
                    if not is_duplicate:
                        field_changes_to_record.append(change)
                
                if not field_changes_to_record:
                    continue
                
                # Create consolidated entry - if multiple fields, group them; if single, use simple format
                if len(field_changes_to_record) == 1:
                    # Single field change - use simple format (backward compatible)
                    change = field_changes_to_record[0]
                    log_entry = {
                        "field": change.get("field"),
                        "old_value": change.get("old_value"),
                        "new_value": change.get("new_value"),
                        "change_type": change.get("change_type"),
                        "timestamp": change_datetime,
                        "job_id": job_id,
                        "created_at": datetime.utcnow()
                    }
                else:
                    # Multiple fields changed on same date - use consolidated format
                    field_changes_dict = {}
                    for change in field_changes_to_record:
                        field = change.get("field")
                        field_changes_dict[field] = {
                            "old_value": change.get("old_value"),
                            "new_value": change.get("new_value"),
                            "change_type": change.get("change_type")
                        }
                    
                    log_entry = {
                        "field": None,  # None indicates consolidated entry
                        "field_changes": field_changes_dict,  # All fields that changed
                        "timestamp": change_datetime,
                        "job_id": job_id,
                        "created_at": datetime.utcnow(),
                        "change_count": len(field_changes_to_record)
                    }
                
                new_entries.append(log_entry)
            
            if not new_entries:
                return True
            
            # Get current change_logs and add new ones
            current_change_logs = existing_change_logs.copy()
            current_change_logs.extend(new_entries)
            
            # Sort by timestamp (newest first) and limit to 200 most recent
            current_change_logs.sort(key=lambda x: x.get("timestamp", datetime.min), reverse=True)
            current_change_logs = current_change_logs[:200]  # Keep only 200 most recent
            
            # Also filter out entries older than 90 days
            current_change_logs = [
                log for log in current_change_logs
                if not log.get("timestamp") or (isinstance(log.get("timestamp"), datetime) and log.get("timestamp") >= cutoff_date)
            ]
            
            # Update property with embedded change_logs
            await self.properties_collection.update_one(
                {"property_id": property_id},
                {
                    "$set": {
                        "change_logs": current_change_logs,
                        "change_logs_updated_at": datetime.utcnow()
                    }
                }
            )
            
            total_changes = sum(
                entry.get("change_count", 1) for entry in new_entries
            )
            logger.info(f"Recorded {len(new_entries)} change log entry/entries ({total_changes} total field changes) for property {property_id} (total: {len(current_change_logs)})")
            return True
            
        except Exception as e:
            logger.error(f"Error recording change logs for property {property_id}: {e}")
            return False
    
    def _get_log_old_value(self, log_entry: Dict[str, Any], field: str) -> Any:
        """Get old value from log entry (supports both simple and consolidated formats)"""
        if log_entry.get("field") == field:
            return log_entry.get("old_value")
        elif isinstance(log_entry.get("field_changes"), dict):
            return log_entry.get("field_changes", {}).get(field, {}).get("old_value")
        return None
    
    def _get_log_new_value(self, log_entry: Dict[str, Any], field: str) -> Any:
        """Get new value from log entry (supports both simple and consolidated formats)"""
        if log_entry.get("field") == field:
            return log_entry.get("new_value")
        elif isinstance(log_entry.get("field_changes"), dict):
            return log_entry.get("field_changes", {}).get(field, {}).get("new_value")
        return None
    
    async def get_property_history(self, property_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get property history (price and status changes) - combines price and status by timestamp.
        Each entry contains both price change (if any) and status change (if any) that occurred at the same time.
        """
        try:
            # Get change logs from embedded array
            property_doc = await self.properties_collection.find_one(
                {"property_id": property_id},
                {"change_logs": 1}
            )
            
            if not property_doc or "change_logs" not in property_doc:
                return []
            
            change_logs = property_doc.get("change_logs", [])
            
            # Group changes by timestamp (normalize to date for grouping)
            from collections import defaultdict
            changes_by_timestamp = defaultdict(lambda: {"price": None, "status": None, "timestamp": None, "job_id": None})
            
            price_fields = {'financial.list_price', 'financial.original_list_price', 'financial.price_per_sqft'}
            status_fields = {'status', 'mls_status'}
            
            # Priority order for price fields (list_price is most important)
            price_priority = {
                'financial.list_price': 1,
                'financial.original_list_price': 2,
                'financial.price_per_sqft': 3
            }
            
            for log in change_logs:
                timestamp = log.get("timestamp")
                job_id = log.get("job_id")
                
                # Normalize timestamp to date for grouping (changes on same day are grouped)
                if isinstance(timestamp, datetime):
                    normalized_date = timestamp.date()
                elif isinstance(timestamp, str):
                    try:
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        if dt.tzinfo:
                            dt = dt.replace(tzinfo=None)
                        normalized_date = dt.date()
                    except:
                        normalized_date = datetime.utcnow().date()
                else:
                    normalized_date = datetime.utcnow().date()
                
                # Handle simple format (single field)
                if log.get("field"):
                    field = log.get("field")
                    old_value = log.get("old_value")
                    new_value = log.get("new_value")
                    
                    if field in price_fields:
                        # Only use if we don't have a price yet, or if this field has higher priority
                        current_price = changes_by_timestamp[normalized_date]["price"]
                        if current_price is None or price_priority.get(field, 99) < price_priority.get(current_price.get("field", ""), 99):
                            changes_by_timestamp[normalized_date]["price"] = {
                                "field": field,
                                "old_value": old_value,
                                "new_value": new_value
                            }
                            changes_by_timestamp[normalized_date]["timestamp"] = timestamp
                            changes_by_timestamp[normalized_date]["job_id"] = job_id
                    elif field in status_fields:
                        # Status field - use 'status' field if available, otherwise mls_status
                        if field == "status" or changes_by_timestamp[normalized_date]["status"] is None:
                            changes_by_timestamp[normalized_date]["status"] = {
                                "field": field,
                                "old_value": old_value,
                                "new_value": new_value
                            }
                            if changes_by_timestamp[normalized_date]["timestamp"] is None:
                                changes_by_timestamp[normalized_date]["timestamp"] = timestamp
                            if changes_by_timestamp[normalized_date]["job_id"] is None:
                                changes_by_timestamp[normalized_date]["job_id"] = job_id
                
                # Handle consolidated format (multiple fields in one entry)
                elif isinstance(log.get("field_changes"), dict):
                    field_changes = log.get("field_changes", {})
                    
                    # Process price fields (prioritize list_price)
                    price_changes_in_entry = []
                    for field, change_data in field_changes.items():
                        if field in price_fields:
                            price_changes_in_entry.append((field, change_data))
                    
                    if price_changes_in_entry:
                        # Sort by priority and take the highest priority one
                        price_changes_in_entry.sort(key=lambda x: price_priority.get(x[0], 99))
                        field, change_data = price_changes_in_entry[0]
                        current_price = changes_by_timestamp[normalized_date]["price"]
                        if current_price is None or price_priority.get(field, 99) < price_priority.get(current_price.get("field", ""), 99):
                            changes_by_timestamp[normalized_date]["price"] = {
                                "field": field,
                                "old_value": change_data.get("old_value"),
                                "new_value": change_data.get("new_value")
                            }
                            changes_by_timestamp[normalized_date]["timestamp"] = timestamp
                            changes_by_timestamp[normalized_date]["job_id"] = job_id
                    
                    # Process status fields
                    for field, change_data in field_changes.items():
                        if field in status_fields:
                            if field == "status" or changes_by_timestamp[normalized_date]["status"] is None:
                                changes_by_timestamp[normalized_date]["status"] = {
                                    "field": field,
                                    "old_value": change_data.get("old_value"),
                                    "new_value": change_data.get("new_value")
                                }
                                if changes_by_timestamp[normalized_date]["timestamp"] is None:
                                    changes_by_timestamp[normalized_date]["timestamp"] = timestamp
                                if changes_by_timestamp[normalized_date]["job_id"] is None:
                                    changes_by_timestamp[normalized_date]["job_id"] = job_id
            
            # Convert grouped changes to history entries
            history = []
            for normalized_date, change_data in sorted(changes_by_timestamp.items(), reverse=True):
                timestamp = change_data["timestamp"] or datetime.combine(normalized_date, datetime.min.time())
                price_info = change_data["price"]
                status_info = change_data["status"]
                
                # Only include entries that have at least price or status change
                if price_info is None and status_info is None:
                    continue
                
                entry = {
                    "property_id": property_id,
                    "timestamp": timestamp,
                    "job_id": change_data["job_id"]
                }
                
                # Add price change data (prioritize list_price)
                if price_info:
                    old_price = price_info.get("old_value")
                    new_price = price_info.get("new_value")
                    if old_price is not None and new_price is not None:
                        price_diff = new_price - old_price
                        percent_change = ((price_diff / old_price) * 100) if old_price > 0 else 0
                        entry["price_change"] = {
                            "old_price": old_price,
                            "new_price": new_price,
                            "price_difference": price_diff,
                            "percent_change": round(percent_change, 2),
                            "change_type": "reduction" if price_diff < 0 else "increase",
                            "field": price_info.get("field")
                        }
                
                # Add status change data
                if status_info:
                    entry["status_change"] = {
                        "old_status": status_info.get("old_value"),
                        "new_status": status_info.get("new_value"),
                        "field": status_info.get("field")
                    }
                
                history.append(entry)
            
            # Limit results
            return history[:limit]
            
        except Exception as e:
            logger.error(f"Error getting property history for {property_id}: {e}")
            return []
    
    async def get_property_change_logs(self, property_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get detailed change logs for a property (from embedded array)"""
        try:
            property_doc = await self.properties_collection.find_one(
                {"property_id": property_id},
                {"change_logs": 1}
            )
            
            if not property_doc or "change_logs" not in property_doc:
                return []
            
            change_logs = property_doc.get("change_logs", [])
            
            # Sort by timestamp (newest first) and limit
            change_logs.sort(key=lambda x: x.get("timestamp", datetime.min), reverse=True)
            
            return change_logs[:limit]
            
        except Exception as e:
            logger.error(f"Error getting change logs for {property_id}: {e}")
            return []
    
    async def get_price_history(self, property_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Get price history for a property (from embedded change_logs)"""
        try:
            property_doc = await self.properties_collection.find_one(
                {"property_id": property_id},
                {"change_logs": 1}
            )
            
            if not property_doc or "change_logs" not in property_doc:
                return []
            
            change_logs = property_doc.get("change_logs", [])
            
            # Filter to price-related fields (supports both simple and consolidated formats)
            # Priority: list_price > original_list_price > price_per_sqft
            price_fields = {'financial.list_price', 'financial.original_list_price', 'financial.price_per_sqft'}
            price_priority = {
                'financial.list_price': 1,
                'financial.original_list_price': 2,
                'financial.price_per_sqft': 3
            }
            
            # Group price changes by timestamp, keeping only the highest priority field per timestamp
            price_changes_by_timestamp = {}
            
            for log in change_logs:
                timestamp = log.get("timestamp")
                job_id = log.get("job_id")
                created_at = log.get("created_at")
                
                # Normalize timestamp for grouping
                if isinstance(timestamp, datetime):
                    normalized_ts = timestamp
                elif isinstance(timestamp, str):
                    try:
                        normalized_ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        if normalized_ts.tzinfo:
                            normalized_ts = normalized_ts.replace(tzinfo=None)
                    except:
                        normalized_ts = datetime.utcnow()
                else:
                    normalized_ts = datetime.utcnow()
                
                # Handle simple format (single field)
                if log.get("field") and log.get("field") in price_fields:
                    field = log.get("field")
                    old_value = log.get("old_value")
                    new_value = log.get("new_value")
                    
                    if old_value is not None and new_value is not None:
                        # Check if we already have a price change for this timestamp
                        if normalized_ts not in price_changes_by_timestamp:
                            price_changes_by_timestamp[normalized_ts] = {
                                "field": field,
                                "old_value": old_value,
                                "new_value": new_value,
                                "timestamp": timestamp,
                                "job_id": job_id,
                                "created_at": created_at
                            }
                        else:
                            # Replace if this field has higher priority
                            current_priority = price_priority.get(price_changes_by_timestamp[normalized_ts]["field"], 99)
                            new_priority = price_priority.get(field, 99)
                            if new_priority < current_priority:
                                price_changes_by_timestamp[normalized_ts] = {
                                    "field": field,
                                    "old_value": old_value,
                                    "new_value": new_value,
                                    "timestamp": timestamp,
                                    "job_id": job_id,
                                    "created_at": created_at
                                }
                
                # Handle consolidated format (multiple fields in one entry)
                elif isinstance(log.get("field_changes"), dict):
                    field_changes = log.get("field_changes", {})
                    
                    # Find the highest priority price field in this entry
                    price_fields_in_entry = []
                    for field, change_data in field_changes.items():
                        if field in price_fields:
                            old_val = change_data.get("old_value")
                            new_val = change_data.get("new_value")
                            if old_val is not None and new_val is not None:
                                price_fields_in_entry.append((field, change_data))
                    
                    if price_fields_in_entry:
                        # Sort by priority and take the highest priority one
                        price_fields_in_entry.sort(key=lambda x: price_priority.get(x[0], 99))
                        field, change_data = price_fields_in_entry[0]
                        
                        if normalized_ts not in price_changes_by_timestamp:
                            price_changes_by_timestamp[normalized_ts] = {
                                "field": field,
                                "old_value": change_data.get("old_value"),
                                "new_value": change_data.get("new_value"),
                                "timestamp": timestamp,
                                "job_id": job_id,
                                "created_at": created_at
                            }
                        else:
                            # Replace if this field has higher priority
                            current_priority = price_priority.get(price_changes_by_timestamp[normalized_ts]["field"], 99)
                            new_priority = price_priority.get(field, 99)
                            if new_priority < current_priority:
                                price_changes_by_timestamp[normalized_ts] = {
                                    "field": field,
                                    "old_value": change_data.get("old_value"),
                                    "new_value": change_data.get("new_value"),
                                    "timestamp": timestamp,
                                    "job_id": job_id,
                                    "created_at": created_at
                                }
            
            # Convert to list and sort by timestamp (newest first)
            price_changes = list(price_changes_by_timestamp.values())
            price_changes.sort(key=lambda x: x.get("timestamp", datetime.min), reverse=True)
            
            # Convert to price_history format for backward compatibility
            price_history = []
            for log in price_changes[:limit]:
                old_value = log.get("old_value")
                new_value = log.get("new_value")
                if old_value is None or new_value is None:
                    continue
                
                # Calculate price difference
                price_diff = new_value - old_value
                percent_change = ((price_diff / old_value) * 100) if old_value > 0 else 0
                
                entry = {
                    "property_id": property_id,
                    "change_type": "price_change",
                    "timestamp": log.get("timestamp"),
                    "data": {
                        "old_price": old_value,
                        "new_price": new_value,
                        "price_difference": price_diff,
                        "percent_change": round(percent_change, 2),
                        "change_type": "reduction" if price_diff < 0 else "increase",
                        "field": log.get("field")  # Include field name for debugging
                    },
                    "job_id": log.get("job_id"),
                    "created_at": log.get("created_at")
                }
                price_history.append(entry)
            
            return price_history
            
        except Exception as e:
            logger.error(f"Error getting price history for {property_id}: {e}")
            return []
    
    async def get_status_history(self, property_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Get status history for a property (from embedded change_logs)"""
        try:
            property_doc = await self.properties_collection.find_one(
                {"property_id": property_id},
                {"change_logs": 1}
            )
            
            if not property_doc or "change_logs" not in property_doc:
                return []
            
            change_logs = property_doc.get("change_logs", [])
            
            # Filter to status-related fields (supports both simple and consolidated formats)
            status_fields = {'status', 'mls_status'}
            status_changes = []
            
            for log in change_logs:
                # Handle simple format (single field)
                if log.get("field") and log.get("field") in status_fields:
                    status_changes.append({
                        "field": log.get("field"),
                        "old_value": log.get("old_value"),
                        "new_value": log.get("new_value"),
                        "timestamp": log.get("timestamp"),
                        "job_id": log.get("job_id"),
                        "created_at": log.get("created_at")
                    })
                # Handle consolidated format (multiple fields in one entry)
                elif isinstance(log.get("field_changes"), dict):
                    field_changes = log.get("field_changes", {})
                    for field, change_data in field_changes.items():
                        if field in status_fields:
                            status_changes.append({
                                "field": field,
                                "old_value": change_data.get("old_value"),
                                "new_value": change_data.get("new_value"),
                                "timestamp": log.get("timestamp"),
                                "job_id": log.get("job_id"),
                                "created_at": log.get("created_at")
                            })
            
            # Sort by timestamp (newest first) and limit
            status_changes.sort(key=lambda x: x.get("timestamp", datetime.min), reverse=True)
            
            # Convert to status_history format for backward compatibility
            status_history = []
            for log in status_changes[:limit]:
                entry = {
                    "property_id": property_id,
                    "change_type": "status_change",
                    "timestamp": log.get("timestamp"),
                    "data": {
                        "old_status": log.get("old_value"),
                        "new_status": log.get("new_value")
                    },
                    "job_id": log.get("job_id"),
                    "created_at": log.get("created_at")
                }
                status_history.append(entry)
            
            return status_history
            
        except Exception as e:
            logger.error(f"Error getting status history for {property_id}: {e}")
            return []
    
    async def get_recent_changes(self, property_id: str, days: int = 30) -> Dict[str, Any]:
        """Get recent changes summary for a property (from embedded change_logs)"""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            # Get recent change logs from embedded array
            property_doc = await self.properties_collection.find_one(
                {"property_id": property_id},
                {"change_logs": 1}
            )
            
            recent_logs = []
            if property_doc and "change_logs" in property_doc:
                change_logs = property_doc.get("change_logs", [])
                for log in change_logs:
                    timestamp = log.get("timestamp")
                    if timestamp and isinstance(timestamp, datetime):
                        if timestamp >= cutoff_date:
                            recent_logs.append(log)
            
            # Sort by timestamp
            recent_logs.sort(key=lambda x: x.get("timestamp", datetime.min), reverse=True)
            
            # Build price_history and status_history from recent_logs for backward compatibility
            price_history = []
            status_history = []
            
            price_fields = {'financial.list_price', 'financial.original_list_price', 'financial.price_per_sqft'}
            status_fields = {'status', 'mls_status'}
            
            for log in recent_logs:
                # Handle simple format (single field)
                if log.get("field"):
                    field = log.get("field")
                    old_value = log.get("old_value")
                    new_value = log.get("new_value")
                    
                    if field in price_fields and old_value is not None and new_value is not None:
                        price_diff = new_value - old_value
                        percent_change = ((price_diff / old_value) * 100) if old_value > 0 else 0
                        price_history.append({
                            "property_id": property_id,
                            "change_type": "price_change",
                            "timestamp": log.get("timestamp"),
                            "data": {
                                "old_price": old_value,
                                "new_price": new_value,
                                "price_difference": price_diff,
                                "percent_change": percent_change,
                                "change_type": "reduction" if price_diff < 0 else "increase"
                            }
                        })
                    elif field in status_fields:
                        status_history.append({
                            "property_id": property_id,
                            "change_type": "status_change",
                            "timestamp": log.get("timestamp"),
                            "data": {
                                "old_status": old_value,
                                "new_status": new_value
                            }
                        })
                # Handle consolidated format (multiple fields in one entry)
                elif isinstance(log.get("field_changes"), dict):
                    field_changes = log.get("field_changes", {})
                    timestamp = log.get("timestamp")
                    
                    for field, change_data in field_changes.items():
                        old_value = change_data.get("old_value")
                        new_value = change_data.get("new_value")
                        
                        if field in price_fields and old_value is not None and new_value is not None:
                            price_diff = new_value - old_value
                            percent_change = ((price_diff / old_value) * 100) if old_value > 0 else 0
                            price_history.append({
                                "property_id": property_id,
                                "change_type": "price_change",
                                "timestamp": timestamp,
                                "data": {
                                    "old_price": old_value,
                                    "new_price": new_value,
                                    "price_difference": price_diff,
                                    "percent_change": percent_change,
                                    "change_type": "reduction" if price_diff < 0 else "increase"
                                }
                            })
                        elif field in status_fields:
                            status_history.append({
                                "property_id": property_id,
                                "change_type": "status_change",
                                "timestamp": timestamp,
                                "data": {
                                    "old_status": old_value,
                                    "new_status": new_value
                                }
                            })
            
            return {
                "history": price_history + status_history,  # Combined for backward compatibility
                "change_logs": recent_logs,
                "price_history": price_history,
                "status_history": status_history,
                "summary": {
                    "history_count": len(price_history) + len(status_history),
                    "logs_count": len(recent_logs),
                    "days_analyzed": days
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting recent changes for {property_id}: {e}")
            return {"history": [], "change_logs": [], "price_history": [], "status_history": [], "summary": {"history_count": 0, "logs_count": 0, "days_analyzed": days}}