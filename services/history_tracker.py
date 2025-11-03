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
        self.history_collection = db.property_history
        self.properties_collection = db.properties
        # Note: change_logs are now embedded in properties collection, not separate
    
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
            
            # Create new change log entries (excluding duplicates)
            cutoff_date = datetime.utcnow() - timedelta(days=90)  # TTL: 90 days
            new_entries = []
            
            for change in filtered_changes:
                # Check for duplicate
                is_duplicate = any(
                    log.get("field") == change.get("field") and
                    log.get("old_value") == change.get("old_value") and
                    log.get("new_value") == change.get("new_value") and
                    log.get("timestamp") == change.get("timestamp")
                    for log in existing_change_logs
                )
                
                if is_duplicate:
                    logger.debug(f"Change log already exists for property {property_id}, field {change['field']}, skipping duplicate")
                    continue
                
                # Check TTL (skip if older than 90 days)
                timestamp = change.get("timestamp")
                if timestamp and isinstance(timestamp, datetime):
                    if timestamp < cutoff_date:
                        logger.debug(f"Change log older than 90 days, skipping TTL")
                        continue
                
                log_entry = {
                    "field": change.get("field"),
                    "old_value": change.get("old_value"),
                    "new_value": change.get("new_value"),
                    "change_type": change.get("change_type"),
                    "timestamp": change.get("timestamp"),
                    "job_id": job_id,
                    "created_at": datetime.utcnow()
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
            
            logger.info(f"Recorded {len(new_entries)} change logs for property {property_id} (total: {len(current_change_logs)})")
            return True
            
        except Exception as e:
            logger.error(f"Error recording change logs for property {property_id}: {e}")
            return False
    
    async def get_property_history(self, property_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get property history (price and status changes)"""
        try:
            cursor = self.history_collection.find(
                {"property_id": property_id}
            ).sort("timestamp", -1).limit(limit)
            
            history = []
            async for entry in cursor:
                entry["_id"] = str(entry["_id"])
                history.append(entry)
            
            return history
            
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
        """Get price history for a property"""
        try:
            cursor = self.history_collection.find(
                {
                    "property_id": property_id,
                    "change_type": "price_change"
                }
            ).sort("timestamp", -1).limit(limit)
            
            price_history = []
            async for entry in cursor:
                entry["_id"] = str(entry["_id"])
                price_history.append(entry)
            
            return price_history
            
        except Exception as e:
            logger.error(f"Error getting price history for {property_id}: {e}")
            return []
    
    async def get_status_history(self, property_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Get status history for a property"""
        try:
            cursor = self.history_collection.find(
                {
                    "property_id": property_id,
                    "change_type": "status_change"
                }
            ).sort("timestamp", -1).limit(limit)
            
            status_history = []
            async for entry in cursor:
                entry["_id"] = str(entry["_id"])
                status_history.append(entry)
            
            return status_history
            
        except Exception as e:
            logger.error(f"Error getting status history for {property_id}: {e}")
            return []
    
    async def get_recent_changes(self, property_id: str, days: int = 30) -> Dict[str, Any]:
        """Get recent changes summary for a property"""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            # Get recent history entries
            history_cursor = self.history_collection.find(
                {
                    "property_id": property_id,
                    "timestamp": {"$gte": cutoff_date}
                }
            ).sort("timestamp", -1)
            
            # Get recent change logs from embedded array
            property_doc = await self.properties_collection.find_one(
                {"property_id": property_id},
                {"change_logs": 1}
            )
            
            recent_history = []
            async for entry in history_cursor:
                entry["_id"] = str(entry["_id"])
                recent_history.append(entry)
            
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
            
            return {
                "history": recent_history,
                "change_logs": recent_logs,
                "summary": {
                    "history_count": len(recent_history),
                    "logs_count": len(recent_logs),
                    "days_analyzed": days
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting recent changes for {property_id}: {e}")
            return {"history": [], "change_logs": [], "summary": {"history_count": 0, "logs_count": 0, "days_analyzed": days}}