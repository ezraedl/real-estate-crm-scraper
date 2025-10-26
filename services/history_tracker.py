"""
History Tracker Service

Records property history (price changes, status changes) and detailed change logs
in dedicated MongoDB collections for auditing and analytics.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
from motor.motor_asyncio import AsyncIOMotorDatabase

logger = logging.getLogger(__name__)

class HistoryTracker:
    """Manages property history and change logging"""
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.history_collection = db.property_history
        self.change_logs_collection = db.property_change_logs
    
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
            
            # Change logs indexes
            await self.change_logs_collection.create_index([
                ("property_id", 1),
                ("timestamp", -1)
            ])
            await self.change_logs_collection.create_index([
                ("property_id", 1),
                ("field", 1)
            ])
            await self.change_logs_collection.create_index([
                ("job_id", 1)
            ])
            
            logger.info("History tracker indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating history tracker indexes: {e}")
    
    async def record_price_change(self, property_id: str, price_data: Dict[str, Any], job_id: Optional[str] = None) -> bool:
        """Record a price change in the history"""
        try:
            history_entry = {
                "property_id": property_id,
                "change_type": "price_change",
                "timestamp": price_data["timestamp"],
                "data": {
                    "old_price": price_data["old_price"],
                    "new_price": price_data["new_price"],
                    "price_difference": price_data["price_difference"],
                    "percent_change": price_data["percent_change"],
                    "change_type": price_data["change_type"]
                },
                "job_id": job_id,
                "created_at": datetime.utcnow()
            }
            
            await self.history_collection.insert_one(history_entry)
            logger.info(f"Recorded price change for property {property_id}: {price_data['old_price']} -> {price_data['new_price']}")
            return True
            
        except Exception as e:
            logger.error(f"Error recording price change for property {property_id}: {e}")
            return False
    
    async def record_status_change(self, property_id: str, status_data: Dict[str, Any], job_id: Optional[str] = None) -> bool:
        """Record a status change in the history"""
        try:
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
            logger.info(f"Recorded status change for property {property_id}: {status_data['old_status']} -> {status_data['new_status']}")
            return True
            
        except Exception as e:
            logger.error(f"Error recording status change for property {property_id}: {e}")
            return False
    
    async def record_change_logs(self, property_id: str, changes: List[Dict[str, Any]], job_id: Optional[str] = None) -> bool:
        """Record detailed field-level changes"""
        try:
            if not changes:
                return True
            
            change_log_entries = []
            for change in changes:
                log_entry = {
                    "property_id": property_id,
                    "field": change["field"],
                    "old_value": change["old_value"],
                    "new_value": change["new_value"],
                    "change_type": change["change_type"],
                    "timestamp": change["timestamp"],
                    "job_id": job_id,
                    "created_at": datetime.utcnow()
                }
                change_log_entries.append(log_entry)
            
            if change_log_entries:
                await self.change_logs_collection.insert_many(change_log_entries)
                logger.info(f"Recorded {len(change_log_entries)} change logs for property {property_id}")
            
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
        """Get detailed change logs for a property"""
        try:
            cursor = self.change_logs_collection.find(
                {"property_id": property_id}
            ).sort("timestamp", -1).limit(limit)
            
            change_logs = []
            async for entry in cursor:
                entry["_id"] = str(entry["_id"])
                change_logs.append(entry)
            
            return change_logs
            
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
            cutoff_date = datetime.utcnow()
            cutoff_date = cutoff_date.replace(day=cutoff_date.day - days)
            
            # Get recent history entries
            history_cursor = self.history_collection.find(
                {
                    "property_id": property_id,
                    "timestamp": {"$gte": cutoff_date}
                }
            ).sort("timestamp", -1)
            
            # Get recent change logs
            logs_cursor = self.change_logs_collection.find(
                {
                    "property_id": property_id,
                    "timestamp": {"$gte": cutoff_date}
                }
            ).sort("timestamp", -1)
            
            recent_history = []
            async for entry in history_cursor:
                entry["_id"] = str(entry["_id"])
                recent_history.append(entry)
            
            recent_logs = []
            async for entry in logs_cursor:
                entry["_id"] = str(entry["_id"])
                recent_logs.append(entry)
            
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