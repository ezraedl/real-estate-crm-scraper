import motor.motor_asyncio
from pymongo import ASCENDING, DESCENDING
from typing import Optional, List, Dict, Any
from datetime import datetime
import asyncio
from urllib.parse import urlparse
from config import settings
from models import ScrapingJob, Property, JobStatus, JobPriority

class Database:
    def __init__(self):
        self.client = None
        self.db = None
        self.jobs_collection = None
        self.properties_collection = None
        self.scheduled_jobs_collection = None
    
    async def connect(self):
        """Connect to MongoDB"""
        try:
            # Parse the MongoDB URI to extract database name
            parsed_uri = urlparse(settings.MONGODB_URI)
            database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'rei-crm'
            
            self.client = motor.motor_asyncio.AsyncIOMotorClient(settings.MONGODB_URI)
            self.db = self.client[database_name]
            
            # Collections
            self.jobs_collection = self.db.jobs
            self.properties_collection = self.db.properties
            self.scheduled_jobs_collection = self.db.scheduled_jobs
            
            # Create indexes
            await self._create_indexes()
            
            print(f"Connected to MongoDB: {database_name}")
            return True
        except Exception as e:
            print(f"Failed to connect to MongoDB: {e}")
            return False
    
    async def _create_indexes(self):
        """Create necessary indexes for optimal performance"""
        try:
            # Jobs collection indexes
            await self.jobs_collection.create_index([("job_id", ASCENDING)], unique=True)
            await self.jobs_collection.create_index([("status", ASCENDING)])
            await self.jobs_collection.create_index([("priority", ASCENDING), ("created_at", ASCENDING)])
            await self.jobs_collection.create_index([("scheduled_at", ASCENDING)])
            
            # Properties collection indexes
            await self.properties_collection.create_index([("property_id", ASCENDING)], unique=True)
            await self.properties_collection.create_index([("mls_id", ASCENDING)])
            await self.properties_collection.create_index([("address.city", ASCENDING)])
            await self.properties_collection.create_index([("address.state", ASCENDING)])
            await self.properties_collection.create_index([("address.zip_code", ASCENDING)])
            await self.properties_collection.create_index([("status", ASCENDING)])
            await self.properties_collection.create_index([("scraped_at", DESCENDING)])
            await self.properties_collection.create_index([("job_id", ASCENDING)])
            
            # Scheduled jobs collection indexes
            await self.scheduled_jobs_collection.create_index([("cron_expression", ASCENDING)])
            await self.scheduled_jobs_collection.create_index([("next_run", ASCENDING)])
            
            print("Database indexes created successfully")
        except Exception as e:
            print(f"Error creating indexes: {e}")
    
    async def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
    
    # Job Management Methods
    async def create_job(self, job: ScrapingJob) -> str:
        """Create a new scraping job"""
        try:
            result = await self.jobs_collection.insert_one(job.dict(by_alias=True, exclude={"id"}))
            return str(result.inserted_id)
        except Exception as e:
            print(f"Error creating job: {e}")
            raise
    
    async def get_job(self, job_id: str) -> Optional[ScrapingJob]:
        """Get job by job_id"""
        try:
            job_data = await self.jobs_collection.find_one({"job_id": job_id})
            if job_data:
                job_data["_id"] = str(job_data["_id"])
                return ScrapingJob(**job_data)
            return None
        except Exception as e:
            print(f"Error getting job: {e}")
            return None
    
    async def update_job_status(self, job_id: str, status: JobStatus, **kwargs) -> bool:
        """Update job status and other fields"""
        try:
            update_data = {"status": status.value}
            update_data.update(kwargs)
            
            if status == JobStatus.RUNNING:
                update_data["started_at"] = datetime.utcnow()
            elif status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                update_data["completed_at"] = datetime.utcnow()
            
            result = await self.jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"Error updating job status: {e}")
            return False
    
    async def update_recurring_job_run_history(self, original_job_id: str, run_job_id: str, status: JobStatus) -> bool:
        """Update the run history of a recurring job"""
        try:
            # First get current run_count
            current_job = await self.jobs_collection.find_one({"job_id": original_job_id})
            if not current_job:
                return False
            
            current_run_count = current_job.get("run_count", 0)
            
            update_data = {
                "run_count": current_run_count + 1,
                "last_run": datetime.utcnow(),
                "last_run_status": status.value,
                "last_run_job_id": run_job_id
            }
            
            result = await self.jobs_collection.update_one(
                {"job_id": original_job_id},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"Error updating recurring job run history: {e}")
            return False
    
    async def get_pending_jobs(self, limit: int = 10) -> List[ScrapingJob]:
        """Get pending jobs ordered by priority and creation time"""
        try:
            cursor = self.jobs_collection.find(
                {"status": JobStatus.PENDING.value}
            ).sort([
                ("priority", ASCENDING),
                ("created_at", ASCENDING)
            ]).limit(limit)
            
            jobs = []
            async for job_data in cursor:
                job_data["_id"] = str(job_data["_id"])
                jobs.append(ScrapingJob(**job_data))
            
            return jobs
        except Exception as e:
            print(f"Error getting pending jobs: {e}")
            return []
    
    async def get_jobs_by_status(self, status: JobStatus, limit: int = 50) -> List[ScrapingJob]:
        """Get jobs by status"""
        try:
            cursor = self.jobs_collection.find(
                {"status": status.value}
            ).sort([("created_at", DESCENDING)]).limit(limit)
            
            jobs = []
            async for job_data in cursor:
                job_data["_id"] = str(job_data["_id"])
                jobs.append(ScrapingJob(**job_data))
            
            return jobs
        except Exception as e:
            print(f"Error getting jobs by status: {e}")
            return []
    
    # Property Management Methods
    async def save_property(self, property_data: Property) -> Dict[str, Any]:
        """Save or update a property with hash-based change detection"""
        try:
            # Check if property already exists
            existing_property = await self.properties_collection.find_one(
                {"property_id": property_data.property_id}
            )
            
            if existing_property:
                # Property exists - check if content has changed
                existing_hash = existing_property.get("content_hash")
                new_hash = property_data.content_hash
                
                if existing_hash == new_hash:
                    # Content unchanged - only update days_on_mls and scraped_at
                    result = await self.properties_collection.update_one(
                        {"property_id": property_data.property_id},
                        {
                            "$set": {
                                "days_on_mls": property_data.days_on_mls,
                                "scraped_at": property_data.scraped_at,
                                "job_id": property_data.job_id
                            }
                        }
                    )
                    return {
                        "action": "skipped",
                        "reason": "content_unchanged",
                        "property_id": property_data.property_id
                    }
                else:
                    # Content changed - update the property
                    result = await self.properties_collection.replace_one(
                        {"property_id": property_data.property_id},
                        property_data.dict(by_alias=True, exclude={"id"}),
                        upsert=True
                    )
                    return {
                        "action": "updated",
                        "reason": "content_changed",
                        "property_id": property_data.property_id,
                        "old_hash": existing_hash,
                        "new_hash": new_hash
                    }
            else:
                # New property - insert it
                result = await self.properties_collection.insert_one(
                    property_data.dict(by_alias=True, exclude={"id"})
                )
                return {
                    "action": "inserted",
                    "reason": "new_property",
                    "property_id": property_data.property_id,
                    "new_hash": property_data.content_hash
                }
                
        except Exception as e:
            print(f"Error saving property {property_data.property_id}: {e}")
            print(f"Error type: {type(e).__name__}")
            print(f"Property data keys: {list(property_data.dict().keys()) if hasattr(property_data, 'dict') else 'No dict method'}")
            return {
                "action": "error",
                "reason": str(e),
                "property_id": property_data.property_id
            }
    
    async def save_properties_batch(self, properties: List[Property]) -> Dict[str, Any]:
        """Save multiple properties in batch with hash-based change detection"""
        try:
            if not properties:
                return {"total": 0, "inserted": 0, "updated": 0, "skipped": 0, "errors": 0}
            
            results = {
                "total": len(properties),
                "inserted": 0,
                "updated": 0,
                "skipped": 0,
                "errors": 0,
                "details": []
            }
            
            # Process each property individually for hash comparison
            for prop in properties:
                result = await self.save_property(prop)
                results["details"].append(result)
                
                if result["action"] == "inserted":
                    results["inserted"] += 1
                elif result["action"] == "updated":
                    results["updated"] += 1
                elif result["action"] == "skipped":
                    results["skipped"] += 1
                elif result["action"] == "error":
                    results["errors"] += 1
            
            print(f"Batch save results: {results['inserted']} inserted, {results['updated']} updated, {results['skipped']} skipped, {results['errors']} errors")
            return results
            
        except Exception as e:
            print(f"Error saving properties batch: {e}")
            return {"total": len(properties), "inserted": 0, "updated": 0, "skipped": 0, "errors": len(properties), "details": []}
    
    async def get_property(self, property_id: str) -> Optional[Property]:
        """Get property by property_id"""
        try:
            property_data = await self.properties_collection.find_one({"property_id": property_id})
            if property_data:
                property_data["_id"] = str(property_data["_id"])
                return Property(**property_data)
            return None
        except Exception as e:
            print(f"Error getting property: {e}")
            return None
    
    async def find_properties_by_location(self, location: str, listing_type: str = None, radius: float = 0.1, limit: int = 100) -> List[Property]:
        """Find properties by location with flexible matching"""
        try:
            print(f"[DB DEBUG] Searching for location: '{location}', listing_type: '{listing_type}', limit: {limit}")
            
            # Normalize the location for better matching
            location_lower = location.lower().strip()
            print(f"[DB DEBUG] Normalized location: '{location_lower}'")
            
            # Try multiple search patterns - more comprehensive for exact matches
            search_queries = [
                # Exact match on formatted_address
                {"address.formatted_address": {"$regex": location_lower, "$options": "i"}},
                # Match on street only
                {"address.street": {"$regex": location_lower.split(',')[0].strip(), "$options": "i"}},
                # Try with abbreviated format (East -> E, Street -> St)
                {"address.formatted_address": {"$regex": location_lower.replace('east', 'e').replace('street', 'st'), "$options": "i"}},
                # Try with full format (E -> East, St -> Street)
                {"address.formatted_address": {"$regex": location_lower.replace(' e ', ' east ').replace(' st ', ' street '), "$options": "i"}},
            ]
            
            # Add listing_type filter if specified
            if listing_type:
                for query in search_queries:
                    query["listing_type"] = listing_type
            
            # Try each search pattern until we find results
            for i, query in enumerate(search_queries):
                print(f"[DB DEBUG] Trying query {i+1}: {query}")
                cursor = self.properties_collection.find(query).limit(limit)
                properties = []
                
                async for prop_data in cursor:
                    prop_data["_id"] = str(prop_data["_id"])
                    properties.append(Property(**prop_data))
                
                print(f"[DB DEBUG] Query {i+1} returned {len(properties)} properties")
                if properties:
                    print(f"[DB DEBUG] Found {len(properties)} properties using query: {query}")
                    return properties
            
            print(f"[DB DEBUG] No properties found for any query")
            return []
            
        except Exception as e:
            print(f"Error finding properties by location: {e}")
            return []
    
    async def search_properties(self, filters: Dict[str, Any], limit: int = 100) -> List[Property]:
        """Search properties with filters"""
        try:
            cursor = self.properties_collection.find(filters).limit(limit)
            
            properties = []
            async for prop_data in cursor:
                prop_data["_id"] = str(prop_data["_id"])
                properties.append(Property(**prop_data))
            
            return properties
        except Exception as e:
            print(f"Error searching properties: {e}")
            return []
    
    # Statistics Methods
    async def get_job_stats(self) -> Dict[str, Any]:
        """Get job statistics"""
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": "$status",
                        "count": {"$sum": 1}
                    }
                }
            ]
            
            stats = {}
            async for doc in self.jobs_collection.aggregate(pipeline):
                stats[doc["_id"]] = doc["count"]
            
            return stats
        except Exception as e:
            print(f"Error getting job stats: {e}")
            return {}
    
    async def get_property_count(self) -> int:
        """Get total property count"""
        try:
            return await self.properties_collection.count_documents({})
        except Exception as e:
            print(f"Error getting property count: {e}")
            return 0

# Global database instance
db = Database()
