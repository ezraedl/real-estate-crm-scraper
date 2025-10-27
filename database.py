import motor.motor_asyncio
from pymongo import ASCENDING, DESCENDING
from typing import Optional, List, Dict, Any
from datetime import datetime
import asyncio
from urllib.parse import urlparse
from config import settings
from models import ScrapingJob, Property, JobStatus, JobPriority, ScheduledJob, ScheduledJobStatus
from services import PropertyEnrichmentPipeline

class Database:
    def __init__(self):
        self.client = None
        self.db = None
        self.jobs_collection = None
        self.properties_collection = None
        self.scheduled_jobs_collection = None
        self.enrichment_pipeline = None
    
    async def connect(self):
        """Connect to MongoDB"""
        try:
            # Parse the MongoDB URI to extract database name
            parsed_uri = urlparse(settings.MONGODB_URI)
            database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
            
            self.client = motor.motor_asyncio.AsyncIOMotorClient(settings.MONGODB_URI)
            self.db = self.client[database_name]
            
            # Collections
            self.jobs_collection = self.db.jobs
            self.properties_collection = self.db.properties
            self.scheduled_jobs_collection = self.db.scheduled_jobs
            
            # Create indexes
            await self._create_indexes()
            
            # Initialize enrichment pipeline
            self.enrichment_pipeline = PropertyEnrichmentPipeline(self.db)
            await self.enrichment_pipeline.initialize()
            
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
            await self.properties_collection.create_index([("crm_property_ids", ASCENDING)])
            
            # Scheduled jobs collection indexes
            await self.scheduled_jobs_collection.create_index([("scheduled_job_id", ASCENDING)], unique=True)
            await self.scheduled_jobs_collection.create_index([("status", ASCENDING)])
            await self.scheduled_jobs_collection.create_index([("cron_expression", ASCENDING)])
            await self.scheduled_jobs_collection.create_index([("next_run_at", ASCENDING)])
            await self.scheduled_jobs_collection.create_index([("last_run_at", DESCENDING)])
            
            # Jobs collection - add index for scheduled_job_id reference
            await self.jobs_collection.create_index([("scheduled_job_id", ASCENDING)])
            
            # Enrichment indexes
            await self.properties_collection.create_index([("enrichment.motivated_seller.score", DESCENDING)])
            await self.properties_collection.create_index([("is_motivated_seller", ASCENDING)])
            await self.properties_collection.create_index([("has_price_reduction", ASCENDING)])
            await self.properties_collection.create_index([("has_distress_signals", ASCENDING)])
            await self.properties_collection.create_index([("enrichment.is_auction", ASCENDING)])
            await self.properties_collection.create_index([("enrichment.is_reo", ASCENDING)])
            await self.properties_collection.create_index([("enrichment.is_probate", ASCENDING)])
            await self.properties_collection.create_index([("enrichment.is_short_sale", ASCENDING)])
            await self.properties_collection.create_index([("enrichment.is_as_is", ASCENDING)])
            
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
                # Only set started_at if not already set (don't overwrite on progress updates)
                existing_job = await self.jobs_collection.find_one({"job_id": job_id})
                if existing_job and not existing_job.get("started_at"):
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
    
    # Scheduled Jobs Management Methods
    async def create_scheduled_job(self, scheduled_job: ScheduledJob) -> str:
        """Create a new scheduled job (cron job)"""
        try:
            result = await self.scheduled_jobs_collection.insert_one(
                scheduled_job.dict(by_alias=True, exclude={"id"})
            )
            return str(result.inserted_id)
        except Exception as e:
            print(f"Error creating scheduled job: {e}")
            raise
    
    async def get_scheduled_job(self, scheduled_job_id: str) -> Optional[ScheduledJob]:
        """Get scheduled job by ID"""
        try:
            job_data = await self.scheduled_jobs_collection.find_one(
                {"scheduled_job_id": scheduled_job_id}
            )
            if job_data:
                job_data["_id"] = str(job_data["_id"])
                return ScheduledJob(**job_data)
            return None
        except Exception as e:
            print(f"Error getting scheduled job: {e}")
            return None
    
    async def get_active_scheduled_jobs(self) -> List[ScheduledJob]:
        """Get all active scheduled jobs"""
        try:
            cursor = self.scheduled_jobs_collection.find(
                {"status": ScheduledJobStatus.ACTIVE.value}
            ).sort([("next_run_at", ASCENDING)])
            
            jobs = []
            async for job_data in cursor:
                job_data["_id"] = str(job_data["_id"])
                jobs.append(ScheduledJob(**job_data))
            
            return jobs
        except Exception as e:
            print(f"Error getting active scheduled jobs: {e}")
            return []
    
    async def update_scheduled_job_status(self, scheduled_job_id: str, status: ScheduledJobStatus) -> bool:
        """Update scheduled job status"""
        try:
            result = await self.scheduled_jobs_collection.update_one(
                {"scheduled_job_id": scheduled_job_id},
                {
                    "$set": {
                        "status": status.value,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"Error updating scheduled job status: {e}")
            return False
    
    async def update_scheduled_job_run_history(
        self,
        scheduled_job_id: str,
        run_job_id: str,
        status: JobStatus,
        next_run_at: Optional[datetime] = None
    ) -> bool:
        """Update the run history of a scheduled job after execution"""
        try:
            # Get current run_count
            current_job = await self.scheduled_jobs_collection.find_one(
                {"scheduled_job_id": scheduled_job_id}
            )
            if not current_job:
                return False
            
            current_run_count = current_job.get("run_count", 0)
            
            update_data = {
                "run_count": current_run_count + 1,
                "last_run_at": datetime.utcnow(),
                "last_run_status": status.value,
                "last_run_job_id": run_job_id,
                "updated_at": datetime.utcnow()
            }
            
            if next_run_at:
                update_data["next_run_at"] = next_run_at
            
            result = await self.scheduled_jobs_collection.update_one(
                {"scheduled_job_id": scheduled_job_id},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"Error updating scheduled job run history: {e}")
            return False
    
    async def update_scheduled_job(self, scheduled_job_id: str, update_data: Dict[str, Any]) -> bool:
        """Update scheduled job fields"""
        try:
            update_data["updated_at"] = datetime.utcnow()
            result = await self.scheduled_jobs_collection.update_one(
                {"scheduled_job_id": scheduled_job_id},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"Error updating scheduled job: {e}")
            return False
    
    async def delete_scheduled_job(self, scheduled_job_id: str) -> bool:
        """Delete a scheduled job"""
        try:
            result = await self.scheduled_jobs_collection.delete_one(
                {"scheduled_job_id": scheduled_job_id}
            )
            return result.deleted_count > 0
        except Exception as e:
            print(f"Error deleting scheduled job: {e}")
            return False
    
    async def get_all_scheduled_jobs(self, limit: int = 100) -> List[ScheduledJob]:
        """Get all scheduled jobs (active and inactive)"""
        try:
            cursor = self.scheduled_jobs_collection.find().sort([
                ("status", ASCENDING),
                ("next_run_at", ASCENDING)
            ]).limit(limit)
            
            jobs = []
            async for job_data in cursor:
                job_data["_id"] = str(job_data["_id"])
                jobs.append(ScheduledJob(**job_data))
            
            return jobs
        except Exception as e:
            print(f"Error getting all scheduled jobs: {e}")
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
                    property_dict = property_data.dict(by_alias=True, exclude={"id"})
                    result = await self.properties_collection.replace_one(
                        {"property_id": property_data.property_id},
                        property_dict,
                        upsert=True
                    )
                    
                    # Update CRM properties that reference this MLS property
                    crm_updated_count = await self.update_crm_properties_from_mls(property_data.property_id)
                    
                    # Trigger enrichment in background (non-blocking!)
                    if self.enrichment_pipeline:
                        asyncio.create_task(
                            self._enrich_property_background(
                                property_id=property_data.property_id,
                                property_dict=property_dict,
                                existing_property=existing_property,
                                job_id=property_data.job_id
                            )
                        )
                    
                    return {
                        "action": "updated",
                        "reason": "content_changed",
                        "property_id": property_data.property_id,
                        "old_hash": existing_hash,
                        "new_hash": new_hash,
                        "crm_properties_updated": crm_updated_count
                    }
            else:
                # New property - insert it
                property_dict = property_data.dict(by_alias=True, exclude={"id"})
                result = await self.properties_collection.insert_one(property_dict)
                
                # Trigger enrichment in background (non-blocking!)
                if self.enrichment_pipeline:
                    asyncio.create_task(
                        self._enrich_property_background(
                            property_id=property_data.property_id,
                            property_dict=property_dict,
                            existing_property=None,
                            job_id=property_data.job_id
                        )
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
            print(f"[DEBUG] Searching for property_id: '{property_id}' (type: {type(property_id)})")
            
            # Try searching as string first
            property_data = await self.properties_collection.find_one({"property_id": property_id})
            
            # If not found, try searching as number
            if not property_data:
                try:
                    property_id_num = int(property_id)
                    print(f"[DEBUG] Trying as number: {property_id_num}")
                    property_data = await self.properties_collection.find_one({"property_id": property_id_num})
                except ValueError:
                    print(f"[DEBUG] Cannot convert '{property_id}' to number")
            
            if property_data:
                print(f"[DEBUG] Found property: {property_data.get('property_id')}")
                property_data["_id"] = str(property_data["_id"])
                return Property(**property_data)
            else:
                print(f"[DEBUG] Property not found: {property_id}")
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
    
    # CRM Integration Methods
    async def add_crm_property_reference(self, mls_property_id: str, crm_property_id: str) -> bool:
        """Add a CRM property reference to an MLS property"""
        try:
            result = await self.properties_collection.update_one(
                {"property_id": mls_property_id},
                {
                    "$addToSet": {"crm_property_ids": crm_property_id},
                    "$set": {"last_content_updated": datetime.utcnow()}
                }
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"Error adding CRM property reference: {e}")
            return False
    
    async def remove_crm_property_reference(self, mls_property_id: str, crm_property_id: str) -> bool:
        """Remove a CRM property reference from an MLS property"""
        try:
            result = await self.properties_collection.update_one(
                {"property_id": mls_property_id},
                {
                    "$pull": {"crm_property_ids": crm_property_id},
                    "$set": {"last_content_updated": datetime.utcnow()}
                }
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"Error removing CRM property reference: {e}")
            return False
    
    async def get_crm_property_references(self, mls_property_id: str) -> List[str]:
        """Get all CRM property IDs that reference an MLS property"""
        try:
            property_doc = await self.properties_collection.find_one(
                {"property_id": mls_property_id},
                {"crm_property_ids": 1}
            )
            return property_doc.get("crm_property_ids", []) if property_doc else []
        except Exception as e:
            print(f"Error getting CRM property references: {e}")
            return []
    
    async def update_crm_properties_from_mls(self, mls_property_id: str) -> int:
        """Update all CRM properties that reference an MLS property with the latest MLS data"""
        try:
            # Get the MLS property data
            mls_property = await self.properties_collection.find_one({"property_id": mls_property_id})
            if not mls_property:
                print(f"MLS property not found: {mls_property_id}")
                return 0
            
            # Get CRM property references
            crm_property_ids = mls_property.get("crm_property_ids", [])
            if not crm_property_ids:
                return 0
            
            # Connect to CRM database to update properties
            # Note: This assumes both databases are in the same MongoDB instance
            # You may need to adjust this based on your setup
            crm_db = self.db  # Same database instance
            crm_properties_collection = crm_db.properties  # Same collection name
            
            updated_count = 0
            
            # Update each CRM property with MLS data
            for crm_property_id in crm_property_ids:
                try:
                    # Prepare update data (exclude MLS-specific fields and preserve CRM-specific fields)
                    update_data = {
                        # Core property data
                        "mls_id": mls_property.get("mls_id"),
                        "mls": mls_property.get("mls"),
                        "status": mls_property.get("status"),
                        "mls_status": mls_property.get("mls_status"),
                        "listing_type": mls_property.get("listing_type"),
                        
                        # Address data
                        "address": mls_property.get("address"),
                        
                        # Description data
                        "description": mls_property.get("description"),
                        
                        # Financial data
                        "financial": mls_property.get("financial"),
                        
                        # Dates data
                        "dates": mls_property.get("dates"),
                        
                        # Location data
                        "location": mls_property.get("location"),
                        
                        # Contact information
                        "agent": mls_property.get("agent"),
                        "broker": mls_property.get("broker"),
                        "builder": mls_property.get("builder"),
                        "office": mls_property.get("office"),
                        
                        # URLs and references
                        "property_url": mls_property.get("property_url"),
                        "listing_id": mls_property.get("listing_id"),
                        "permalink": mls_property.get("permalink"),
                        
                        # Property images
                        "primary_photo": mls_property.get("primary_photo"),
                        "alt_photos": mls_property.get("alt_photos"),
                        
                        # Additional data
                        "days_on_mls": mls_property.get("days_on_mls"),
                        "new_construction": mls_property.get("new_construction"),
                        "monthly_fees": mls_property.get("monthly_fees"),
                        "one_time_fees": mls_property.get("one_time_fees"),
                        "tax_history": mls_property.get("tax_history"),
                        "nearby_schools": mls_property.get("nearby_schools"),
                        
                        # Content tracking
                        "content_hash": mls_property.get("content_hash"),
                        "last_content_updated": mls_property.get("last_content_updated"),
                        
                        # Metadata
                        "scraped_at": mls_property.get("scraped_at"),
                        "job_id": mls_property.get("job_id"),
                        "source": mls_property.get("source"),
                        
                        # Update timestamp
                        "updatedAt": datetime.utcnow()
                    }
                    
                    # Update the CRM property
                    result = await crm_properties_collection.update_one(
                        {"_id": crm_property_id},
                        {"$set": update_data}
                    )
                    
                    if result.modified_count > 0:
                        updated_count += 1
                        print(f"Updated CRM property {crm_property_id} with MLS data from {mls_property_id}")
                    
                except Exception as e:
                    print(f"Error updating CRM property {crm_property_id}: {e}")
                    continue
            
            print(f"Updated {updated_count} CRM properties from MLS property {mls_property_id}")
            return updated_count
            
        except Exception as e:
            print(f"Error updating CRM properties from MLS: {e}")
            return 0
    
    async def _enrich_property_background(self, property_id: str, property_dict: Dict[str, Any], existing_property: Optional[Dict[str, Any]], job_id: Optional[str]):
        """Background enrichment task - runs asynchronously without blocking scraping"""
        try:
            print(f"Starting background enrichment for property {property_id}")
            
            # Run enrichment
            enrichment_data = await self.enrichment_pipeline.enrich_property(
                property_id=property_id,
                property_dict=property_dict,
                existing_property=existing_property,
                job_id=job_id
            )
            
            print(f"Background enrichment completed for property {property_id}")
            
        except Exception as e:
            print(f"Error in background enrichment for property {property_id}: {e}")
            # Don't re-raise - this is a background task

# Global database instance
db = Database()
