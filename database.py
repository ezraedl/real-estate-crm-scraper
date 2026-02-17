import motor.motor_asyncio
from pymongo import ASCENDING, DESCENDING
from pymongo.operations import InsertOne, UpdateOne, ReplaceOne
from pymongo.errors import BulkWriteError
from bson import ObjectId
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import asyncio
import logging
from urllib.parse import urlparse
from config import settings
from models import ScrapingJob, Property, JobStatus, JobPriority, ScheduledJob, ScheduledJobStatus
from services import PropertyEnrichmentPipeline
from services.history_tracker import HistoryTracker
from services.property_differ import PropertyDiffer
from services.contact_service import ContactService

logger = logging.getLogger(__name__)

# Fields to preserve from existing property when rescraping with content change.
# replace_one() overwrites the whole document. Only HomeHarvest-originated fields (from
# Property model / convert_to_property_model) should be replaced; all other fields are
# from enrichment, RentCast, CRM, or tracking and must be preserved.
#
# HomeHarvest-originated (replaced): property_id, mls_id, mls, status, mls_status, listing_type,
# address, description, financial (only: list_price, list_price_min/max, sold_price, last_sold_price,
# price_per_sqft, estimated_value, tax_assessed_value, hoa_fee, tax), dates, location,
# agent/broker/builder/office (nested), agent_id/broker_id/builder_id/office_id, property_url,
# listing_id, permalink, primary_photo, alt_photos, days_on_mls, new_construction, monthly_fees,
# one_time_fees, tax_history, nearby_schools, content_hash, last_content_updated, scraped_at,
# job_id, scheduled_job_id, last_scraped, source, is_comp, crm_property_ids (handled separately).
_PRESERVE_ON_RECSCRAPE = frozenset([
    # RentCast
    "rent_estimation", "rent_estimation_custom",
    # Enrichment
    "enrichment", "is_motivated_seller", "has_price_reduction", "has_distress_signals", "last_enriched_at",
    # Change / price history (embedded)
    "change_logs", "change_logs_updated_at",
    # Tracking for enrichment (list_price_old etc. merged under financial)
    "_old_values_scraped_at", "status_old", "mls_status_old", "listing_type_old",
    # Census (backend)
    "census_tract_geoid", "census_tract_class_label", "census_tract_class_numeric", "census_tract_class_score",
    # CRM / frontend / GHL
    "comps", "crm_status", "ghl_final_update_sent", "is_favorite",
    "margin_percent", "renovation_cost_per_sqft", "renovation_enabled",
    # ARV Calculation
    "arv",
    "updatedAt",
])


def _normalize_bson_for_pydantic(value: Any) -> Any:
    """Recursively convert BSON types (e.g. ObjectId) to JSON-serializable types for Pydantic.
    Prevents validation errors when building Property from MongoDB documents."""
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, dict):
        return {k: _normalize_bson_for_pydantic(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_bson_for_pydantic(v) for v in value]
    return value


def _merge_preserved_fields_on_rescrape(existing: dict, target: dict) -> None:
    """Copy non–HomeHarvest fields from existing into target before replace_one so only
    HomeHarvest-originated data is replaced."""
    for k in _PRESERVE_ON_RECSCRAPE:
        if existing.get(k) is not None:
            target[k] = existing[k]
    # Merge financial fields that are NOT from HomeHarvest (PropertyFinancial has only
    # list_price, list_price_min/max, sold_price, last_sold_price, price_per_sqft,
    # estimated_value, tax_assessed_value, hoa_fee, tax). Preserve:
    # - _old fields (enrichment change detection)
    # - original_list_price (if set by enrichment or another source)
    ef = existing.get("financial") or {}
    if ef:
        tf = target.get("financial")
        if tf is None:
            tf = {}
            target["financial"] = tf
        for f in ("list_price_old", "original_list_price_old", "price_per_sqft_old", "original_list_price"):
            if ef.get(f) is not None:
                tf[f] = ef[f]

class Database:
    def __init__(self):
        self.client = None
        self.db = None
        self.jobs_collection = None
        self.properties_collection = None
        self.scheduled_jobs_collection = None
        self.enrichment_pipeline = None
        self.contact_service = ContactService()
    
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
            
            logger.info(f"Connected to MongoDB: {database_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
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
            await self.properties_collection.create_index([("scheduled_job_id", ASCENDING)])
            await self.properties_collection.create_index([("last_scraped", ASCENDING)])
            await self.properties_collection.create_index([("scheduled_job_id", ASCENDING), ("last_scraped", ASCENDING)])
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
            
            # New indexes for v2 scoring system
            await self.properties_collection.create_index([("enrichment.motivated_seller.score_uncapped", DESCENDING)])
            await self.properties_collection.create_index([("enrichment.motivated_seller.config_hash", ASCENDING)])
            
            # Indexes for embedded change_logs array (only price, status, listing_type)
            try:
                await self.properties_collection.create_index([
                    ("change_logs.field", ASCENDING),
                    ("change_logs.change_type", ASCENDING)
                ])
                print("Created index on properties.change_logs.field and change_type")
            except Exception as e:
                print(f"Index may already exist: {e}")
            
            try:
                await self.properties_collection.create_index([
                    ("change_logs.timestamp", DESCENDING)
                ])
                print("Created index on properties.change_logs.timestamp")
            except Exception as e:
                print(f"Index may already exist: {e}")
            
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
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            # CRITICAL: Check current status to prevent downgrading from final states
            existing_job = await self.jobs_collection.find_one({"job_id": job_id})
            if existing_job:
                current_status = existing_job.get('status')
                final_states = [JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.CANCELLED.value]
                
                # Prevent downgrading from final states to RUNNING
                if current_status in final_states and status == JobStatus.RUNNING:
                    logger.warning(
                        f"⚠️ Attempted to downgrade job {job_id} from {current_status} to RUNNING. "
                        f"This is blocked to prevent background tasks from overwriting completion status. "
                        f"Updating only non-status fields (progress_logs, etc.)"
                    )
                    # Remove status from update_data - only update other fields
                    update_data = {"updated_at": datetime.utcnow()}
                    update_data.update({k: v for k, v in kwargs.items() if k != "status"})
                    
                    # Update only non-status fields
                    result = await self.jobs_collection.update_one(
                        {"job_id": job_id},
                        {"$set": update_data}
                    )
                    return result.modified_count > 0
            
            update_data = {"status": status.value, "updated_at": datetime.utcnow()}
            update_data.update(kwargs)
            
            if status == JobStatus.RUNNING:
                # Only set started_at if not already set (don't overwrite on progress updates)
                if existing_job and not existing_job.get("started_at"):
                    update_data["started_at"] = datetime.utcnow()
            elif status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                # CRITICAL: Always set completed_at when status becomes final
                # This ensures completed_at is set atomically with the status change
                update_data["completed_at"] = datetime.utcnow()
                logger.debug(f"Setting completed_at for job {job_id} with status {status.value}")
            
            # Use atomic update to ensure status and completed_at are set together
            result = await self.jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": update_data}
            )
            
            success = result.modified_count > 0
            if not success:
                logger.warning(
                    f"Job status update returned modified_count=0 for job {job_id}, status={status.value}. "
                    f"Job may not exist or update was not applied."
                )
                # Verify the job exists
                job_exists = await self.jobs_collection.find_one({"job_id": job_id})
                if not job_exists:
                    logger.error(f"Job {job_id} does not exist in database!")
                else:
                    current_status = job_exists.get('status')
                    current_completed_at = job_exists.get('completed_at')
                    logger.warning(
                        f"Job {job_id} exists but update did not modify it. "
                        f"Current status: {current_status}, completed_at: {current_completed_at}"
                    )
                    # If status is already what we want, consider it success
                    if current_status == status.value:
                        logger.info(f"Job {job_id} already has status {status.value}, considering update successful")
                        success = True
            else:
                if status == JobStatus.COMPLETED:
                    logger.info(f"✅ Successfully updated job {job_id} status to COMPLETED with completed_at timestamp")
                else:
                    logger.debug(f"Successfully updated job {job_id} status to {status.value}")
            
            return success
        except Exception as e:
            logger.error(f"Error updating job status for {job_id}: {e}", exc_info=True)
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
        next_run_at: Optional[datetime] = None,
        was_full_scrape: Optional[bool] = None,
        error_message: Optional[str] = None,
        properties_scraped: Optional[int] = None,
        properties_saved: Optional[int] = None
    ) -> bool:
        """Update the run history of a scheduled job after execution"""
        try:
            # Get current job data
            current_job = await self.scheduled_jobs_collection.find_one(
                {"scheduled_job_id": scheduled_job_id}
            )
            if not current_job:
                return False
            
            current_run_count = current_job.get("run_count", 0)
            incremental_config = current_job.get("incremental_runs_before_full")
            current_incremental_count = current_job.get("incremental_runs_count", 0)
            
            update_data = {
                "run_count": current_run_count + 1,
                "last_run_at": datetime.utcnow(),
                "last_run_status": status.value,
                "last_run_job_id": run_job_id,
                "updated_at": datetime.utcnow()
            }
            
            if next_run_at:
                update_data["next_run_at"] = next_run_at
            
            # Add error message and property counts if provided
            if error_message is not None:
                update_data["last_run_error_message"] = error_message
            if properties_scraped is not None:
                update_data["last_run_properties_scraped"] = properties_scraped
            if properties_saved is not None:
                update_data["last_run_properties_saved"] = properties_saved
            
            # Update incremental scraping counters if was_full_scrape is provided
            if was_full_scrape is not None and incremental_config is not None:
                if was_full_scrape:
                    # Full scrape: reset counter and update last_full_scrape_at
                    update_data["incremental_runs_count"] = 0
                    update_data["last_full_scrape_at"] = datetime.utcnow()
                else:
                    # Incremental scrape: increment counter
                    update_data["incremental_runs_count"] = current_incremental_count + 1
            
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
                    # Content unchanged - only update days_on_mls, scraped_at, scheduled_job_id, and last_scraped
                    # But also ensure contact IDs are preserved/updated if contacts exist
                    update_fields = {
                        "days_on_mls": property_data.days_on_mls,
                        "scraped_at": property_data.scraped_at,
                        "job_id": property_data.job_id
                    }
                    
                    # Update scheduled_job_id and last_scraped
                    if property_data.scheduled_job_id:
                        update_fields["scheduled_job_id"] = property_data.scheduled_job_id
                    if property_data.last_scraped:
                        update_fields["last_scraped"] = property_data.last_scraped
                    
                    # When content is unchanged but new scrape has last_sold data and existing doesn't,
                    # backfill so ARV and UI have it (Realtor may add sold history after first scrape)
                    existing_fin = existing_property.get("financial") or {}
                    existing_dates = existing_property.get("dates") or {}
                    if property_data.financial and property_data.financial.last_sold_price is not None and existing_fin.get("last_sold_price") is None:
                        update_fields["financial.last_sold_price"] = property_data.financial.last_sold_price
                    if property_data.dates and property_data.dates.last_sold_date is not None and existing_dates.get("last_sold_date") is None:
                        update_fields["dates.last_sold_date"] = property_data.dates.last_sold_date
                    
                    # FIXED: Check both scraped data AND existing nested data in database
                    # This handles cases where existing property has nested data but scraped data doesn't include it
                    needs_contact_processing = False
                    contact_ids_changed = False
                    unset_fields = {}
                    
                    # Check agent
                    has_nested_agent = (existing_property.get("agent") and 
                                       (existing_property.get("agent", {}).get("agent_name") or 
                                        existing_property.get("agent", {}).get("agent_email") or 
                                        existing_property.get("agent", {}).get("agent_phones")))
                    has_scraped_agent = (property_data.agent and property_data.agent.agent_name)
                    has_agent_id = existing_property.get("agent_id")
                    
                    if (has_nested_agent or has_scraped_agent) and not has_agent_id:
                        needs_contact_processing = True
                    elif has_agent_id and has_nested_agent:
                        # Has ID but still has nested data - remove nested data
                        unset_fields["agent"] = ""
                    
                    # Check broker
                    has_nested_broker = (existing_property.get("broker") and 
                                        existing_property.get("broker", {}).get("broker_name"))
                    has_scraped_broker = (property_data.broker and property_data.broker.broker_name)
                    has_broker_id = existing_property.get("broker_id")
                    
                    if (has_nested_broker or has_scraped_broker) and not has_broker_id:
                        needs_contact_processing = True
                    elif has_broker_id and has_nested_broker:
                        unset_fields["broker"] = ""
                    
                    # Check builder
                    has_nested_builder = (existing_property.get("builder") and 
                                         existing_property.get("builder", {}).get("builder_name"))
                    has_scraped_builder = (property_data.builder and property_data.builder.builder_name)
                    has_builder_id = existing_property.get("builder_id")
                    
                    if (has_nested_builder or has_scraped_builder) and not has_builder_id:
                        needs_contact_processing = True
                    elif has_builder_id and has_nested_builder:
                        unset_fields["builder"] = ""
                    
                    # Check office
                    has_nested_office = (existing_property.get("office") and 
                                        (existing_property.get("office", {}).get("office_name") or 
                                         existing_property.get("office", {}).get("office_email") or 
                                         existing_property.get("office", {}).get("office_phones")))
                    has_scraped_office = (property_data.office and property_data.office.office_name)
                    has_office_id = existing_property.get("office_id")
                    
                    if (has_nested_office or has_scraped_office) and not has_office_id:
                        needs_contact_processing = True
                    elif has_office_id and has_nested_office:
                        unset_fields["office"] = ""
                    
                    # Process contacts if needed
                    if needs_contact_processing:
                        # Use existing nested data if scraped data doesn't have it
                        # Create a temporary property object with merged contact data
                        temp_property = property_data
                        if has_nested_agent and not has_scraped_agent:
                            # Create PropertyAgent from existing nested data
                            from models import PropertyAgent
                            temp_property.agent = PropertyAgent(**existing_property.get("agent", {}))
                        if has_nested_broker and not has_scraped_broker:
                            from models import PropertyBroker
                            temp_property.broker = PropertyBroker(**existing_property.get("broker", {}))
                        if has_nested_builder and not has_scraped_builder:
                            from models import PropertyBuilder
                            temp_property.builder = PropertyBuilder(**existing_property.get("builder", {}))
                        if has_nested_office and not has_scraped_office:
                            from models import PropertyOffice
                            temp_property.office = PropertyOffice(**existing_property.get("office", {}))
                        
                        try:
                            # Create/update contacts if they don't exist
                            contact_ids = await self.contact_service.process_property_contacts(temp_property)
                            
                            if not contact_ids:
                                logger.warning(f"Failed to process contacts for property {property_data.property_id} - API may be unavailable")
                            
                            # Preserve existing contact IDs if they exist
                            if existing_property.get("agent_id"):
                                contact_ids["agent_id"] = existing_property.get("agent_id")
                            if existing_property.get("broker_id"):
                                contact_ids["broker_id"] = existing_property.get("broker_id")
                            if existing_property.get("builder_id"):
                                contact_ids["builder_id"] = existing_property.get("builder_id")
                            if existing_property.get("office_id"):
                                contact_ids["office_id"] = existing_property.get("office_id")

                            # Detect contact ID changes (only if new IDs were produced)
                            if contact_ids.get("agent_id") and contact_ids.get("agent_id") != existing_property.get("agent_id"):
                                contact_ids_changed = True
                            if contact_ids.get("broker_id") and contact_ids.get("broker_id") != existing_property.get("broker_id"):
                                contact_ids_changed = True
                            if contact_ids.get("builder_id") and contact_ids.get("builder_id") != existing_property.get("builder_id"):
                                contact_ids_changed = True
                            if contact_ids.get("office_id") and contact_ids.get("office_id") != existing_property.get("office_id"):
                                contact_ids_changed = True
                            
                            # Add contact IDs to update
                            if contact_ids.get("agent_id"):
                                update_fields["agent_id"] = contact_ids["agent_id"]
                                # Remove nested data after successful linking
                                unset_fields["agent"] = ""
                            if contact_ids.get("broker_id"):
                                update_fields["broker_id"] = contact_ids["broker_id"]
                                unset_fields["broker"] = ""
                            if contact_ids.get("builder_id"):
                                update_fields["builder_id"] = contact_ids["builder_id"]
                                unset_fields["builder"] = ""
                            if contact_ids.get("office_id"):
                                update_fields["office_id"] = contact_ids["office_id"]
                                unset_fields["office"] = ""

                            if contact_ids_changed:
                                update_fields["contacts_updated_at"] = datetime.utcnow()
                        except Exception as e:
                            logger.error(f"Error processing contacts for property {property_data.property_id}: {e}")
                            # Continue without contact processing - don't fail the entire save
                    
                    # Build update operation
                    update_operation = {}
                    if update_fields:
                        update_operation["$set"] = update_fields
                    if unset_fields:
                        update_operation["$unset"] = unset_fields
                    
                    if update_operation:
                        result = await self.properties_collection.update_one(
                            {"property_id": property_data.property_id},
                            update_operation
                        )
                    else:
                        result = type('obj', (object,), {'modified_count': 0})()
                    return {
                        "action": "skipped",
                        "reason": "content_unchanged",
                        "property_id": property_data.property_id
                    }
                else:
                    # Content changed - update the property
                    differ = PropertyDiffer()
                    history_tracker = HistoryTracker(self.db)
                    # First, create/update contacts and get contact IDs
                    try:
                        contact_ids = await self.contact_service.process_property_contacts(property_data)
                        
                        if not contact_ids:
                            logger.warning(f"Failed to process contacts for property {property_data.property_id} - API may be unavailable")
                    except Exception as e:
                        logger.error(f"Error processing contacts for property {property_data.property_id}: {e}")
                        contact_ids = {}
                    
                    # Preserve existing contact IDs if they exist and new ones weren't created
                    if existing_property.get("agent_id") and not contact_ids.get("agent_id"):
                        contact_ids["agent_id"] = existing_property.get("agent_id")
                    if existing_property.get("broker_id") and not contact_ids.get("broker_id"):
                        contact_ids["broker_id"] = existing_property.get("broker_id")
                    if existing_property.get("builder_id") and not contact_ids.get("builder_id"):
                        contact_ids["builder_id"] = existing_property.get("builder_id")
                    if existing_property.get("office_id") and not contact_ids.get("office_id"):
                        contact_ids["office_id"] = existing_property.get("office_id")
                    
                    # Update property data with contact IDs
                    property_data.agent_id = contact_ids.get("agent_id")
                    property_data.broker_id = contact_ids.get("broker_id")
                    property_data.builder_id = contact_ids.get("builder_id")
                    property_data.office_id = contact_ids.get("office_id")

                    # Track contact ID changes
                    if (contact_ids.get("agent_id") and contact_ids.get("agent_id") != existing_property.get("agent_id")) or \
                       (contact_ids.get("broker_id") and contact_ids.get("broker_id") != existing_property.get("broker_id")) or \
                       (contact_ids.get("builder_id") and contact_ids.get("builder_id") != existing_property.get("builder_id")) or \
                       (contact_ids.get("office_id") and contact_ids.get("office_id") != existing_property.get("office_id")):
                        property_data.contacts_updated_at = datetime.utcnow()
                    
                    # Ensure scheduled_job_id and last_scraped are set
                    if property_data.scheduled_job_id:
                        property_data.scheduled_job_id = property_data.scheduled_job_id
                    if not property_data.last_scraped:
                        property_data.last_scraped = datetime.utcnow()
                    
                    property_dict = property_data.dict(by_alias=True, exclude={"id"})
                    
                    # CRITICAL: Preserve crm_property_ids from existing property
                    # This field is not in the Property model, so it gets lost during replace_one
                    if existing_property.get("crm_property_ids"):
                        property_dict["crm_property_ids"] = existing_property["crm_property_ids"]
                    
                    # Preserve rent_estimation (RentCast), enrichment, change_logs, and _old tracking fields.
                    # replace_one overwrites the full document; without this, rescraping would delete them.
                    _merge_preserved_fields_on_rescrape(existing_property, property_dict)
                    
                    # FIXED: Remove nested contact data after linking contacts
                    # Use update_one with $unset instead of replace_one to avoid re-adding nested data
                    # First, replace the property with contact IDs set
                    result = await self.properties_collection.replace_one(
                        {"property_id": property_data.property_id},
                        property_dict,
                        upsert=True
                    )
                    
                    # Then remove nested contact data if contacts were successfully linked
                    unset_fields = {}
                    if contact_ids.get("agent_id") and property_dict.get("agent"):
                        unset_fields["agent"] = ""
                    if contact_ids.get("broker_id") and property_dict.get("broker"):
                        unset_fields["broker"] = ""
                    if contact_ids.get("builder_id") and property_dict.get("builder"):
                        unset_fields["builder"] = ""
                    if contact_ids.get("office_id") and property_dict.get("office"):
                        unset_fields["office"] = ""
                    
                    if unset_fields:
                        await self.properties_collection.update_one(
                            {"property_id": property_data.property_id},
                            {"$unset": unset_fields}
                        )

                    # Record change logs for content changes (status/price/etc.)
                    try:
                        changes = differ.detect_changes(existing_property, property_dict)
                        if changes.get("has_changes"):
                            await history_tracker.record_change_logs(
                                property_data.property_id,
                                changes.get("field_changes", []),
                                property_data.job_id
                            )
                    except Exception as e:
                        logger.error(f"Error recording change logs for property {property_data.property_id}: {e}")
                    
                    # Update CRM properties that reference this MLS property
                    # Note: For internal scraper updates, we update all platforms
                    # In production, you may want to specify which platform to update
                    # For now, we'll skip platform-specific updates during scraping
                    # as the CRM system will handle syncing when it queries MLS properties
                    # crm_updated_count = await self.update_crm_properties_from_mls(property_data.property_id, platform)
                    crm_updated_count = 0  # Disabled for now - CRM systems handle their own syncing
                    
                    # Trigger enrichment in background (non-blocking!)
                    if self.enrichment_pipeline:
                        asyncio.create_task(
                            self._enrich_property_background(
                                property_id=property_data.property_id,
                                property_dict=property_dict,
                                existing_property=None,  # Deprecated - enrichment will fetch from DB
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
                # New property - create contacts and get contact IDs
                try:
                    contact_ids = await self.contact_service.process_property_contacts(property_data)
                    
                    if not contact_ids:
                        logger.warning(f"Failed to process contacts for new property {property_data.property_id} - API may be unavailable")
                except Exception as e:
                    logger.error(f"Error processing contacts for new property {property_data.property_id}: {e}")
                    contact_ids = {}
                
                # Update property data with contact IDs
                property_data.agent_id = contact_ids.get("agent_id")
                property_data.broker_id = contact_ids.get("broker_id")
                property_data.builder_id = contact_ids.get("builder_id")
                property_data.office_id = contact_ids.get("office_id")
                
                # Ensure scheduled_job_id and last_scraped are set
                if property_data.scheduled_job_id:
                    property_data.scheduled_job_id = property_data.scheduled_job_id
                if not property_data.last_scraped:
                    property_data.last_scraped = datetime.utcnow()
                
                property_dict = property_data.dict(by_alias=True, exclude={"id"})
                
                # FIXED: Remove nested contact data for new properties after linking
                # Since contacts are linked, we don't need nested data
                if contact_ids.get("agent_id") and property_dict.get("agent"):
                    property_dict.pop("agent", None)
                if contact_ids.get("broker_id") and property_dict.get("broker"):
                    property_dict.pop("broker", None)
                if contact_ids.get("builder_id") and property_dict.get("builder"):
                    property_dict.pop("builder", None)
                if contact_ids.get("office_id") and property_dict.get("office"):
                    property_dict.pop("office", None)
                
                result = await self.properties_collection.insert_one(property_dict)
                
                # Trigger enrichment in background (non-blocking!)
                if self.enrichment_pipeline:
                    asyncio.create_task(
                        self._enrich_property_background(
                            property_id=property_data.property_id,
                            property_dict=property_dict,
                            existing_property=None,  # Deprecated - enrichment will fetch from DB
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
            logger.error(f"Error saving property {property_data.property_id}: {e}")
            logger.debug(f"Error type: {type(e).__name__}")
            logger.debug(f"Property data keys: {list(property_data.dict().keys()) if hasattr(property_data, 'dict') else 'No dict method'}")
            return {
                "action": "error",
                "reason": str(e),
                "property_id": property_data.property_id
            }

    async def update_property_with_hash_and_logs(
        self,
        property_id: str,
        update_fields: Dict[str, Any],
        change_entries: Optional[List[Dict[str, Any]]] = None,
        job_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update a property and ensure content_hash/last_content_updated and change_logs stay consistent."""
        try:
            existing_property = await self.properties_collection.find_one({"property_id": property_id})
            if not existing_property:
                return {"action": "not_found", "property_id": property_id}

            # Merge updates onto existing document for hash recalculation
            merged = {**existing_property, **update_fields}
            # Normalize BSON types (ObjectId -> str) so Pydantic validation succeeds
            merged = _normalize_bson_for_pydantic(merged)

            # Filter to Property model fields for hash generation
            model_fields = set(Property.model_fields.keys())
            prop_payload = {k: merged.get(k) for k in model_fields if k in merged}
            # MongoDB uses _id; Property model uses id (alias _id)
            if "_id" in merged and "id" not in prop_payload:
                prop_payload["id"] = merged["_id"]

            prop_model = Property(**prop_payload)
            new_hash = prop_model.generate_content_hash()
            existing_hash = existing_property.get("content_hash")

            set_fields = dict(update_fields)
            set_fields["last_updated"] = datetime.utcnow()
            if existing_hash != new_hash:
                set_fields["content_hash"] = new_hash
                set_fields["last_content_updated"] = datetime.utcnow()

            if set_fields:
                await self.properties_collection.update_one(
                    {"property_id": property_id},
                    {"$set": set_fields}
                )

            if change_entries:
                history_tracker = HistoryTracker(self.db)
                await history_tracker.record_change_logs(
                    property_id,
                    change_entries,
                    job_id or "property_update"
                )

            return {
                "action": "updated",
                "property_id": property_id,
                "hash_changed": existing_hash != new_hash
            }
        except Exception as e:
            logger.error(f"Error updating property {property_id} with hash/logs: {e}")
            return {
                "action": "error",
                "reason": str(e),
                "property_id": property_id
            }
    
    def _validate_property_date_compliance(self, property_data: Property, job: Optional[ScrapingJob] = None) -> tuple[bool, Optional[str]]:
        """
        Validate that a property complies with job date requirements.
        Returns (is_valid, reason_if_invalid)
        
        Rules:
        - If job has past_days, check that property dates are within that range
        - For sold properties: check dates.last_sold_date or dates.list_date
        - For rent properties: check dates.list_date
        - For other properties: check dates.list_date or dates.last_sold_date
        """
        if not job or not job.past_days:
            # No date restriction, allow all properties
            return True, None
        
        cutoff_date = datetime.utcnow() - timedelta(days=job.past_days)
        
        # Get relevant date based on listing type
        property_date = None
        if property_data.listing_type == "sold":
            # For sold properties, check last_sold_date first, then list_date
            property_date = property_data.dates.last_sold_date if property_data.dates else None
            if not property_date and property_data.dates:
                property_date = property_data.dates.list_date
        elif property_data.listing_type == "for_rent":
            # For rent properties, check list_date
            property_date = property_data.dates.list_date if property_data.dates else None
        else:
            # For other properties (for_sale, pending), check list_date or last_sold_date
            if property_data.dates:
                property_date = property_data.dates.list_date or property_data.dates.last_sold_date
        
        if not property_date:
            # No date available - allow it (might be a new listing without date yet)
            return True, None
        
        # Check if property date is within the allowed range
        if property_date < cutoff_date:
            days_old = (datetime.utcnow() - property_date).days
            return False, f"Property date ({property_date.date()}) is {days_old} days old, exceeds job limit of {job.past_days} days"
        
        return True, None
    
    async def save_properties_batch(self, properties: List[Property], job: Optional[ScrapingJob] = None) -> Dict[str, Any]:
        """Save multiple properties in batch with hash-based change detection using bulk operations"""
        try:
            if not properties:
                return {"total": 0, "inserted": 0, "updated": 0, "skipped": 0, "errors": 0, "enrichment_queue": []}
            
            # Filter out properties that don't comply with job date requirements
            valid_properties = []
            date_filtered_count = 0
            date_filtered_reasons = []
            
            for prop in properties:
                is_valid, reason = self._validate_property_date_compliance(prop, job)
                if is_valid:
                    valid_properties.append(prop)
                else:
                    date_filtered_count += 1
                    date_filtered_reasons.append(f"{prop.property_id}: {reason}")
                    logger.debug(f"[DATE FILTER] Filtered out property {prop.property_id}: {reason}")
            
            if date_filtered_count > 0:
                logger.info(f"[DATE FILTER] Filtered out {date_filtered_count} properties that don't comply with job date requirements (past_days={job.past_days if job else None})")
                if len(date_filtered_reasons) <= 5:
                    for reason in date_filtered_reasons:
                        logger.debug(f"[DATE FILTER]   - {reason}")
                else:
                    for reason in date_filtered_reasons[:5]:
                        logger.debug(f"[DATE FILTER]   - {reason}")
                    logger.debug(f"[DATE FILTER]   ... and {len(date_filtered_reasons) - 5} more")
            
            # Use only valid properties for the rest of the batch save
            properties = valid_properties
            
            # Extract property IDs for batch fetch
            property_ids = [prop.property_id for prop in properties if prop.property_id]
            
            # Batch fetch all existing properties in one query
            existing_properties = {}
            if property_ids:
                cursor = self.properties_collection.find({"property_id": {"$in": property_ids}})
                async for prop_data in cursor:
                    existing_properties[prop_data["property_id"]] = prop_data
            
            # Prepare bulk operations
            bulk_operations = []
            results = {
                "total": len(properties),
                "inserted": 0,
                "updated": 0,
                "skipped": 0,
                "errors": 0,
                "details": [],
                "enrichment_queue": []  # Property IDs that need enrichment
            }
            
            # Step 1: Identify which properties need contact processing
            # This allows us to process all contacts in parallel
            properties_needing_contacts = []
            property_processing_info = {}  # Store processing info for each property
            
            for prop in properties:
                try:
                    existing_prop = existing_properties.get(prop.property_id)
                    needs_contacts = False
                    
                    if existing_prop:
                        existing_hash = existing_prop.get("content_hash")
                        new_hash = prop.content_hash
                        
                        if existing_hash == new_hash:
                            # Content unchanged - check if contacts need updating
                            # FIXED: Check both scraped data AND existing nested data in database
                            has_nested_agent = (existing_prop.get("agent") and 
                                               (existing_prop.get("agent", {}).get("agent_name") or 
                                                existing_prop.get("agent", {}).get("agent_email") or 
                                                existing_prop.get("agent", {}).get("agent_phones")))
                            has_nested_broker = (existing_prop.get("broker") and 
                                                existing_prop.get("broker", {}).get("broker_name"))
                            has_nested_builder = (existing_prop.get("builder") and 
                                                 existing_prop.get("builder", {}).get("builder_name"))
                            has_nested_office = (existing_prop.get("office") and 
                                                (existing_prop.get("office", {}).get("office_name") or 
                                                 existing_prop.get("office", {}).get("office_email") or 
                                                 existing_prop.get("office", {}).get("office_phones")))
                            
                            needs_contacts = (
                                ((prop.agent and prop.agent.agent_name) or has_nested_agent) and not existing_prop.get("agent_id") or
                                ((prop.broker and prop.broker.broker_name) or has_nested_broker) and not existing_prop.get("broker_id") or
                                ((prop.builder and prop.builder.builder_name) or has_nested_builder) and not existing_prop.get("builder_id") or
                                ((prop.office and prop.office.office_name) or has_nested_office) and not existing_prop.get("office_id")
                            )
                        else:
                            # Content changed - always process contacts
                            needs_contacts = True
                    else:
                        # New property - always process contacts
                        needs_contacts = True
                    
                    property_processing_info[prop.property_id] = {
                        "prop": prop,
                        "existing_prop": existing_prop,
                        "needs_contacts": needs_contacts
                    }
                    
                    if needs_contacts:
                        properties_needing_contacts.append(prop)
                        
                except Exception as e:
                    logger.error(f"Error analyzing property {prop.property_id if prop else 'unknown'}: {e}")
                    results["errors"] += 1
            
            # Step 2: Collect all contacts from all properties and send in one batch request
            contact_results = {}
            if properties_needing_contacts:
                logger.debug(f"Collecting contacts from {len(properties_needing_contacts)} properties for batch processing")
                
                # Collect all contacts with unique keys
                batch_contacts = []
                contact_key_map = {}  # Maps (property_id, contact_type) -> contact_key in batch
                
                for prop in properties_needing_contacts:
                    info = property_processing_info.get(prop.property_id)
                    existing_prop = info.get("existing_prop") if info else None
                    
                    # Agent contact - use scraped data or existing nested data
                    agent_data = None
                    if prop.agent and prop.agent.agent_name:
                        agent_data = prop.agent
                    elif existing_prop and existing_prop.get("agent"):
                        # Use existing nested data if scraped data doesn't have it
                        from models import PropertyAgent
                        agent_data = PropertyAgent(**existing_prop.get("agent", {}))
                    
                    if agent_data and agent_data.agent_name:
                        phones = None
                        phone = None
                        if agent_data.agent_phones:
                            phones = []
                            for p in agent_data.agent_phones:
                                if isinstance(p, dict):
                                    phones.append({
                                        "number": p.get('number'),
                                        "type": p.get('type')
                                    })
                                    if not phone and p.get('number'):
                                        phone = p.get('number')
                                elif isinstance(p, str):
                                    phones.append({"number": p})
                                    if not phone:
                                        phone = p
                        
                        contact_key = f"{prop.property_id}_agent"
                        contact_key_map[(prop.property_id, 'agent')] = contact_key
                        batch_contacts.append({
                            "key": contact_key,
                            "contact_type": "agent",
                            "name": agent_data.agent_name,
                            "email": agent_data.agent_email,
                            "phone": phone,
                            "phones": phones,
                            "agent_id": agent_data.agent_id,
                            "agent_mls_set": agent_data.agent_mls_set,
                            "agent_nrds_id": agent_data.agent_nrds_id,
                            "source": "scraper"
                        })
                    
                    # Broker contact - use scraped data or existing nested data
                    broker_data = None
                    if prop.broker and prop.broker.broker_name:
                        broker_data = prop.broker
                    elif existing_prop and existing_prop.get("broker"):
                        from models import PropertyBroker
                        broker_data = PropertyBroker(**existing_prop.get("broker", {}))
                    
                    if broker_data and broker_data.broker_name:
                        contact_key = f"{prop.property_id}_broker"
                        contact_key_map[(prop.property_id, 'broker')] = contact_key
                        batch_contacts.append({
                            "key": contact_key,
                            "contact_type": "broker",
                            "name": broker_data.broker_name,
                            "source": "scraper"
                        })
                    
                    # Builder contact - use scraped data or existing nested data
                    builder_data = None
                    if prop.builder and prop.builder.builder_name:
                        builder_data = prop.builder
                    elif existing_prop and existing_prop.get("builder"):
                        from models import PropertyBuilder
                        builder_data = PropertyBuilder(**existing_prop.get("builder", {}))
                    
                    if builder_data and builder_data.builder_name:
                        contact_key = f"{prop.property_id}_builder"
                        contact_key_map[(prop.property_id, 'builder')] = contact_key
                        batch_contacts.append({
                            "key": contact_key,
                            "contact_type": "builder",
                            "name": builder_data.builder_name,
                            "source": "scraper"
                        })
                    
                    # Office contact - use scraped data or existing nested data
                    office_data = None
                    if prop.office and prop.office.office_name:
                        office_data = prop.office
                    elif existing_prop and existing_prop.get("office"):
                        from models import PropertyOffice
                        office_data = PropertyOffice(**existing_prop.get("office", {}))
                    
                    if office_data and office_data.office_name:
                        phones = None
                        phone = None
                        if office_data.office_phones:
                            phones = []
                            for p in office_data.office_phones:
                                if isinstance(p, dict):
                                    phones.append({
                                        "number": p.get('number'),
                                        "type": p.get('type')
                                    })
                                    if not phone and p.get('number'):
                                        phone = p.get('number')
                                elif isinstance(p, str):
                                    phones.append({"number": p})
                                    if not phone:
                                        phone = p
                        
                        contact_key = f"{prop.property_id}_office"
                        contact_key_map[(prop.property_id, 'office')] = contact_key
                        batch_contacts.append({
                            "key": contact_key,
                            "contact_type": "office",
                            "name": office_data.office_name,
                            "email": office_data.office_email,
                            "phone": phone,
                            "phones": phones,
                            "office_id": office_data.office_id,
                            "office_mls_set": office_data.office_mls_set,
                            "source": "scraper"
                        })
                
                # Send all contacts in one batch request
                if batch_contacts:
                    logger.debug(f"Sending {len(batch_contacts)} contacts in batch request")
                    batch_result = await self.contact_service.batch_create_or_find_contacts(batch_contacts)
                    
                    # Map results back to properties
                    for prop in properties_needing_contacts:
                        contact_ids = {
                            "agent_id": None,
                            "broker_id": None,
                            "builder_id": None,
                            "office_id": None
                        }
                        
                        for contact_type in ['agent', 'broker', 'builder', 'office']:
                            contact_key = contact_key_map.get((prop.property_id, contact_type))
                            if contact_key and contact_key in batch_result:
                                contact = batch_result[contact_key]
                                if contact and contact.get('_id'):
                                    contact_ids[f'{contact_type}_id'] = str(contact['_id'])
                        
                        contact_results[prop.property_id] = contact_ids
                else:
                    # No contacts to process, initialize empty results
                    for prop in properties_needing_contacts:
                        contact_results[prop.property_id] = {
                            "agent_id": None,
                            "broker_id": None,
                            "builder_id": None,
                            "office_id": None
                        }
            
            # Step 3: Build bulk operations using contact results
            differ = PropertyDiffer()
            history_tracker = HistoryTracker(self.db)
            for prop in properties:
                try:
                    info = property_processing_info.get(prop.property_id)
                    if not info:
                        continue
                    
                    existing_prop = info["existing_prop"]
                    
                    if existing_prop:
                        # Property exists - check hash
                        existing_hash = existing_prop.get("content_hash")
                        new_hash = prop.content_hash
                        
                        if existing_hash == new_hash:
                            # Content unchanged - skip (only update metadata)
                            update_fields = {
                                "days_on_mls": prop.days_on_mls,
                                "scraped_at": prop.scraped_at,
                                "job_id": prop.job_id
                            }
                            if prop.scheduled_job_id:
                                update_fields["scheduled_job_id"] = prop.scheduled_job_id
                            if prop.last_scraped:
                                update_fields["last_scraped"] = prop.last_scraped
                            
                            # Add contact IDs if they were processed
                            unset_fields = {}
                            if info["needs_contacts"]:
                                contact_ids = contact_results.get(prop.property_id, {})
                                # Preserve existing contact IDs if new ones weren't created
                                if existing_prop.get("agent_id"):
                                    contact_ids["agent_id"] = existing_prop.get("agent_id")
                                if existing_prop.get("broker_id"):
                                    contact_ids["broker_id"] = existing_prop.get("broker_id")
                                if existing_prop.get("builder_id"):
                                    contact_ids["builder_id"] = existing_prop.get("builder_id")
                                if existing_prop.get("office_id"):
                                    contact_ids["office_id"] = existing_prop.get("office_id")
                                
                                if contact_ids.get("agent_id"):
                                    update_fields["agent_id"] = contact_ids["agent_id"]
                                    # Remove nested data after successful linking
                                    if existing_prop.get("agent"):
                                        unset_fields["agent"] = ""
                                if contact_ids.get("broker_id"):
                                    update_fields["broker_id"] = contact_ids["broker_id"]
                                    if existing_prop.get("broker"):
                                        unset_fields["broker"] = ""
                                if contact_ids.get("builder_id"):
                                    update_fields["builder_id"] = contact_ids["builder_id"]
                                    if existing_prop.get("builder"):
                                        unset_fields["builder"] = ""
                                if contact_ids.get("office_id"):
                                    update_fields["office_id"] = contact_ids["office_id"]
                                    if existing_prop.get("office"):
                                        unset_fields["office"] = ""
                            
                            # Build update operation
                            update_op = {}
                            if update_fields:
                                update_op["$set"] = update_fields
                            if unset_fields:
                                update_op["$unset"] = unset_fields
                            
                            if update_op:
                                bulk_operations.append(UpdateOne(
                                    {"property_id": prop.property_id},
                                    update_op
                                ))
                            results["skipped"] += 1
                            results["details"].append({
                                "action": "skipped",
                                "reason": "content_unchanged",
                                "property_id": prop.property_id
                            })
                        else:
                            # Content changed - full update
                            contact_ids = contact_results.get(prop.property_id, {})
                            
                            # Preserve existing contact IDs if new ones weren't created
                            if existing_prop.get("agent_id") and not contact_ids.get("agent_id"):
                                contact_ids["agent_id"] = existing_prop.get("agent_id")
                            if existing_prop.get("broker_id") and not contact_ids.get("broker_id"):
                                contact_ids["broker_id"] = existing_prop.get("broker_id")
                            if existing_prop.get("builder_id") and not contact_ids.get("builder_id"):
                                contact_ids["builder_id"] = existing_prop.get("builder_id")
                            if existing_prop.get("office_id") and not contact_ids.get("office_id"):
                                contact_ids["office_id"] = existing_prop.get("office_id")
                            
                            prop.agent_id = contact_ids.get("agent_id")
                            prop.broker_id = contact_ids.get("broker_id")
                            prop.builder_id = contact_ids.get("builder_id")
                            prop.office_id = contact_ids.get("office_id")
                            
                            if prop.scheduled_job_id:
                                prop.scheduled_job_id = prop.scheduled_job_id
                            if not prop.last_scraped:
                                prop.last_scraped = datetime.utcnow()
                            
                            property_dict = prop.dict(by_alias=True, exclude={"id"})
                            
                            # CRITICAL: Preserve crm_property_ids from existing property
                            # This field is not in the Property model, so it gets lost during ReplaceOne
                            if existing_prop.get("crm_property_ids"):
                                property_dict["crm_property_ids"] = existing_prop["crm_property_ids"]
                            
                            # Preserve rent_estimation (RentCast), enrichment, change_logs, and _old tracking.
                            # ReplaceOne overwrites the full document; without this, rescraping would delete them.
                            _merge_preserved_fields_on_rescrape(existing_prop, property_dict)
                            
                            # FIXED: Remove nested contact data after linking
                            if contact_ids.get("agent_id") and property_dict.get("agent"):
                                property_dict.pop("agent", None)
                            if contact_ids.get("broker_id") and property_dict.get("broker"):
                                property_dict.pop("broker", None)
                            if contact_ids.get("builder_id") and property_dict.get("builder"):
                                property_dict.pop("builder", None)
                            if contact_ids.get("office_id") and property_dict.get("office"):
                                property_dict.pop("office", None)
                            
                            bulk_operations.append(ReplaceOne(
                                {"property_id": prop.property_id},
                                property_dict,
                                upsert=True
                            ))

                            # Record change logs for content changes (status/price/etc.)
                            try:
                                changes = differ.detect_changes(existing_prop, property_dict)
                                if changes.get("has_changes"):
                                    await history_tracker.record_change_logs(
                                        prop.property_id,
                                        changes.get("field_changes", []),
                                        prop.job_id
                                    )
                            except Exception as e:
                                logger.error(f"Error recording change logs for property {prop.property_id}: {e}")
                            results["updated"] += 1
                            results["enrichment_queue"].append({
                                "property_id": prop.property_id,
                                "property_dict": property_dict,
                                "job_id": prop.job_id,
                                "listing_type": prop.listing_type  # Add listing_type for tracking
                            })
                            results["details"].append({
                                "action": "updated",
                                "reason": "content_changed",
                                "property_id": prop.property_id,
                                "old_hash": existing_hash,
                                "new_hash": new_hash
                            })
                    else:
                        # New property - insert
                        contact_ids = contact_results.get(prop.property_id, {})
                        
                        prop.agent_id = contact_ids.get("agent_id")
                        prop.broker_id = contact_ids.get("broker_id")
                        prop.builder_id = contact_ids.get("builder_id")
                        prop.office_id = contact_ids.get("office_id")
                        
                        if prop.scheduled_job_id:
                            prop.scheduled_job_id = prop.scheduled_job_id
                        if not prop.last_scraped:
                            prop.last_scraped = datetime.utcnow()
                        
                        property_dict = prop.dict(by_alias=True, exclude={"id"})
                        
                        # FIXED: Remove nested contact data for new properties after linking
                        if contact_ids.get("agent_id") and property_dict.get("agent"):
                            property_dict.pop("agent", None)
                        if contact_ids.get("broker_id") and property_dict.get("broker"):
                            property_dict.pop("broker", None)
                        if contact_ids.get("builder_id") and property_dict.get("builder"):
                            property_dict.pop("builder", None)
                        if contact_ids.get("office_id") and property_dict.get("office"):
                            property_dict.pop("office", None)
                        
                        bulk_operations.append(InsertOne(property_dict))
                        results["inserted"] += 1
                        results["enrichment_queue"].append({
                            "property_id": prop.property_id,
                            "property_dict": property_dict,
                            "job_id": prop.job_id,
                            "listing_type": prop.listing_type  # Add listing_type for tracking
                        })
                        results["details"].append({
                            "action": "inserted",
                            "reason": "new_property",
                            "property_id": prop.property_id,
                            "new_hash": prop.content_hash
                        })
                except Exception as e:
                    logger.error(f"Error processing property {prop.property_id if prop else 'unknown'}: {e}")
                    results["errors"] += 1
                    results["details"].append({
                        "action": "error",
                        "reason": str(e),
                        "property_id": prop.property_id if prop else None
                    })
            
            # Execute bulk write operation
            if bulk_operations:
                try:
                    bulk_result = await self.properties_collection.bulk_write(bulk_operations, ordered=False)
                    logger.debug(f"Bulk write: {bulk_result.inserted_count} inserted, {bulk_result.modified_count} modified, {bulk_result.upserted_count} upserted")
                except BulkWriteError as e:
                    details = e.details
                    results["inserted"] = details.get("nInserted", 0) + details.get("nUpserted", 0)
                    results["updated"] = details.get("nModified", 0)
                    results["errors"] = len(details.get("writeErrors", []))
                    failed_ids = set()
                    for we in details.get("writeErrors", []):
                        kv = we.get("keyValue") or {}
                        pid = kv.get("property_id")
                        if pid is not None:
                            failed_ids.add(pid)
                            failed_ids.add(str(pid))
                    results["enrichment_queue"] = [x for x in results["enrichment_queue"] if x.get("property_id") not in failed_ids]
                    logger.warning(
                        f"Bulk write partial failure: {results['errors']} writeErrors (e.g. duplicate key); "
                        f"inserted={results['inserted']}, updated={results['updated']}"
                    )
            
            logger.debug(f"Batch save results: {results['inserted']} inserted, {results['updated']} updated, {results['skipped']} skipped, {results['errors']} errors")
            return results
            
        except Exception as e:
            logger.error(f"Error saving properties batch: {e}")
            return {"total": len(properties), "inserted": 0, "updated": 0, "skipped": 0, "errors": len(properties), "details": [], "enrichment_queue": []}
    
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
                property_data = _normalize_bson_for_pydantic(property_data)
                property_data["_id"] = str(property_data.get("_id", ""))
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
                    prop_data = _normalize_bson_for_pydantic(prop_data)
                    prop_data["_id"] = str(prop_data.get("_id", ""))
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
                prop_data = _normalize_bson_for_pydantic(prop_data)
                prop_data["_id"] = str(prop_data.get("_id", ""))
                properties.append(Property(**prop_data))
            
            return properties
        except Exception as e:
            print(f"Error searching properties: {e}")
            return []
    
    async def find_properties_by_city_state(self, city: str, state: str, listing_types: Optional[List[str]] = None, limit: int = 1000) -> List[Property]:
        """Find properties by city and state, optionally filtered by listing_types. Returns sorted by most recently scraped."""
        try:
            # Build query
            query = {
                "address.city": {"$regex": f"^{city}$", "$options": "i"},
                "address.state": {"$regex": f"^{state}$", "$options": "i"}
            }
            
            # Add listing_type filter if provided
            if listing_types:
                query["listing_type"] = {"$in": listing_types}
            
            # Sort by most recently scraped (descending) to prioritize recent properties
            cursor = self.properties_collection.find(query).sort("scraped_at", DESCENDING).limit(limit)
            
            properties = []
            async for prop_data in cursor:
                prop_data = _normalize_bson_for_pydantic(prop_data)
                prop_data["_id"] = str(prop_data.get("_id", ""))
                properties.append(Property(**prop_data))
            
            return properties
        except Exception as e:
            print(f"Error finding properties by city/state: {e}")
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
    async def add_crm_property_reference(self, mls_property_id: str, crm_property_id: str, platform: str) -> bool:
        """Add a CRM property reference to an MLS property for a specific platform"""
        try:
            # First, initialize crm_property_ids as an object if it doesn't exist or is a legacy array
            init_result = await self.properties_collection.update_one(
                {
                    "property_id": mls_property_id,
                    "$or": [
                        {"crm_property_ids": None},
                        {"crm_property_ids": {"$exists": False}},
                        {"crm_property_ids": {"$type": "array"}}  # Legacy array format
                    ]
                },
                {"$set": {"crm_property_ids": {}, "last_updated": datetime.utcnow()}}
            )
            
            # Initialize platform array if it doesn't exist
            init_platform_result = await self.properties_collection.update_one(
                {
                    "property_id": mls_property_id,
                    f"crm_property_ids.{platform}": {"$exists": False}
                },
                {"$set": {f"crm_property_ids.{platform}": [], "last_updated": datetime.utcnow()}}
            )
            
            # Add the CRM property ID to the platform-specific array
            result = await self.properties_collection.update_one(
                {"property_id": mls_property_id},
                {
                    "$addToSet": {f"crm_property_ids.{platform}": crm_property_id},
                    "$set": {"last_updated": datetime.utcnow()}
                }
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"Error adding CRM property reference: {e}")
            return False
    
    async def remove_crm_property_reference(self, mls_property_id: str, crm_property_id: str, platform: str) -> bool:
        """Remove a CRM property reference from an MLS property for a specific platform"""
        try:
            result = await self.properties_collection.update_one(
                {"property_id": mls_property_id},
                {
                    "$pull": {f"crm_property_ids.{platform}": crm_property_id},
                    "$set": {"last_updated": datetime.utcnow()}
                }
            )
            return result.modified_count > 0
        except Exception as e:
            print(f"Error removing CRM property reference: {e}")
            return False
    
    async def get_crm_property_references(self, mls_property_id: str, platform: str) -> List[str]:
        """Get all CRM property IDs that reference an MLS property for a specific platform"""
        try:
            property_doc = await self.properties_collection.find_one(
                {"property_id": mls_property_id},
                {"crm_property_ids": 1}
            )
            if not property_doc:
                return []
            
            crm_property_ids = property_doc.get("crm_property_ids", {})
            
            # Handle both nested structure (new) and legacy array format (for backward compatibility)
            if isinstance(crm_property_ids, dict):
                # New nested structure: crm_property_ids: { "platform": [...] }
                return crm_property_ids.get(platform, [])
            elif isinstance(crm_property_ids, list):
                # Legacy array format: crm_property_ids: [...]
                return crm_property_ids
            else:
                return []
        except Exception as e:
            print(f"Error getting CRM property references: {e}")
            return []
    
    async def update_crm_properties_from_mls(self, mls_property_id: str, platform: str) -> int:
        """Update all CRM properties that reference an MLS property with the latest MLS data for a specific platform"""
        try:
            # Get the MLS property data
            mls_property = await self.properties_collection.find_one({"property_id": mls_property_id})
            if not mls_property:
                print(f"MLS property not found: {mls_property_id}")
                return 0
            
            # Get CRM property references for the specified platform
            crm_property_ids_obj = mls_property.get("crm_property_ids", {})
            
            # Handle both nested structure (new) and legacy array format (for backward compatibility)
            if isinstance(crm_property_ids_obj, dict):
                # New nested structure: crm_property_ids: { "platform": [...] }
                crm_property_ids = crm_property_ids_obj.get(platform, [])
            elif isinstance(crm_property_ids_obj, list):
                # Legacy array format: crm_property_ids: [...]
                crm_property_ids = crm_property_ids_obj
            else:
                crm_property_ids = []
            
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
            logger.debug(f"Starting background enrichment for property {property_id}")
            
            # Run enrichment (existing_property is deprecated, enrichment will fetch from DB)
            enrichment_data = await self.enrichment_pipeline.enrich_property(
                property_id=property_id,
                property_dict=property_dict,
                existing_property=None,  # Deprecated - enrichment will fetch from DB
                job_id=job_id
            )
            
            logger.debug(f"Background enrichment completed for property {property_id}")
            
        except Exception as e:
            logger.error(f"Error in background enrichment for property {property_id}: {e}")
            # Don't re-raise - this is a background task
    
    # Enrichment Recalculation Methods
    
    async def recalculate_all_scores(self, limit: Optional[int] = None, min_score: Optional[float] = None) -> Dict[str, Any]:
        """
        Recalculate scores for all properties with findings (scores_only recalculation)
        
        Args:
            limit: Max properties to process (None = all)
            min_score: Only recalc properties above this score (None = all)
            
        Returns:
            Statistics about the recalculation
        """
        try:
            start_time = datetime.utcnow()
            total_processed = 0
            total_updated = 0
            total_errors = 0
            errors = []
            
            # Build query
            query = {
                "enrichment.motivated_seller.findings": {"$exists": True}
            }
            if min_score is not None:
                query["enrichment.motivated_seller.score"] = {"$gte": min_score}
            
            # Get cursor
            cursor = self.properties_collection.find(query, {"property_id": 1})
            if limit:
                cursor = cursor.limit(limit)
            
            # Process in batches
            batch_size = 100
            property_ids = []
            
            async for doc in cursor:
                property_ids.append(doc["property_id"])
                total_processed += 1
                
                if len(property_ids) >= batch_size:
                    # Process batch
                    for prop_id in property_ids:
                        try:
                            result = await self.enrichment_pipeline.recalculate_scores_only(prop_id)
                            if result:
                                total_updated += 1
                        except Exception as e:
                            total_errors += 1
                            errors.append(f"Property {prop_id}: {str(e)}")
                            logger.error(f"Error recalculating score for {prop_id}: {e}")
                    
                    property_ids = []
                    
                    # Log progress
                    print(f"Processed {total_processed} properties, updated {total_updated}, errors {total_errors}")
            
            # Process remaining
            for prop_id in property_ids:
                try:
                    result = await self.enrichment_pipeline.recalculate_scores_only(prop_id)
                    if result:
                        total_updated += 1
                except Exception as e:
                    total_errors += 1
                    errors.append(f"Property {prop_id}: {str(e)}")
                    logger.error(f"Error recalculating score for {prop_id}: {e}")
            
            completed_at = datetime.utcnow()
            duration = (completed_at - start_time).total_seconds()
            
            return {
                "total_processed": total_processed,
                "total_updated": total_updated,
                "total_errors": total_errors,
                "started_at": start_time,
                "completed_at": completed_at,
                "duration_seconds": duration,
                "errors": errors[:50]  # Limit error list
            }
            
        except Exception as e:
            logger.error(f"Error in recalculate_all_scores: {e}")
            return {
                "total_processed": total_processed,
                "total_updated": total_updated,
                "total_errors": total_errors + 1,
                "started_at": start_time,
                "completed_at": datetime.utcnow(),
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
                "errors": errors + [str(e)]
            }
    
    async def cleanup_old_properties(self) -> Dict[str, int]:
        """
        Daily cleanup job to delete non-relevant properties:
        - Properties sold more than 1 year ago
        - Rent properties with listing date more than 2 years ago
        - All other properties with last updated listing date more than 1 year ago
        
        Returns dict with counts of deleted properties by type.
        """
        try:
            now = datetime.utcnow()
            results = {
                "sold_deleted": 0,
                "rent_deleted": 0,
                "other_deleted": 0,
                "total_deleted": 0
            }
            
            # 1. Delete sold properties sold more than 1 year ago
            sold_cutoff = now - timedelta(days=365)
            sold_query = {
                "listing_type": "sold",
                "$or": [
                    {"dates.last_sold_date": {"$lt": sold_cutoff}},
                    {"dates.list_date": {"$lt": sold_cutoff}},  # Fallback to list_date if no last_sold_date
                ],
            }
            sold_result = await self.properties_collection.delete_many(sold_query)
            results["sold_deleted"] = sold_result.deleted_count
            
            # 2. Delete rent properties with listing date more than 2 years ago
            rent_cutoff = now - timedelta(days=730)  # 2 years
            rent_query = {
                "listing_type": "for_rent",
                "$or": [
                    {"dates.list_date": {"$lt": rent_cutoff}},
                    {"last_updated": {"$lt": rent_cutoff}},  # Fallback to last_updated
                ],
            }
            rent_result = await self.properties_collection.delete_many(rent_query)
            results["rent_deleted"] = rent_result.deleted_count
            
            # 3. Delete all other properties (for_sale, pending, off_market) with last updated listing date more than 1 year ago
            other_cutoff = now - timedelta(days=365)
            # Get the most recent date from list_date, last_sold_date, or last_updated
            other_query = {
                "listing_type": {"$in": ["for_sale", "pending", "off_market"]},
                "$or": [
                    {"dates.list_date": {"$lt": other_cutoff}},
                    {"dates.last_sold_date": {"$lt": other_cutoff}},
                    {"last_updated": {"$lt": other_cutoff}},
                    {"last_content_updated": {"$lt": other_cutoff}},
                ],
            }
            other_result = await self.properties_collection.delete_many(other_query)
            results["other_deleted"] = other_result.deleted_count
            
            results["total_deleted"] = results["sold_deleted"] + results["rent_deleted"] + results["other_deleted"]
            
            if results["total_deleted"] > 0:
                logger.info(
                    f"[Daily Cleanup] Deleted {results['total_deleted']} old properties: "
                    f"{results['sold_deleted']} sold (>1yr), "
                    f"{results['rent_deleted']} rent (>2yr), "
                    f"{results['other_deleted']} other (>1yr)"
                )
            else:
                logger.info("[Daily Cleanup] No old properties to delete")
            
            return results
            
        except Exception as e:
            logger.error(f"[Daily Cleanup] Failed to cleanup old properties: {e}")
            return {
                "sold_deleted": 0,
                "rent_deleted": 0,
                "other_deleted": 0,
                "total_deleted": 0,
                "error": str(e)
            }
    
    async def redetect_keywords_all(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Re-detect keywords for all properties
        
        Args:
            limit: Max properties to process (None = all)
            
        Returns:
            Statistics about the re-detection
        """
        try:
            start_time = datetime.utcnow()
            total_processed = 0
            total_updated = 0
            total_errors = 0
            errors = []
            
            # Build query - get all properties with descriptions
            query = {
                "$or": [
                    {"description.text": {"$exists": True, "$ne": None, "$ne": ""}},
                    {"description.full_description": {"$exists": True, "$ne": None, "$ne": ""}},
                    {"description": {"$exists": True, "$type": "string", "$ne": ""}}
                ]
            }
            
            # Get cursor
            cursor = self.properties_collection.find(query, {"property_id": 1})
            if limit:
                cursor = cursor.limit(limit)
            
            # Process in batches
            batch_size = 100
            property_ids = []
            
            async for doc in cursor:
                property_ids.append(doc["property_id"])
                total_processed += 1
                
                if len(property_ids) >= batch_size:
                    # Process batch
                    for prop_id in property_ids:
                        try:
                            result = await self.enrichment_pipeline.detect_keywords_only(prop_id)
                            if result:
                                total_updated += 1
                        except Exception as e:
                            total_errors += 1
                            errors.append(f"Property {prop_id}: {str(e)}")
                            logger.error(f"Error re-detecting keywords for {prop_id}: {e}")
                    
                    property_ids = []
                    print(f"Processed {total_processed} properties, updated {total_updated}, errors {total_errors}")
            
            # Process remaining
            for prop_id in property_ids:
                try:
                    result = await self.enrichment_pipeline.detect_keywords_only(prop_id)
                    if result:
                        total_updated += 1
                except Exception as e:
                    total_errors += 1
                    errors.append(f"Property {prop_id}: {str(e)}")
                    logger.error(f"Error re-detecting keywords for {prop_id}: {e}")
            
            completed_at = datetime.utcnow()
            duration = (completed_at - start_time).total_seconds()
            
            return {
                "total_processed": total_processed,
                "total_updated": total_updated,
                "total_errors": total_errors,
                "started_at": start_time,
                "completed_at": completed_at,
                "duration_seconds": duration,
                "errors": errors[:50]
            }
            
        except Exception as e:
            logger.error(f"Error in redetect_keywords_all: {e}")
            return {
                "total_processed": total_processed,
                "total_updated": total_updated,
                "total_errors": total_errors + 1,
                "started_at": start_time,
                "completed_at": datetime.utcnow(),
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
                "errors": errors + [str(e)]
            }
    
    async def update_dom_all(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Update days on market for all properties and recalculate scores
        
        Args:
            limit: Max properties to process (None = all)
            
        Returns:
            Statistics about the update
        """
        try:
            start_time = datetime.utcnow()
            total_processed = 0
            total_updated = 0
            total_errors = 0
            errors = []
            
            # Get all properties (with or without findings)
            query = {}
            cursor = self.properties_collection.find(query, {"property_id": 1})
            if limit:
                cursor = cursor.limit(limit)
            
            # Process in batches
            batch_size = 100
            property_ids = []
            
            async for doc in cursor:
                property_ids.append(doc["property_id"])
                total_processed += 1
                
                if len(property_ids) >= batch_size:
                    # Process batch
                    for prop_id in property_ids:
                        try:
                            result = await self.enrichment_pipeline.update_dom_only(prop_id)
                            if result:
                                total_updated += 1
                        except Exception as e:
                            total_errors += 1
                            errors.append(f"Property {prop_id}: {str(e)}")
                            logger.error(f"Error updating DOM for {prop_id}: {e}")
                    
                    property_ids = []
                    print(f"Processed {total_processed} properties, updated {total_updated}, errors {total_errors}")
            
            # Process remaining
            for prop_id in property_ids:
                try:
                    result = await self.enrichment_pipeline.update_dom_only(prop_id)
                    if result:
                        total_updated += 1
                except Exception as e:
                    total_errors += 1
                    errors.append(f"Property {prop_id}: {str(e)}")
                    logger.error(f"Error updating DOM for {prop_id}: {e}")
            
            completed_at = datetime.utcnow()
            duration = (completed_at - start_time).total_seconds()
            
            return {
                "total_processed": total_processed,
                "total_updated": total_updated,
                "total_errors": total_errors,
                "started_at": start_time,
                "completed_at": completed_at,
                "duration_seconds": duration,
                "errors": errors[:50]
            }
            
        except Exception as e:
            logger.error(f"Error in update_dom_all: {e}")
            return {
                "total_processed": total_processed,
                "total_updated": total_updated,
                "total_errors": total_errors + 1,
                "started_at": start_time,
                "completed_at": datetime.utcnow(),
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
                "errors": errors + [str(e)]
            }

# Global database instance
db = Database()
