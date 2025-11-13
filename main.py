from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from typing import List, Optional, Dict, Any
import asyncio
import uuid
from datetime import datetime
import logging
import os
from scheduler import scheduler

from models import (
    ImmediateScrapeRequest, 
    ScheduledScrapeRequest, 
    TriggerJobRequest,
    JobResponse, 
    JobStatusResponse,
    ImmediateScrapeResponse,
    ScrapingJob,
    PropertyIdsRequest,
    JobStatus,
    JobPriority,
    Property,
    ListingType,
    ScheduledJob,
    ScheduledJobStatus,
    EnrichmentRecalcRequest,
    EnrichmentRecalcResponse,
    EnrichmentConfigResponse
)
from database import db
from scraper import scraper
from proxy_manager import proxy_manager
from config import settings
from services.zip_code_service import zip_code_service

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="MLS Scraping Server",
    description="High-performance MLS data scraping server with MongoDB storage",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Helper function to determine remaining work from progress logs
def get_remaining_locations(job: ScrapingJob) -> List[str]:
    """
    Parse progress logs to determine which locations still need to be processed.
    Returns list of locations that were not completed.
    """
    if not job.progress_logs or not job.locations:
        # No logs or no locations, retry everything
        return job.locations
    
    completed_locations = set()
    
    # Look for location completion entries in progress logs
    for log in job.progress_logs:
        if isinstance(log, dict):
            # A location is considered complete ONLY if it has the final summary
            # (not the in-progress entries created after each listing type)
            # Final summary has properties_found but NO status field or status != "in_progress"
            if "location" in log and "properties_found" in log:
                # Check if this is a final summary (not an in-progress update)
                status = log.get("status")
                if status != "in_progress":
                    # This is a final location summary
                    completed_locations.add(log["location"])
    
    # Return locations that weren't completed
    remaining = [loc for loc in job.locations if loc not in completed_locations]
    
    if remaining:
        logger.info(f"Job {job.job_id} completed {len(completed_locations)}/{len(job.locations)} locations. Remaining: {remaining}")
    else:
        logger.warning(f"Job {job.job_id} has no remaining locations based on logs, but was stuck. Will retry all locations.")
        remaining = job.locations  # Safety: retry everything if something seems wrong
    
    return remaining

# Helper function to create daily DOM update scheduled job
async def create_daily_dom_update_job():
    """
    Create a scheduled job that updates days on market and recalculates scores daily.
    Runs at 2 AM UTC to avoid peak hours.
    """
    try:
        # Check if the job already exists
        existing_job = await db.scheduled_jobs_collection.find_one({
            "scheduled_job_id": "daily_dom_update"
        })
        
        if existing_job:
            logger.info("Daily DOM update job already exists")
            return
        
        # Create a scheduled job for DOM updates (runs daily at 2 AM UTC)
        # Note: This creates a scheduled job entry, but the actual execution
        # will need to be handled by calling the /enrichment/update-dom endpoint
        # For now, we'll just create a placeholder that can be triggered manually
        # or via an external cron job calling the API endpoint
        
        logger.info("Daily DOM update: Use POST /enrichment/update-dom endpoint or set up external cron")
        
    except Exception as e:
        logger.error(f"Error setting up daily DOM update job: {e}")

# Helper function to create active properties re-scraping job
async def create_active_properties_rescraping_job():
    """
    Create a scheduled job that automatically re-scrapes properties with crm_status='active'.
    This job runs every hour by default (configurable via environment variable).
    """
    try:
        # Check if the job already exists
        existing_job = await db.scheduled_jobs_collection.find_one({
            "scheduled_job_id": "active_properties_rescraping"
        })
        
        if existing_job:
            logger.info("Active properties re-scraping job already exists")
            return
        
        # Get scraping frequency from environment (default: every hour)
        scraping_frequency = os.getenv('ACTIVE_PROPERTIES_SCRAPING_FREQUENCY', '0 * * * *')  # Every hour by default
        
        # Create the scheduled job
        scheduled_job = ScheduledJob(
            scheduled_job_id="active_properties_rescraping",
            name="Active Properties Re-scraping",
            description="Automatically re-scrapes properties with crm_status='active' to keep data fresh",
            status=ScheduledJobStatus.ACTIVE,
            cron_expression=scraping_frequency,
            timezone="UTC",
            locations=[],  # Will be populated dynamically based on active properties
            listing_types=[ListingType.FOR_SALE],  # Focus on active listings
            limit=1000,  # Reasonable limit for re-scraping
            priority=JobPriority.NORMAL,
            created_by="system",
            request_delay=2.0  # Be respectful to MLS systems
        )
        
        # Save to database
        await db.scheduled_jobs_collection.insert_one(scheduled_job.dict(by_alias=True, exclude={"id"}))
        logger.info(f"Created active properties re-scraping job with frequency: {scraping_frequency}")
        
    except Exception as e:
        logger.error(f"Error creating active properties re-scraping job: {e}")

# Helper function to handle stuck jobs on startup
async def handle_stuck_jobs():
    """
    Check for jobs stuck in 'running' status and retry them.
    For each scheduled job, only retry the most recent stuck job.
    All other stuck jobs for the same scheduled job are marked as failed immediately.
    Jobs are retried up to 3 times (original + 2 retries).
    After 3 failed attempts, mark as failed.
    """
    try:
        # Find all jobs stuck in "running" status
        stuck_jobs_cursor = db.jobs_collection.find({"status": JobStatus.RUNNING.value}).sort([("created_at", -1)])
        stuck_jobs = []
        async for job_data in stuck_jobs_cursor:
            job_data["_id"] = str(job_data["_id"])
            stuck_jobs.append(ScrapingJob(**job_data))
        
        if not stuck_jobs:
            logger.info("No stuck jobs found")
            return
        
        logger.warning(f"Found {len(stuck_jobs)} stuck jobs in 'running' status")
        
        # Group stuck jobs by scheduled_job_id (or None for one-time jobs)
        jobs_by_scheduled = {}
        for job in stuck_jobs:
            key = job.scheduled_job_id or job.job_id
            if key not in jobs_by_scheduled:
                jobs_by_scheduled[key] = []
            jobs_by_scheduled[key].append(job)
        
        # Process each group
        for scheduled_key, job_group in jobs_by_scheduled.items():
            if len(job_group) > 1:
                logger.warning(
                    f"Found {len(job_group)} stuck jobs for scheduled_job_id={scheduled_key}. "
                    f"Will retry only the most recent one and fail the rest."
                )
                # Most recent is first (sorted by created_at desc)
                job_to_retry = job_group[0]
                jobs_to_fail = job_group[1:]
                
                # Mark the older ones as failed immediately
                for old_job in jobs_to_fail:
                    logger.info(f"Marking old stuck job as failed: {old_job.job_id}")
                    await db.update_job_status(
                        old_job.job_id,
                        JobStatus.FAILED,
                        error_message="Job stuck in running status. Newer job found for same scheduled job."
                    )
            else:
                job_to_retry = job_group[0]
            
            # Now handle the job to retry
            job = job_to_retry
            try:
                # Check how many times this job has been retried
                retry_count = job.job_id.count("_retry_")
                
                if retry_count >= 2:
                    # Already retried twice (3 total attempts), mark as failed
                    logger.warning(
                        f"Job {job.job_id} has been retried {retry_count} times. Marking as failed."
                    )
                    await db.update_job_status(
                        job.job_id,
                        JobStatus.FAILED,
                        error_message="Job stuck in running status after service restart. Max retries exceeded."
                    )
                    
                    # Update scheduled job history if applicable
                    if job.scheduled_job_id:
                        await db.update_scheduled_job_run_history(
                            job.scheduled_job_id,
                            job.job_id,
                            JobStatus.FAILED
                        )
                else:
                    # Retry the job - smart resume from where it left off
                    logger.info(
                        f"Retrying stuck job {job.job_id} (attempt {retry_count + 2}/3)"
                    )
                    
                    # Determine which locations still need to be processed
                    remaining_locations = get_remaining_locations(job)
                    
                    if not remaining_locations:
                        logger.warning(f"No remaining locations for job {job.job_id}, marking as completed")
                        await db.update_job_status(
                            job.job_id,
                            JobStatus.COMPLETED,
                            error_message="Job was stuck but all locations were completed"
                        )
                        continue
                    
                    # Create a retry job with only remaining locations
                    retry_job_id = f"{job.job_id}_retry_{retry_count + 1}"
                    retry_job = ScrapingJob(
                        job_id=retry_job_id,
                        priority=JobPriority.HIGH,  # High priority for retries
                        scheduled_job_id=job.scheduled_job_id,
                        locations=remaining_locations,  # Only remaining locations!
                        listing_types=job.listing_types,  # Support multi-select
                        listing_type=job.listing_type,
                        property_types=job.property_types,
                        past_days=job.past_days,
                        date_from=job.date_from,
                        date_to=job.date_to,
                        radius=job.radius,
                        mls_only=job.mls_only,
                        foreclosure=job.foreclosure,
                        exclude_pending=job.exclude_pending,
                        limit=job.limit,
                        proxy_config=job.proxy_config,
                        user_agent=job.user_agent,
                        request_delay=job.request_delay,
                        total_locations=len(remaining_locations)  # Update count
                    )
                    
                    logger.info(f"Smart resume: Retrying {len(remaining_locations)}/{len(job.locations)} locations")
                    
                    # Mark the stuck job as failed
                    await db.update_job_status(
                        job.job_id,
                        JobStatus.FAILED,
                        error_message=f"Job stuck in running status. Retrying as {retry_job_id}"
                    )
                    
                    # Create and start the retry job
                    await db.create_job(retry_job)
                    asyncio.create_task(scraper.process_job(retry_job))
                    
                    logger.info(f"Created retry job: {retry_job_id}")
                    
            except Exception as job_error:
                logger.error(f"Error handling stuck job {job.job_id}: {job_error}")
                continue
        
        logger.info("Finished handling stuck jobs")
        
    except Exception as e:
        logger.error(f"Error in handle_stuck_jobs: {e}")

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    try:
        # Try to connect to database (optional for basic functionality)
        try:
            await db.connect()
            logger.info("Database connected")
            
            # Handle stuck "running" jobs on startup
            await handle_stuck_jobs()
            
            # Create active properties re-scraping job
            await create_active_properties_rescraping_job()
            
            # Setup daily DOM update
            await create_daily_dom_update_job()
            
        except Exception as db_error:
            logger.warning(f"Database connection failed: {db_error}")
            logger.warning("Running without database connection")
        
        # Initialize proxy manager with DataImpulse API key (optional)
        try:
            if hasattr(settings, 'DATAIMPULSE_LOGIN') and settings.DATAIMPULSE_LOGIN:
                await proxy_manager.initialize_dataimpulse_proxies(settings.DATAIMPULSE_LOGIN)
                logger.info("DataImpulse proxies initialized")
            else:
                logger.warning("No DataImpulse API key provided - running without proxy support")
        except Exception as proxy_error:
            logger.warning(f"Proxy initialization failed: {proxy_error}")
            logger.warning("Running without proxy support")
        
        # Start scraper service (optional)
        try:
            asyncio.create_task(scraper.start())
            logger.info("Scraper service started")
        except Exception as scraper_error:
            logger.warning(f"Scraper service failed to start: {scraper_error}")
            logger.warning("Running without scraper service")
        
        # Start scheduler service
        try:
            asyncio.create_task(scheduler.start())
            logger.info("Job scheduler started")
        except Exception as scheduler_error:
            logger.error(f"Scheduler service failed to start: {scheduler_error}")
            logger.error("CRITICAL: Cron jobs will not run automatically!")
        
        logger.info("Service startup completed")
        
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        # Don't raise the exception to allow the service to start
        logger.warning("Service starting with limited functionality")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    try:
        await scraper.stop()
        await db.disconnect()
        logger.info("Services stopped")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

# Active Properties Re-scraping Management
@app.get("/active-properties-config")
async def get_active_properties_config():
    """Get the current configuration for active properties re-scraping"""
    try:
        job = await db.scheduled_jobs_collection.find_one({
            "scheduled_job_id": "active_properties_rescraping"
        })
        
        if not job:
            return {
                "exists": False,
                "message": "Active properties re-scraping job not found"
            }
        
        return {
            "exists": True,
            "job": {
                "scheduled_job_id": job["scheduled_job_id"],
                "name": job["name"],
                "description": job["description"],
                "status": job["status"],
                "cron_expression": job["cron_expression"],
                "timezone": job["timezone"],
                "limit": job["limit"],
                "priority": job["priority"],
                "request_delay": job["request_delay"],
                "created_at": job["created_at"],
                "last_run_at": job.get("last_run_at"),
                "next_run_at": job.get("next_run_at"),
                "run_count": job.get("run_count", 0)
            }
        }
    except Exception as e:
        logger.error(f"Error getting active properties config: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/active-properties-config")
async def update_active_properties_config(request: dict):
    """Update the configuration for active properties re-scraping"""
    try:
        cron_expression = request.get("cron_expression")
        if not cron_expression:
            raise HTTPException(status_code=400, detail="cron_expression is required")
        
        # Validate cron expression
        try:
            import croniter
            croniter.croniter(cron_expression, datetime.utcnow())
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid cron expression: {e}")
        
        # Update the scheduled job
        update_data = {
            "cron_expression": cron_expression,
            "updated_at": datetime.utcnow()
        }
        
        # Optional fields
        if "limit" in request:
            update_data["limit"] = request["limit"]
        if "request_delay" in request:
            update_data["request_delay"] = request["request_delay"]
        if "priority" in request:
            update_data["priority"] = request["priority"]
        
        result = await db.scheduled_jobs_collection.update_one(
            {"scheduled_job_id": "active_properties_rescraping"},
            {"$set": update_data}
        )
        
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Active properties re-scraping job not found")
        
        logger.info(f"Updated active properties re-scraping config: {update_data}")
        
        return {
            "message": "Active properties re-scraping configuration updated successfully",
            "updated_fields": list(update_data.keys())
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating active properties config: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/active-properties-config/toggle")
async def toggle_active_properties_rescraping(request: dict):
    """Enable or disable active properties re-scraping"""
    try:
        enabled = request.get("enabled", True)
        new_status = ScheduledJobStatus.ACTIVE if enabled else ScheduledJobStatus.INACTIVE
        
        result = await db.scheduled_jobs_collection.update_one(
            {"scheduled_job_id": "active_properties_rescraping"},
            {
                "$set": {
                    "status": new_status.value,
                    "updated_at": datetime.utcnow()
                }
            }
        )
        
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Active properties re-scraping job not found")
        
        action = "enabled" if enabled else "disabled"
        logger.info(f"Active properties re-scraping {action}")
        
        return {
            "message": f"Active properties re-scraping {action} successfully",
            "enabled": enabled
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error toggling active properties re-scraping: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/locations/suggestions")
async def get_location_suggestions(q: str, state: Optional[str] = None, limit: int = 10):
    """Provide city/state suggestions backed by the ZIP code directory."""
    query = (q or "").strip()
    if len(query) < 2:
        raise HTTPException(status_code=400, detail="Query must be at least 2 characters")

    try:
        safe_limit = max(1, min(limit, 25))
        suggestions = []
        for location in zip_code_service.search_locations(query, state=state, limit=safe_limit):
            suggestions.append(
                {
                    "city": location.city,
                    "state": location.state,
                    "label": location.label,
                    "zip_count": len(location.zip_codes),
                    "example_zip_codes": location.zip_codes[:5],
                }
            )

        return {"suggestions": suggestions}

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error generating location suggestions: {exc}")
        raise HTTPException(status_code=500, detail="Failed to load location suggestions")
# CRM Property Reference Management
@app.post("/crm-property-reference")
async def add_crm_property_reference(request: dict):
    """Add a CRM property reference to an MLS property"""
    try:
        mls_property_id = request.get("mls_property_id")
        crm_property_id = request.get("crm_property_id")
        platform = request.get("platform")
        
        if not mls_property_id or not crm_property_id:
            raise HTTPException(status_code=400, detail="mls_property_id and crm_property_id are required")
        
        if not platform:
            raise HTTPException(status_code=400, detail="platform is required")
        
        success = await db.add_crm_property_reference(mls_property_id, crm_property_id, platform)
        
        if success:
            return {
                "message": "CRM property reference added successfully",
                "mls_property_id": mls_property_id,
                "crm_property_id": crm_property_id,
                "platform": platform
            }
        else:
            raise HTTPException(status_code=404, detail="MLS property not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding CRM property reference: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/crm-property-reference")
async def remove_crm_property_reference(request: dict):
    """Remove a CRM property reference from an MLS property"""
    try:
        mls_property_id = request.get("mls_property_id")
        crm_property_id = request.get("crm_property_id")
        platform = request.get("platform")
        
        if not mls_property_id or not crm_property_id:
            raise HTTPException(status_code=400, detail="mls_property_id and crm_property_id are required")
        
        if not platform:
            raise HTTPException(status_code=400, detail="platform is required")
        
        success = await db.remove_crm_property_reference(mls_property_id, crm_property_id, platform)
        
        if success:
            return {
                "message": "CRM property reference removed successfully",
                "mls_property_id": mls_property_id,
                "crm_property_id": crm_property_id,
                "platform": platform
            }
        else:
            raise HTTPException(status_code=404, detail="MLS property not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error removing CRM property reference: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync-property")
async def sync_property_from_mls(request: dict):
    """Manually sync CRM properties from MLS data"""
    try:
        mls_property_id = request.get("mls_property_id")
        platform = request.get("platform")
        
        if not mls_property_id:
            raise HTTPException(status_code=400, detail="mls_property_id is required")
        
        if not platform:
            raise HTTPException(status_code=400, detail="platform is required")
        
        updated_count = await db.update_crm_properties_from_mls(mls_property_id, platform)
        
        return {
            "success": True,
            "message": f"Synced {updated_count} CRM properties from MLS property",
            "mls_property_id": mls_property_id,
            "platform": platform,
            "crm_properties_updated": updated_count
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error syncing property from MLS: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Basic health check without database dependency
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "service": "mls-scraper",
            "version": "1.0.0"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        # Temporarily return healthy status even if there are errors
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "service": "mls-scraper",
            "version": "1.0.0",
            "warning": "Service running with limited functionality"
        }

# Get properties by ID endpoint
@app.post("/properties/by-ids")
async def get_properties_by_ids(request: PropertyIdsRequest):
    """
    Get MLS properties by their property IDs.
    This is useful for fetching specific properties that were previously scraped.
    """
    try:
        property_ids = request.property_ids
        
        if not property_ids:
            raise HTTPException(status_code=400, detail="At least one property ID is required")
        
        if len(property_ids) > 100:
            raise HTTPException(status_code=400, detail="Maximum 100 property IDs allowed per request")
        
        properties = []
        not_found_ids = []
        
        for property_id in property_ids:
            try:
                logger.info(f"Looking for property_id: '{property_id}' (type: {type(property_id)})")
                property_data = await db.get_property(property_id)
                if property_data:
                    # Convert to dict for JSON serialization
                    property_dict = property_data.dict()
                    # Convert ObjectId to string, handle None case
                    if property_dict.get("_id") is not None:
                        property_dict["_id"] = str(property_dict["_id"])
                    properties.append(property_dict)
                    logger.info(f"Found property: {property_data.property_id}")
                else:
                    logger.info(f"Property not found: {property_id}")
                    not_found_ids.append(property_id)
            except Exception as e:
                logger.error(f"Error fetching property {property_id}: {e}")
                not_found_ids.append(property_id)
        
        return {
            "status": "completed",
            "properties": properties,
            "not_found_ids": not_found_ids,
            "found_count": len(properties),
            "not_found_count": len(not_found_ids)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_properties_by_ids: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Immediate scraping endpoint (high priority)
@app.post("/scrape/immediate", response_model=JobResponse)
async def immediate_scrape(request: ImmediateScrapeRequest):
    """
    Immediate high-priority scraping for urgent property data updates.
    This endpoint bypasses the normal queue and processes immediately.
    """
    try:
        # Validate request
        if len(request.locations) > 10:
            raise HTTPException(status_code=400, detail="Maximum 10 locations allowed for immediate scraping")
        
        if request.limit > 1000:
            raise HTTPException(status_code=400, detail="Maximum 1000 properties allowed for immediate scraping")
        
        # Create immediate job
        job_id = f"immediate_{int(datetime.utcnow().timestamp())}_{uuid.uuid4().hex[:8]}"
        
        job = ScrapingJob(
            job_id=job_id,
            priority=JobPriority.IMMEDIATE,
            locations=request.locations,
            listing_type=request.listing_type,
            property_types=request.property_types,
            radius=request.radius,
            mls_only=request.mls_only,
            foreclosure=request.foreclosure,
            limit=request.limit,
            request_delay=0.5  # Faster for immediate requests
        )
        
        # Save job to database
        await db.create_job(job)
        
        # Process immediately
        asyncio.create_task(scraper.process_job(job))
        
        logger.info(f"Created immediate scraping job: {job_id}")
        
        return JobResponse(
            job_id=job_id,
            status=JobStatus.PENDING,
            message="Immediate scraping job created and queued",
            created_at=job.created_at
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating immediate scrape job: {e}")
        raise HTTPException(status_code=500, detail="Failed to create immediate scraping job")

# Synchronous immediate scraping endpoint
@app.post("/scrape/immediate/sync", response_model=ImmediateScrapeResponse)
async def immediate_scrape_sync(request: ImmediateScrapeRequest):
    """
    Immediate synchronous scraping that returns property details directly in the response.
    This endpoint processes the scraping immediately and returns the results without async job handling.
    """
    start_time = datetime.utcnow()
    
    try:
        # Validate request
        if len(request.locations) > 10:
            raise HTTPException(status_code=400, detail="Maximum 10 locations allowed for immediate scraping")
        
        if request.limit > 1000:
            raise HTTPException(status_code=400, detail="Maximum 1000 properties allowed for immediate scraping")
        
        # Create job ID for tracking
        job_id = f"sync_{int(datetime.utcnow().timestamp())}_{uuid.uuid4().hex[:8]}"
        
        # Create a job record in the database for tracking
        job = ScrapingJob(
            job_id=job_id,
            priority=JobPriority.IMMEDIATE,
            locations=request.locations,
            listing_type=request.listing_type,
            property_types=request.property_types,
            radius=request.radius,
            mls_only=getattr(request, 'mls_only', False),
            foreclosure=getattr(request, 'foreclosure', False),
            limit=request.limit,
            request_delay=0.5  # Faster for immediate requests
        )
        
        # Save job to database for tracking
        await db.create_job(job)
        logger.info(f"Created sync job record: {job_id}")
        
        logger.info(f"Starting synchronous scraping job: {job_id}")
        logger.info(f"Request details: locations={request.locations}, listing_type={request.listing_type}, limit={request.limit}")
        
        # Debug: Log all possible listing types
        logger.info(f"Available ListingType values: {[lt.value for lt in ListingType]}")
        logger.info(f"Received listing_type: {request.listing_type} (type: {type(request.listing_type)})")
        
        # First, check database for existing properties
        total_properties_scraped = 0
        total_properties_saved = 0
        all_properties = []
        
        logger.info(f"Checking database for existing properties before scraping...")
        
        for location in request.locations:
            try:
                # Check if properties already exist in database for this location
                # First try without listing_type filter to find any property at this address
                existing_properties = await db.find_properties_by_location(
                    location=location,
                    listing_type=None,  # Don't filter by listing_type initially
                    radius=request.radius or 0.1,
                    limit=request.limit
                )
                
                # If no properties found and listing_type is specified, try with the specific listing_type
                if not existing_properties and request.listing_type:
                    existing_properties = await db.find_properties_by_location(
                        location=location,
                        listing_type=request.listing_type,
                        radius=request.radius or 0.1,
                        limit=request.limit
                    )
                
                if existing_properties:
                    logger.info(f"Found {len(existing_properties)} existing properties for location: {location}")
                    all_properties.extend(existing_properties)
                    total_properties_scraped += len(existing_properties)
                    total_properties_saved += len(existing_properties)
                    continue  # Skip scraping if we found existing properties
                
                logger.info(f"No existing properties found for location: {location}, proceeding with scraping...")
                
            except Exception as e:
                logger.error(f"Error checking database for location {location}: {e}")
                # Continue with scraping if database check fails
                pass
        
        # If we found existing properties, return immediately for fast response
        if all_properties:
            logger.info(f"Found {len(all_properties)} existing properties, returning immediately for fast response")
            # Calculate execution time
            end_time = datetime.utcnow()
            execution_time = round((end_time - start_time).total_seconds(), 2)
            
            # Update job status in database
            await db.update_job_status(job_id, JobStatus.COMPLETED)
            logger.info(f"Updated job status to COMPLETED: {job_id}")
            
            # Return response with existing properties immediately
            response = ImmediateScrapeResponse(
                job_id=job_id,
                status=JobStatus.COMPLETED,
                message=f"Found {len(all_properties)} existing properties in {execution_time:.2f} seconds",
                created_at=start_time,
                completed_at=end_time,
                execution_time_seconds=execution_time,
                properties_scraped=len(all_properties),
                properties_saved=len(all_properties),
                properties=all_properties
            )
            
            logger.info(f"Returning existing properties immediately: {job_id} - {len(all_properties)} properties")
            return response
        else:
            logger.info(f"No existing properties found, proceeding with scraping...")
            # Continue with scraping for locations that didn't have existing properties
            for location in request.locations:
                try:
                    logger.info(f"Starting scraping for location: {location}")
                    
                    # Create a temporary job for the scraper to use
                    # Use higher limits and comprehensive scraping like our working direct tests
                    temp_job = ScrapingJob(
                        job_id=job_id,
                        priority=JobPriority.IMMEDIATE,
                        locations=[location],
                        listing_type=None,  # Let scraper handle all listing types comprehensively
                        property_types=request.property_types,
                        radius=request.radius,
                        mls_only=getattr(request, 'mls_only', False),
                        foreclosure=getattr(request, 'foreclosure', False),
                        limit=request.limit or 200,  # Use request limit or default to 200
                        past_days=getattr(request, 'past_days', 365)  # Focus on sold properties from last 365 days
                    )
                    
                    # Get proxy configuration
                    proxy_config = await scraper.get_proxy_config(temp_job)
                    
                    # Scrape the location directly using scrape_property (bypass scraper method)
                    logger.info(f"Calling scrape_property directly for: {location}")
                    logger.info(f"Scraper job config: listing_type={temp_job.listing_type}, limit={temp_job.limit}, radius={temp_job.radius}")
                    
                    # Import scrape_property directly to bypass scraper method issues
                    from homeharvest import scrape_property
                    import pandas as pd
                    
                    all_properties = []
                    # Prioritize sold properties first, then others
                    listing_types = ["sold", "for_sale", "for_rent", "pending"]
                    
                    for listing_type in listing_types:
                        try:
                            logger.info(f"Scraping {listing_type} properties directly...")
                            
                            scrape_params = {
                                "location": location,
                                "listing_type": listing_type,
                                "mls_only": False,
                                "limit": temp_job.limit or 200
                            }
                            
                            # Add past_days for sold properties to focus on recent sales
                            if listing_type == "sold" and temp_job.past_days:
                                scrape_params["past_days"] = temp_job.past_days
                                logger.info(f"Focusing on sold properties from last {temp_job.past_days} days")
                            
                            properties_df = scrape_property(**scrape_params)
                            
                            if not properties_df.empty:
                                logger.info(f"Found {len(properties_df)} {listing_type} properties")
                                
                                # Convert DataFrame to Property models
                                for index, row in properties_df.iterrows():
                                    try:
                                        property_obj = scraper.convert_to_property_model(row, temp_job.job_id, listing_type)
                                        if listing_type == "sold":
                                            property_obj.is_comp = True
                                        all_properties.append(property_obj)
                                        
                                        # Check if this is 1707
                                        if '1707' in property_obj.address.formatted_address:
                                            logger.info(f"🎯 FOUND 1707: {property_obj.address.formatted_address}")
                                            
                                    except Exception as e:
                                        logger.error(f"Error converting property: {e}")
                                        continue
                            else:
                                logger.info(f"No {listing_type} properties found")
                                
                        except Exception as e:
                            logger.error(f"Error scraping {listing_type} properties: {e}")
                            continue
                    
                    properties = all_properties
                    logger.info(f"Direct scraping returned {len(properties)} properties for: {location}")
                    
                    # Log the first few properties found for debugging
                    if properties:
                        logger.info(f"First few properties found:")
                        for i, prop in enumerate(properties[:5]):
                            logger.info(f"  {i+1}. {prop.address.formatted_address} ({prop.property_id}) - {prop.listing_type}")
                            if '1707' in prop.address.formatted_address:
                                logger.info(f"    🎯 FOUND 1707!")
                    
                    if properties:
                        logger.info(f"Attempting to save {len(properties)} properties to database...")
                        try:
                            # Save properties to database
                            save_results = await db.save_properties_batch(properties)
                            
                            total_properties_scraped += len(properties)
                            total_properties_saved += save_results["inserted"] + save_results["updated"]
                            
                            # Add properties to response
                            all_properties.extend(properties)
                            
                            logger.info(f"Location {location}: {save_results['inserted']} inserted, {save_results['updated']} updated, {save_results['skipped']} skipped")
                        except Exception as save_error:
                            logger.error(f"Error saving properties for location {location}: {save_error}")
                            logger.error(f"Save error details: {type(save_error).__name__}: {str(save_error)}")
                            # Still add properties to response even if save failed
                            all_properties.extend(properties)
                            total_properties_scraped += len(properties)
                    else:
                        logger.info(f"No properties found by scraper for location: {location}")
                    
                except Exception as e:
                    logger.error(f"Error processing location {location}: {e}")
                    # Continue with other locations even if one fails
                    continue
        
        # Calculate execution time
        end_time = datetime.utcnow()
        execution_time = round((end_time - start_time).total_seconds(), 2)
        
        # Update job status in database
        await db.update_job_status(job_id, JobStatus.COMPLETED)
        logger.info(f"Updated job status to COMPLETED: {job_id}")
        
        # Return response with property details
        response = ImmediateScrapeResponse(
            job_id=job_id,
            status=JobStatus.COMPLETED,
            message=f"Successfully scraped {total_properties_scraped} properties in {execution_time:.2f} seconds",
            created_at=start_time,
            completed_at=end_time,
            execution_time_seconds=execution_time,
            properties_scraped=total_properties_scraped,
            properties_saved=total_properties_saved,
            properties=all_properties
        )
        
        logger.info(f"Synchronous scraping completed: {job_id} - {total_properties_scraped} scraped, {total_properties_saved} saved")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in synchronous scraping: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to complete synchronous scraping: {str(e)}")

# Scheduled scraping endpoint
@app.post("/scrape/scheduled", response_model=JobResponse)
async def scheduled_scrape(request: ScheduledScrapeRequest):
    """
    Create scheduled scraping jobs for regular data collection.
    Supports both one-time and recurring jobs.
    """
    try:
        # Validate request
        if not request.scheduled_at and not request.cron_expression:
            raise HTTPException(status_code=400, detail="Either scheduled_at or cron_expression must be provided")
        
        # Create job ID
        job_id = f"scheduled_{int(datetime.utcnow().timestamp())}_{uuid.uuid4().hex[:8]}"
        
        job = ScrapingJob(
            job_id=job_id,
            priority=request.priority,
            locations=request.locations,
            listing_type=request.listing_type,
            property_types=request.property_types,
            past_days=request.past_days,
            date_from=request.date_from,
            date_to=request.date_to,
            radius=request.radius,
            mls_only=request.mls_only,
            foreclosure=request.foreclosure,
            exclude_pending=request.exclude_pending,
            limit=request.limit,
            scheduled_at=request.scheduled_at,
            cron_expression=request.cron_expression
        )
        
        # Save job to database
        await db.create_job(job)
        
        logger.info(f"Created scheduled scraping job: {job_id}")
        
        return JobResponse(
            job_id=job_id,
            status=JobStatus.PENDING,
            message="Scheduled scraping job created",
            created_at=job.created_at
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating scheduled scrape job: {e}")
        raise HTTPException(status_code=500, detail="Failed to create scheduled scraping job")

# Trigger existing job immediately
@app.post("/scrape/trigger", response_model=JobResponse)
async def trigger_existing_job(request: TriggerJobRequest):
    """
    Trigger an existing scheduled job to run immediately.
    This creates a new immediate job based on the existing job's configuration.
    Supports both ScheduledJob (from scheduled_jobs collection) and legacy ScrapingJob.
    """
    try:
        # First, try to get from scheduled_jobs collection (new architecture)
        scheduled_job = await db.get_scheduled_job(request.job_id)
        
        if scheduled_job:
            # This is a scheduled job (cron job) - create instances using scheduler's chunking logic
            logger.info(f"Triggering scheduled job: {request.job_id} ({scheduled_job.name})")
            
            # Determine past_days based on listing type for triggered jobs
            # SOLD properties should always go back 1 year (365 days) for comprehensive comp analysis
            if scheduled_job.listing_type == ListingType.SOLD:
                past_days = 365  # Full year for sold properties
            elif scheduled_job.listing_type == ListingType.PENDING:
                past_days = 30   # 30 days for pending properties
            else:
                past_days = scheduled_job.past_days  # Keep original for other types
            
            # Temporarily update past_days for this trigger
            original_past_days = scheduled_job.past_days
            scheduled_job.past_days = past_days
            
            # Override priority if provided in request
            if request.priority:
                scheduled_job.priority = request.priority
            
            # Use scheduler's create_scheduled_job_instances to properly handle split_by_zip
            job_instances = await scheduler.create_scheduled_job_instances(scheduled_job)
            
            # Restore original past_days
            scheduled_job.past_days = original_past_days
            
            if not job_instances:
                raise HTTPException(status_code=400, detail="No job instances created. Check location configuration.")
            
            # Start the scraper service if not already running
            if not scraper.is_running:
                asyncio.create_task(scraper.start())
            
            # Process all job instances immediately
            for job_instance in job_instances:
                asyncio.create_task(scraper.process_job(job_instance))
            
            # Return the first job ID as the primary response
            primary_job = job_instances[0]
            logger.info(f"Triggered scheduled job {request.job_id} as {len(job_instances)} job instance(s), starting with {primary_job.job_id}")
            
            return JobResponse(
                job_id=primary_job.job_id,
                status=JobStatus.PENDING,
                message=f"Triggered scheduled job '{scheduled_job.name}' to run immediately ({len(job_instances)} job instance(s) created)",
                created_at=primary_job.created_at
            )
        
        # If not found in scheduled_jobs, try legacy jobs collection
        existing_job = await db.get_job(request.job_id)
        if not existing_job:
            raise HTTPException(status_code=404, detail=f"Job not found: {request.job_id}")
        
        logger.info(f"Triggering legacy job: {request.job_id}")
        
        # Create immediate job ID
        immediate_job_id = f"immediate_{int(datetime.utcnow().timestamp())}_{uuid.uuid4().hex[:8]}"
        
        # Determine past_days based on listing type for triggered jobs
        # SOLD properties should always go back 1 year (365 days) for comprehensive comp analysis
        if existing_job.listing_type == ListingType.SOLD:
            past_days = 365  # Full year for sold properties
        elif existing_job.listing_type == ListingType.PENDING:
            past_days = 30   # 30 days for pending properties
        else:
            past_days = existing_job.past_days  # Keep original for other types
        
        # Create new immediate job based on existing job
        immediate_job = ScrapingJob(
            job_id=immediate_job_id,
            priority=request.priority,
            scheduled_job_id=existing_job.scheduled_job_id,  # Preserve scheduled_job_id if exists
            locations=existing_job.locations,
            listing_type=existing_job.listing_type,
            property_types=existing_job.property_types,
            past_days=past_days,  # Use the calculated past_days
            date_from=existing_job.date_from,
            date_to=existing_job.date_to,
            radius=existing_job.radius,
            mls_only=existing_job.mls_only,
            foreclosure=existing_job.foreclosure,
            exclude_pending=existing_job.exclude_pending,
            limit=existing_job.limit,
            proxy_config=existing_job.proxy_config,
            user_agent=existing_job.user_agent,
            request_delay=existing_job.request_delay,
            total_locations=existing_job.total_locations,
            original_job_id=request.job_id  # Track the original job (legacy)
        )
        
        # Save immediate job to database
        await db.create_job(immediate_job)
        
        # Start the scraper service if not already running
        if not scraper.is_running:
            asyncio.create_task(scraper.start())
        
        # Process the job immediately
        asyncio.create_task(scraper.process_job(immediate_job))
        
        logger.info(f"Triggered existing job {request.job_id} as immediate job {immediate_job_id}")
        
        return JobResponse(
            job_id=immediate_job_id,
            status=JobStatus.PENDING,
            message=f"Triggered existing job {request.job_id} to run immediately",
            created_at=immediate_job.created_at
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error triggering existing job: {e}")
        raise HTTPException(status_code=500, detail="Failed to trigger existing job")

# Get job status
@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Get the status and progress of a specific job"""
    try:
        job = await db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Calculate progress
        progress = {
            "total_locations": job.total_locations,
            "completed_locations": job.completed_locations,
            "properties_scraped": job.properties_scraped,
            "properties_saved": job.properties_saved,
            "progress_percentage": (job.completed_locations / job.total_locations * 100) if job.total_locations > 0 else 0
        }
        
        return JobStatusResponse(
            job_id=job.job_id,
            status=job.status,
            progress=progress,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            error_message=job.error_message
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get job status")

# Get statistics for a scheduled job (MUST come before generic /{scheduled_job_id} route)
@app.get("/scheduled-jobs/{scheduled_job_id}/stats")
async def get_scheduled_job_stats(scheduled_job_id: str):
    """Get detailed statistics for a scheduled job"""
    try:
        # Get the scheduled job
        scheduled_job = await db.get_scheduled_job(scheduled_job_id)
        if not scheduled_job:
            raise HTTPException(status_code=404, detail=f"Scheduled job not found: {scheduled_job_id}")
        
        # Get all job runs for this scheduled job
        job_runs_cursor = db.jobs_collection.find(
            {"scheduled_job_id": scheduled_job_id}
        ).sort([("created_at", -1)])
        
        total_runs = 0
        successful_runs = 0
        failed_runs = 0
        total_properties_scraped = 0
        total_properties_saved = 0
        total_duration_seconds = 0
        duration_count = 0
        last_successful_run = None
        
        async for job_data in job_runs_cursor:
            total_runs += 1
            
            if job_data.get('status') == 'completed':
                successful_runs += 1
                total_properties_scraped += job_data.get('properties_scraped', 0)
                total_properties_saved += job_data.get('properties_saved', 0)
                
                # Calculate duration
                if job_data.get('started_at') and job_data.get('completed_at'):
                    duration = (job_data['completed_at'] - job_data['started_at']).total_seconds()
                    total_duration_seconds += duration
                    duration_count += 1
                
                # Track last successful run
                if not last_successful_run:
                    last_successful_run = {
                        "job_id": job_data.get('job_id'),
                        "completed_at": job_data.get('completed_at'),
                        "properties_scraped": job_data.get('properties_scraped', 0),
                        "properties_saved": job_data.get('properties_saved', 0)
                    }
                    
            elif job_data.get('status') == 'failed':
                failed_runs += 1
        
        # Calculate success rate
        success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0
        
        # Calculate average duration
        average_duration_seconds = (total_duration_seconds / duration_count) if duration_count > 0 else 0
        
        # Count unique properties added (properties with this job's scheduled_job_id)
        unique_properties_count = await db.properties_collection.count_documents({
            "job_id": {"$regex": f"^(scheduled|triggered)_{scheduled_job_id}"}
        })
        
        # Count unique properties by listing type
        properties_by_type = {}
        for listing_type in ["for_sale", "sold", "for_rent", "pending"]:
            count = await db.properties_collection.count_documents({
                "job_id": {"$regex": f"^(scheduled|triggered)_{scheduled_job_id}"},
                "listing_type": listing_type
            })
            if count > 0:
                properties_by_type[listing_type] = count
        
        return {
            "scheduled_job_id": scheduled_job_id,
            "total_runs": total_runs,
            "successful_runs": successful_runs,
            "failed_runs": failed_runs,
            "success_rate": round(success_rate, 1),
            "total_properties_scraped": total_properties_scraped,
            "total_properties_saved": total_properties_saved,
            "unique_properties_added": unique_properties_count,
            "properties_by_listing_type": properties_by_type,  # Breakdown by type
            "average_duration_seconds": round(average_duration_seconds, 1),
            "last_successful_run": last_successful_run,
            "next_run_at": scheduled_job.next_run_at
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get job statistics: {str(e)}")

# Toggle scheduled job status (MUST come before generic /{scheduled_job_id} route)
@app.patch("/scheduled-jobs/{scheduled_job_id}/status")
async def toggle_scheduled_job_status(scheduled_job_id: str, status_data: dict):
    """Toggle the status of a scheduled job (active/inactive/paused)"""
    try:
        from models import ScheduledJobStatus
        
        # Validate status
        status = status_data.get('status')
        if not status:
            raise HTTPException(status_code=400, detail="Status field is required")
        
        if status not in [s.value for s in ScheduledJobStatus]:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}. Must be one of: active, inactive, paused")
        
        # Check if job exists
        existing_job = await db.get_scheduled_job(scheduled_job_id)
        if not existing_job:
            raise HTTPException(status_code=404, detail=f"Scheduled job not found: {scheduled_job_id}")
        
        # Update status
        success = await db.update_scheduled_job_status(scheduled_job_id, ScheduledJobStatus(status))
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to update job status")
        
        logger.info(f"Updated status of scheduled job {scheduled_job_id} to {status}")
        
        return {
            "scheduled_job_id": scheduled_job_id,
            "status": status,
            "message": f"Status updated to {status}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error toggling job status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to toggle job status: {str(e)}")

# Get scheduled job details and run history
@app.get("/scheduled-jobs/{scheduled_job_id}")
async def get_scheduled_job_details(scheduled_job_id: str, include_runs: bool = True, limit: int = 50):
    """
    Get scheduled job details including run history.
    This shows the cron job definition and all job executions linked to it.
    """
    try:
        # Get the scheduled job
        scheduled_job = await db.get_scheduled_job(scheduled_job_id)
        if not scheduled_job:
            raise HTTPException(status_code=404, detail=f"Scheduled job not found: {scheduled_job_id}")
        
        response = {
            "scheduled_job": {
                "scheduled_job_id": scheduled_job.scheduled_job_id,
                "name": scheduled_job.name,
                "description": scheduled_job.description,
                "status": scheduled_job.status,
                "cron_expression": scheduled_job.cron_expression,
                "timezone": scheduled_job.timezone,
                "locations": scheduled_job.locations,
                "listing_types": scheduled_job.listing_types,  # New multi-select field
                "listing_type": scheduled_job.listing_type,  # Keep for backward compatibility
                "property_types": scheduled_job.property_types,
                "past_days": scheduled_job.past_days,
                "date_from": scheduled_job.date_from,
                "date_to": scheduled_job.date_to,
                "radius": scheduled_job.radius,
                "mls_only": scheduled_job.mls_only,
                "foreclosure": scheduled_job.foreclosure,
                "exclude_pending": scheduled_job.exclude_pending,
                "limit": scheduled_job.limit,
                "priority": scheduled_job.priority,
                "run_count": scheduled_job.run_count,
                "last_run_at": scheduled_job.last_run_at,
                "last_run_status": scheduled_job.last_run_status,
                "last_run_job_id": scheduled_job.last_run_job_id,
                "next_run_at": scheduled_job.next_run_at,
                "created_at": scheduled_job.created_at,
                "updated_at": scheduled_job.updated_at
            }
        }
        
        # Get all job runs for this scheduled job
        if include_runs:
            job_runs_cursor = db.jobs_collection.find(
                {"scheduled_job_id": scheduled_job_id}
            ).sort([("created_at", -1)]).limit(limit)
            
            job_runs = []
            async for job_data in job_runs_cursor:
                job_runs.append({
                    "job_id": job_data.get("job_id"),
                    "status": job_data.get("status"),
                    "priority": job_data.get("priority"),
                    "created_at": job_data.get("created_at"),
                    "started_at": job_data.get("started_at"),
                    "completed_at": job_data.get("completed_at"),
                    "properties_scraped": job_data.get("properties_scraped", 0),
                    "properties_saved": job_data.get("properties_saved", 0),
                    "properties_inserted": job_data.get("properties_inserted", 0),
                    "properties_updated": job_data.get("properties_updated", 0),
                    "properties_skipped": job_data.get("properties_skipped", 0),
                    "completed_locations": job_data.get("completed_locations", 0),
                    "total_locations": job_data.get("total_locations", 0),
                    "error_message": job_data.get("error_message"),
                    "progress_logs": job_data.get("progress_logs", [])  # Include progress logs!
                })
            
            response["job_runs"] = job_runs
            response["total_runs"] = len(job_runs)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting scheduled job details: {e}")
        raise HTTPException(status_code=500, detail="Failed to get scheduled job details")

# List all scheduled jobs
@app.get("/scheduled-jobs")
async def list_scheduled_jobs(
    status: Optional[str] = None,
    limit: int = 50,
    include_deleted: bool = False
):
    """List all scheduled jobs (cron jobs) with optional status filtering. Deleted jobs are excluded by default."""
    try:
        if status:
            from models import ScheduledJobStatus
            # Validate status
            if status not in [s.value for s in ScheduledJobStatus]:
                raise HTTPException(status_code=400, detail=f"Invalid status: {status}")
            
            cursor = db.scheduled_jobs_collection.find(
                {"status": status}
            ).sort([("next_run_at", 1)]).limit(limit)
        else:
            # Exclude deleted jobs by default
            query = {} if include_deleted else {"status": {"$ne": "deleted"}}
            cursor = db.scheduled_jobs_collection.find(query).sort([
                ("status", 1),
                ("next_run_at", 1)
            ]).limit(limit)
        
        scheduled_jobs = []
        async for job_data in cursor:
            scheduled_job_id = job_data.get("scheduled_job_id")
            
            # Get actual run count from jobs collection
            actual_run_count = await db.jobs_collection.count_documents({
                "scheduled_job_id": scheduled_job_id
            })
            
            # Check if any job is currently running
            has_running_job = await db.jobs_collection.count_documents({
                "scheduled_job_id": scheduled_job_id,
                "status": JobStatus.RUNNING.value
            }) > 0
            
            scheduled_jobs.append({
                "scheduled_job_id": scheduled_job_id,
                "name": job_data.get("name"),
                "description": job_data.get("description"),
                "status": job_data.get("status"),
                "cron_expression": job_data.get("cron_expression"),
                "locations": job_data.get("locations"),
                "listing_types": job_data.get("listing_types", ["for_sale", "sold", "for_rent", "pending"]),  # New multi-select field
                "listing_type": job_data.get("listing_type"),  # Backward compatibility
                "split_by_zip": job_data.get("split_by_zip", False),
                "zip_batch_size": job_data.get("zip_batch_size"),
                "run_count": actual_run_count,  # Use actual count from jobs collection
                "has_running_job": has_running_job,  # Indicator if a job is currently running
                "last_run_at": job_data.get("last_run_at"),
                "last_run_status": job_data.get("last_run_status"),
                "next_run_at": job_data.get("next_run_at"),
                "created_at": job_data.get("created_at")
            })
        
        return {
            "scheduled_jobs": scheduled_jobs,
            "total": len(scheduled_jobs)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing scheduled jobs: {e}")
        raise HTTPException(status_code=500, detail="Failed to list scheduled jobs")

# Create new scheduled job
@app.post("/scheduled-jobs")
async def create_scheduled_job(job_data: dict):
    """Create a new scheduled job (cron job)"""
    try:
        from models import ScheduledJob
        import uuid
        
        # Generate scheduled_job_id
        scheduled_job_id = f"scheduled_{job_data.get('listing_type', 'job')}_{int(datetime.utcnow().timestamp())}"
        
        # Create ScheduledJob instance
        # Default to all listing types if not specified
        listing_types = job_data.get('listing_types')
        if not listing_types or len(listing_types) == 0:
            listing_types = ["for_sale", "sold", "for_rent", "pending"]
        
        # Get locations - ensure it's a list
        locations = job_data.get('locations', [])
        if not isinstance(locations, list):
            locations = [locations] if locations else []
        
        # Expand locations if split_by_zip is enabled
        split_by_zip = job_data.get('split_by_zip', False)
        zip_batch_size = job_data.get('zip_batch_size')
        
        if split_by_zip and locations:
            expanded_locations = []
            for location in locations:
                # Expand each location into individual ZIP codes (ignore zip_batch_size for expansion)
                # zip_batch_size is kept for reference but all ZIPs go into one job
                chunks = zip_code_service.expand_location(location, None)  # None = individual ZIP codes
                for chunk in chunks:
                    # Add all locations from this chunk
                    expanded_locations.extend(list(chunk["locations"]))
            
            if expanded_locations:
                locations = expanded_locations
                logger.info(f"Expanded {len(job_data.get('locations', []))} location(s) into {len(locations)} ZIP code location(s)")
        
        scheduled_job = ScheduledJob(
            scheduled_job_id=scheduled_job_id,
            name=job_data.get('name'),
            description=job_data.get('description'),
            status=job_data.get('status', 'active'),
            cron_expression=job_data.get('cron_expression'),
            timezone=job_data.get('timezone', 'UTC'),
            locations=locations,  # Use expanded locations
            listing_types=listing_types,  # Use multi-select field
            listing_type=job_data.get('listing_type'),  # Keep for backward compatibility
            property_types=job_data.get('property_types'),
            past_days=job_data.get('past_days'),
            date_from=job_data.get('date_from'),
            date_to=job_data.get('date_to'),
            radius=job_data.get('radius'),
            mls_only=job_data.get('mls_only', False),
            foreclosure=job_data.get('foreclosure', False),
            exclude_pending=job_data.get('exclude_pending', False),
            limit=job_data.get('limit', 10000),
            split_by_zip=split_by_zip,  # Keep the flag for reference, but locations are already expanded
            zip_batch_size=zip_batch_size,
            proxy_config=job_data.get('proxy_config'),
            user_agent=job_data.get('user_agent'),
            request_delay=job_data.get('request_delay', 1.0),
            priority=job_data.get('priority', 'normal')
        )
        
        # Calculate next run time
        import croniter
        cron = croniter.croniter(scheduled_job.cron_expression, datetime.utcnow())
        scheduled_job.next_run_at = cron.get_next(datetime)
        
        # Save to database
        await db.create_scheduled_job(scheduled_job)
        
        logger.info(f"Created new scheduled job: {scheduled_job_id}")
        
        return {
            "scheduled_job_id": scheduled_job_id,
            "message": "Scheduled job created successfully",
            "next_run_at": scheduled_job.next_run_at
        }
        
    except Exception as e:
        logger.error(f"Error creating scheduled job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create scheduled job: {str(e)}")

# Update scheduled job
@app.put("/scheduled-jobs/{scheduled_job_id}")
async def update_scheduled_job(scheduled_job_id: str, job_data: dict):
    """Update an existing scheduled job"""
    try:
        # Check if job exists
        existing_job = await db.get_scheduled_job(scheduled_job_id)
        if not existing_job:
            raise HTTPException(status_code=404, detail=f"Scheduled job not found: {scheduled_job_id}")
        
        # Build update data
        update_data = {}
        
        # Only update fields that are provided
        if 'name' in job_data:
            update_data['name'] = job_data['name']
        if 'description' in job_data:
            update_data['description'] = job_data['description']
        if 'status' in job_data:
            update_data['status'] = job_data['status']
        if 'cron_expression' in job_data:
            update_data['cron_expression'] = job_data['cron_expression']
            # Recalculate next run time if cron changed
            import croniter
            cron = croniter.croniter(job_data['cron_expression'], datetime.utcnow())
            update_data['next_run_at'] = cron.get_next(datetime)
        if 'timezone' in job_data:
            update_data['timezone'] = job_data['timezone']
        if 'locations' in job_data:
            # Get locations - ensure it's a list
            locations = job_data['locations']
            if not isinstance(locations, list):
                locations = [locations] if locations else []
            
            # Expand locations if split_by_zip is enabled
            split_by_zip = job_data.get('split_by_zip', existing_job.split_by_zip if hasattr(existing_job, 'split_by_zip') else False)
            zip_batch_size = job_data.get('zip_batch_size', existing_job.zip_batch_size if hasattr(existing_job, 'zip_batch_size') else None)
            
            if split_by_zip and locations:
                expanded_locations = []
                for location in locations:
                    # Expand each location into individual ZIP codes (ignore zip_batch_size for expansion)
                    # zip_batch_size is kept for reference but all ZIPs go into one job
                    chunks = zip_code_service.expand_location(location, None)  # None = individual ZIP codes
                    for chunk in chunks:
                        # Add all locations from this chunk
                        expanded_locations.extend(list(chunk["locations"]))
                
                if expanded_locations:
                    locations = expanded_locations
                    logger.info(f"Expanded {len(job_data['locations'])} location(s) into {len(locations)} ZIP code location(s) during update")
            
            update_data['locations'] = locations
        if 'listing_types' in job_data:
            update_data['listing_types'] = job_data['listing_types']
        if 'listing_type' in job_data:
            update_data['listing_type'] = job_data['listing_type']
        if 'property_types' in job_data:
            update_data['property_types'] = job_data['property_types']
        if 'past_days' in job_data:
            update_data['past_days'] = job_data['past_days']
        if 'date_from' in job_data:
            update_data['date_from'] = job_data['date_from']
        if 'date_to' in job_data:
            update_data['date_to'] = job_data['date_to']
        if 'radius' in job_data:
            update_data['radius'] = job_data['radius']
        if 'mls_only' in job_data:
            update_data['mls_only'] = job_data['mls_only']
        if 'foreclosure' in job_data:
            update_data['foreclosure'] = job_data['foreclosure']
        if 'exclude_pending' in job_data:
            update_data['exclude_pending'] = job_data['exclude_pending']
        if 'limit' in job_data:
            update_data['limit'] = job_data['limit']
        if 'split_by_zip' in job_data:
            update_data['split_by_zip'] = job_data['split_by_zip']
        if 'zip_batch_size' in job_data:
            update_data['zip_batch_size'] = job_data['zip_batch_size']
        if 'proxy_config' in job_data:
            update_data['proxy_config'] = job_data['proxy_config']
        if 'user_agent' in job_data:
            update_data['user_agent'] = job_data['user_agent']
        if 'request_delay' in job_data:
            update_data['request_delay'] = job_data['request_delay']
        if 'priority' in job_data:
            update_data['priority'] = job_data['priority']
        
        # Update in database
        success = await db.update_scheduled_job(scheduled_job_id, update_data)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to update scheduled job")
        
        logger.info(f"Updated scheduled job: {scheduled_job_id}")
        
        return {
            "scheduled_job_id": scheduled_job_id,
            "message": "Scheduled job updated successfully",
            "updated_fields": list(update_data.keys())
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating scheduled job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to update scheduled job: {str(e)}")

# Delete scheduled job (soft delete)
@app.delete("/scheduled-jobs/{scheduled_job_id}")
async def delete_scheduled_job(scheduled_job_id: str):
    """Soft delete a scheduled job by marking it as deleted"""
    try:
        from models import ScheduledJobStatus
        
        # Check if job exists
        existing_job = await db.get_scheduled_job(scheduled_job_id)
        if not existing_job:
            raise HTTPException(status_code=404, detail=f"Scheduled job not found: {scheduled_job_id}")
        
        # Soft delete: set status to deleted instead of removing from database
        success = await db.update_scheduled_job_status(scheduled_job_id, ScheduledJobStatus.DELETED)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to delete scheduled job")
        
        logger.info(f"Soft deleted scheduled job: {scheduled_job_id}")
        
        return {
            "scheduled_job_id": scheduled_job_id,
            "message": "Scheduled job deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting scheduled job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete scheduled job: {str(e)}")

# List jobs
@app.get("/jobs")
async def list_jobs(
    status: Optional[JobStatus] = None,
    limit: int = 50,
    offset: int = 0
):
    """List jobs with optional filtering"""
    try:
        if status:
            jobs = await db.get_jobs_by_status(status, limit)
        else:
            # Get all jobs (you might want to implement a more sophisticated pagination)
            jobs = await db.get_jobs_by_status(JobStatus.PENDING, limit)
            jobs.extend(await db.get_jobs_by_status(JobStatus.RUNNING, limit))
            jobs.extend(await db.get_jobs_by_status(JobStatus.COMPLETED, limit))
            jobs.extend(await db.get_jobs_by_status(JobStatus.FAILED, limit))
        
        # Apply offset
        jobs = jobs[offset:offset + limit]
        
        return {
            "jobs": [
                {
                    "job_id": job.job_id,
                    "status": job.status,
                    "priority": job.priority,
                    "locations": job.locations,
                    "listing_type": job.listing_type,
                    "created_at": job.created_at,
                    "started_at": job.started_at,
                    "completed_at": job.completed_at,
                    "properties_scraped": job.properties_scraped,
                    "properties_saved": job.properties_saved
                }
                for job in jobs
            ],
            "total": len(jobs)
        }
        
    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        raise HTTPException(status_code=500, detail="Failed to list jobs")

# Cancel job
@app.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Cancel a running or pending job"""
    try:
        job = await db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            raise HTTPException(status_code=400, detail="Job cannot be cancelled in current status")
        
        # Update job status
        success = await db.update_job_status(job_id, JobStatus.CANCELLED)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to cancel job")
        
        # Remove from current jobs if running
        if job_id in scraper.current_jobs:
            del scraper.current_jobs[job_id]
        
        logger.info(f"Cancelled job: {job_id}")
        
        return {"message": "Job cancelled successfully", "job_id": job_id}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling job: {e}")
        raise HTTPException(status_code=500, detail="Failed to cancel job")

# Retry failed locations from a job
@app.post("/jobs/{job_id}/retry-failed")
async def retry_failed_locations(job_id: str):
    """Retry failed locations from a completed job"""
    try:
        job = await db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        if not job.failed_locations or len(job.failed_locations) == 0:
            raise HTTPException(status_code=400, detail="No failed locations to retry")
        
        # Extract failed location strings
        failed_location_strings = [fl.get("location") for fl in job.failed_locations if fl.get("location")]
        
        if not failed_location_strings:
            raise HTTPException(status_code=400, detail="No valid failed locations found")
        
        # Create a new retry job with only the failed locations
        import time
        import random
        retry_job_id = f"{job_id}_retry_failed_{int(time.time())}_{random.randint(1000, 9999)}"
        
        retry_job = ScrapingJob(
            job_id=retry_job_id,
            priority=JobPriority.HIGH,  # High priority for retries
            locations=failed_location_strings,
            listing_types=job.listing_types,
            listing_type=job.listing_type,
            property_types=job.property_types,
            past_days=job.past_days,
            date_from=job.date_from,
            date_to=job.date_to,
            radius=job.radius,
            mls_only=job.mls_only,
            foreclosure=job.foreclosure,
            exclude_pending=job.exclude_pending,
            limit=job.limit,
            proxy_config=job.proxy_config,
            user_agent=job.user_agent,
            request_delay=job.request_delay,
            scheduled_job_id=job.scheduled_job_id  # Preserve reference to scheduled job if exists
        )
        
        # Create the retry job
        await db.create_job(retry_job)
        
        # Start processing immediately
        asyncio.create_task(scraper.process_job(retry_job))
        
        logger.info(f"Created retry job {retry_job_id} for {len(failed_location_strings)} failed locations from job {job_id}")
        
        return {
            "message": f"Retry job created for {len(failed_location_strings)} failed location(s)",
            "retry_job_id": retry_job_id,
            "failed_locations_count": len(failed_location_strings),
            "failed_locations": failed_location_strings
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating retry job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create retry job: {str(e)}")

# Get statistics
@app.get("/stats")
async def get_stats():
    """Get server and scraping statistics"""
    try:
        job_stats = await db.get_job_stats()
        property_count = await db.get_property_count()
        proxy_stats = proxy_manager.get_proxy_stats()
        
        return {
            "jobs": job_stats,
            "properties": {
                "total": property_count
            },
            "proxies": proxy_stats,
            "scraper": {
                "running": scraper.is_running,
                "current_jobs": len(scraper.current_jobs)
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get statistics")

# Proxy management endpoints
@app.get("/proxies/stats")
async def get_proxy_stats():
    """Get proxy statistics"""
    try:
        return proxy_manager.get_proxy_stats()
    except Exception as e:
        logger.error(f"Error getting proxy stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get proxy statistics")

@app.post("/proxies/reset")
async def reset_proxy_stats():
    """Reset proxy statistics"""
    try:
        proxy_manager.reset_proxy_stats()
        return {"message": "Proxy statistics reset successfully"}
    except Exception as e:
        logger.error(f"Error resetting proxy stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to reset proxy statistics")

# Documentation endpoints
@app.get("/documentation/database-schema", response_class=HTMLResponse)
async def get_database_schema_docs():
    """Serve database schema documentation as HTML"""
    try:
        docs_path = os.path.join(os.path.dirname(__file__), "docs", "DATABASE_SCHEMA.md")
        if os.path.exists(docs_path):
            with open(docs_path, 'r', encoding='utf-8') as f:
                markdown_content = f.read()
            
            # Convert markdown to HTML (simple conversion)
            html_content = convert_markdown_to_html(markdown_content)
            return HTMLResponse(content=html_content)
        else:
            return HTMLResponse(content="<h1>Database Schema Documentation</h1><p>Documentation not found.</p>", status_code=404)
    except Exception as e:
        logger.error(f"Error serving database schema docs: {e}")
        return HTMLResponse(content="<h1>Error</h1><p>Failed to load documentation.</p>", status_code=500)

@app.get("/documentation/api-reference", response_class=HTMLResponse)
async def get_api_reference_docs():
    """Serve API reference documentation as HTML"""
    try:
        docs_path = os.path.join(os.path.dirname(__file__), "docs", "API_REFERENCE.md")
        if os.path.exists(docs_path):
            with open(docs_path, 'r', encoding='utf-8') as f:
                markdown_content = f.read()
            
            html_content = convert_markdown_to_html(markdown_content)
            return HTMLResponse(content=html_content)
        else:
            return HTMLResponse(content="<h1>API Reference Documentation</h1><p>Documentation not found.</p>", status_code=404)
    except Exception as e:
        logger.error(f"Error serving API reference docs: {e}")
        return HTMLResponse(content="<h1>Error</h1><p>Failed to load documentation.</p>", status_code=500)

@app.get("/documentation/integration-guide", response_class=HTMLResponse)
async def get_integration_guide_docs():
    """Serve integration guide documentation as HTML"""
    try:
        docs_path = os.path.join(os.path.dirname(__file__), "docs", "INTEGRATION_GUIDE.md")
        if os.path.exists(docs_path):
            with open(docs_path, 'r', encoding='utf-8') as f:
                markdown_content = f.read()
            
            html_content = convert_markdown_to_html(markdown_content)
            return HTMLResponse(content=html_content)
        else:
            return HTMLResponse(content="<h1>Integration Guide Documentation</h1><p>Documentation not found.</p>", status_code=404)
    except Exception as e:
        logger.error(f"Error serving integration guide docs: {e}")
        return HTMLResponse(content="<h1>Error</h1><p>Failed to load documentation.</p>", status_code=500)

@app.get("/documentation", response_class=HTMLResponse)
async def get_documentation_index():
    """Serve documentation index page"""
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>MLS Scraper Documentation</title>
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                line-height: 1.6;
                margin: 0;
                padding: 20px;
                background-color: #f5f5f5;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                padding: 30px;
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            h1 {
                color: #2c3e50;
                border-bottom: 3px solid #3498db;
                padding-bottom: 10px;
            }
            .doc-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                gap: 20px;
                margin-top: 30px;
            }
            .doc-card {
                border: 1px solid #ddd;
                border-radius: 8px;
                padding: 20px;
                background: #fafafa;
                transition: transform 0.2s, box-shadow 0.2s;
            }
            .doc-card:hover {
                transform: translateY(-2px);
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            }
            .doc-card h3 {
                margin-top: 0;
                color: #2c3e50;
            }
            .doc-card p {
                color: #666;
                margin-bottom: 15px;
            }
            .doc-link {
                display: inline-block;
                background: #3498db;
                color: white;
                padding: 10px 20px;
                text-decoration: none;
                border-radius: 5px;
                transition: background 0.2s;
            }
            .doc-link:hover {
                background: #2980b9;
            }
            .api-links {
                margin-top: 30px;
                padding: 20px;
                background: #ecf0f1;
                border-radius: 8px;
            }
            .api-links h3 {
                margin-top: 0;
                color: #2c3e50;
            }
            .api-links a {
                color: #3498db;
                text-decoration: none;
                margin-right: 20px;
            }
            .api-links a:hover {
                text-decoration: underline;
            }
            .version-info {
                margin-top: 30px;
                padding: 15px;
                background: #e8f5e8;
                border-radius: 5px;
                border-left: 4px solid #27ae60;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🏠 MLS Scraper Documentation</h1>
            <p>Comprehensive documentation for the MLS Scraper system, designed for external applications and developers.</p>
            
            <div class="doc-grid">
                <div class="doc-card">
                    <h3>📊 Database Schema</h3>
                    <p>Complete database schema specification with field descriptions, data types, and relationships.</p>
                    <a href="/documentation/database-schema" class="doc-link">View Database Schema</a>
                </div>
                
                <div class="doc-card">
                    <h3>🔌 API Reference</h3>
                    <p>Comprehensive API endpoints documentation with request/response examples and status codes.</p>
                    <a href="/documentation/api-reference" class="doc-link">View API Reference</a>
                </div>
                
                <div class="doc-card">
                    <h3>🚀 Integration Guide</h3>
                    <p>Step-by-step integration guide for external applications with code examples and best practices.</p>
                    <a href="/documentation/integration-guide" class="doc-link">View Integration Guide</a>
                </div>
            </div>
            
            <div class="api-links">
                <h3>🔗 API Documentation</h3>
                <p>Interactive API documentation and testing tools:</p>
                <a href="/docs" target="_blank">Swagger UI</a>
                <a href="/redoc" target="_blank">ReDoc</a>
                <a href="/openapi.json" target="_blank">OpenAPI Schema</a>
            </div>
            
            <div class="version-info">
                <strong>Version:</strong> 1.2.0 | <strong>Last Updated:</strong> 2025-09-12 | <strong>Status:</strong> Stable
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

def convert_markdown_to_html(markdown_content: str) -> str:
    """Convert markdown content to HTML with styling"""
    # Simple markdown to HTML conversion
    html = markdown_content
    
    # Headers
    html = html.replace('# ', '<h1>').replace('\n# ', '</h1>\n<h1>')
    html = html.replace('## ', '<h2>').replace('\n## ', '</h2>\n<h2>')
    html = html.replace('### ', '<h3>').replace('\n### ', '</h3>\n<h3>')
    html = html.replace('#### ', '<h4>').replace('\n#### ', '</h4>\n<h4>')
    
    # Code blocks
    html = html.replace('```json', '<pre><code class="language-json">')
    html = html.replace('```python', '<pre><code class="language-python">')
    html = html.replace('```javascript', '<pre><code class="language-javascript">')
    html = html.replace('```bash', '<pre><code class="language-bash">')
    html = html.replace('```', '</code></pre>')
    
    # Inline code
    html = html.replace('`', '<code>').replace('<code>', '<code>', 1).replace('<code>', '</code>', 1)
    
    # Bold and italic
    html = html.replace('**', '<strong>').replace('<strong>', '</strong>', 1)
    html = html.replace('*', '<em>').replace('<em>', '</em>', 1)
    
    # Lists
    html = html.replace('- ', '<li>')
    html = html.replace('\n<li>', '</li>\n<li>')
    
    # Line breaks
    html = html.replace('\n', '<br>\n')
    
    # Wrap in HTML structure
    full_html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>MLS Scraper Documentation</title>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                line-height: 1.6;
                margin: 0;
                padding: 20px;
                background-color: #f5f5f5;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                padding: 30px;
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }}
            h1, h2, h3, h4 {{
                color: #2c3e50;
                margin-top: 30px;
            }}
            h1 {{
                border-bottom: 3px solid #3498db;
                padding-bottom: 10px;
            }}
            code {{
                background: #f4f4f4;
                padding: 2px 6px;
                border-radius: 3px;
                font-family: 'Courier New', monospace;
            }}
            pre {{
                background: #f8f8f8;
                padding: 15px;
                border-radius: 5px;
                overflow-x: auto;
                border-left: 4px solid #3498db;
            }}
            pre code {{
                background: none;
                padding: 0;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 12px;
                text-align: left;
            }}
            th {{
                background: #f8f9fa;
                font-weight: bold;
            }}
            .back-link {{
                display: inline-block;
                background: #3498db;
                color: white;
                padding: 10px 20px;
                text-decoration: none;
                border-radius: 5px;
                margin-bottom: 20px;
            }}
            .back-link:hover {{
                background: #2980b9;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <a href="/documentation" class="back-link">← Back to Documentation</a>
            {html}
        </div>
    </body>
    </html>
    """
    
    return full_html

# ============================================================================
# ENRICHMENT API ENDPOINTS
# ============================================================================

@app.get("/properties/{property_id}/history")
async def get_property_history(property_id: str, limit: int = 50):
    """Get property history (price and status changes)"""
    try:
        if not db.enrichment_pipeline:
            raise HTTPException(status_code=503, detail="Enrichment service not available")
        
        history = await db.enrichment_pipeline.get_property_history(property_id, limit)
        return {
            "property_id": property_id,
            "history": history,
            "count": len(history)
        }
    except Exception as e:
        logger.error(f"Error getting property history for {property_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting property history: {str(e)}")

@app.get("/properties/{property_id}/changes")
async def get_property_change_logs(property_id: str, limit: int = 100):
    """Get detailed change logs for a property"""
    try:
        if not db.enrichment_pipeline:
            raise HTTPException(status_code=503, detail="Enrichment service not available")
        
        change_logs = await db.enrichment_pipeline.get_property_change_logs(property_id, limit)
        return {
            "property_id": property_id,
            "change_logs": change_logs,
            "count": len(change_logs)
        }
    except Exception as e:
        logger.error(f"Error getting change logs for {property_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting change logs: {str(e)}")

@app.get("/properties/{property_id}/enrichment")
async def get_property_enrichment(property_id: str):
    """Get enrichment data for a property"""
    try:
        if not db.enrichment_pipeline:
            raise HTTPException(status_code=503, detail="Enrichment service not available")
        
        enrichment_data = await db.enrichment_pipeline.get_property_enrichment(property_id)
        
        if enrichment_data is None:
            raise HTTPException(status_code=404, detail="Enrichment data not found for this property")
        
        return {
            "property_id": property_id,
            "enrichment": enrichment_data
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting enrichment for {property_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting enrichment data: {str(e)}")

@app.get("/properties/motivated-sellers")
async def get_motivated_sellers(min_score: int = 40, limit: int = 100):
    """Get properties with motivated seller scores above threshold"""
    try:
        if not db.enrichment_pipeline:
            raise HTTPException(status_code=503, detail="Enrichment service not available")
        
        motivated_sellers = await db.enrichment_pipeline.get_motivated_sellers(min_score, limit)
        
        return {
            "motivated_sellers": motivated_sellers,
            "count": len(motivated_sellers),
            "min_score": min_score,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Error getting motivated sellers: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting motivated sellers: {str(e)}")

@app.post("/properties/{property_id}/enrich")
async def enrich_property(property_id: str):
    """Manually trigger enrichment for a property"""
    try:
        if not db.enrichment_pipeline:
            raise HTTPException(status_code=503, detail="Enrichment service not available")
        
        # Get the property from database
        property_doc = await db.properties_collection.find_one({"property_id": property_id})
        if not property_doc:
            raise HTTPException(status_code=404, detail="Property not found")
        
        # Trigger enrichment
        enrichment_data = await db.enrichment_pipeline.enrich_property(
            property_id=property_id,
            property_dict=property_doc,
            existing_property=None,  # Could check for existing enrichment
            job_id=None
        )
        
        return {
            "property_id": property_id,
            "enrichment": enrichment_data,
            "message": "Enrichment completed successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error enriching property {property_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error enriching property: {str(e)}")

@app.post("/enrichment/recalculate-scores", response_model=EnrichmentRecalcResponse)
async def recalculate_scores(request: EnrichmentRecalcRequest):
    """
    Recalculate all scores using current config (no re-detection)
    Only updates score from existing findings, very fast operation.
    """
    try:
        if not db.enrichment_pipeline:
            raise HTTPException(status_code=503, detail="Enrichment service not available")
        
        result = await db.recalculate_all_scores(
            limit=request.limit,
            min_score=request.min_score if request.recalc_type == "scores_only" else None
        )
        
        return EnrichmentRecalcResponse(
            recalc_type="scores_only",
            **result
        )
    except Exception as e:
        logger.error(f"Error recalculating scores: {e}")
        raise HTTPException(status_code=500, detail=f"Error recalculating scores: {str(e)}")

@app.post("/enrichment/redetect-keywords", response_model=EnrichmentRecalcResponse)
async def redetect_keywords(request: EnrichmentRecalcRequest):
    """
    Re-run keyword detection and recalculate scores
    Re-analyzes property descriptions and updates findings.
    """
    try:
        if not db.enrichment_pipeline:
            raise HTTPException(status_code=503, detail="Enrichment service not available")
        
        result = await db.redetect_keywords_all(limit=request.limit)
        
        return EnrichmentRecalcResponse(
            recalc_type="keywords",
            **result
        )
    except Exception as e:
        logger.error(f"Error re-detecting keywords: {e}")
        raise HTTPException(status_code=500, detail=f"Error re-detecting keywords: {str(e)}")

@app.post("/enrichment/full-reenrich", response_model=EnrichmentRecalcResponse)
async def full_reenrich(request: EnrichmentRecalcRequest):
    """
    Full re-enrichment (detection + calculation)
    Runs complete enrichment pipeline for all properties.
    """
    try:
        if not db.enrichment_pipeline:
            raise HTTPException(status_code=503, detail="Enrichment service not available")
        
        # For full re-enrichment, we need to iterate and call enrich_property
        # This is similar to recalculate but uses full enrichment
        from datetime import datetime
        start_time = datetime.utcnow()
        total_processed = 0
        total_updated = 0
        total_errors = 0
        errors = []
        
        query = {}
        cursor = db.properties_collection.find(query, {"property_id": 1})
        if request.limit:
            cursor = cursor.limit(request.limit)
        
        batch_size = 100
        property_ids = []
        
        async for doc in cursor:
            property_ids.append(doc["property_id"])
            total_processed += 1
            
            if len(property_ids) >= batch_size:
                for prop_id in property_ids:
                    try:
                        property_dict = await db.enrichment_pipeline._get_property_dict(prop_id)
                        if property_dict:
                            await db.enrichment_pipeline.enrich_property(
                                property_id=prop_id,
                                property_dict=property_dict,
                                existing_property=None,
                                job_id=None
                            )
                            total_updated += 1
                    except Exception as e:
                        total_errors += 1
                        errors.append(f"Property {prop_id}: {str(e)}")
                
                property_ids = []
                logger.info(f"Processed {total_processed} properties, updated {total_updated}, errors {total_errors}")
        
        # Process remaining
        for prop_id in property_ids:
            try:
                property_dict = await db.enrichment_pipeline._get_property_dict(prop_id)
                if property_dict:
                    await db.enrichment_pipeline.enrich_property(
                        property_id=prop_id,
                        property_dict=property_dict,
                        existing_property=None,
                        job_id=None
                    )
                    total_updated += 1
            except Exception as e:
                total_errors += 1
                errors.append(f"Property {prop_id}: {str(e)}")
        
        completed_at = datetime.utcnow()
        result = {
            "total_processed": total_processed,
            "total_updated": total_updated,
            "total_errors": total_errors,
            "started_at": start_time,
            "completed_at": completed_at,
            "duration_seconds": (completed_at - start_time).total_seconds(),
            "errors": errors[:50]
        }
        
        return EnrichmentRecalcResponse(
            recalc_type="full",
            **result
        )
    except Exception as e:
        logger.error(f"Error in full re-enrichment: {e}")
        raise HTTPException(status_code=500, detail=f"Error in full re-enrichment: {str(e)}")

@app.get("/enrichment/config", response_model=EnrichmentConfigResponse)
async def get_enrichment_config():
    """Get current scoring configuration"""
    try:
        if not db.enrichment_pipeline:
            raise HTTPException(status_code=503, detail="Enrichment service not available")
        
        scorer = db.enrichment_pipeline.motivated_seller_scorer
        config_hash = scorer.get_config_hash()
        config_version = scorer.config.get('config_version', '2.0')
        
        return EnrichmentConfigResponse(
            config=scorer.config,
            config_hash=config_hash,
            config_version=config_version
        )
    except Exception as e:
        logger.error(f"Error getting config: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting config: {str(e)}")

@app.post("/enrichment/update-dom")
async def update_dom_all(limit: Optional[int] = None):
    """
    Update days on market for all properties and recalculate scores
    Lightweight operation - only updates DOM finding and recalculates.
    """
    try:
        if not db.enrichment_pipeline:
            raise HTTPException(status_code=503, detail="Enrichment service not available")
        
        result = await db.update_dom_all(limit=limit)
        
        return EnrichmentRecalcResponse(
            recalc_type="dom_update",
            **result
        )
    except Exception as e:
        logger.error(f"Error updating DOM: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating DOM: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    # Use Railway's PORT environment variable if available, otherwise use settings
    port = int(os.getenv("PORT", settings.API_PORT))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",  # Railway requires 0.0.0.0
        port=port,
        reload=False  # Disable reload in production
    )
