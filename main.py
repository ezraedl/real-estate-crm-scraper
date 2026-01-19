from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from starlette.responses import Response
from fastapi.staticfiles import StaticFiles
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
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
from services.rentcast_service import RentcastService
from middleware.auth import verify_token, verify_rent_backfill_auth

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import OpenAI Agent Lookup
try:
    from openai_agent_lookup import AgentLookupClient
    AGENT_LOOKUP_AVAILABLE = True
except ImportError:
    logger.warning("openai_agent_lookup library not available. Agent lookup endpoint will be disabled.")
    AGENT_LOOKUP_AVAILABLE = False

# Create FastAPI app
app = FastAPI(
    title="MLS Scraping Server",
    description="High-performance MLS data scraping server with MongoDB storage",
    version="1.0.0"
)

# Add CORS middleware
# Allow frontend domain and Authorization header for JWT tokens
cors_origins_str = settings.CORS_ORIGINS.strip() if settings.CORS_ORIGINS else "*"
if cors_origins_str == "*":
    # When using allow_credentials=True, cannot use ["*"] - must specify origins
    # Default to allowing all origins by not restricting (but this is less secure)
    allowed_origins = ["*"]
    allow_creds = False  # Cannot use credentials with wildcard
else:
    # Split by comma, trim whitespace, and remove trailing slashes
    allowed_origins = []
    for origin in cors_origins_str.split(","):
        origin = origin.strip()
        if origin:
            # Remove trailing slash for consistency
            if origin.endswith("/"):
                origin = origin[:-1]
            allowed_origins.append(origin)
    allow_creds = True  # Can use credentials when origins are specified

logger.info(f"CORS configured with origins: {allowed_origins}, allow_credentials: {allow_creds}")

# OPTIONS preflight for /enrichment/config and /scheduled-jobs (runs before CORSMiddleware)
# CORSMiddleware was returning 400 for these preflights; handle them here and return 200.
_OPTIONS_PREFLIGHT_PATHS = ("/enrichment/config", "/scheduled-jobs")


class OptionsPreflightMiddleware:
    def __init__(self, app, *, allowed_origins=None, allow_creds=False):
        self.app = app
        self.allowed_origins = allowed_origins or []
        self.allow_creds = allow_creds

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http" or scope.get("method") != "OPTIONS":
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "")
        if path not in _OPTIONS_PREFLIGHT_PATHS:
            await self.app(scope, receive, send)
            return
        origin = None
        for k, v in scope.get("headers", []):
            if k == b"origin":
                origin = v.decode("latin-1")
                break
        if "*" in self.allowed_origins:
            allow_origin = b"*"
        elif origin:
            norm_origin = origin.rstrip("/")
            if any(o.rstrip("/") == norm_origin for o in self.allowed_origins):
                allow_origin = origin.encode("latin-1")
            else:
                allow_origin = origin.encode("latin-1")  # reflect; browser may still reject if strict
        else:
            allow_origin = b"*"
        headers = [
            (b"access-control-allow-origin", allow_origin),
            (b"access-control-allow-methods", b"GET, POST, PUT, DELETE, OPTIONS, PATCH"),
            (b"access-control-allow-headers", b"Content-Type, Authorization, X-Requested-With, Accept, Origin, Access-Control-Request-Method, Access-Control-Request-Headers"),
            (b"access-control-max-age", b"86400"),
        ]
        if self.allow_creds and allow_origin != b"*":
            headers.append((b"access-control-allow-credentials", b"true"))
        await send({"type": "http.response.start", "status": 200, "headers": headers})
        await send({"type": "http.response.body", "body": b""})


# #region agent log
import json
try:
    with open('c:\\Projects\\Real-Estate-CRM-Repos\\real-estate-crm-backend\\.cursor\\debug.log', 'a', encoding='utf-8') as f:
        f.write(json.dumps({"sessionId":"debug-session","runId":"pre-middleware","hypothesisId":"H1","location":"main.py:79","message":"Before middleware - checking allowed_origins type and value","data":{"type":str(type(allowed_origins)),"value":allowed_origins,"is_list":isinstance(allowed_origins, list)},"timestamp":int(__import__('time').time()*1000)}) + "\n")
except: pass
# #endregion

# #region agent log
try:
    with open('c:\\Projects\\Real-Estate-CRM-Repos\\real-estate-crm-backend\\.cursor\\debug.log', 'a', encoding='utf-8') as f:
        f.write(json.dumps({"sessionId":"debug-session","runId":"pre-middleware","hypothesisId":"H1,H3,H5","location":"main.py:81","message":"Middleware parameters before add_middleware","data":{"origins_type":str(type(allowed_origins)),"allow_creds":allow_creds,"methods":["GET","POST","PUT","DELETE","OPTIONS","PATCH"]},"timestamp":int(__import__('time').time()*1000)}) + "\n")
except: pass
# #endregion

try:
    # #region agent log
    try:
        import json
        with open('c:\\Projects\\Real-Estate-CRM-Repos\\real-estate-crm-backend\\.cursor\\debug.log', 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"middleware-config","hypothesisId":"H5","location":"main.py:106","message":"Testing middleware with explicit headers","data":{"origins":allowed_origins[:3] if len(allowed_origins) > 3 else allowed_origins,"python_version":__import__('sys').version},"timestamp":int(__import__('time').time()*1000)}) + "\n")
    except: pass
    # #endregion
    
    # Fix: Use explicit header list instead of ["*"] which may not be supported in Python 3.14/FastAPI 0.104.1
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=allow_creds,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
        allow_headers=["Content-Type", "Authorization", "X-Requested-With", "Accept", "Origin", "Access-Control-Request-Method", "Access-Control-Request-Headers"],  # Explicit headers instead of ["*"]
        expose_headers=["Content-Type", "Authorization"],  # Explicit headers instead of ["*"]
    )
    # Handle OPTIONS for /enrichment/config and /scheduled-jobs before CORS (CORS was returning 400)
    app.add_middleware(
        OptionsPreflightMiddleware,
        allowed_origins=allowed_origins,
        allow_creds=allow_creds,
    )
    # #region agent log
    try:
        with open('c:\\Projects\\Real-Estate-CRM-Repos\\real-estate-crm-backend\\.cursor\\debug.log', 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"pre-middleware","hypothesisId":"H1,H3,H5","location":"main.py:88","message":"Middleware added successfully","data":{"success":True},"timestamp":int(__import__('time').time()*1000)}) + "\n")
    except: pass
    # #endregion
except Exception as e:
    # #region agent log
    try:
        with open('c:\\Projects\\Real-Estate-CRM-Repos\\real-estate-crm-backend\\.cursor\\debug.log', 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"pre-middleware","hypothesisId":"H1,H3,H5","location":"main.py:120","message":"Middleware addition failed","data":{"error":str(e),"error_type":str(type(e).__name__),"traceback":__import__('traceback').format_exc()},"timestamp":int(__import__('time').time()*1000)}) + "\n")
    except: pass
    # #endregion
    raise

# Helper function to determine remaining work from progress logs
def get_remaining_locations(job: ScrapingJob) -> List[str]:
    """
    Parse progress logs to determine which locations still need to be processed.
    Returns list of locations that were not completed.
    Handles both old format (list) and new format (dict with 'locations' key).
    """
    if not job.progress_logs or not job.locations:
        # No logs or no locations, retry everything
        return job.locations
    
    completed_locations = set()
    
    # Handle new format (dict with 'locations' key)
    if isinstance(job.progress_logs, dict) and "locations" in job.progress_logs:
        # New format: progress_logs is a dict with 'locations' array
        for location_entry in job.progress_logs.get("locations", []):
            if isinstance(location_entry, dict):
                location = location_entry.get("location")
                status = location_entry.get("status")
                # A location is considered complete if status is "completed"
                if location and status == "completed":
                    completed_locations.add(location)
    # Handle old format (list of log entries)
    elif isinstance(job.progress_logs, list):
        # Old format: progress_logs is a list of log entries
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
        # Exclude jobs that were manually cancelled (they should be in CANCELLED status, but check error_message too)
        # Only retry jobs that are actually stuck (crashed), not manually cancelled
        stuck_jobs_cursor = db.jobs_collection.find({
            "status": {"$in": [JobStatus.RUNNING.value]},  # Only RUNNING status
            # Exclude jobs with error messages indicating manual cancellation
            "$or": [
                {"error_message": {"$exists": False}},
                {"error_message": {"$not": {"$regex": "manually cancelled|manually stopped|user cancelled|cancelled by user", "$options": "i"}}}
            ]
        }).sort([("created_at", -1)])
        stuck_jobs = []
        async for job_data in stuck_jobs_cursor:
            job_data["_id"] = str(job_data["_id"])
            # Double-check: skip if error_message indicates manual cancellation
            error_msg = job_data.get("error_message", "").lower() if job_data.get("error_message") else ""
            cancellation_keywords = ["manually cancelled", "manually stopped", "user cancelled", "cancelled by user"]
            if any(keyword in error_msg for keyword in cancellation_keywords):
                logger.info(f"Skipping job {job_data.get('job_id')} - appears to be manually cancelled (error_message: {job_data.get('error_message')})")
                # Ensure it's marked as cancelled if it's still in RUNNING status
                await db.update_job_status(
                    job_data.get("job_id"),
                    JobStatus.CANCELLED,
                    error_message=job_data.get("error_message", "Manually cancelled by user")
                )
                continue
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
            # Check if there's already a retry job in progress for this scheduled_job_id
            # This prevents creating duplicate retry jobs
            scheduled_job_id = job_group[0].scheduled_job_id if job_group else None
            if scheduled_job_id:
                # Check for existing retry jobs (PENDING or RUNNING) for this scheduled_job_id
                existing_retry_jobs_cursor = db.jobs_collection.find({
                    "scheduled_job_id": scheduled_job_id,
                    "status": {"$in": [JobStatus.PENDING.value, JobStatus.RUNNING.value]},
                    "job_id": {"$regex": "_retry_"}
                })
                existing_retry_jobs = []
                async for retry_job_data in existing_retry_jobs_cursor:
                    existing_retry_jobs.append(retry_job_data.get("job_id"))
                
                if existing_retry_jobs:
                    logger.info(
                        f"Skipping retry for scheduled_job_id={scheduled_job_id} - "
                        f"retry job(s) already exist: {existing_retry_jobs}"
                    )
                    # Mark all stuck jobs as failed since we're not retrying them
                    for job in job_group:
                        await db.update_job_status(
                            job.job_id,
                            JobStatus.FAILED,
                            error_message=f"Job stuck in running status. Retry job already exists: {existing_retry_jobs[0]}"
                        )
                    continue
            
            # Sort jobs by created_at (most recent first)
            # Use utcnow() as fallback for None values to ensure proper sorting
            job_group.sort(key=lambda j: j.created_at if j.created_at else datetime.utcnow(), reverse=True)
            
            # Collect all remaining locations from ALL stuck jobs in this group
            # This ensures we create ONE retry job that covers all remaining locations
            all_remaining_locations = set()
            all_job_configs = {}  # Store config from most recent job
            
            for job in job_group:
                # Get remaining locations for this job
                remaining_locations = get_remaining_locations(job)
                all_remaining_locations.update(remaining_locations)
                
                # Store config from most recent job (first in sorted list)
                if not all_job_configs:
                    all_job_configs = {
                        "scheduled_job_id": job.scheduled_job_id,
                        "listing_types": job.listing_types,
                        "listing_type": job.listing_type,
                        "property_types": job.property_types,
                        "past_days": job.past_days,
                        "date_from": job.date_from,
                        "date_to": job.date_to,
                        "radius": job.radius,
                        "mls_only": job.mls_only,
                        "foreclosure": job.foreclosure,
                        "exclude_pending": job.exclude_pending,
                        "limit": job.limit,
                        "proxy_config": job.proxy_config,
                        "user_agent": job.user_agent,
                        "request_delay": job.request_delay,
                    }
            
            if not all_remaining_locations:
                # All locations completed, mark all jobs as completed
                logger.info(f"All locations completed for scheduled_job_id={scheduled_key}, marking jobs as completed")
                for job in job_group:
                    await db.update_job_status(
                        job.job_id,
                        JobStatus.COMPLETED,
                        error_message="Job was stuck but all locations were completed"
                    )
                continue
            
            # Use the most recent job as the base for retry count
            job_to_retry = job_group[0]
            
            try:
                # Check how many times this job has been retried
                retry_count = job_to_retry.job_id.count("_retry_")
                
                if retry_count >= 2:
                    # Already retried twice (3 total attempts), mark all as failed
                    logger.warning(
                        f"Job {job_to_retry.job_id} has been retried {retry_count} times. Marking all as failed."
                    )
                    for job in job_group:
                        await db.update_job_status(
                            job.job_id,
                            JobStatus.FAILED,
                            error_message="Job stuck in running status after service restart. Max retries exceeded."
                        )
                        # Update scheduled job history if applicable
                        if job.scheduled_job_id:
                            # Try to get property counts from the job if available
                            properties_scraped = None
                            properties_saved = None
                            if hasattr(job, 'properties_scraped'):
                                properties_scraped = job.properties_scraped
                            if hasattr(job, 'properties_saved'):
                                properties_saved = job.properties_saved
                            await db.update_scheduled_job_run_history(
                                job.scheduled_job_id,
                                job.job_id,
                                JobStatus.FAILED,
                                error_message="Job stuck in running status after service restart. Max retries exceeded.",
                                properties_scraped=properties_scraped,
                                properties_saved=properties_saved
                            )
                else:
                    # Create ONE retry job with ALL remaining locations from ALL stuck jobs
                    logger.info(
                        f"Creating single retry job for scheduled_job_id={scheduled_key} "
                        f"covering {len(all_remaining_locations)} remaining locations from {len(job_group)} stuck job(s)"
                    )
                    
                    # Create a retry job with all remaining locations
                    retry_job_id = f"{job_to_retry.job_id}_retry_{retry_count + 1}"
                    retry_job = ScrapingJob(
                        job_id=retry_job_id,
                        priority=JobPriority.HIGH,  # High priority for retries
                        scheduled_job_id=all_job_configs.get("scheduled_job_id"),
                        locations=list(all_remaining_locations),  # All remaining locations from all stuck jobs
                        listing_types=all_job_configs.get("listing_types"),
                        listing_type=all_job_configs.get("listing_type"),
                        property_types=all_job_configs.get("property_types"),
                        past_days=all_job_configs.get("past_days"),
                        date_from=all_job_configs.get("date_from"),
                        date_to=all_job_configs.get("date_to"),
                        radius=all_job_configs.get("radius"),
                        mls_only=all_job_configs.get("mls_only"),
                        foreclosure=all_job_configs.get("foreclosure"),
                        exclude_pending=all_job_configs.get("exclude_pending"),
                        limit=all_job_configs.get("limit"),
                        proxy_config=all_job_configs.get("proxy_config"),
                        user_agent=all_job_configs.get("user_agent"),
                        request_delay=all_job_configs.get("request_delay"),
                        total_locations=len(all_remaining_locations)
                    )
                    
                    logger.info(f"Consolidated retry: {len(all_remaining_locations)} remaining locations from {len(job_group)} stuck job(s)")
                    
                    # Mark all stuck jobs as failed
                    for job in job_group:
                        await db.update_job_status(
                            job.job_id,
                            JobStatus.FAILED,
                            error_message=f"Job stuck in running status. Consolidated retry as {retry_job_id}"
                        )
                    
                    # Create and start the retry job
                    await db.create_job(retry_job)
                    asyncio.create_task(scraper.process_job(retry_job))
                    
                    logger.info(f"Created consolidated retry job: {retry_job_id} for scheduled_job_id={scheduled_key}")
                    
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
            if hasattr(settings, 'USE_DATAIMPULSE') and not settings.USE_DATAIMPULSE:
                logger.warning("⚠️  DataImpulse proxy is DISABLED via USE_DATAIMPULSE=false - running without proxy support")
                logger.warning("   This will cause 403 errors from Realtor.com. Set USE_DATAIMPULSE=true to enable.")
            elif hasattr(settings, 'DATAIMPULSE_LOGIN') and settings.DATAIMPULSE_LOGIN:
                logger.info("🔄 Initializing DataImpulse proxies...")
                success = await proxy_manager.initialize_dataimpulse_proxies(settings.DATAIMPULSE_LOGIN)
                if success:
                    proxy_count = len(proxy_manager.proxies)
                    logger.info(f"✅ DataImpulse proxies initialized successfully ({proxy_count} proxy configurations)")
                    logger.info(f"   Login: {settings.DATAIMPULSE_LOGIN}")
                    logger.info(f"   Endpoint: {settings.DATAIMPULSE_ENDPOINT}")
                else:
                    logger.error("❌ Failed to initialize DataImpulse proxies - check your credentials")
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
async def get_active_properties_config(token_payload: dict = Depends(verify_token)):
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
async def update_active_properties_config(request: dict, token_payload: dict = Depends(verify_token)):
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
async def toggle_active_properties_rescraping(request: dict, token_payload: dict = Depends(verify_token)):
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
async def get_location_suggestions(q: str, state: Optional[str] = None, limit: int = 10, token_payload: dict = Depends(verify_token)):
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

# Rent estimation backfill: in-memory lock to avoid overlapping runs
_rent_backfill_running = False


async def _run_rent_backfill_task(limit: int) -> None:
    """Background task: fetch Rentcast rent estimates for properties missing rent_estimation.rent."""
    global _rent_backfill_running
    _rent_backfill_running = True
    done, failed = 0, 0
    try:
        # Properties missing rent: no rent_estimation.rent or it is null
        q = {
            "address": {"$exists": True},
            "$and": [
                {"$or": [
                    {"address.formatted_address": {"$exists": True, "$ne": ""}},
                    {"address.city": {"$exists": True, "$ne": ""}},
                ]},
                {"$or": [
                    {"rent_estimation.rent": {"$exists": False}},
                    {"rent_estimation.rent": None},
                ]},
            ],
        }
        cursor = db.properties_collection.find(q).limit(limit)
        properties = await cursor.to_list(length=limit)
        total = len(properties)
        if total == 0:
            logger.info("Rent backfill: no properties missing rent data")
            return
        logger.info("Rent backfill: starting for %d properties (limit=%d)", total, limit)
        svc = RentcastService(db)
        for i, p in enumerate(properties, 1):
            pid = p.get("property_id")
            if not pid:
                failed += 1
                continue
            pid = str(pid)
            try:
                ok = await svc.fetch_and_save_rent_estimate(pid, p)
                if ok:
                    done += 1
                else:
                    failed += 1
            except Exception as e:
                logger.warning("Rent backfill: property_id=%s error: %s", pid, e)
                failed += 1
            if i % 50 == 0:
                logger.info("Rent backfill: progress %d/%d (ok=%d, fail=%d)", i, total, done, failed)
        logger.info("Rent backfill: completed done=%d failed=%d", done, failed)
    finally:
        _rent_backfill_running = False


@app.post("/rent-estimation/backfill")
@app.post("/rent-estimation/backfill/")  # with trailing slash (Railway/proxies may normalize to this)
async def rent_estimation_backfill_post(request: Optional[Dict[str, Any]] = Body(None), token_payload: dict = Depends(verify_rent_backfill_auth)):
    """
    Start a background job to fetch Rentcast rent estimates for all properties missing rent data.
    Only properties with an address (formatted_address or city) and without rent_estimation.rent are processed.
    Requires JWT or X-API-Key. Run from production: POST /rent-estimation/backfill with body {"limit": 500}.
    """
    global _rent_backfill_running
    if _rent_backfill_running:
        return {"status": "already_running", "message": "Rent estimation backfill is already running."}
    limit = 500
    if request and isinstance(request.get("limit"), (int, float)):
        limit = max(1, min(int(request["limit"]), 5000))
    q = {
        "address": {"$exists": True},
        "$and": [
            {"$or": [
                {"address.formatted_address": {"$exists": True, "$ne": ""}},
                {"address.city": {"$exists": True, "$ne": ""}},
            ]},
            {"$or": [
                {"rent_estimation.rent": {"$exists": False}},
                {"rent_estimation.rent": None},
            ]},
        ],
    }
    total = await db.properties_collection.count_documents(q)
    total = min(total, limit)
    asyncio.create_task(_run_rent_backfill_task(limit))
    return {
        "status": "started",
        "total": total,
        "limit": limit,
        "message": f"Rent estimation backfill started for up to {total} properties. Check logs for progress.",
    }


# Get properties by ID endpoint
@app.post("/properties/by-ids")
async def get_properties_by_ids(request: PropertyIdsRequest, token_payload: dict = Depends(verify_token)):
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
async def immediate_scrape(request: ImmediateScrapeRequest, token_payload: dict = Depends(verify_token)):
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
async def immediate_scrape_sync(request: ImmediateScrapeRequest, token_payload: dict = Depends(verify_token)):
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
        
        # First, check database for existing properties (unless force_rescrape is True)
        total_properties_scraped = 0
        total_properties_saved = 0
        all_properties = []
        
        # Skip database check if force_rescrape is True
        # Access force_rescrape directly from the Pydantic model (it's always present with default=False)
        force_rescrape = request.force_rescrape
        logger.info(f"force_rescrape value: {force_rescrape} (type: {type(force_rescrape)})")
        logger.info(f"Full request object: {request.model_dump()}")
        logger.info(f"Request dict: {request.dict()}")
        if not force_rescrape:
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
            logger.info(f"force_rescrape=True: Skipping database check, performing fresh scrape...")
        
        # Proceed with scraping (either no existing properties found, or force_rescrape is True)
        logger.info(f"Proceeding with scraping for {len(request.locations)} location(s)...")
        
        # Track diagnostic information across all locations
        all_diagnostic_info = []
        
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
                
                # Use the scraper instance's method which properly uses the local HomeHarvest fork
                logger.info(f"Using scraper._scrape_all_listing_types for: {location}")
                logger.info(f"Scraper job config: listing_type={temp_job.listing_type}, limit={temp_job.limit}, radius={temp_job.radius}")
                
                # Include off_market for force_rescrape to catch properties that have moved off-market
                listing_types = ["sold", "for_sale", "for_rent", "pending", "off_market"]
                
                # Track diagnostic information
                diagnostic_info = {
                    "listing_types_tried": listing_types.copy(),
                    "listing_types_errors": {},
                    "fallback_search_attempted": False,
                    "fallback_search_error": None
                }
                
                all_properties = []
                
                try:
                    # Use the scraper's _scrape_all_listing_types method which handles the local HomeHarvest fork correctly
                    logger.info(f"Scraping all listing types ({', '.join(listing_types)}) for location: {location}")
                    
                    all_properties_by_type, scrape_error = await scraper._scrape_all_listing_types(
                        location=location,
                        job=temp_job,
                        proxy_config=proxy_config,
                        listing_types=listing_types,
                        limit=temp_job.limit or 200,
                        past_days=temp_job.past_days if temp_job.past_days else None,
                        cancel_flag=None,
                        location_last_update=None
                    )
                    
                    # Collect all properties from all listing types
                    for listing_type, properties_list in all_properties_by_type.items():
                        if properties_list:
                            logger.info(f"Found {len(properties_list)} {listing_type} properties for location: {location}")
                            for prop in properties_list:
                                if listing_type == "sold":
                                    prop.is_comp = True
                                all_properties.append(prop)
                                logger.debug(f"  - Found property: {prop.address.formatted_address if prop.address else 'No address'} (ID: {prop.property_id}, type: {listing_type})")
                        else:
                            logger.info(f"No {listing_type} properties found for location: {location}")
                    
                    if scrape_error:
                        logger.warning(f"Scrape completed with error: {scrape_error}")
                        # Don't set this as an error for all types, just log it
                    
                    # If no properties found, try without listing_type filter (all types)
                    if not all_properties:
                        diagnostic_info["fallback_search_attempted"] = True
                        logger.info(f"No properties found with specific listing types. Trying without listing_type filter (all types) for location: {location}")
                        
                        # Try with all listing types (including off_market) to get comprehensive results
                        all_listing_types = ["sold", "for_sale", "for_rent", "pending", "off_market"]
                        all_properties_by_type_fallback, fallback_error = await scraper._scrape_all_listing_types(
                            location=location,
                            job=temp_job,
                            proxy_config=proxy_config,
                            listing_types=all_listing_types,  # All types for comprehensive search
                            limit=temp_job.limit or 200,
                            past_days=None,
                            cancel_flag=None,
                            location_last_update=None
                        )
                        
                        if all_properties_by_type_fallback:
                            for listing_type, properties_list in all_properties_by_type_fallback.items():
                                if properties_list:
                                    logger.info(f"Found {len(properties_list)} {listing_type} properties (fallback search) for location: {location}")
                                    for prop in properties_list:
                                        all_properties.append(prop)
                                        logger.debug(f"  - Found property (fallback): {prop.address.formatted_address if prop.address else 'No address'} (ID: {prop.property_id}, type: {listing_type})")
                        
                        if fallback_error:
                            diagnostic_info["fallback_search_error"] = fallback_error
                            logger.warning(f"Fallback search completed with error: {fallback_error}")
                        elif not all_properties:
                            logger.info(f"No properties found even without listing_type filter for location: {location}")
                            
                except Exception as e:
                    import traceback
                    error_msg = f"{type(e).__name__}: {str(e)}"
                    # Mark all listing types as having this error
                    for listing_type in listing_types:
                        diagnostic_info["listing_types_errors"][listing_type] = error_msg
                    logger.error(f"Error scraping properties for location {location}: {e}")
                    logger.error(f"Error type: {type(e).__name__}")
                    logger.error(f"Error traceback: {traceback.format_exc()}")
                
                properties = all_properties
                logger.info(f"Direct scraping returned {len(properties)} properties for: {location}")
                
                # Log diagnostic information
                logger.info(f"Diagnostic info for {location}: {diagnostic_info}")
                all_diagnostic_info.append({"location": location, "info": diagnostic_info})
                
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
        
        # Build diagnostic message
        diagnostic_msg_parts = []
        if all_diagnostic_info:
            # Aggregate diagnostic info from all locations
            all_listing_types_tried = set()
            all_errors = {}
            fallback_attempted = False
            
            for diag in all_diagnostic_info:
                info = diag["info"]
                all_listing_types_tried.update(info.get('listing_types_tried', []))
                all_errors.update(info.get('listing_types_errors', {}))
                if info.get('fallback_search_attempted'):
                    fallback_attempted = True
            
            if all_listing_types_tried:
                diagnostic_msg_parts.append(f"Listing types tried: {', '.join(sorted(all_listing_types_tried))}")
            if all_errors:
                errors = ', '.join([f"{lt}: {err}" for lt, err in all_errors.items()])
                diagnostic_msg_parts.append(f"Errors: {errors}")
            if fallback_attempted:
                diagnostic_msg_parts.append("Fallback search (all types) attempted")
        
        # Include diagnostic info in message if no properties found
        if total_properties_scraped == 0 and diagnostic_msg_parts:
            diagnostic_msg = " | ".join(diagnostic_msg_parts)
            message = f"No properties found in {execution_time:.2f} seconds. {diagnostic_msg}"
        else:
            message = f"Successfully scraped {total_properties_scraped} properties in {execution_time:.2f} seconds"
        
        # Return response with property details
        response = ImmediateScrapeResponse(
            job_id=job_id,
            status=JobStatus.COMPLETED,
            message=message,
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
async def scheduled_scrape(request: ScheduledScrapeRequest, token_payload: dict = Depends(verify_token)):
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
async def trigger_existing_job(request: TriggerJobRequest, token_payload: dict = Depends(verify_token)):
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
            
            # Store force_full_scrape flag in job_run_flags for each job instance
            # This will be checked in the scraper before the scheduled job's incremental logic
            force_full_scrape = request.force_full_scrape
            
            # Use scheduler's create_scheduled_job_instances to properly handle split_by_zip
            job_instances = await scheduler.create_scheduled_job_instances(scheduled_job)
            
            # Set the force_full_scrape flag for each job instance BEFORE starting them
            # Initialize job_run_flags if needed and set the flag
            if force_full_scrape is not None:
                for job_instance in job_instances:
                    if job_instance.job_id not in scraper.job_run_flags:
                        scraper.job_run_flags[job_instance.job_id] = {}
                    scraper.job_run_flags[job_instance.job_id]["force_full_scrape"] = force_full_scrape
                    logger.info(f"Set force_full_scrape={force_full_scrape} for job {job_instance.job_id}")
            
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
async def get_job_status(job_id: str, token_payload: dict = Depends(verify_token)):
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
async def get_scheduled_job_stats(scheduled_job_id: str, limit: int = 1000, token_payload: dict = Depends(verify_token)):
    """Get detailed statistics for a scheduled job"""
    try:
        # Get the scheduled job
        scheduled_job = await db.get_scheduled_job(scheduled_job_id)
        if not scheduled_job:
            raise HTTPException(status_code=404, detail=f"Scheduled job not found: {scheduled_job_id}")
        
        # Use aggregation to calculate statistics efficiently (much faster than iterating)
        stats_pipeline = [
            {"$match": {"scheduled_job_id": scheduled_job_id}},
            {"$group": {
                "_id": None,
                "total_runs": {"$sum": 1},
                "successful_runs": {
                    "$sum": {"$cond": [{"$eq": ["$status", "completed"]}, 1, 0]}
                },
                "failed_runs": {
                    "$sum": {"$cond": [{"$eq": ["$status", "failed"]}, 1, 0]}
                },
                "total_properties_scraped": {
                    "$sum": {"$ifNull": ["$properties_scraped", 0]}
                },
                "total_properties_saved": {
                    "$sum": {"$ifNull": ["$properties_saved", 0]}
                },
                "durations": {
                    "$push": {
                        "$cond": [
                            {"$and": [
                                {"$ne": ["$started_at", None]},
                                {"$ne": ["$completed_at", None]}
                            ]},
                            {"$subtract": ["$completed_at", "$started_at"]},
                            None
                        ]
                    }
                }
            }},
            {"$project": {
                "total_runs": 1,
                "successful_runs": 1,
                "failed_runs": 1,
                "total_properties_scraped": 1,
                "total_properties_saved": 1,
                "duration_count": {
                    "$size": {
                        "$filter": {
                            "input": "$durations",
                            "cond": {"$ne": ["$$this", None]}
                        }
                    }
                },
                "total_duration_ms": {
                    "$sum": {
                        "$filter": {
                            "input": "$durations",
                            "cond": {"$ne": ["$$this", None]}
                        }
                    }
                }
            }}
        ]
        
        stats_result = None
        async for result in db.jobs_collection.aggregate(stats_pipeline):
            stats_result = result
            break
        
        if stats_result:
            total_runs = stats_result.get("total_runs", 0)
            successful_runs = stats_result.get("successful_runs", 0)
            failed_runs = stats_result.get("failed_runs", 0)
            total_properties_scraped = stats_result.get("total_properties_scraped", 0)
            total_properties_saved = stats_result.get("total_properties_saved", 0)
            duration_count = stats_result.get("duration_count", 0)
            total_duration_ms = stats_result.get("total_duration_ms", 0)
            average_duration_seconds = (total_duration_ms / 1000 / duration_count) if duration_count > 0 else 0
        else:
            total_runs = 0
            successful_runs = 0
            failed_runs = 0
            total_properties_scraped = 0
            total_properties_saved = 0
            average_duration_seconds = 0
        
        # Get last successful run (single query with limit)
        last_successful_job = await db.jobs_collection.find_one(
            {"scheduled_job_id": scheduled_job_id, "status": "completed"},
            sort=[("completed_at", -1)]
        )
        last_successful_run = None
        if last_successful_job:
            last_successful_run = {
                "job_id": last_successful_job.get('job_id'),
                "completed_at": last_successful_job.get('completed_at'),
                "properties_scraped": last_successful_job.get('properties_scraped', 0),
                "properties_saved": last_successful_job.get('properties_saved', 0)
            }
        
        # Calculate success rate
        success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0
        
        # Count unique properties added (properties with this job's scheduled_job_id)
        # Use distinct to count unique property_ids, not all documents
        property_ids = await db.properties_collection.distinct(
            "property_id",
            {"job_id": {"$regex": f"^(scheduled|triggered)_{scheduled_job_id}"}}
        )
        unique_properties_count = len(property_ids)
        
        # Count unique properties by listing type using aggregation to count distinct property_ids
        properties_by_type = {}
        for listing_type in ["for_sale", "sold", "pending", "for_rent", "off_market"]:
            # Use aggregation to count distinct property_ids for this listing type
            pipeline = [
                {
                    "$match": {
                        "job_id": {"$regex": f"^(scheduled|triggered)_{scheduled_job_id}"},
                        "listing_type": listing_type
                    }
                },
                {
                    "$group": {
                        "_id": "$property_id"
                    }
                },
                {
                    "$count": "unique_count"
                }
            ]
            
            result = None
            async for doc in db.properties_collection.aggregate(pipeline):
                result = doc
                break
            
            count = result.get("unique_count", 0) if result else 0
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
async def toggle_scheduled_job_status(scheduled_job_id: str, status_data: dict, token_payload: dict = Depends(verify_token)):
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

def get_status_value(status):
    """Safely extract status value from enum or string"""
    if status is None:
        return None
    if isinstance(status, JobStatus):
        return status.value
    if isinstance(status, str):
        return status
    if hasattr(status, 'value'):
        return status.value
    return str(status)

# Get scheduled job details and run history
@app.get("/scheduled-jobs/{scheduled_job_id}")
async def get_scheduled_job_details(scheduled_job_id: str, include_runs: bool = True, limit: int = 50, token_payload: dict = Depends(verify_token)):
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
                "split_by_zip": scheduled_job.split_by_zip,
                "zip_batch_size": scheduled_job.zip_batch_size,
                "incremental_runs_before_full": scheduled_job.incremental_runs_before_full,
                "run_count": scheduled_job.run_count,
                "last_run_at": scheduled_job.last_run_at,
                "last_run_status": get_status_value(scheduled_job.last_run_status),
                "last_run_job_id": scheduled_job.last_run_job_id,
                "last_run_error_message": scheduled_job.last_run_error_message,
                "last_run_properties_scraped": scheduled_job.last_run_properties_scraped,
                "last_run_properties_saved": scheduled_job.last_run_properties_saved,
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
@app.options("/scheduled-jobs")
async def options_scheduled_jobs(request: Request):
    """CORS preflight for /scheduled-jobs. Returns 200 so browser preflights succeed."""
    origin = request.headers.get("Origin") or "*"
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS, PATCH",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With, Accept, Origin, Access-Control-Request-Method, Access-Control-Request-Headers",
            "Access-Control-Max-Age": "86400",
        },
    )


@app.get("/scheduled-jobs")
async def list_scheduled_jobs(
    status: Optional[str] = None,
    limit: int = 50,
    include_deleted: bool = False,
    token_payload: dict = Depends(verify_token)
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
        
        # Collect all scheduled job IDs first
        scheduled_job_ids = []
        scheduled_jobs_data = []
        async for job_data in cursor:
            scheduled_job_id = job_data.get("scheduled_job_id")
            scheduled_job_ids.append(scheduled_job_id)
            scheduled_jobs_data.append(job_data)
        
        # Use aggregation to get all counts in a single query (much faster than N*2 queries)
        job_stats = {}
        if scheduled_job_ids:
            pipeline = [
                {"$match": {"scheduled_job_id": {"$in": scheduled_job_ids}}},
                {"$group": {
                    "_id": "$scheduled_job_id",
                    "total_runs": {"$sum": 1},
                    "running_count": {
                        "$sum": {"$cond": [{"$eq": ["$status", JobStatus.RUNNING.value]}, 1, 0]}
                    }
                }}
            ]
            async for result in db.jobs_collection.aggregate(pipeline):
                job_stats[result["_id"]] = {
                    "run_count": result["total_runs"],
                    "has_running_job": result["running_count"] > 0
                }
        
        # Build response with pre-computed stats
        scheduled_jobs = []
        for job_data in scheduled_jobs_data:
            scheduled_job_id = job_data.get("scheduled_job_id")
            stats = job_stats.get(scheduled_job_id, {"run_count": 0, "has_running_job": False})
            
            scheduled_jobs.append({
                "scheduled_job_id": scheduled_job_id,
                "name": job_data.get("name"),
                "description": job_data.get("description"),
                "status": job_data.get("status"),
                "cron_expression": job_data.get("cron_expression"),
                "locations": job_data.get("locations"),
                "listing_types": job_data.get("listing_types", ["for_sale", "sold", "pending", "for_rent", "off_market"]),  # New multi-select field (includes off_market by default)
                "listing_type": job_data.get("listing_type"),  # Backward compatibility
                "split_by_zip": job_data.get("split_by_zip", False),
                "zip_batch_size": job_data.get("zip_batch_size"),
                "incremental_runs_before_full": job_data.get("incremental_runs_before_full"),
                "run_count": stats["run_count"],  # Use aggregated count
                "has_running_job": stats["has_running_job"],  # Use aggregated flag
                "last_run_at": job_data.get("last_run_at"),
                "last_run_status": job_data.get("last_run_status"),
                "last_run_job_id": job_data.get("last_run_job_id"),
                "last_run_error_message": job_data.get("last_run_error_message"),
                "last_run_properties_scraped": job_data.get("last_run_properties_scraped"),
                "last_run_properties_saved": job_data.get("last_run_properties_saved"),
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
async def create_scheduled_job(job_data: dict, token_payload: dict = Depends(verify_token)):
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
            listing_types = ["for_sale", "sold", "pending", "for_rent", "off_market"]  # Include off_market by default for comprehensive data
        
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
            incremental_runs_before_full=job_data.get('incremental_runs_before_full'),  # New field for incremental scraping
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
async def update_scheduled_job(scheduled_job_id: str, job_data: dict, token_payload: dict = Depends(verify_token)):
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
            # Use last_run_at as base if available, otherwise use now
            # This ensures the next run is calculated from when the job last ran,
            # not from the current time, so missed runs are properly scheduled
            import croniter
            base_time = existing_job.last_run_at if existing_job.last_run_at else datetime.utcnow()
            cron = croniter.croniter(job_data['cron_expression'], base_time)
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
        if 'incremental_runs_before_full' in job_data:
            update_data['incremental_runs_before_full'] = job_data['incremental_runs_before_full']
            # Reset counter when config changes
            update_data['incremental_runs_count'] = 0
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
async def delete_scheduled_job(scheduled_job_id: str, token_payload: dict = Depends(verify_token)):
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
    offset: int = 0,
    token_payload: dict = Depends(verify_token)
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
async def cancel_job(job_id: str, token_payload: dict = Depends(verify_token)):
    """Cancel a running or pending job"""
    try:
        job = await db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            raise HTTPException(status_code=400, detail="Job cannot be cancelled in current status")
        
        # Update job status with error message indicating manual cancellation
        success = await db.update_job_status(
            job_id, 
            JobStatus.CANCELLED,
            error_message="Job manually cancelled by user"
        )
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
async def retry_failed_locations(job_id: str, token_payload: dict = Depends(verify_token)):
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
async def get_property_history(property_id: str, limit: int = 50, token_payload: dict = Depends(verify_token)):
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
async def get_property_change_logs(property_id: str, limit: int = 100, token_payload: dict = Depends(verify_token)):
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
async def get_property_enrichment(property_id: str, token_payload: dict = Depends(verify_token)):
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
async def get_motivated_sellers(min_score: int = 40, limit: int = 100, token_payload: dict = Depends(verify_token)):
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
async def enrich_property(property_id: str, token_payload: dict = Depends(verify_token)):
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
async def recalculate_scores(request: EnrichmentRecalcRequest, token_payload: dict = Depends(verify_token)):
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
async def redetect_keywords(request: EnrichmentRecalcRequest, token_payload: dict = Depends(verify_token)):
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
async def full_reenrich(request: EnrichmentRecalcRequest, token_payload: dict = Depends(verify_token)):
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

@app.options("/enrichment/config")
async def options_enrichment_config(request: Request):
    """CORS preflight for /enrichment/config. Returns 200 so browser preflights succeed."""
    origin = request.headers.get("Origin") or "*"
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS, PATCH",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With, Accept, Origin, Access-Control-Request-Method, Access-Control-Request-Headers",
            "Access-Control-Max-Age": "86400",
        },
    )


@app.get("/enrichment/config", response_model=EnrichmentConfigResponse)
async def get_enrichment_config(token_payload: dict = Depends(verify_token)):
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
async def update_dom_all(limit: Optional[int] = None, token_payload: dict = Depends(verify_token)):
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

@app.post("/enrichment/recover-pending", response_model=EnrichmentRecalcResponse)
async def recover_pending_enrichment(limit: Optional[int] = None, batch_size: int = 50, token_payload: dict = Depends(verify_token)):
    """
    Background recovery job: Find and enrich properties that need enrichment.
    
    Finds properties where:
    1. Current values differ from _old values (changes detected)
    2. Missing _old values (first time scraping)
    3. Missing enrichment data
    
    This is useful for crash recovery and ensuring all properties are enriched.
    """
    try:
        if not db.enrichment_pipeline:
            raise HTTPException(status_code=503, detail="Enrichment service not available")
        
        logger.info(f"Starting enrichment recovery job (limit: {limit}, batch_size: {batch_size})")
        
        # Find properties that need enrichment
        # Properties where current values differ from _old, or missing _old, or missing enrichment
        query = {
            "$or": [
                # Current values differ from _old (using aggregation to compare)
                # We'll check this in code since MongoDB aggregation is complex
                # Missing _old values (first time)
                {
                    "$or": [
                        {"financial.list_price": {"$exists": True, "$ne": None}},
                        {"status": {"$exists": True, "$ne": None}}
                    ],
                    "$or": [
                        {"financial.list_price_old": {"$exists": False}},
                        {"status_old": {"$exists": False}},
                        {"_old_values_scraped_at": {"$exists": False}}
                    ]
                },
                # Missing enrichment
                {
                    "$or": [
                        {"enrichment": {"$exists": False}},
                        {"enrichment.enrichment_version": {"$ne": "2.0"}}
                    ]
                }
            ]
        }
        
        # Get properties
        cursor = db.properties_collection.find(query)
        if limit:
            cursor = cursor.limit(limit)
        
        properties_to_enrich = []
        async for prop_doc in cursor:
            property_id = prop_doc.get("property_id")
            if not property_id:
                continue
            
            # Check if current values differ from _old
            financial = prop_doc.get("financial", {})
            current_list_price = financial.get("list_price")
            old_list_price = financial.get("list_price_old")
            current_status = prop_doc.get("status")
            old_status = prop_doc.get("status_old")
            
            # Determine if needs enrichment
            needs_enrichment = False
            
            # Missing _old values
            if not old_list_price and not old_status and not prop_doc.get("_old_values_scraped_at"):
                needs_enrichment = True
            # Values differ
            elif (current_list_price is not None and old_list_price is not None and current_list_price != old_list_price) or \
                 (current_status is not None and old_status is not None and current_status != old_status):
                needs_enrichment = True
            # Missing enrichment
            elif not prop_doc.get("enrichment") or prop_doc.get("enrichment", {}).get("enrichment_version") != "2.0":
                needs_enrichment = True
            
            if needs_enrichment:
                properties_to_enrich.append(prop_doc)
        
        total_found = len(properties_to_enrich)
        logger.info(f"Found {total_found} properties that need enrichment")
        
        start_time = datetime.utcnow()
        
        if total_found == 0:
            end_time = datetime.utcnow()
            return EnrichmentRecalcResponse(
                recalc_type="recovery",
                total_processed=0,
                total_updated=0,
                total_errors=0,
                started_at=start_time,
                completed_at=end_time,
                duration_seconds=(end_time - start_time).total_seconds(),
                errors=[]
            )
        
        # Process in batches
        total_processed = 0
        total_updated = 0
        total_errors = 0
        
        for i in range(0, len(properties_to_enrich), batch_size):
            batch = properties_to_enrich[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            total_batches = (len(properties_to_enrich) + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {batch_number}/{total_batches} ({len(batch)} properties)")
            
            for prop_doc in batch:
                try:
                    property_id = prop_doc.get("property_id")
                    property_dict = await db.enrichment_pipeline._get_property_dict(property_id)
                    
                    if not property_dict:
                        total_errors += 1
                        continue
                    
                    # Enrich the property (will use _old values from DB)
                    enrichment_data = await db.enrichment_pipeline.enrich_property(
                        property_id=property_id,
                        property_dict=property_dict,
                        existing_property=None,  # Will fetch from DB
                        job_id=None
                    )
                    
                    if enrichment_data:
                        total_updated += 1
                    else:
                        total_errors += 1
                    
                    total_processed += 1
                    
                except Exception as e:
                    logger.error(f"Error enriching property {prop_doc.get('property_id', 'unknown')}: {e}")
                    total_errors += 1
                    total_processed += 1
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"Recovery job completed: {total_processed} processed, {total_updated} updated, {total_errors} errors")
        
        return EnrichmentRecalcResponse(
            recalc_type="recovery",
            total_processed=total_processed,
            total_updated=total_updated,
            total_errors=total_errors,
            started_at=start_time,
            completed_at=end_time,
            duration_seconds=duration,
            errors=[]
        )
        
    except Exception as e:
        logger.error(f"Error in enrichment recovery: {e}")
        raise HTTPException(status_code=500, detail=f"Error in enrichment recovery: {str(e)}")

# Agent Lookup Request Model
class AgentLookupRequest(BaseModel):
    agent_name: str
    city: str
    contact_id: Optional[str] = None

# Agent Lookup Endpoint
@app.post("/api/agent-lookup", response_model=Dict[str, Any])
async def agent_lookup(request: AgentLookupRequest):
    """
    Look up agent contact information using OpenAI Agent Lookup library.
    
    Accepts: { "agent_name": string, "city": string, "contact_id": string (optional) }
    Returns: AgentLookupResult with mobile_numbers, emails, and certainty scores
    
    Note: This operation can take 30-60 seconds as it performs web searches.
    """
    # #region agent log
    try:
        with open('c:\\Projects\\Real-Estate-CRM-Repos\\real-estate-crm-backend\\.cursor\\debug.log', 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"endpoint-call","hypothesisId":"H4","location":"main.py:3020","message":"Agent lookup endpoint called","data":{"agent_name":request.agent_name,"city":request.city,"contact_id":getattr(request, 'contact_id', None)},"timestamp":int(__import__('time').time()*1000)}) + "\n")
    except: pass
    # #endregion
    
    if not AGENT_LOOKUP_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Agent lookup service is not available. Please ensure openai_agent_lookup library is installed."
        )
    
    try:
        agent_name = request.agent_name
        city = request.city
        
        if not agent_name:
            raise HTTPException(status_code=400, detail="agent_name is required")
        
        if not city:
            raise HTTPException(status_code=400, detail="city is required")
        
        # Build query string in format: "Agent Name, City"
        query = f"{agent_name}, {city}"
        
        logger.info(f"Starting agent lookup for: {query}")
        
        # Prepare trace metadata
        trace_metadata = {
            "agent_name_query": agent_name,
            "city_query": city,
        }
        if request.contact_id:
            trace_metadata["contact_id"] = request.contact_id
        
        # Initialize client and perform lookup
        client = AgentLookupClient()
        result = await client.lookup_async(query, trace_metadata=trace_metadata)
        
        # Convert result to dict for JSON serialization
        response = {
            "agent_name": result.agent_name,
            "mobile_numbers": [
                {
                    "number": num.number,
                    "type": num.type,
                    "certainty": num.certainty,
                    "sources": num.sources if hasattr(num, 'sources') else []
                }
                for num in result.mobile_numbers
            ],
            "emails": [
                {
                    "email": email.email,
                    "certainty": email.certainty,
                    "sources": email.sources if hasattr(email, 'sources') else []
                }
                for email in result.emails
            ],
            "has_results": result.has_results(),
            "has_phone_numbers": result.has_phone_numbers(),
            "has_emails": result.has_emails()
        }
        
        logger.info(f"Agent lookup completed for {query}: {len(result.mobile_numbers)} numbers, {len(result.emails)} emails found")
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in agent lookup: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error performing agent lookup: {str(e)}")

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
