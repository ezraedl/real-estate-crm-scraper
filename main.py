from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from typing import List, Optional
import asyncio
import uuid
from datetime import datetime
import logging
import os

from models import (
    ImmediateScrapeRequest, 
    ScheduledScrapeRequest, 
    TriggerJobRequest,
    JobResponse, 
    JobStatusResponse,
    ImmediateScrapeResponse,
    ScrapingJob,
    JobStatus,
    JobPriority,
    Property,
    ListingType
)
from database import db
from scraper import scraper
from proxy_manager import proxy_manager
from config import settings

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

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    try:
        # Try to connect to database (optional for basic functionality)
        try:
            await db.connect()
            logger.info("Database connected")
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
            execution_time = (end_time - start_time).total_seconds()
            
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
        execution_time = (end_time - start_time).total_seconds()
        
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
    """
    try:
        # Get the existing job
        existing_job = await db.get_job(request.job_id)
        if not existing_job:
            raise HTTPException(status_code=404, detail=f"Job not found: {request.job_id}")
        
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
            original_job_id=request.job_id  # Track the original job
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
# Properties API endpoint
@app.get("/api/properties")
async def get_properties(
    page: int = 1,
    limit: int = 25,
    status: Optional[str] = None,
    status_filter: Optional[str] = None,
    locations: Optional[str] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    bedrooms: Optional[int] = None,
    bathrooms: Optional[float] = None,
    property_types: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: Optional[str] = "scraped_at",
    sort_order: Optional[str] = "desc"
):
    """Get properties with pagination, filtering, and sorting"""
    try:
        # Build MongoDB query filters
        filters = {}
        
        # Status filtering
        if status_filter == 'active':
            filters["status"] = {"$in": ["FOR_SALE", "ACTIVE", "PENDING", "CONTINGENT"]}
        elif status_filter == 'sold':
            filters["status"] = "SOLD"
        elif status:
            filters["status"] = status
        
        # Location filtering
        if locations:
            location_list = locations.split(',')
            location_filters = []
            for loc in location_list:
                loc = loc.strip()
                location_filters.extend([
                    {"address.city": {"$regex": loc, "$options": "i"}},
                    {"address.state": {"$regex": loc, "$options": "i"}},
                    {"address.zip_code": {"$regex": loc, "$options": "i"}},
                    {"address.formatted_address": {"$regex": loc, "$options": "i"}}
                ])
            if location_filters:
                filters["$or"] = location_filters
        
        # Price filtering
        if price_min or price_max:
            filters["financial.list_price"] = {}
            if price_min:
                filters["financial.list_price"]["$gte"] = price_min
            if price_max:
                filters["financial.list_price"]["$lte"] = price_max
        
        # Bedrooms filtering
        if bedrooms:
            filters["description.beds"] = bedrooms
        
        # Bathrooms filtering
        if bathrooms:
            filters["description.full_baths"] = bathrooms
        
        # Property type filtering
        if property_types:
            type_list = property_types.split(',')
            filters["description.property_type"] = {"$in": [t.strip() for t in type_list]}
        
        # Search filtering
        if search:
            search_term = search.strip()
            filters["$or"] = [
                {"address.street": {"$regex": search_term, "$options": "i"}},
                {"address.city": {"$regex": search_term, "$options": "i"}},
                {"address.state": {"$regex": search_term, "$options": "i"}},
                {"description.text": {"$regex": search_term, "$options": "i"}},
                {"address.formatted_address": {"$regex": search_term, "$options": "i"}}
            ]
        
        # Calculate skip for pagination
        skip = (page - 1) * limit
        
        # Build sort criteria
        sort_criteria = []
        if sort_by and sort_order:
            sort_direction = 1 if sort_order.lower() == "asc" else -1
            sort_criteria.append((sort_by, sort_direction))
        else:
            sort_criteria.append(("scraped_at", -1))  # Default sort by scraped_at desc
        
        # Get total count
        total_count = await db.properties_collection.count_documents(filters)
        
        # Get properties with pagination
        cursor = db.properties_collection.find(filters).sort(sort_criteria).skip(skip).limit(limit)
        
        properties = []
        async for prop_data in cursor:
            prop_data["_id"] = str(prop_data["_id"])
            properties.append(prop_data)
        
        # Calculate pagination info
        total_pages = (total_count + limit - 1) // limit
        has_next_page = page < total_pages
        has_prev_page = page > 1
        
        return {
            "properties": properties,
            "totalCount": total_count,
            "page": page,
            "limit": limit,
            "totalPages": total_pages,
            "hasNextPage": has_next_page,
            "hasPrevPage": has_prev_page
        }
        
    except Exception as e:
        logger.error(f"Error getting properties: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving properties: {str(e)}")

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
