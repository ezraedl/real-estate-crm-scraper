import asyncio
import random
import re
import time
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import httpx
import pandas as pd
import numpy as np
from homeharvest import scrape_property
from models import ScrapingJob, Property, PropertyAddress, PropertyDescription, PropertyFinancial, PropertyDates, PropertyLocation, PropertyAgent, PropertyBroker, PropertyBuilder, PropertyOffice, JobStatus, JobPriority
from database import db
from proxy_manager import proxy_manager
from config import settings

logger = logging.getLogger(__name__)

class MLSScraper:
    def __init__(self):
        self.is_running = False
        self.current_jobs = {}
        self.executor = ThreadPoolExecutor(max_workers=3)  # Thread pool for blocking operations
        # Use asyncio.Semaphore instead of ThreadPoolExecutor for enrichment to avoid event loop conflicts
        self.enrichment_semaphore = asyncio.Semaphore(settings.ENRICHMENT_WORKERS)  # Limit concurrent enrichment tasks
    
    async def start(self):
        """Start the scraper service"""
        self.is_running = True
        logger.info("MLS Scraper started")
        
        while self.is_running:
            try:
                # Get pending jobs
                pending_jobs = await db.get_pending_jobs(limit=settings.MAX_CONCURRENT_JOBS)
                
                # Process jobs
                for job in pending_jobs:
                    if job.job_id not in self.current_jobs:
                        asyncio.create_task(self.process_job(job))
                
                # Wait before checking for new jobs
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Error in scraper main loop: {e}")
                await asyncio.sleep(10)
    
    async def stop(self):
        """Stop the scraper service"""
        self.is_running = False
        logger.info("MLS Scraper stopped")
    
    async def check_cancellation_loop(self, job_id: str, cancel_flag: dict):
        """Background task that checks for job cancellation every 2 seconds"""
        try:
            while not cancel_flag.get("cancelled", False):
                await asyncio.sleep(2)  # Check every 2 seconds
                current_job_status = await db.get_job(job_id)
                if current_job_status and current_job_status.status == JobStatus.CANCELLED:
                    logger.debug(f"[MONITOR] Cancellation detected for job {job_id}")
                    cancel_flag["cancelled"] = True
                    break
        except Exception as e:
            logger.error(f"[MONITOR] Error in cancellation monitor: {e}")
    
    async def check_location_timeout_loop(self, job_id: str, cancel_flag: dict, location_last_update: dict):
        """Background task that checks for location timeouts every 30 seconds"""
        try:
            from config import settings
            timeout_seconds = settings.LOCATION_TIMEOUT_MINUTES * 60
            
            while not cancel_flag.get("cancelled", False):
                await asyncio.sleep(30)  # Check every 30 seconds
                
                if cancel_flag.get("cancelled", False):
                    break
                
                now = datetime.utcnow()
                timed_out_locations = []
                
                # Check each location for timeout
                for location, last_update_time in location_last_update.items():
                    if last_update_time:
                        time_since_update = (now - last_update_time).total_seconds()
                        if time_since_update > timeout_seconds:
                            timed_out_locations.append(location)
                            logger.warning(f"[TIMEOUT] Location {location} has not updated properties in {time_since_update/60:.1f} minutes (timeout: {settings.LOCATION_TIMEOUT_MINUTES} minutes)")
                    else:
                        # If location is in the dict but has no timestamp, it might be stuck
                        # Check if it's been more than timeout since job started
                        logger.debug(f"[TIMEOUT-CHECK] Location {location} has no update timestamp")
                
                # Mark timed out locations (will be handled in main loop)
                if timed_out_locations:
                    cancel_flag["timed_out_locations"] = timed_out_locations
                    
        except Exception as e:
            logger.error(f"[MONITOR] Error in location timeout monitor: {e}")

    async def process_job(self, job: ScrapingJob):
        """Process a single scraping job"""
        self.current_jobs[job.job_id] = job
        
        # Cancellation flag shared between main task and monitor
        cancel_flag = {"cancelled": False}
        
        # Track last property update time per location for timeout detection
        location_last_update = {}  # {location: datetime}
        
        # Start background cancellation monitor
        monitor_task = asyncio.create_task(self.check_cancellation_loop(job.job_id, cancel_flag))
        
        # Start background location timeout monitor
        location_timeout_monitor = asyncio.create_task(
            self.check_location_timeout_loop(job.job_id, cancel_flag, location_last_update)
        )
        
        try:
            # Initialize progress logs with new table format
            progress_logs = {
                "locations": [],
                "summary": {
                    "total_locations": len(job.locations),
                    "completed_locations": 0,
                    "in_progress_locations": 0,
                    "failed_locations": 0
                },
                "job_started": {
                    "timestamp": datetime.utcnow().isoformat(),
                    "event": "job_started",
                    "message": f"Job started - Processing {len(job.locations)} location(s)",
                    "listing_types": job.listing_types or (job.listing_type and [job.listing_type]) or ["for_sale", "sold", "for_rent", "pending"]
                }
            }
            
            # Update job status to running with initial log
            await db.update_job_status(job.job_id, JobStatus.RUNNING, progress_logs=progress_logs)
            
            logger.info(f"Processing job {job.job_id} for {len(job.locations)} locations")
            
            # Capture job start time for off-market detection
            job_start_time = datetime.utcnow()
            
            total_properties = 0
            saved_properties = 0
            total_inserted = 0
            total_updated = 0
            total_skipped = 0
            failed_locations = []
            successful_locations = 0
            last_progress_log_time = datetime.utcnow()
            
            # Process each location
            for i, location in enumerate(job.locations):
                # Check if job has been cancelled (via background monitor)
                if cancel_flag.get("cancelled", False):
                    logger.info(f"Job {job.job_id} was cancelled, stopping execution")
                    return  # Exit immediately
                
                # Check if this location has timed out
                timed_out_locations = cancel_flag.get("timed_out_locations", [])
                if location in timed_out_locations:
                    from config import settings
                    location_failed = True
                    location_error = f"Location timeout: no properties added/updated in {settings.LOCATION_TIMEOUT_MINUTES} minutes"
                    logger.warning(f"[TIMEOUT] Location {location} timed out, marking as failed and moving to next location")
                    # Remove from timed out list to avoid reprocessing
                    timed_out_locations.remove(location)
                else:
                    # Initialize last update time for this location
                    location_last_update[location] = datetime.utcnow()
                    
                    # Location entry will be created in scrape_location, no need to append here
                    # Update current_location field so GUI can show the active location
                    await db.update_job_status(
                        job.job_id, 
                        JobStatus.RUNNING, 
                        progress_logs=progress_logs,
                        current_location=location,
                        current_location_index=i + 1
                    )
                    
                    location_failed = False
                    location_error = None
                    
                    try:
                        logger.info(f"Scraping location {i+1}/{len(job.locations)}: {location}")
                        
                        # Get proxy configuration
                        proxy_config = await self.get_proxy_config(job)
                        
                        # Scrape properties for this location (saves after each listing type)
                        running_totals = {
                            "total_properties": total_properties,
                            "saved_properties": saved_properties,
                            "total_inserted": total_inserted,
                            "total_updated": total_updated,
                            "total_skipped": total_skipped
                        }
                        
                        location_summary = await self.scrape_location(
                            location=location,
                            job=job,
                            proxy_config=proxy_config,
                            cancel_flag=cancel_flag,
                            progress_logs=progress_logs,
                            running_totals=running_totals,
                            location_index=i + 1,
                            total_locations=len(job.locations),
                            job_start_time=job_start_time,
                            location_last_update=location_last_update
                        )
                        
                        # Check if location scraping failed (returns None or has error status)
                        if location_summary is None or location_summary.get("status") == "failed":
                            location_failed = True
                            location_error = location_summary.get("error", "Unknown error") if location_summary else "Scraping returned None"
                            logger.warning(f"Location {location} failed: {location_error}")
                        else:
                            # Update totals from running_totals (modified by scrape_location)
                            total_properties = running_totals["total_properties"]
                            saved_properties = running_totals["saved_properties"]
                            total_inserted = running_totals["total_inserted"]
                            total_updated = running_totals["total_updated"]
                            total_skipped = running_totals["total_skipped"]
                            
                            # Location summary is already in progress_logs (replaced temp entry in scrape_location)
                            # So we don't need to append it again here
                            if location_summary:
                                logger.info(f"Location {location} complete: {location_summary.get('inserted', 0)} inserted, {location_summary.get('updated', 0)} updated, {location_summary.get('skipped', 0)} skipped")
                            
                            successful_locations += 1
                    
                    except Exception as e:
                        location_failed = True
                        location_error = str(e)
                        logger.error(f"Error scraping location {location}: {e}")
                        import traceback
                        error_traceback = traceback.format_exc()
                        logger.debug(f"Traceback: {error_traceback}")
                
                # Handle failed location
                if location_failed:
                    failed_location_entry = {
                        "location": location,
                        "location_index": i + 1,
                        "error": location_error,
                        "failed_at": datetime.utcnow().isoformat(),
                        "retry_count": 0
                    }
                    failed_locations.append(failed_location_entry)
                    
                    # Failure is already tracked in location entry status, no need for separate event log
                    logger.debug(f"[SKIP] Location {location} failed, continuing with next location...")
                
                # Update job progress with detailed breakdown and logs (including failed locations)
                # Include current_location so GUI can show which location is being processed
                await db.update_job_status(
                    job.job_id,
                    JobStatus.RUNNING,
                    completed_locations=successful_locations,
                    properties_scraped=total_properties,
                    properties_saved=saved_properties,
                    properties_inserted=total_inserted,
                    properties_updated=total_updated,
                    properties_skipped=total_skipped,
                    failed_locations=failed_locations,
                    progress_logs=progress_logs,
                    current_location=location,  # Keep current_location updated
                    current_location_index=i + 1
                )
                
                # Periodic progress logging (every 10 locations or 5 minutes)
                now = datetime.utcnow()
                time_since_last_log = (now - last_progress_log_time).total_seconds()
                should_log_progress = (
                    (i + 1) % 10 == 0 or  # Every 10 locations
                    time_since_last_log >= 300  # Every 5 minutes
                )
                
                if should_log_progress:
                    logger.info(
                        f"Job {job.job_id} progress: {i+1}/{len(job.locations)} locations, "
                        f"{successful_locations} successful, {len(failed_locations)} failed, "
                        f"{total_properties} properties scraped, {saved_properties} saved "
                        f"({total_inserted} inserted, {total_updated} updated)"
                    )
                    last_progress_log_time = now
                
                # Random delay between locations (even failed ones to avoid hammering)
                if not location_failed:
                    delay = job.request_delay + random.uniform(0, 1)
                    await asyncio.sleep(delay)
                else:
                    # Shorter delay after failures
                    await asyncio.sleep(0.5)
            
            # Retry failed locations once if there are any
            if failed_locations:
                logger.info(f"[RETRY] Retrying {len(failed_locations)} failed location(s)...")
                retry_failed_locations = []
                
                for failed_location_entry in failed_locations:
                    location = failed_location_entry["location"]
                    failed_location_entry["retry_count"] = failed_location_entry.get("retry_count", 0) + 1
                    
                    # Reset timeout tracking for retry
                    location_last_update[location] = datetime.utcnow()
                    
                    # Retry will update location entry in scrape_location, no need for separate event log
                    await db.update_job_status(job.job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                    
                    try:
                        logger.info(f"[RETRY] Scraping location: {location}")
                        location_summary = await self.scrape_location(
                            location=location,
                            job=job,
                            proxy_config=proxy_config,
                            cancel_flag=cancel_flag,
                            progress_logs=progress_logs,
                            running_totals=running_totals,
                            location_index=len(job.locations) + len(retry_failed_locations) + 1,
                            total_locations=len(job.locations) + len(failed_locations),
                            job_start_time=job_start_time,
                            location_last_update=location_last_update
                        )
                        
                        if location_summary and location_summary.get("status") != "failed":
                            # Retry succeeded
                            total_properties = running_totals["total_properties"]
                            saved_properties = running_totals["saved_properties"]
                            total_inserted = running_totals["total_inserted"]
                            total_updated = running_totals["total_updated"]
                            total_skipped = running_totals["total_skipped"]
                            successful_locations += 1
                            logger.info(f"[RETRY] Location {location} succeeded on retry")
                        else:
                            # Retry also failed
                            retry_failed_locations.append(failed_location_entry)
                            logger.warning(f"[RETRY] Location {location} failed again on retry")
                    except Exception as e:
                        retry_failed_locations.append(failed_location_entry)
                        logger.error(f"[RETRY] Error retrying location {location}: {e}")
                
                # Update failed_locations with final retry results
                failed_locations = retry_failed_locations
            
            # Mark job as completed (even if some locations failed)
            final_status = JobStatus.COMPLETED
            completion_message = f"Job completed successfully. {successful_locations}/{len(job.locations)} locations processed."
            
            if failed_locations:
                completion_message += f" {len(failed_locations)} location(s) failed after retry."
                logger.info(f"[SUMMARY] Job {job.job_id} completed with {len(failed_locations)} failed location(s) out of {len(job.locations)} total")
            
            # Add completion info to summary
            progress_logs["job_completed"] = {
                "timestamp": datetime.utcnow().isoformat(),
                "event": "job_completed",
                "message": completion_message,
                "successful_locations": successful_locations,
                "failed_locations": len(failed_locations),
                "total_locations": len(job.locations)
            }
            
            await db.update_job_status(
                job.job_id,
                final_status,
                properties_scraped=total_properties,
                properties_saved=saved_properties,
                failed_locations=failed_locations,
                progress_logs=progress_logs,
                current_location=None,  # Clear current_location when job completes
                current_location_index=None
            )
            
            # Update run history for scheduled jobs (new architecture)
            if job.scheduled_job_id:
                # Calculate next run time
                scheduled_job = await db.get_scheduled_job(job.scheduled_job_id)
                if scheduled_job and scheduled_job.cron_expression:
                    import croniter
                    cron = croniter.croniter(scheduled_job.cron_expression, datetime.utcnow())
                    next_run = cron.get_next(datetime)
                    
                    await db.update_scheduled_job_run_history(
                        job.scheduled_job_id,
                        job.job_id,
                        JobStatus.COMPLETED,
                        next_run_at=next_run
                    )
                    logger.debug(f"Updated run history for scheduled job: {job.scheduled_job_id}")
            
            # Legacy: Update run history for old recurring jobs
            elif job.original_job_id:
                await db.update_recurring_job_run_history(
                    job.original_job_id,
                    job.job_id,
                    JobStatus.COMPLETED
                )
                logger.debug(f"Updated run history for legacy recurring job: {job.original_job_id}")
            
            logger.info(f"Job {job.job_id} completed: {saved_properties} properties saved")
            
        except Exception as e:
            logger.error(f"Error processing job {job.job_id}: {e}")
            await db.update_job_status(
                job.job_id,
                JobStatus.FAILED,
                error_message=str(e)
            )
            
            # Update run history for scheduled jobs (new architecture)
            if job.scheduled_job_id:
                scheduled_job = await db.get_scheduled_job(job.scheduled_job_id)
                if scheduled_job and scheduled_job.cron_expression:
                    import croniter
                    cron = croniter.croniter(scheduled_job.cron_expression, datetime.utcnow())
                    next_run = cron.get_next(datetime)
                    
                    await db.update_scheduled_job_run_history(
                        job.scheduled_job_id,
                        job.job_id,
                        JobStatus.FAILED,
                        next_run_at=next_run
                    )
                    logger.debug(f"Updated run history for scheduled job (failed): {job.scheduled_job_id}")
            
            # Legacy: Update run history for old recurring jobs (failed)
            elif job.original_job_id:
                await db.update_recurring_job_run_history(
                    job.original_job_id,
                    job.job_id,
                    JobStatus.FAILED
                )
                logger.debug(f"Updated run history for legacy recurring job (failed): {job.original_job_id}")
        
        finally:
            # Stop the cancellation and timeout monitors
            cancel_flag["cancelled"] = True  # Signal monitors to stop
            if 'monitor_task' in locals():
                monitor_task.cancel()
                try:
                    await monitor_task
                except asyncio.CancelledError:
                    pass  # Expected
            if 'location_timeout_monitor' in locals():
                location_timeout_monitor.cancel()
                try:
                    await location_timeout_monitor
                except asyncio.CancelledError:
                    pass  # Expected
            
            # Remove from current jobs
            if job.job_id in self.current_jobs:
                del self.current_jobs[job.job_id]
    
    async def scrape_location(self, location: str, job: ScrapingJob, proxy_config: Optional[Dict[str, Any]] = None, cancel_flag: dict = None, progress_logs: dict = None, running_totals: dict = None, location_index: int = 1, total_locations: int = 1, job_start_time: Optional[datetime] = None, location_last_update: dict = None):
        """Scrape properties for a specific location based on job's listing_types. Saves after each type and returns summary."""
        if cancel_flag is None:
            cancel_flag = {"cancelled": False}
        if progress_logs is None:
            progress_logs = {"locations": [], "summary": {"total_locations": total_locations, "completed_locations": 0, "in_progress_locations": 0, "failed_locations": 0}}
        if running_totals is None:
            running_totals = {
                "total_properties": 0,
                "saved_properties": 0,
                "total_inserted": 0,
                "total_updated": 0,
                "total_skipped": 0
            }
        if location_last_update is None:
            location_last_update = {}
        try:
            listing_type_logs = []
            location_total_found = 0
            location_total_inserted = 0
            location_total_updated = 0
            location_total_skipped = 0
            location_total_errors = 0
            temp_idx = None  # Track temp entry index for this location
            found_property_ids = set()  # Track property IDs found in current scrape
            enrichment_queue = []  # Collect properties that need enrichment
            
            # Determine which listing types to scrape
            if job.listing_types and len(job.listing_types) > 0:
                # Use specified listing types
                listing_types_to_scrape = job.listing_types
                logger.debug(f"   [TARGET] Scraping specified types in {location}: {listing_types_to_scrape}")
            elif job.listing_type:
                # Backward compatibility: use single listing_type
                listing_types_to_scrape = [job.listing_type]
                logger.debug(f"   [TARGET] Scraping single type in {location}: {job.listing_type}")
            else:
                # Default: scrape all types for comprehensive data
                listing_types_to_scrape = ["for_sale", "sold", "pending", "for_rent"]
                logger.debug(f"   [TARGET] Scraping ALL property types in {location} (default)")
            
            # Enforce consistent order: for_sale, sold, pending, for_rent
            preferred_order = ["for_sale", "sold", "pending", "for_rent"]
            listing_types_to_scrape = sorted(
                listing_types_to_scrape,
                key=lambda x: preferred_order.index(x) if x in preferred_order else 999
            )
            
            logger.debug(f"   [DEBUG] Listing types to scrape (ordered): {listing_types_to_scrape}")
            logger.debug(f"   [NOTE] 'off_market' not supported by homeharvest library")
            
            # Initialize location entry in progress_logs with new structure
            location_entry = {
                "location": location,
                "location_index": location_index,
                "status": "in_progress",
                "listing_types": {},
                "enrichment": {
                    "status": "pending",
                    "total": 0,
                    "completed": 0,
                    "failed": 0
                },
                "off_market_check": {
                    "status": "pending",
                    "checked": 0,
                    "found": 0,
                    "errors": 0
                },
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Initialize listing_types tracking for each listing type
            for listing_type in listing_types_to_scrape:
                location_entry["listing_types"][listing_type] = {
                    "found": 0,
                    "inserted": 0,
                    "updated": 0,
                    "skipped": 0,
                    "enriched": 0,
                    "off_market": 0
                }
            
            # Find or create location entry in progress_logs
            location_idx = None
            for idx, loc_entry in enumerate(progress_logs.get("locations", [])):
                if loc_entry.get("location") == location:
                    location_idx = idx
                    break
            
            if location_idx is not None:
                # Update existing entry
                progress_logs["locations"][location_idx] = location_entry
            else:
                # Add new location entry
                progress_logs["locations"].append(location_entry)
                location_idx = len(progress_logs["locations"]) - 1
            
            # Update summary
            progress_logs["summary"]["in_progress_locations"] = sum(
                1 for loc in progress_logs["locations"] if loc.get("status") == "in_progress"
            )
            
            for listing_type in listing_types_to_scrape:
                # Check if job was cancelled (checked every 2 seconds by background monitor)
                if cancel_flag.get("cancelled", False):
                    logger.info(f"   [CANCELLED] Job was cancelled, stopping listing type fetch")
                    break  # Exit the listing type loop
                
                try:
                    start_time = datetime.utcnow()
                    logger.info(f"   [FETCH] Fetching {listing_type} properties from {location}...")
                    
                    # Update location_last_update when scraping starts (for timeout detection)
                    if location_last_update is not None:
                        location_last_update[location] = datetime.utcnow()
                    
                    # Update location entry to show we're fetching this listing type
                    if location_idx is not None and location_idx < len(progress_logs["locations"]):
                        progress_logs["locations"][location_idx]["timestamp"] = start_time.isoformat()
                        # Update will happen after properties are found
                    
                    # Push to database immediately for real-time UI update
                    await db.update_job_status(job.job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                    
                    # Use job's limit (or None to get all properties)
                    # Wrap in timeout to prevent getting stuck
                    from config import settings
                    scrape_timeout = min(settings.LOCATION_TIMEOUT_MINUTES * 60, 600)  # Max 10 minutes per listing type
                    try:
                        properties = await asyncio.wait_for(
                            self._scrape_listing_type(
                                location, 
                                job, 
                                proxy_config, 
                                listing_type, 
                                limit=job.limit if job.limit else None,
                                past_days=job.past_days if job.past_days else 90
                            ),
                            timeout=scrape_timeout
                        )
                    except asyncio.TimeoutError:
                        logger.warning(f"   [TIMEOUT] Scraping {listing_type} in {location} exceeded {scrape_timeout}s timeout, skipping this listing type")
                        properties = []  # Continue with next listing type
                    
                    end_time = datetime.utcnow()
                    duration = (end_time - start_time).total_seconds()
                    
                    logger.info(f"   [OK] Found {len(properties)} {listing_type} properties in {duration:.1f}s")
                    logger.debug(f"   [DEBUG] Properties type: {type(properties)}, truthy: {bool(properties)}, len: {len(properties)}")
                    
                    # Initialize save_results for use later
                    save_results = {"inserted": 0, "updated": 0, "skipped": 0, "errors": 0}
                    
                    # Save properties immediately after each listing type fetch
                    if properties:
                        logger.debug(f"   [DEBUG] Entering save block...")
                        save_results = await db.save_properties_batch(properties)
                        location_total_found += len(properties)
                        location_total_inserted += save_results["inserted"]
                        location_total_updated += save_results["updated"]
                        location_total_skipped += save_results["skipped"]
                        location_total_errors += save_results["errors"]
                        
                        # Collect properties for enrichment (queued after location completes)
                        if "enrichment_queue" in save_results:
                            enrichment_queue.extend(save_results["enrichment_queue"])
                        
                        # Track property IDs found in this scrape
                        for prop in properties:
                            if prop.property_id:
                                found_property_ids.add(prop.property_id)
                        
                        # Update running totals
                        running_totals["total_properties"] += len(properties)
                        running_totals["saved_properties"] += save_results["inserted"] + save_results["updated"]
                        running_totals["total_inserted"] += save_results["inserted"]
                        running_totals["total_updated"] += save_results["updated"]
                        running_totals["total_skipped"] += save_results["skipped"]
                        
                        # Update last property update time for this location (for timeout detection)
                        if location_last_update is not None:
                            location_last_update[location] = datetime.utcnow()
                        
                        logger.debug(f"   [SAVED] {save_results['inserted']} inserted, {save_results['updated']} updated, {save_results['skipped']} skipped")
                    
                    # Update location entry's listing_types with completion stats
                    if location_idx is not None and location_idx < len(progress_logs["locations"]):
                        location_entry = progress_logs["locations"][location_idx]
                        if listing_type in location_entry.get("listing_types", {}):
                            location_entry["listing_types"][listing_type]["found"] = len(properties)
                            location_entry["listing_types"][listing_type]["inserted"] = save_results.get("inserted", 0) if properties else 0
                            location_entry["listing_types"][listing_type]["updated"] = save_results.get("updated", 0) if properties else 0
                            location_entry["listing_types"][listing_type]["skipped"] = save_results.get("skipped", 0) if properties else 0
                        location_entry["timestamp"] = end_time.isoformat()
                    
                    # Push to database immediately with updated job totals
                    logger.debug(f"   [DB-UPDATE] Updating job counts: scraped={running_totals['total_properties']}, saved={running_totals['saved_properties']}, inserted={running_totals['total_inserted']}, updated={running_totals['total_updated']}")
                    await db.update_job_status(
                        job.job_id, 
                        JobStatus.RUNNING,
                        properties_scraped=running_totals["total_properties"],
                        properties_saved=running_totals["saved_properties"],
                        properties_inserted=running_totals["total_inserted"],
                        properties_updated=running_totals["total_updated"],
                        properties_skipped=running_totals["total_skipped"],
                        progress_logs=progress_logs
                    )
                    logger.debug(f"   [DB-UPDATE] Job status updated successfully")
                    
                    # Reduced delay for faster response
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.warning(f"   [WARNING] Error scraping {listing_type} properties: {e}")
                    # Log the error
                    error_log = {
                        "listing_type": listing_type,
                        "status": "error",
                        "properties_found": 0,
                        "error": str(e),
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    listing_type_logs.append(error_log)
                    
                    # Update location entry with error
                    if location_idx is not None and location_idx < len(progress_logs.get("locations", [])):
                        location_entry = progress_logs["locations"][location_idx]
                        location_entry["timestamp"] = datetime.utcnow().isoformat()
                        # Error is logged but location continues with other listing types
                    
                    # Push to database immediately with current totals
                    await db.update_job_status(
                        job.job_id, 
                        JobStatus.RUNNING,
                        properties_scraped=running_totals["total_properties"],
                        properties_saved=running_totals["saved_properties"],
                        properties_inserted=running_totals["total_inserted"],
                        properties_updated=running_totals["total_updated"],
                        properties_skipped=running_totals["total_skipped"],
                        progress_logs=progress_logs
                    )
                    
                    continue
            
            logger.info(f"   [TOTAL] Location complete: {location_total_found} found, {location_total_inserted} inserted, {location_total_updated} updated, {location_total_skipped} skipped")
            
            # Update location entry to mark as completed
            if location_idx is not None and location_idx < len(progress_logs["locations"]):
                location_entry = progress_logs["locations"][location_idx]
                location_entry["status"] = "completed"
                location_entry["timestamp"] = datetime.utcnow().isoformat()
                
                # Update summary
                progress_logs["summary"]["completed_locations"] = sum(
                    1 for loc in progress_logs["locations"] if loc.get("status") == "completed"
                )
                progress_logs["summary"]["in_progress_locations"] = sum(
                    1 for loc in progress_logs["locations"] if loc.get("status") == "in_progress"
                )
            
            # Create final summary for return value (backward compatibility)
            final_summary = {
                "timestamp": datetime.utcnow().isoformat(),
                "location": location,
                "location_index": location_index,
                "total_locations": total_locations,
                "properties_found": location_total_found,
                "inserted": location_total_inserted,
                "updated": location_total_updated,
                "skipped": location_total_skipped,
                "errors": location_total_errors,
                "status": "completed"
            }
            
            # Check for missing properties that may have gone off-market
            # Run this in the background so it doesn't block location completion
            if found_property_ids and listing_types_to_scrape:
                # Only check if we're scraping for_sale or pending
                if any(lt in ['for_sale', 'pending'] for lt in listing_types_to_scrape):
                    # Use job start time (not end time) to ensure we only check properties
                    # that weren't scraped in THIS job run
                    job_start = job_start_time or datetime.utcnow()
                    
                    # Run off-market detection in background (non-blocking)
                    # This allows the location to be marked as complete immediately
                    # Only check properties for THIS specific location (zip code), not all properties in the job
                    logger.info(f"   [OFF-MARKET] Starting background off-market detection for location {location}")
                    asyncio.create_task(
                        self._run_off_market_detection_background(
                            job=job,
                            location=location,  # Pass location to filter by zip code
                            listing_types_to_scrape=listing_types_to_scrape,
                            found_property_ids=found_property_ids,
                            job_start_time=job_start,
                            proxy_config=proxy_config,
                            cancel_flag=cancel_flag,
                            progress_logs=progress_logs
                        )
                    )
            
            # Queue enrichment for all properties from this location (non-blocking, async tasks)
            if enrichment_queue and db.enrichment_pipeline:
                enrichment_count = len(enrichment_queue)
                logger.info(f"   [ENRICHMENT] Starting enrichment for {enrichment_count} properties from {location}")
                
                # Initialize enrichment status in location entry
                if location_idx is not None and location_idx < len(progress_logs["locations"]):
                    location_entry = progress_logs["locations"][location_idx]
                    location_entry["enrichment"]["status"] = "in_progress"
                    location_entry["enrichment"]["total"] = enrichment_count
                    location_entry["enrichment"]["completed"] = 0
                    location_entry["enrichment"]["failed"] = 0
                
                # Process enrichment in batches if configured
                batch_size = settings.ENRICHMENT_BATCH_SIZE
                if batch_size and isinstance(batch_size, str):
                    try:
                        batch_size = int(batch_size)
                    except ValueError:
                        batch_size = None
                
                if batch_size and batch_size > 0:
                    # Process in batches
                    for i in range(0, len(enrichment_queue), batch_size):
                        batch = enrichment_queue[i:i + batch_size]
                        for enrichment_item in batch:
                            # Create async task with semaphore for concurrency control
                            asyncio.create_task(
                                self._enrich_property_async(
                                    enrichment_item["property_id"],
                                    enrichment_item["property_dict"],
                                    enrichment_item["job_id"],
                                    location=location,
                                    location_idx=location_idx,
                                    listing_type=enrichment_item.get("listing_type"),
                                    progress_logs=progress_logs
                                )
                            )
                else:
                    # Process all at once
                    for enrichment_item in enrichment_queue:
                        # Create async task with semaphore for concurrency control
                        asyncio.create_task(
                            self._enrich_property_async(
                                enrichment_item["property_id"],
                                enrichment_item["property_dict"],
                                enrichment_item["job_id"],
                                location=location,
                                location_idx=location_idx,
                                listing_type=enrichment_item.get("listing_type"),
                                progress_logs=progress_logs
                            )
                        )
            
            # Update database with final summary
            await db.update_job_status(
                job.job_id,
                JobStatus.RUNNING,
                properties_scraped=running_totals["total_properties"],
                properties_saved=running_totals["saved_properties"],
                properties_inserted=running_totals["total_inserted"],
                properties_updated=running_totals["total_updated"],
                properties_skipped=running_totals["total_skipped"],
                progress_logs=progress_logs
            )
            
            # Return final summary for this location
            return final_summary
            
        except Exception as e:
            logger.error(f"Error scraping location {location}: {e}")
            import traceback
            error_traceback = traceback.format_exc()
            logger.debug(f"Traceback: {error_traceback}")
            
            # Update location entry to mark as failed
            if location_idx is not None and location_idx < len(progress_logs.get("locations", [])):
                location_entry = progress_logs["locations"][location_idx]
                location_entry["status"] = "failed"
                location_entry["error"] = str(e)
                location_entry["timestamp"] = datetime.utcnow().isoformat()
                
                # Update summary
                progress_logs["summary"]["failed_locations"] = sum(
                    1 for loc in progress_logs["locations"] if loc.get("status") == "failed"
                )
                progress_logs["summary"]["in_progress_locations"] = sum(
                    1 for loc in progress_logs["locations"] if loc.get("status") == "in_progress"
                )
            
            # Return failure information instead of None
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "location": location,
                "location_index": location_index,
                "total_locations": total_locations,
                "status": "failed",
                "error": str(e),
                "properties_found": 0,
                "inserted": 0,
                "updated": 0,
                "skipped": 0,
                "errors": 1,
                "message": f"Location scraping failed: {str(e)}"
            }
    
    async def _enrich_property_async(
        self, 
        property_id: str, 
        property_dict: Dict[str, Any], 
        job_id: Optional[str],
        location: Optional[str] = None,
        location_idx: Optional[int] = None,
        listing_type: Optional[str] = None,
        progress_logs: Optional[Dict[str, Any]] = None
    ):
        """Async enrichment task with semaphore for concurrency control"""
        async with self.enrichment_semaphore:
            success = False
            try:
                await db.enrichment_pipeline.enrich_property(
                    property_id=property_id,
                    property_dict=property_dict,
                    existing_property=None,  # Deprecated - enrichment will fetch from DB
                    job_id=job_id
                )
                success = True
            except Exception as e:
                logger.error(f"Error in enrichment task for property {property_id}: {e}")
            finally:
                # Update progress logs if provided
                if progress_logs and location_idx is not None and location_idx < len(progress_logs.get("locations", [])):
                    location_entry = progress_logs["locations"][location_idx]
                    enrichment_status = location_entry.get("enrichment", {})
                    
                    if success:
                        enrichment_status["completed"] = enrichment_status.get("completed", 0) + 1
                        # Update per-listing-type enriched counter
                        if listing_type and listing_type in location_entry.get("listing_types", {}):
                            location_entry["listing_types"][listing_type]["enriched"] = \
                                location_entry["listing_types"][listing_type].get("enriched", 0) + 1
                    else:
                        enrichment_status["failed"] = enrichment_status.get("failed", 0) + 1
                    
                    # Check if enrichment is complete
                    total = enrichment_status.get("total", 0)
                    completed = enrichment_status.get("completed", 0)
                    failed = enrichment_status.get("failed", 0)
                    
                    if completed + failed >= total and total > 0:
                        enrichment_status["status"] = "completed"
                    
                    # Update job status periodically (every 10 completions to avoid too many DB writes)
                    if (completed + failed) % 10 == 0 or (completed + failed) >= total:
                        try:
                            await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                        except Exception as e:
                            logger.error(f"Error updating job status with enrichment progress: {e}")
    
    async def _scrape_listing_type(self, location: str, job: ScrapingJob, proxy_config: Optional[Dict[str, Any]], listing_type: str, limit: Optional[int] = None, past_days: Optional[int] = None) -> List[Property]:
        """Scrape properties for a specific listing type"""
        try:
            logger.debug(f"   [DEBUG] _scrape_listing_type called with listing_type: '{listing_type}' (type: {type(listing_type)})")
            
            # Prepare scraping parameters - remove all filtering for comprehensive data
            scrape_params = {
                "location": location,
                "listing_type": listing_type,
                "mls_only": False,  # Always use all sources for maximum data
                "limit": limit or job.limit or 10000  # Use high limit for comprehensive scraping
            }
            
            # Add optional parameters if specified
            if job.radius:
                scrape_params["radius"] = job.radius
            
            # Use provided past_days or job past_days
            if past_days:
                scrape_params["past_days"] = past_days
            elif job.past_days:
                scrape_params["past_days"] = job.past_days
            
            # Add proxy configuration if available
            if proxy_config:
                scrape_params["proxy"] = proxy_config.get("proxy_url")
            
            # Remove all filtering parameters to get ALL properties
            # Note: We're not setting foreclosure=False, exclude_pending=False, etc.
            # This allows the scraper to get off-market, foreclosures, and all other property types
            
            # Scrape properties - Run blocking call in thread pool to avoid blocking event loop
            # Add timeout to prevent locations from getting stuck (default 5 minutes per listing type)
            timeout_seconds = 300  # 5 minutes timeout per listing type
            loop = asyncio.get_event_loop()
            
            try:
                properties_df = await asyncio.wait_for(
                    loop.run_in_executor(
                        self.executor,
                        lambda: scrape_property(**scrape_params)
                    ),
                    timeout=timeout_seconds
                )
            except asyncio.TimeoutError:
                error_msg = f"Scraping {listing_type} properties in {location} timed out after {timeout_seconds} seconds"
                logger.warning(f"   [TIMEOUT] {error_msg}")
                raise TimeoutError(error_msg)
            
            # Convert DataFrame to our Property models
            properties = []
            for index, row in properties_df.iterrows():
                try:
                    property_obj = self.convert_to_property_model(row, job.job_id, listing_type, job.scheduled_job_id)
                    # Mark sold properties as comps
                    if listing_type == "sold":
                        property_obj.is_comp = True
                    properties.append(property_obj)
                except Exception as e:
                    logger.error(f"Error converting property: {e}")
                    continue
            
            return properties
            
        except Exception as e:
            logger.error(f"Error scraping {listing_type} properties in {location}: {e}")
            return []
    
    def convert_to_property_model(self, prop_data: Any, job_id: str, listing_type: str = None, scheduled_job_id: Optional[str] = None) -> Property:
        """Convert HomeHarvest property data (pandas Series) to our Property model"""
        try:
            # Helper function to safely get values from pandas Series
            def safe_get(key, default=None):
                try:
                    value = prop_data.get(key, default)
                    # Convert pandas NaN to None
                    if pd.isna(value):
                        return None
                    # Handle None values
                    if value is None:
                        return None
                    return value
                except:
                    return default
            
            # Extract address information
            address = PropertyAddress(
                street=safe_get('street'),
                unit=safe_get('unit'),
                city=safe_get('city'),
                state=safe_get('state'),
                zip_code=safe_get('zip_code'),
                formatted_address=safe_get('formatted_address'),
                full_street_line=safe_get('full_street_line')
            )
            
            # Extract description information
            garage_value = safe_get('parking_garage')
            garage_str = str(garage_value) if garage_value is not None else None
            
            description = PropertyDescription(
                style=safe_get('style'),
                beds=safe_get('beds'),
                full_baths=safe_get('full_baths'),
                half_baths=safe_get('half_baths'),
                sqft=safe_get('sqft'),
                year_built=safe_get('year_built'),
                stories=safe_get('stories'),
                garage=garage_str,
                lot_sqft=safe_get('lot_sqft'),
                text=safe_get('text'),
                property_type=safe_get('style')  # Use style as property_type since type field is None
            )
            
            # Extract financial information
            financial = PropertyFinancial(
                list_price=safe_get('list_price'),
                list_price_min=safe_get('list_price_min'),
                list_price_max=safe_get('list_price_max'),
                sold_price=safe_get('sold_price'),
                last_sold_price=safe_get('last_sold_price'),
                price_per_sqft=safe_get('price_per_sqft'),
                estimated_value=safe_get('estimated_value'),
                tax_assessed_value=safe_get('assessed_value'),
                hoa_fee=safe_get('hoa_fee'),
                tax=safe_get('tax')
            )
            
            # Extract date information
            dates = PropertyDates(
                list_date=safe_get('list_date'),
                pending_date=safe_get('pending_date'),
                last_sold_date=safe_get('last_sold_date')
            )
            
            # Extract location information
            neighborhoods_value = safe_get('neighborhoods')
            neighborhoods_list = None
            if neighborhoods_value and isinstance(neighborhoods_value, str):
                # Split comma-separated neighborhoods into list
                neighborhoods_list = [n.strip() for n in neighborhoods_value.split(',') if n.strip()]
            
            location = PropertyLocation(
                latitude=safe_get('latitude'),
                longitude=safe_get('longitude'),
                neighborhoods=neighborhoods_list,
                county=safe_get('county'),
                fips_code=safe_get('fips_code'),
                parcel_number=safe_get('parcel_number')
            )
            
            # Extract agent information
            agent_phones_value = safe_get('agent_phones')
            agent_phones_list = None
            if agent_phones_value and isinstance(agent_phones_value, (list, str)):
                if isinstance(agent_phones_value, str):
                    # Handle string format - might be JSON or comma-separated
                    try:
                        import json
                        agent_phones_list = json.loads(agent_phones_value)
                    except:
                        agent_phones_list = [{"number": agent_phones_value, "type": None, "primary": True}]
                else:
                    agent_phones_list = agent_phones_value
            
            agent = PropertyAgent(
                agent_id=safe_get('agent_id'),
                agent_name=safe_get('agent_name'),
                agent_email=safe_get('agent_email'),
                agent_phones=agent_phones_list,
                agent_mls_set=safe_get('agent_mls_set'),
                agent_nrds_id=safe_get('agent_nrds_id')
            )
            
            # Extract broker information
            broker = PropertyBroker(
                broker_id=safe_get('broker_id'),
                broker_name=safe_get('broker_name')
            )
            
            # Extract builder information
            builder = PropertyBuilder(
                builder_id=safe_get('builder_id'),
                builder_name=safe_get('builder_name')
            )
            
            # Extract office information
            office_phones_value = safe_get('office_phones')
            office_phones_list = None
            if office_phones_value and isinstance(office_phones_value, (list, str)):
                if isinstance(office_phones_value, str):
                    try:
                        import json
                        office_phones_list = json.loads(office_phones_value)
                    except:
                        office_phones_list = [{"number": office_phones_value, "type": None, "primary": True}]
                else:
                    office_phones_list = office_phones_value
            
            office = PropertyOffice(
                office_id=safe_get('office_id'),
                office_mls_set=safe_get('office_mls_set'),
                office_name=safe_get('office_name'),
                office_email=safe_get('office_email'),
                office_phones=office_phones_list
            )
            
            # Extract image information
            alt_photos_value = safe_get('alt_photos')
            alt_photos_list = None
            if alt_photos_value and isinstance(alt_photos_value, str):
                # Split comma-separated photo URLs
                alt_photos_list = [url.strip() for url in alt_photos_value.split(',') if url.strip()]
            
            # Handle nearby_schools - convert string to list if needed
            nearby_schools_value = safe_get('nearby_schools')
            nearby_schools_list = None
            if nearby_schools_value and isinstance(nearby_schools_value, str):
                # Split comma-separated schools
                school_names = [school.strip() for school in nearby_schools_value.split(',') if school.strip()]
                nearby_schools_list = [{"name": school, "type": None} for school in school_names]
            
            # Create Property object
            property_obj = Property(
                property_id=safe_get('property_id', f"prop_{int(time.time())}_{random.randint(1000, 9999)}"),
                mls_id=safe_get('mls_id'),
                mls=safe_get('mls'),
                status=safe_get('status'),
                mls_status=safe_get('mls_status'),
                listing_type=listing_type or safe_get('listing_type'),  # Use job listing_type or scraped data
                address=address,
                description=description,
                financial=financial,
                dates=dates,
                location=location,
                agent=agent,
                broker=broker,
                builder=builder,
                office=office,
                property_url=safe_get('property_url'),
                listing_id=safe_get('listing_id'),
                permalink=safe_get('permalink'),
                primary_photo=safe_get('primary_photo'),
                alt_photos=alt_photos_list,
                days_on_mls=safe_get('days_on_mls'),
                new_construction=safe_get('new_construction'),
                monthly_fees=safe_get('monthly_fees'),
                one_time_fees=safe_get('one_time_fees'),
                tax_history=safe_get('tax_history'),
                nearby_schools=nearby_schools_list,
                job_id=job_id,
                scheduled_job_id=scheduled_job_id,
                last_scraped=datetime.utcnow()  # Set last_scraped timestamp
            )
            
            # Generate property_id from formatted address if not provided
            if not property_obj.property_id and property_obj.address and property_obj.address.formatted_address:
                property_obj.property_id = property_obj.generate_property_id()
            
            # Update content tracking (generate hash and check for changes)
            property_obj.update_content_tracking()
            
            return property_obj
            
        except Exception as e:
            logger.error(f"Error converting property data: {e}")
            raise
    
    async def get_proxy_config(self, job: ScrapingJob) -> Optional[Dict[str, Any]]:
        """Get proxy configuration for a job"""
        try:
            # Use job-specific proxy if available
            if job.proxy_config:
                return job.proxy_config
            
            # Get next available proxy
            proxy = proxy_manager.get_next_proxy()
            if not proxy:
                return None
            
            # Build proxy URL
            if proxy.username and proxy.password:
                proxy_url = f"http://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
            else:
                proxy_url = f"http://{proxy.host}:{proxy.port}"
            
            return {
                "proxy_url": proxy_url,
                "proxy_host": proxy.host,
                "proxy_port": proxy.port
            }
            
        except Exception as e:
            logger.error(f"Error getting proxy config: {e}")
            return None
    
    def is_off_market_status(self, status: Optional[str], mls_status: Optional[str]) -> bool:
        """Check if a property status indicates it's off-market"""
        if not status and not mls_status:
            return False
        
        # Check status field
        if status and status.upper() == 'OFF_MARKET':
            return True
        
        # Check mls_status field for off-market indicators
        if mls_status:
            mls_status_upper = mls_status.upper()
            off_market_indicators = [
                'EXPIRED', 'WITHDRAWN', 'CANCELLED', 'INACTIVE', 'DELISTED',
                'TEMPORARILY OFF MARKET', 'TEMPORARILY_OFF_MARKET'
            ]
            if mls_status_upper in off_market_indicators:
                return True
        
        return False
    
    async def query_property_by_address(self, formatted_address: str, proxy_config: Optional[Dict[str, Any]] = None) -> Optional[Property]:
        """Query a property directly by address using HomeHarvest"""
        try:
            if not formatted_address:
                return None
            
            # Prepare scraping parameters
            scrape_params = {
                "location": formatted_address,
                "limit": 1  # We only expect one property per address
            }
            
            # Add proxy configuration if available
            if proxy_config:
                scrape_params["proxy"] = proxy_config.get("proxy_url")
            
            # Scrape property - Run blocking call in thread pool
            loop = asyncio.get_event_loop()
            timeout_seconds = 30  # 30 second timeout for direct address query
            
            try:
                properties_df = await asyncio.wait_for(
                    loop.run_in_executor(
                        self.executor,
                        lambda: scrape_property(**scrape_params)
                    ),
                    timeout=timeout_seconds
                )
            except asyncio.TimeoutError:
                print(f"   [TIMEOUT] Querying {formatted_address} timed out")
                return None
            
            # Convert DataFrame to Property model
            if properties_df is not None and not properties_df.empty:
                # Take the first property found
                row = properties_df.iloc[0]
                property_obj = self.convert_to_property_model(row, "off_market_check", None, None)
                return property_obj
            
            return None
            
        except Exception as e:
            print(f"Error querying property by address {formatted_address}: {e}")
            return None
    
    def _parse_city_state_from_location(self, location: str) -> Tuple[Optional[str], Optional[str]]:
        """Parse city and state from location string (e.g., 'Indianapolis, IN' or 'Indianapolis, IN 46201')"""
        try:
            # Remove zip code if present
            location_clean = re.sub(r'\s+\d{5}(-\d{4})?$', '', location.strip())
            
            # Split by comma
            parts = [p.strip() for p in location_clean.split(',')]
            
            if len(parts) >= 2:
                city = parts[0]
                state = parts[1].upper()
                return city, state
            elif len(parts) == 1:
                # Try to extract state abbreviation (2 letters at end)
                match = re.search(r'\b([A-Z]{2})\b$', parts[0].upper())
                if match:
                    state = match.group(1)
                    city = parts[0][:match.start()].strip()
                    return city, state
            
            return None, None
        except Exception as e:
            print(f"Error parsing city/state from location '{location}': {e}")
            return None, None
    
    async def check_missing_properties_for_off_market(
        self,
        scheduled_job_id: str,
        listing_types_scraped: List[str],
        found_property_ids: set[str],
        job_start_time: datetime,
        proxy_config: Optional[Dict[str, Any]] = None,
        cancel_flag: Optional[dict] = None,
        batch_size: int = 50,
        progress_logs: Optional[Dict[str, Any]] = None,
        job_id: Optional[str] = None,
        location: Optional[str] = None  # Add location parameter to filter by specific location
    ) -> Dict[str, Any]:
        """
        Check for properties that belong to a scheduled job but weren't scraped in the current run.
        Query them directly by address to check if they went off-market.
        Processes all missing properties in batches until complete.
        
        Uses scheduled_job_id and last_scraped timestamp to identify properties that need checking.
        Only checks properties where last_scraped < job_start_time to ensure we don't check
        properties that were already scraped in this job run.
        
        If location is provided, only checks properties in that specific location (zip code).
        """
        try:
            # Only check for for_sale and pending properties
            listing_types_to_check = ['for_sale', 'pending']
            
            location_info = f" for location {location}" if location else ""
            logger.debug(f"   [OFF-MARKET] Checking for missing properties for scheduled job {scheduled_job_id}{location_info}")
            logger.debug(f"   [OFF-MARKET] Job start time: {job_start_time.isoformat()}")
            
            # Query database for properties with this scheduled_job_id that weren't scraped in THIS job run
            # Properties that have last_scraped < job_start_time (weren't updated in this scrape)
            # OR have last_scraped = null (never been scraped by this job)
            query = {
                "scheduled_job_id": scheduled_job_id,
                "listing_type": {"$in": listing_types_to_check},
                "$or": [
                    {"last_scraped": {"$lt": job_start_time}},
                    {"last_scraped": None},
                    {"last_scraped": {"$exists": False}}
                ]
            }
            
            # If location is provided, filter by location to only check properties in this specific location
            if location:
                # Try to extract zip code from location string (e.g., "Indianapolis, IN 46201" -> "46201")
                import re
                zip_match = re.search(r'\b(\d{5})\b', location)
                if zip_match:
                    # Location contains zip code - filter by zip code for precise matching
                    zip_code = zip_match.group(1)
                    query["address.zip_code"] = zip_code
                    logger.debug(f"   [OFF-MARKET] Filtering by zip code: {zip_code}")
                else:
                    # No zip code found - parse city/state for broader matching
                    city, state = self._parse_city_state_from_location(location)
                    if city and state:
                        # Filter by city and state (covers all zip codes in that city)
                        query["address.city"] = {"$regex": f"^{city}$", "$options": "i"}
                        query["address.state"] = {"$regex": f"^{state}$", "$options": "i"}
                        logger.debug(f"   [OFF-MARKET] Filtering by city/state: {city}, {state} (no zip code in location)")
                    else:
                        # Could not parse location - log warning but continue with scheduled_job_id filter only
                        logger.warning(f"   [OFF-MARKET] Could not parse location '{location}' - checking all properties for scheduled job (may be slow)")
            
            # Optimize query: only fetch property_id and address fields needed for checking
            # This reduces memory usage and speeds up the query significantly
            cursor = db.properties_collection.find(
                query,
                {"property_id": 1, "address": 1, "status": 1, "mls_status": 1, "listing_type": 1}  # Include listing_type for tracking
            ).sort("last_scraped", 1).limit(10000)  # Sort by oldest last_scraped first
            
            # Store lightweight property data instead of full Property objects
            # Already filtered to only missing properties (not in found_property_ids)
            missing_properties = []
            async for prop_data in cursor:
                property_id = prop_data.get("property_id")
                if property_id and property_id not in found_property_ids:
                    # Only include properties not found in current scrape
                    missing_properties.append({
                        "property_id": property_id,
                        "address": prop_data.get("address", {}),
                        "status": prop_data.get("status"),
                        "mls_status": prop_data.get("mls_status"),
                        "listing_type": prop_data.get("listing_type")  # For per-listing-type tracking
                    })
            
            if not missing_properties:
                logger.debug(f"   [OFF-MARKET] No missing properties found for scheduled job {scheduled_job_id}")
                return {
                    "missing_checked": 0,
                    "off_market_found": 0,
                    "errors": 0,
                    "skipped": False
                }
            
            total_missing = len(missing_properties)
            logger.info(f"   [OFF-MARKET] Found {total_missing} missing properties, processing in batches of {batch_size}")
            
            off_market_count = 0
            error_count = 0
            total_checked = 0
            
            # Import history_tracker for recording changes
            from services.history_tracker import HistoryTracker
            history_tracker = HistoryTracker(db.db)
            
            # Get location_entry for updating progress_logs (outside batch loop for efficiency)
            location_entry = None
            if progress_logs and location:
                for loc_entry in progress_logs.get("locations", []):
                    if loc_entry.get("location") == location:
                        location_entry = loc_entry
                        break
            
            # Process properties in batches until all are checked
            batch_number = 0
            while missing_properties:
                # Check cancellation flag
                if cancel_flag and cancel_flag.get("cancelled", False):
                    logger.info(f"   [OFF-MARKET] Job cancelled, stopping off-market check")
                    break
                
                batch_number += 1
                # Get next batch
                batch = missing_properties[:batch_size]
                remaining = len(missing_properties) - len(batch)
                
                logger.debug(f"   [OFF-MARKET] Processing batch {batch_number}: {len(batch)} properties ({(total_checked + len(batch))}/{total_missing} total, {remaining} remaining)")
                
                # Update progress logs if provided
                if location_entry is not None and job_id:
                    try:
                        if location_entry is not None:
                            location_entry["off_market_check"] = {
                                "status": "in_progress",
                                "scheduled_job_id": scheduled_job_id,
                                "total_missing": total_missing,
                                "checked": total_checked,
                                "current_batch": batch_number,
                                "batch_size": len(batch),
                                "remaining": remaining,
                                "off_market_found": off_market_count,
                                "errors": error_count
                            }
                            await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                    except Exception as e:
                        logger.error(f"   [OFF-MARKET] Error updating progress logs: {e}")
                
                # Track off-market per listing type
                off_market_by_listing_type = {}
                
                # Check each property in batch
                for i, prop_data in enumerate(batch):
                    # Check cancellation flag
                    if cancel_flag and cancel_flag.get("cancelled", False):
                        logger.info(f"   [OFF-MARKET] Job cancelled, stopping off-market check")
                        break
                    
                    try:
                        property_id = prop_data["property_id"]
                        address = prop_data.get("address", {})
                        formatted_address = address.get("formatted_address") if address else None
                        listing_type = prop_data.get("listing_type")
                        
                        # Query property directly by address
                        if not formatted_address:
                            logger.debug(f"   [OFF-MARKET] Property {property_id} has no formatted_address, skipping")
                            total_checked += 1
                            continue
                        
                        current_num = total_checked + i + 1
                        logger.debug(f"   [OFF-MARKET] Checking property {current_num}/{total_missing} (batch {batch_number}): {formatted_address}")
                        
                        queried_property = await self.query_property_by_address(
                            formatted_address,
                            proxy_config
                        )
                        
                        if queried_property:
                            # Check if property is off-market
                            if self.is_off_market_status(queried_property.status, queried_property.mls_status):
                                logger.info(f"   [OFF-MARKET] [OK] Property {property_id} is OFF_MARKET (status={queried_property.status}, mls_status={queried_property.mls_status})")
                                
                                # Update property status
                                old_status = prop_data.get("status") or 'UNKNOWN'
                                await db.properties_collection.update_one(
                                    {"property_id": property_id},
                                    {
                                        "$set": {
                                            "status": "OFF_MARKET",
                                            "mls_status": queried_property.mls_status or prop_data.get("mls_status"),
                                            "scraped_at": datetime.utcnow()
                                        }
                                    }
                                )
                                
                                # Record status change in change_logs
                                try:
                                    change_entry = {
                                        "field": "status",
                                        "old_value": old_status,
                                        "new_value": "OFF_MARKET",
                                        "change_type": "modified",
                                        "timestamp": datetime.utcnow()
                                    }
                                    await history_tracker.record_change_logs(
                                        property_id,
                                        [change_entry],
                                        "off_market_detection"
                                    )
                                except Exception as e:
                                    logger.error(f"   [OFF-MARKET] Error recording change log: {e}")
                                
                                off_market_count += 1
                                # Track per listing type (only for for_sale and pending)
                                if listing_type in ['for_sale', 'pending']:
                                    off_market_by_listing_type[listing_type] = off_market_by_listing_type.get(listing_type, 0) + 1
                                    
                                    # Update progress_logs per listing type
                                    if location_entry and listing_type in location_entry.get("listing_types", {}):
                                        location_entry["listing_types"][listing_type]["off_market"] = \
                                            location_entry["listing_types"][listing_type].get("off_market", 0) + 1
                            else:
                                logger.debug(f"   [OFF-MARKET] Property {property_id} still active (status={queried_property.status}, mls_status={queried_property.mls_status})")
                        else:
                            logger.debug(f"   [OFF-MARKET] Property {property_id} not found in HomeHarvest (may be deleted)")
                        
                        # Add delay between queries to avoid rate limiting
                        if i < len(batch) - 1:  # Don't delay after last property in batch
                            await asyncio.sleep(1.0)  # 1 second delay between queries
                        
                    except Exception as e:
                        error_count += 1
                        logger.error(f"   [OFF-MARKET] Error checking property {prop_data.get('property_id', 'unknown')}: {e}")
                        continue
                
                # Remove processed batch from list
                total_checked += len(batch)
                missing_properties = missing_properties[batch_size:]
                
                # Add delay between batches (shorter delay)
                if missing_properties:
                    logger.debug(f"   [OFF-MARKET] Batch {batch_number} complete: {off_market_count} off-market found so far, {len(missing_properties)} remaining")
                    await asyncio.sleep(2.0)  # 2 second delay between batches
            
            # Update final off-market check status in progress_logs
            if progress_logs and location and job_id:
                try:
                    location_entry = None
                    for loc_entry in progress_logs.get("locations", []):
                        if loc_entry.get("location") == location:
                            location_entry = loc_entry
                            break
                    
                    if location_entry:
                        location_entry["off_market_check"]["status"] = "completed"
                        location_entry["off_market_check"]["checked"] = total_checked
                        location_entry["off_market_check"]["found"] = off_market_count
                        location_entry["off_market_check"]["errors"] = error_count
                        await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                except Exception as e:
                    logger.error(f"   [OFF-MARKET] Error updating final progress logs: {e}")
            
            result = {
                "missing_checked": total_checked,
                "missing_total": total_missing,
                "missing_skipped": 0,  # No longer skipping - all are checked
                "off_market_found": off_market_count,
                "errors": error_count,
                "batches_processed": batch_number,
                "skipped": False
            }
            
            logger.info(f"   [OFF-MARKET] Check complete: {total_checked} checked, {off_market_count} off-market, {error_count} errors, {batch_number} batches")
            return result
            
        except Exception as e:
            logger.error(f"   [OFF-MARKET] Error in off-market detection: {e}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
            return {
                "missing_checked": 0,
                "off_market_found": 0,
                "errors": 1,
                "skipped": False,
                "error": str(e)
            }
    
    async def check_missing_properties_for_off_market_by_location(
        self,
        location: str,
        listing_types_scraped: List[str],
        found_property_ids: set[str],
        job_start_time: datetime,
        proxy_config: Optional[Dict[str, Any]] = None,
        cancel_flag: Optional[dict] = None,
        batch_size: int = 50,
        progress_logs: Optional[Dict[str, Any]] = None,
        job_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Check for missing properties using location-based matching (for one-time jobs without scheduled_job_id).
        This is a fallback method when scheduled_job_id is not available.
        
        Uses city+state matching to find properties that weren't scraped in the current run.
        """
        try:
            # Only check for for_sale and pending properties
            listing_types_to_check = ['for_sale', 'pending']
            
            # Parse city and state from location
            city, state = self._parse_city_state_from_location(location)
            if not city or not state:
                logger.warning(f"   [OFF-MARKET] Could not parse city/state from location '{location}', skipping check")
                return {
                    "missing_checked": 0,
                    "off_market_found": 0,
                    "errors": 0,
                    "skipped": True,
                    "reason": "Could not parse city/state"
                }
            
            logger.debug(f"   [OFF-MARKET] Checking for missing properties in {city}, {state} (location-based)")
            logger.debug(f"   [OFF-MARKET] Job start time: {job_start_time.isoformat()}")
            
            # Query database for properties in this city/state that weren't scraped recently
            # For one-time jobs, we check properties with last_scraped < job_start_time OR no last_scraped
            query = {
                "address.city": {"$regex": f"^{city}$", "$options": "i"},
                "address.state": {"$regex": f"^{state}$", "$options": "i"},
                "listing_type": {"$in": listing_types_to_check},
                "$or": [
                    {"last_scraped": {"$lt": job_start_time}},
                    {"last_scraped": None},
                    {"last_scraped": {"$exists": False}}
                ]
            }
            
            # Optimize query: only fetch needed fields
            cursor = db.properties_collection.find(
                query,
                {"property_id": 1, "address": 1, "status": 1, "mls_status": 1, "listing_type": 1}
            ).sort("last_scraped", 1).limit(10000)
            
            # Store lightweight property data (already filtered to missing properties)
            missing_properties = []
            async for prop_data in cursor:
                property_id = prop_data.get("property_id")
                if property_id and property_id not in found_property_ids:
                    missing_properties.append({
                        "property_id": property_id,
                        "address": prop_data.get("address", {}),
                        "status": prop_data.get("status"),
                        "mls_status": prop_data.get("mls_status"),
                        "listing_type": prop_data.get("listing_type")
                    })
            
            if not missing_properties:
                logger.debug(f"   [OFF-MARKET] No missing properties found in database for {city}, {state}")
                return {
                    "missing_checked": 0,
                    "off_market_found": 0,
                    "errors": 0,
                    "skipped": False
                }
            
            total_missing = len(missing_properties)
            logger.info(f"   [OFF-MARKET] Found {total_missing} missing properties, processing in batches of {batch_size}")
            
            # Use the same batch processing logic as scheduled job method
            off_market_count = 0
            error_count = 0
            total_checked = 0
            
            # Import history_tracker for recording changes
            from services.history_tracker import HistoryTracker
            history_tracker = HistoryTracker(db.db)
            
            # Process properties in batches until all are checked
            batch_number = 0
            while missing_properties:
                # Check cancellation flag
                if cancel_flag and cancel_flag.get("cancelled", False):
                    logger.info(f"   [OFF-MARKET] Job cancelled, stopping off-market check")
                    break
                
                batch_number += 1
                # Get next batch
                batch = missing_properties[:batch_size]
                remaining = len(missing_properties) - len(batch)
                
                logger.debug(f"   [OFF-MARKET] Processing batch {batch_number}: {len(batch)} properties ({(total_checked + len(batch))}/{total_missing} total, {remaining} remaining)")
                
                # Get location_entry for updating progress_logs
                location_entry = None
                if progress_logs and location:
                    for loc_entry in progress_logs.get("locations", []):
                        if loc_entry.get("location") == location:
                            location_entry = loc_entry
                            break
                
                # Update progress logs if provided
                if location_entry is not None and job_id:
                    try:
                        location_entry["off_market_check"] = {
                            "status": "in_progress",
                            "total_missing": total_missing,
                            "checked": total_checked,
                            "current_batch": batch_number,
                            "batch_size": len(batch),
                            "remaining": remaining,
                            "off_market_found": off_market_count,
                            "errors": error_count
                        }
                        await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                    except Exception as e:
                        logger.error(f"   [OFF-MARKET] Error updating progress logs: {e}")
                
                # Track off-market per listing type
                off_market_by_listing_type = {}
                
                # Check each property in batch
                for i, prop_data in enumerate(batch):
                    # Check cancellation flag
                    if cancel_flag and cancel_flag.get("cancelled", False):
                        logger.info(f"   [OFF-MARKET] Job cancelled, stopping off-market check")
                        break
                    
                    try:
                        property_id = prop_data["property_id"]
                        address = prop_data.get("address", {})
                        formatted_address = address.get("formatted_address") if address else None
                        listing_type = prop_data.get("listing_type")
                        
                        # Query property directly by address
                        if not formatted_address:
                            logger.debug(f"   [OFF-MARKET] Property {property_id} has no formatted_address, skipping")
                            total_checked += 1
                            continue
                        
                        current_num = total_checked + i + 1
                        logger.debug(f"   [OFF-MARKET] Checking property {current_num}/{total_missing} (batch {batch_number}): {formatted_address}")
                        
                        queried_property = await self.query_property_by_address(
                            formatted_address,
                            proxy_config
                        )
                        
                        if queried_property:
                            # Check if property is off-market
                            if self.is_off_market_status(queried_property.status, queried_property.mls_status):
                                logger.info(f"   [OFF-MARKET] [OK] Property {property_id} is OFF_MARKET (status={queried_property.status}, mls_status={queried_property.mls_status})")
                                
                                # Update property status
                                old_status = prop_data.get("status") or 'UNKNOWN'
                                await db.properties_collection.update_one(
                                    {"property_id": property_id},
                                    {
                                        "$set": {
                                            "status": "OFF_MARKET",
                                            "mls_status": queried_property.mls_status or prop_data.get("mls_status"),
                                            "scraped_at": datetime.utcnow()
                                        }
                                    }
                                )
                                
                                # Record status change in change_logs
                                try:
                                    change_entry = {
                                        "field": "status",
                                        "old_value": old_status,
                                        "new_value": "OFF_MARKET",
                                        "change_type": "modified",
                                        "timestamp": datetime.utcnow()
                                    }
                                    await history_tracker.record_change_logs(
                                        property_id,
                                        [change_entry],
                                        "off_market_detection"
                                    )
                                except Exception as e:
                                    logger.error(f"   [OFF-MARKET] Error recording change log: {e}")
                                
                                off_market_count += 1
                                # Track per listing type (only for for_sale and pending)
                                if listing_type in ['for_sale', 'pending']:
                                    off_market_by_listing_type[listing_type] = off_market_by_listing_type.get(listing_type, 0) + 1
                                    
                                    # Update progress_logs per listing type
                                    if location_entry and listing_type in location_entry.get("listing_types", {}):
                                        location_entry["listing_types"][listing_type]["off_market"] = \
                                            location_entry["listing_types"][listing_type].get("off_market", 0) + 1
                            else:
                                logger.debug(f"   [OFF-MARKET] Property {property_id} still active (status={queried_property.status}, mls_status={queried_property.mls_status})")
                        else:
                            logger.debug(f"   [OFF-MARKET] Property {property_id} not found in HomeHarvest (may be deleted)")
                        
                        # Add delay between queries to avoid rate limiting
                        if i < len(batch) - 1:  # Don't delay after last property in batch
                            await asyncio.sleep(1.0)  # 1 second delay between queries
                        
                    except Exception as e:
                        error_count += 1
                        logger.error(f"   [OFF-MARKET] Error checking property {prop_data.get('property_id', 'unknown')}: {e}")
                        continue
                
                # Remove processed batch from list
                total_checked += len(batch)
                missing_properties = missing_properties[batch_size:]
                
                # Add delay between batches (shorter delay)
                if missing_properties:
                    logger.debug(f"   [OFF-MARKET] Batch {batch_number} complete: {off_market_count} off-market found so far, {len(missing_properties)} remaining")
                    await asyncio.sleep(2.0)  # 2 second delay between batches
            
            # Update final off-market check status in progress_logs
            if location_entry and job_id:
                try:
                    location_entry["off_market_check"]["status"] = "completed"
                    location_entry["off_market_check"]["checked"] = total_checked
                    location_entry["off_market_check"]["found"] = off_market_count
                    location_entry["off_market_check"]["errors"] = error_count
                    await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                except Exception as e:
                    logger.error(f"   [OFF-MARKET] Error updating final progress logs: {e}")
            
            result = {
                "missing_checked": total_checked,
                "missing_total": total_missing,
                "missing_skipped": 0,  # No longer skipping - all are checked
                "off_market_found": off_market_count,
                "errors": error_count,
                "batches_processed": batch_number,
                "skipped": False
            }
            
            logger.info(f"   [OFF-MARKET] Check complete: {total_checked} checked, {off_market_count} off-market, {error_count} errors, {batch_number} batches")
            return result
            
        except Exception as e:
            logger.error(f"   [OFF-MARKET] Error in off-market detection (location-based): {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return {
                "missing_checked": 0,
                "off_market_found": 0,
                "errors": 1,
                "skipped": False,
                "error": str(e)
            }
    
    async def _run_off_market_detection_background(
        self,
        job: ScrapingJob,
        location: str,
        listing_types_to_scrape: List[str],
        found_property_ids: set[str],
        job_start_time: datetime,
        proxy_config: Optional[Dict[str, Any]],
        cancel_flag: Optional[dict],
        progress_logs: Optional[List[Dict[str, Any]]]
    ):
        """Background task to run off-market detection without blocking location completion"""
        try:
            if job.scheduled_job_id:
                # Scheduled job: use scheduled_job_id with location filter
                await self.check_missing_properties_for_off_market(
                    scheduled_job_id=job.scheduled_job_id,
                    listing_types_scraped=listing_types_to_scrape,
                    found_property_ids=found_property_ids,
                    job_start_time=job_start_time,
                    proxy_config=proxy_config,
                    cancel_flag=cancel_flag,
                    batch_size=50,
                    progress_logs=progress_logs,
                    job_id=job.job_id,
                    location=location  # Filter by specific location (zip code or city/state)
                )
            else:
                # One-time job: use location-based matching
                await self.check_missing_properties_for_off_market_by_location(
                    location=location,
                    listing_types_scraped=listing_types_to_scrape,
                    found_property_ids=found_property_ids,
                    job_start_time=job_start_time,
                    proxy_config=proxy_config,
                    cancel_flag=cancel_flag,
                    batch_size=50,
                    progress_logs=progress_logs,
                    job_id=job.job_id
                )
        except Exception as e:
            logger.error(f"Error in background off-market detection for location {location}: {e}")
    
    async def immediate_scrape(self, locations: List[str], listing_type: str, **kwargs) -> str:
        """Perform immediate scraping with high priority"""
        try:
            # Create high-priority job
            job = ScrapingJob(
                job_id=f"immediate_{int(time.time())}_{random.randint(1000, 9999)}",
                priority=JobPriority.IMMEDIATE,
                locations=locations,
                listing_type=listing_type,
                limit=kwargs.get('limit', 100),
                **{k: v for k, v in kwargs.items() if k != 'limit'}
            )
            
            # Save job to database
            await db.create_job(job)
            
            # Process immediately
            asyncio.create_task(self.process_job(job))
            
            return job.job_id
            
        except Exception as e:
            print(f"Error creating immediate scrape job: {e}")
            raise

# Global scraper instance
scraper = MLSScraper()
