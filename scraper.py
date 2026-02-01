import asyncio
import random
import re
import time
import logging
import traceback
import secrets
import os
import json

# Ensure curl_cffi is available and configured for anti-bot measures
# homeharvest should automatically use curl_cffi if available
_curl_cffi_available = False
try:
    import curl_cffi
    _curl_cffi_available = True
    # Set environment variable to ensure curl_cffi is used (if homeharvest checks for it)
    os.environ.setdefault("HOMEHARVEST_USE_CURL_CFFI", "true")
except ImportError:
    _curl_cffi_available = False

def _is_realtor_block_exception(exc: Exception) -> bool:
    msg = str(exc).lower()
    name = type(exc).__name__.lower()
    if "retryerror" in name or "retryerror" in msg:
        return True
    if "403" in msg or "forbidden" in msg:
        return True
    if "realtor" in msg and ("blocked" in msg or "forbidden" in msg):
        return True
    return False

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
import jwt
import httpx
import pandas as pd
import numpy as np
from homeharvest import scrape_property
from models import ScrapingJob, Property, PropertyAddress, PropertyDescription, PropertyFinancial, PropertyDates, PropertyLocation, PropertyAgent, PropertyBroker, PropertyBuilder, PropertyOffice, JobStatus, JobPriority
from database import db
from proxy_manager import proxy_manager
from config import settings

logger = logging.getLogger(__name__)

# Log curl_cffi availability at module load
if _curl_cffi_available:
    logger.info("✅ curl_cffi is available - TLS fingerprinting should be enabled for anti-bot measures")
else:
    logger.warning("⚠️  curl_cffi is not available - anti-bot measures may be limited. Install with: pip install curl-cffi")

# Verify homeharvest's curl_cffi status after import
# HomeHarvestLocal should export USE_CURL_CFFI from homeharvest.core.scrapers
# This check is informational and not critical - the service will work regardless
try:
    import homeharvest.core.scrapers as scrapers_module
    # Try direct import first (most reliable)
    try:
        USE_CURL_CFFI = scrapers_module.USE_CURL_CFFI
        DEFAULT_IMPERSONATE = getattr(scrapers_module, 'DEFAULT_IMPERSONATE', None)
        if USE_CURL_CFFI:
            logger.info(f"✅ [HOMEHARVEST] curl_cffi is enabled in homeharvest with impersonate={DEFAULT_IMPERSONATE}")
        else:
            logger.warning(f"⚠️  [HOMEHARVEST] curl_cffi is NOT enabled in homeharvest (DEFAULT_IMPERSONATE={DEFAULT_IMPERSONATE})")
            if _curl_cffi_available:
                logger.error("❌ [HOMEHARVEST] curl_cffi is available in scraper.py but NOT in homeharvest! This indicates an import issue.")
    except AttributeError:
        # Fallback: check if it exists using getattr
        USE_CURL_CFFI = getattr(scrapers_module, 'USE_CURL_CFFI', None)
        DEFAULT_IMPERSONATE = getattr(scrapers_module, 'DEFAULT_IMPERSONATE', None)
        if USE_CURL_CFFI is not None:
            if USE_CURL_CFFI:
                logger.info(f"✅ [HOMEHARVEST] curl_cffi is enabled in homeharvest (via getattr) with impersonate={DEFAULT_IMPERSONATE}")
            else:
                logger.warning(f"⚠️  [HOMEHARVEST] curl_cffi is NOT enabled in homeharvest (DEFAULT_IMPERSONATE={DEFAULT_IMPERSONATE})")
        else:
            # Variable doesn't exist in this version
            if _curl_cffi_available:
                logger.debug(f"[HOMEHARVEST] USE_CURL_CFFI not found in homeharvest.core.scrapers, but curl_cffi is available and HOMEHARVEST_USE_CURL_CFFI is set")
            else:
                logger.debug(f"[HOMEHARVEST] USE_CURL_CFFI not found in homeharvest.core.scrapers (version may not export it)")
except ImportError as e:
    # Can't even import the module
    logger.debug(f"[HOMEHARVEST] Could not import homeharvest.core.scrapers: {e}")
except Exception as e:
    # Unexpected error during check - log at debug level since it's non-critical
    logger.debug(f"[HOMEHARVEST] Could not check curl_cffi status in homeharvest: {e}")


async def _delete_old_sold_properties() -> int:
    """Delete SOLD properties whose sold date is more than 1 year ago.
    Runs after each completed scraping job to enforce retention: we only keep sold
    data for the last year (scheduled jobs typically scrape 180 days; this cleans
    legacy and any API slip-through). Uses dates.last_sold_date or last_sold_date.
    """
    try:
        cutoff = datetime.utcnow() - timedelta(days=365)
        query = {
            "listing_type": "sold",
            "$or": [
                {"dates.last_sold_date": {"$lt": cutoff}},
                {"last_sold_date": {"$lt": cutoff}},
            ],
        }
        result = await db.properties_collection.delete_many(query)
        if result.deleted_count > 0:
            logger.info(
                "[Old sold cleanup] Deleted %d sold properties with sold date > 1 year ago (cutoff: %s)",
                result.deleted_count,
                cutoff.isoformat(),
            )
        return result.deleted_count
    except Exception as e:
        logger.warning("[Old sold cleanup] Failed to delete old sold properties: %s", e)
        return 0


async def _notify_census_backfill() -> None:
    """POST to backend /api/census/backfill-properties to backfill tract geoid/class for properties missing them.
    Auth: JWT signed with JWT_SECRET (aud: census-backfill). Skipped if BACKEND_URL is not set.
    Fire-and-forget; errors are logged only.
    """
    url = getattr(settings, "BACKEND_URL", "") or ""
    if not url.strip():
        return
    secret = getattr(settings, "JWT_SECRET", "") or ""
    if not secret:
        return
    now = datetime.now(timezone.utc)
    payload = {"sub": "scraper", "aud": "census-backfill", "iat": int(now.timestamp()), "exp": int(now.timestamp()) + 300}
    try:
        token = jwt.encode(payload, secret, algorithm="HS256")
    except Exception as e:
        logger.warning(f"[Census backfill] Failed to create JWT: {e}")
        return
    endpoint = f"{url.rstrip('/')}/api/census/backfill-properties"
    try:
        async with httpx.AsyncClient(timeout=300.0) as client:
            r = await client.post(
                endpoint,
                json={"geoidOnlyMissing": True, "classOnlyMissing": True},
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
            )
            if r.is_success:
                logger.info(f"[Census backfill] Backend backfill-properties completed: {r.status_code}")
            else:
                logger.warning(f"[Census backfill] Backend backfill-properties returned {r.status_code}: {r.text[:200]}")
    except Exception as e:
        logger.warning(f"[Census backfill] Failed to call backend backfill-properties: {e}")


class MLSScraper:
    def __init__(self):
        self.is_running = False
        self.current_jobs = {}
        self.executor = ThreadPoolExecutor(max_workers=settings.THREAD_POOL_WORKERS)  # Thread pool for blocking operations
        # Use asyncio.Semaphore instead of ThreadPoolExecutor for enrichment to avoid event loop conflicts
        self.enrichment_semaphore = asyncio.Semaphore(settings.ENRICHMENT_WORKERS)  # Limit concurrent enrichment tasks
        self.rentcast_semaphore = asyncio.Semaphore(settings.RENTCAST_WORKERS)  # Limit concurrent RentCast tasks
        self._homeharvest_inflight = 0  # Tracks active HomeHarvest fetches to prioritize scraping
        self._homeharvest_lock = asyncio.Lock()
        self._rentcast_retry_queues: Dict[str, asyncio.Queue] = {}
        self._rentcast_retry_tasks: Dict[str, asyncio.Task] = {}
        # Track if job runs were full scrapes (keyed by job_id)
        self.job_run_flags: Dict[str, Dict[str, Any]] = {}
        # Global rate limiter: Only allow 1 request to Realtor.com at a time across ALL jobs
        # This prevents multiple parallel jobs from overwhelming Realtor.com with simultaneous requests
        # Even if 3-5 jobs run in parallel, they'll queue their requests through this semaphore
        self.global_request_semaphore = asyncio.Semaphore(1)  # Only 1 concurrent request to Realtor.com
        # No need to track last_request_time - gateway manages rate limiting
    
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
                        # Add to current_jobs BEFORE creating task to prevent race condition
                        # This ensures only one instance of a job can be processed at a time
                        self.current_jobs[job.job_id] = job
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

    async def _increment_homeharvest_inflight(self) -> None:
        """Track HomeHarvest activity so Rentcast can yield priority."""
        async with self._homeharvest_lock:
            self._homeharvest_inflight += 1

    async def _decrement_homeharvest_inflight(self) -> None:
        """Track HomeHarvest activity so Rentcast can yield priority."""
        async with self._homeharvest_lock:
            if self._homeharvest_inflight > 0:
                self._homeharvest_inflight -= 1

    async def _is_homeharvest_busy(self) -> bool:
        """Return True when HomeHarvest fetches are active (best-effort gate for Rentcast)."""
        async with self._homeharvest_lock:
            return self._homeharvest_inflight > 0

    def _get_rentcast_retry_queue(self, job_id: Optional[str]) -> Optional[asyncio.Queue]:
        if not job_id:
            return None
        if job_id not in self._rentcast_retry_queues:
            self._rentcast_retry_queues[job_id] = asyncio.Queue()
        return self._rentcast_retry_queues[job_id]

    async def _enqueue_rentcast_retry(
        self,
        job_id: Optional[str],
        property_id: str,
        property_dict: Dict[str, Any],
    ) -> None:
        queue = self._get_rentcast_retry_queue(job_id)
        if queue is None:
            return
        await queue.put(
            {
                "property_id": property_id,
                "property_dict": property_dict,
                "job_id": job_id,
            }
        )

    async def _drain_rentcast_retry_queue(self, job_id: str) -> None:
        queue = self._get_rentcast_retry_queue(job_id)
        if queue is None:
            return
        # Best-effort: keep this lightweight and yield to HomeHarvest if it resumes.
        while not queue.empty():
            if await self._is_homeharvest_busy():
                await asyncio.sleep(2)
                continue
            try:
                await asyncio.wait_for(self.rentcast_semaphore.acquire(), timeout=0.5)
            except asyncio.TimeoutError:
                await asyncio.sleep(1)
                continue
            try:
                item = await queue.get()
            except Exception:
                self.rentcast_semaphore.release()
                continue
            try:
                from services.rentcast_service import RentcastService
                if not hasattr(self, "_rentcast_service"):
                    self._rentcast_service = RentcastService(db)
                await self._rentcast_service.fetch_and_save_rent_estimate(
                    item["property_id"],
                    item["property_dict"],
                )
            except Exception as e:
                logger.warning(f"Rentcast retry failed for {item.get('property_id')}: {e}")
            finally:
                self.rentcast_semaphore.release()
                queue.task_done()

    async def process_job(self, job: ScrapingJob):
        """Process a single scraping job"""
        try:
            # Update status to RUNNING immediately to prevent jobs from staying in PENDING
            await db.update_job_status(job.job_id, JobStatus.RUNNING)
            logger.info(f"Starting job {job.job_id} - status updated to RUNNING")
        except Exception as e:
            logger.error(f"Failed to update job {job.job_id} status to RUNNING: {e}")
            # Continue anyway - we'll try again below
        
        # Job was already added to current_jobs in the main loop to prevent race conditions
        # Just verify it's there (it should be)
        if job.job_id not in self.current_jobs:
            self.current_jobs[job.job_id] = job
        
        # Cancellation flag shared between main task and monitor
        cancel_flag = {"cancelled": False}
        
        # Track if this job run was a full scrape (vs incremental)
        # This will be set in _scrape_listing_type and used when updating run history
        # Store in class-level dictionary since ScrapingJob is a Pydantic model
        # Preserve any existing flags (like force_full_scrape) that were set before process_job started
        if job.job_id not in self.job_run_flags:
            self.job_run_flags[job.job_id] = {}
        self.job_run_flags[job.job_id]["was_full_scrape"] = None  # None = not determined yet, True/False = determined
        
        # Track last property update time per location for timeout detection
        location_last_update = {}  # {location: datetime}
        
        # Start background cancellation monitor
        monitor_task = None
        location_timeout_monitor = None
        try:
            monitor_task = asyncio.create_task(self.check_cancellation_loop(job.job_id, cancel_flag))
            
            # Start background location timeout monitor
            location_timeout_monitor = asyncio.create_task(
                self.check_location_timeout_loop(job.job_id, cancel_flag, location_last_update)
            )
        except Exception as e:
            logger.error(f"Error starting background monitors for job {job.job_id}: {e}")
        
        try:
            # Determine scrape type early (for scheduled jobs and forced scrapes)
            scrape_type = "full"  # Default for manual jobs
            scrape_type_details = None
            
            # First, check if force_full_scrape was set when triggering the job manually
            force_full_scrape = self.job_run_flags.get(job.job_id, {}).get("force_full_scrape")
            if force_full_scrape is not None:
                # User explicitly chose full or incremental when triggering
                if force_full_scrape:
                    scrape_type = "full"
                    scrape_type_details = "Forced full scrape (user selected)"
                else:
                    scrape_type = "incremental"
                    scrape_type_details = "Forced incremental scrape (user selected)"
            elif job.scheduled_job_id:
                try:
                    scheduled_job = await db.get_scheduled_job(job.scheduled_job_id)
                    if scheduled_job:
                        incremental_config = scheduled_job.incremental_runs_before_full
                        incremental_count = scheduled_job.incremental_runs_count or 0
                        
                        if incremental_config is None:
                            # None = always incremental (if last_run_at exists)
                            if scheduled_job.last_run_at:
                                scrape_type = "incremental"
                                scrape_type_details = f"Always incremental (count: {incremental_count}/∞)"
                            else:
                                scrape_type = "full"
                                scrape_type_details = "First run (no previous run)"
                        elif incremental_config == 0:
                            # 0 = always full scrape
                            scrape_type = "full"
                            scrape_type_details = "Always full scrape (config: 0)"
                        else:
                            # > 0 = do incremental until count reaches config, then full scrape
                            if incremental_count >= incremental_config:
                                scrape_type = "full"
                                scrape_type_details = f"Full scrape (count: {incremental_count}/{incremental_config})"
                            else:
                                scrape_type = "incremental"
                                scrape_type_details = f"Incremental scrape (count: {incremental_count}/{incremental_config})"
                except Exception as e:
                    logger.warning(f"Could not determine scrape type for scheduled job {job.scheduled_job_id}: {e}")
            
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
                    "listing_types": job.listing_types or (job.listing_type and [job.listing_type]) or ["for_sale", "sold", "pending", "for_rent", "off_market"],
                    "scrape_type": scrape_type,
                    "scrape_type_details": scrape_type_details
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
                                
                                # Anti-blocking: Add delay between locations to avoid rapid-fire patterns
                                # This gives Realtor.com time to "forget" us between locations
                                if i < len(job.locations) - 1:  # Don't delay after last location
                                    between_location_delay = random.uniform(15.0, 30.0)  # 15-30 seconds between locations
                                    logger.debug(f"   [THROTTLE] Waiting {between_location_delay:.1f}s before next location to avoid blocking")
                                    await asyncio.sleep(between_location_delay)
                            
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
                # Increased delays to reduce blocking: 3-5 seconds between locations
                if not location_failed:
                    delay = max(job.request_delay, 3.0) + random.uniform(0, 2)  # 3-5 seconds
                    logger.debug(f"   [THROTTLE] Waiting {delay:.1f}s before next location to avoid blocking")
                    await asyncio.sleep(delay)
                else:
                    # Longer delay after failures to avoid immediate retry with same proxy
                    delay = 2.0 + random.uniform(0, 1)  # 2-3 seconds
                    logger.debug(f"   [THROTTLE] Waiting {delay:.1f}s after failure before next location")
                    await asyncio.sleep(delay)
            
            # Retry failed locations once if there are any
            if failed_locations:
                logger.info(f"[RETRY] Retrying {len(failed_locations)} failed location(s)...")
                retry_failed_locations = []
                
                for idx, failed_location_entry in enumerate(failed_locations):
                    location = failed_location_entry["location"]
                    failed_location_entry["retry_count"] = failed_location_entry.get("retry_count", 0) + 1
                    
                    # Anti-blocking: Add delay before retry (exponential backoff)
                    if idx > 0:
                        retry_delay = random.uniform(5.0, 10.0)  # 5-10 seconds between retries
                        logger.debug(f"[RETRY] Waiting {retry_delay:.1f}s before retrying location {location}")
                        await asyncio.sleep(retry_delay)
                    else:
                        # First retry: shorter delay but still significant
                        retry_delay = random.uniform(3.0, 6.0)  # 3-6 seconds
                        logger.debug(f"[RETRY] Waiting {retry_delay:.1f}s before first retry")
                        await asyncio.sleep(retry_delay)
                    
                    # Anti-blocking: Rotate proxy on retry to get a fresh session
                    if getattr(settings, "USE_DATAIMPULSE", False):
                        logger.info(f"[RETRY] Rotating proxy for retry of location {location}...")
                        proxy_config = await self.get_proxy_config(job)
                        if proxy_config:
                            logger.info(f"[RETRY] Using new proxy: {proxy_config.get('proxy_host')}:{proxy_config.get('proxy_port')} username={proxy_config.get('proxy_username')}")
                    
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
            
            # CRITICAL: Verify all locations are done before marking as completed
            total_processed = successful_locations + len(failed_locations)
            all_locations_done = total_processed >= len(job.locations)
            
            if not all_locations_done:
                logger.error(
                    f"CRITICAL: Job {job.job_id} completion check failed! "
                    f"Expected {len(job.locations)} locations, but only {total_processed} were processed "
                    f"(successful: {successful_locations}, failed: {len(failed_locations)}). "
                    f"This should never happen - all locations should be processed before reaching this point."
                )
                # Still mark as completed if we're close (within 1 location) to handle edge cases
                if total_processed >= len(job.locations) - 1:
                    logger.warning(f"Allowing completion with {total_processed}/{len(job.locations)} locations (within tolerance)")
                    all_locations_done = True
                else:
                    # This is a real problem - don't mark as completed
                    raise Exception(f"Job completion check failed: {total_processed}/{len(job.locations)} locations processed")
            
            # Add completion info to summary
            # Get the scrape type from job_started (or determine it again if not set)
            scrape_type = progress_logs.get("job_started", {}).get("scrape_type", "full")
            scrape_type_details = progress_logs.get("job_started", {}).get("scrape_type_details")
            
            # Collect all errors from locations for error summary
            all_errors = []
            has_403_errors = False
            for loc_entry in progress_logs.get("locations", []):
                if loc_entry.get("error"):
                    error_text = str(loc_entry.get("error", "")).lower()
                    error_type = loc_entry.get("error_type", "Unknown")
                    all_errors.append({
                        "location": loc_entry.get("location"),
                        "error": loc_entry.get("error"),
                        "error_type": error_type
                    })
                    # Check for 403/anti-bot errors
                    if "403" in error_text or "forbidden" in error_text or "anti-bot" in error_text or "blocked" in error_text:
                        has_403_errors = True
                # Also check listing_types for errors
                for listing_type, data in loc_entry.get("listing_types", {}).items():
                    if data.get("error"):
                        error_text = str(data.get("error", "")).lower()
                        error_type = data.get("error_type", "Unknown")
                        all_errors.append({
                            "location": loc_entry.get("location"),
                            "listing_type": listing_type,
                            "error": data.get("error"),
                            "error_type": error_type
                        })
                        # Check for 403/anti-bot errors
                        if "403" in error_text or "forbidden" in error_text or "anti-bot" in error_text or "blocked" in error_text:
                            has_403_errors = True
            
            # Determine final status based on results and errors
            # Get final property counts from running_totals
            total_properties = running_totals.get("total_properties", 0)
            saved_properties = running_totals.get("saved_properties", 0)
            
            # Determine if job should be marked as FAILED
            should_fail = False
            error_message = None
            
            # Check conditions for failure:
            # 1. Zero properties AND errors present
            if total_properties == 0 and saved_properties == 0 and len(all_errors) > 0:
                should_fail = True
                if has_403_errors:
                    error_message = f"Job failed: 0 properties scraped due to 403 Forbidden (anti-bot blocking). {successful_locations}/{len(job.locations)} locations processed. {len(all_errors)} error(s) occurred. Realtor.com is blocking requests - check proxy configuration and TLS fingerprinting."
                else:
                    error_message = f"Job failed: 0 properties scraped with {len(all_errors)} error(s). {successful_locations}/{len(job.locations)} locations processed successfully."
            # 2. All locations failed
            elif successful_locations == 0 and len(failed_locations) > 0:
                should_fail = True
                error_message = f"Job failed: All {len(failed_locations)} location(s) failed. {total_properties} properties scraped, {saved_properties} saved."
            # 3. Critical errors (403 blocking) even if some properties were found
            elif has_403_errors and total_properties == 0:
                should_fail = True
                error_message = f"Job failed: 403 Forbidden errors detected. 0 properties scraped. {successful_locations}/{len(job.locations)} locations processed. Realtor.com is blocking requests - check proxy configuration."
            
            if should_fail:
                final_status = JobStatus.FAILED
                completion_message = f"Job failed. {successful_locations}/{len(job.locations)} locations processed. {total_properties} properties scraped, {saved_properties} saved."
                logger.warning(f"[COMPLETION] Job {job.job_id} marked as FAILED: {error_message}")
            else:
                final_status = JobStatus.COMPLETED
                completion_message = f"Job completed successfully. {successful_locations}/{len(job.locations)} locations processed."
                if failed_locations:
                    completion_message += f" {len(failed_locations)} location(s) failed after retry."
                    logger.info(f"[SUMMARY] Job {job.job_id} completed with {len(failed_locations)} failed location(s) out of {len(job.locations)} total")
            
            progress_logs["job_completed"] = {
                "timestamp": datetime.utcnow().isoformat(),
                "event": "job_completed",
                "message": completion_message,
                "successful_locations": successful_locations,
                "failed_locations": len(failed_locations),
                "total_locations": len(job.locations),
                "scrape_type": scrape_type,
                "scrape_type_details": scrape_type_details,
                "has_errors": len(all_errors) > 0,
                "error_count": len(all_errors),
                "errors": all_errors if all_errors else None,
                "total_properties": total_properties,
                "saved_properties": saved_properties
            }
            
            # CRITICAL: Update job status (COMPLETED or FAILED) with ALL completion data in ONE atomic operation
            # This ensures completed_at, status, and all location counts are set together
            status_label = "COMPLETED" if final_status == JobStatus.COMPLETED else "FAILED"
            logger.info(f"[COMPLETION] All {len(job.locations)} locations processed. Updating job {job.job_id} status to {status_label}...")
            status_update_success = False
            max_retries = 5  # Increased retries for reliability
            for retry in range(max_retries):
                try:
                    update_success = await db.update_job_status(
                        job.job_id,
                        final_status,  # COMPLETED or FAILED
                        completed_locations=successful_locations,
                        total_locations=len(job.locations),
                        properties_scraped=total_properties,
                        properties_saved=saved_properties,
                        properties_inserted=total_inserted,
                        properties_updated=total_updated,
                        properties_skipped=total_skipped,
                        failed_locations=failed_locations,
                        progress_logs=progress_logs,
                        error_message=error_message,  # Include error message if status is FAILED
                        current_location=None,  # Clear current_location when job completes
                        current_location_index=None
                    )
                    
                    if update_success:
                        # CRITICAL: Verify the update was actually saved to the database
                        await asyncio.sleep(0.2)  # Brief delay to allow DB write to propagate
                        verify_job = await db.get_job(job.job_id)
                        if verify_job:
                            # Handle both enum and string status values
                            verify_status = verify_job.status
                            if isinstance(verify_status, JobStatus):
                                verify_status_value = verify_status.value
                            else:
                                verify_status_value = str(verify_status).lower()
                            
                            if verify_status_value == final_status.value:
                                status_update_success = True
                                logger.info(
                                    f"[COMPLETION] ✅ Successfully updated and verified job {job.job_id} status to {status_label} "
                                    f"(attempt {retry + 1}/{max_retries}). "
                                    f"completed_locations={successful_locations}, total_locations={len(job.locations)}"
                                )
                                break
                            else:
                                logger.warning(
                                    f"[COMPLETION] ⚠️ Job {job.job_id} status update reported success but verification failed "
                                    f"(attempt {retry + 1}/{max_retries}). "
                                    f"Expected {status_label}, got {verify_status_value}. Retrying..."
                                )
                                await asyncio.sleep(0.5)  # Brief delay before retry
                        else:
                            logger.warning(f"[COMPLETION] ⚠️ Job {job.job_id} not found after update (attempt {retry + 1}). Retrying...")
                            await asyncio.sleep(0.5)
                    else:
                        logger.warning(f"[COMPLETION] ⚠️ Failed to update job {job.job_id} status to COMPLETED (attempt {retry + 1}/{max_retries}). Retrying...")
                        await asyncio.sleep(0.5)  # Brief delay before retry
                except Exception as update_error:
                    logger.error(f"[COMPLETION] ❌ Error updating job {job.job_id} status (attempt {retry + 1}): {update_error}")
                    await asyncio.sleep(0.5)
            
            if not status_update_success:
                logger.error(f"[COMPLETION] ❌ CRITICAL: Failed to update job {job.job_id} status to {status_label} after {max_retries} attempts!")
                # Still try to update in finally block as last resort
                # But also try one more direct update here
                try:
                    logger.warning(f"[COMPLETION] Attempting direct MongoDB update as last resort...")
                    update_doc = {
                        "status": final_status.value,
                        "completed_at": datetime.utcnow(),
                        "completed_locations": successful_locations,
                        "total_locations": len(job.locations),
                        "updated_at": datetime.utcnow()
                    }
                    if error_message:
                        update_doc["error_message"] = error_message
                    await db.jobs_collection.update_one(
                        {"job_id": job.job_id},
                        {"$set": update_doc}
                    )
                    # Verify one more time
                    final_verify = await db.get_job(job.job_id)
                    if final_verify:
                        verify_status_val = final_verify.status.value if isinstance(final_verify.status, JobStatus) else str(final_verify.status).lower()
                        if verify_status_val == final_status.value:
                            logger.info(f"[COMPLETION] ✅ Direct MongoDB update succeeded for job {job.job_id}")
                            status_update_success = True
                        else:
                            logger.error(f"[COMPLETION] ❌ Direct MongoDB update also failed for job {job.job_id} - expected {final_status.value}, got {verify_status_val}")
                    else:
                        logger.error(f"[COMPLETION] ❌ Direct MongoDB update failed - job {job.job_id} not found")
                except Exception as direct_update_error:
                    logger.error(f"[COMPLETION] ❌ Direct MongoDB update error: {direct_update_error}")
            
            # Update run history for scheduled jobs (new architecture)
            # Wrap in try-except to prevent job from being marked as failed if this update fails
            # This is less critical than the status update above
            try:
                if job.scheduled_job_id:
                    # Calculate next run time
                    scheduled_job = await db.get_scheduled_job(job.scheduled_job_id)
                    if scheduled_job and scheduled_job.cron_expression:
                        import croniter
                        cron = croniter.croniter(scheduled_job.cron_expression, datetime.utcnow())
                        next_run = cron.get_next(datetime)
                        
                        was_full_scrape = self.job_run_flags.get(job.job_id, {}).get("was_full_scrape")
                        await db.update_scheduled_job_run_history(
                            job.scheduled_job_id,
                            job.job_id,
                            final_status,  # Use final_status (COMPLETED or FAILED)
                            next_run_at=next_run,
                            was_full_scrape=was_full_scrape,
                            error_message=error_message,  # Include error message if status is FAILED
                            properties_scraped=total_properties,  # Include property counts
                            properties_saved=saved_properties
                        )
                        logger.debug(f"Updated run history for scheduled job: {job.scheduled_job_id} (was_full_scrape={was_full_scrape}, status={status_label}, properties={total_properties} scraped, {saved_properties} saved)")
                
                # Legacy: Update run history for old recurring jobs
                elif job.original_job_id:
                    await db.update_recurring_job_run_history(
                        job.original_job_id,
                        job.job_id,
                        final_status  # Use final_status (COMPLETED or FAILED)
                    )
                    logger.debug(f"Updated run history for legacy recurring job: {job.original_job_id} (status={status_label})")
            except Exception as run_history_error:
                # Log the error but don't fail the job - the job itself completed successfully
                logger.warning(
                    f"Failed to update scheduled job run history for job {job.job_id}, "
                    f"but job completed successfully: {run_history_error}"
                )

            # Delete sold properties with sold date > 1 year (enforce retention; past_days limits only new fetches)
            if status_update_success and final_status == JobStatus.COMPLETED:
                await _delete_old_sold_properties()

            # Notify backend to backfill census tract data for properties missing geoid/class (post-scrape)
            if status_update_success and final_status == JobStatus.COMPLETED and getattr(settings, "BACKEND_URL", "").strip():
                asyncio.create_task(_notify_census_backfill())

            logger.info(f"Job {job.job_id} {status_label.lower()}: {saved_properties} properties saved")

            # Best-effort: retry any Rentcast items skipped during scraping
            if status_update_success and final_status == JobStatus.COMPLETED:
                if job.job_id not in self._rentcast_retry_tasks:
                    self._rentcast_retry_tasks[job.job_id] = asyncio.create_task(
                        self._drain_rentcast_retry_queue(job.job_id)
                    )
            
            # FINAL SAFETY CHECK: Ensure status matches final_status before exiting try block
            # This is the last chance to fix it if something went wrong
            final_check = await db.get_job(job.job_id)
            if final_check and final_check.status != final_status:
                logger.error(
                    f"CRITICAL: Job {job.job_id} status is {final_check.status.value} instead of {status_label}! "
                    f"Force updating to {status_label}..."
                )
                await db.update_job_status(
                    job.job_id, 
                    final_status,  # Use final_status (COMPLETED or FAILED)
                    completed_locations=successful_locations,
                    total_locations=len(job.locations),
                    progress_logs=progress_logs,
                    error_message=error_message
                )
                # Verify one more time
                verify_final = await db.get_job(job.job_id)
                if verify_final and verify_final.status == final_status:
                    logger.info(f"Successfully force-updated job {job.job_id} to {status_label}")
                else:
                    logger.error(f"FAILED to force-update job {job.job_id} to {status_label}! Status: {verify_final.status.value if verify_final else 'NOT FOUND'}")
            
        except Exception as e:
            logger.error(f"Error processing job {job.job_id}: {e}")
            
            # Check if job was already marked as COMPLETED or FAILED before overwriting
            # This prevents jobs from being marked as failed due to post-completion errors
            current_job = await db.get_job(job.job_id)
            if current_job and current_job.status in [JobStatus.COMPLETED, JobStatus.FAILED]:
                status_str = getattr(current_job.status, 'value', current_job.status)
                logger.warning(
                    f"Job {job.job_id} encountered an error after {status_str}, "
                    f"but keeping status as {status_str} since job already finished. Error: {e}"
                )
                # Still try to update run history, but don't change job status
                try:
                    if job.scheduled_job_id:
                        scheduled_job = await db.get_scheduled_job(job.scheduled_job_id)
                        if scheduled_job and scheduled_job.cron_expression:
                            import croniter
                            cron = croniter.croniter(scheduled_job.cron_expression, datetime.utcnow())
                            next_run = cron.get_next(datetime)
                            
                            was_full_scrape = self.job_run_flags.get(job.job_id, {}).get("was_full_scrape")
                            # Try to get property counts from the job if available
                            properties_scraped = None
                            properties_saved = None
                            if current_job:
                                properties_scraped = current_job.get("properties_scraped") if isinstance(current_job, dict) else (current_job.properties_scraped if hasattr(current_job, 'properties_scraped') else None)
                                properties_saved = current_job.get("properties_saved") if isinstance(current_job, dict) else (current_job.properties_saved if hasattr(current_job, 'properties_saved') else None)
                            await db.update_scheduled_job_run_history(
                                job.scheduled_job_id,
                                job.job_id,
                                JobStatus.COMPLETED,
                                next_run_at=next_run,
                                was_full_scrape=was_full_scrape,
                                error_message=str(e) if e else None,  # Include error message even for completed jobs
                                properties_scraped=properties_scraped,
                                properties_saved=properties_saved
                            )
                    elif job.original_job_id:
                        await db.update_recurring_job_run_history(
                            job.original_job_id,
                            job.job_id,
                            JobStatus.COMPLETED
                        )
                except Exception as run_history_error:
                    logger.warning(f"Failed to update run history after completion error: {run_history_error}")
            else:
                # Job was not completed, so it's a real failure
                await db.update_job_status(
                    job.job_id,
                    JobStatus.FAILED,
                    error_message=str(e)
                )
                
                # Update run history for scheduled jobs (new architecture)
                try:
                    if job.scheduled_job_id:
                        scheduled_job = await db.get_scheduled_job(job.scheduled_job_id)
                        if scheduled_job and scheduled_job.cron_expression:
                            import croniter
                            cron = croniter.croniter(scheduled_job.cron_expression, datetime.utcnow())
                            next_run = cron.get_next(datetime)
                            
                            was_full_scrape = self.job_run_flags.get(job.job_id, {}).get("was_full_scrape")
                            # Try to get property counts from the job if available (might be partial)
                            properties_scraped = None
                            properties_saved = None
                            if current_job:
                                properties_scraped = current_job.get("properties_scraped") if isinstance(current_job, dict) else (current_job.properties_scraped if hasattr(current_job, 'properties_scraped') else None)
                                properties_saved = current_job.get("properties_saved") if isinstance(current_job, dict) else (current_job.properties_saved if hasattr(current_job, 'properties_saved') else None)
                            await db.update_scheduled_job_run_history(
                                job.scheduled_job_id,
                                job.job_id,
                                JobStatus.FAILED,
                                next_run_at=next_run,
                                was_full_scrape=was_full_scrape,
                                error_message=str(e),  # Include error message for failed jobs
                                properties_scraped=properties_scraped,
                                properties_saved=properties_saved
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
                except Exception as run_history_error:
                    logger.warning(f"Failed to update run history for failed job: {run_history_error}")
        
        finally:
            # Stop the cancellation and timeout monitors
            cancel_flag["cancelled"] = True  # Signal monitors to stop
            if monitor_task is not None:
                monitor_task.cancel()
                try:
                    await monitor_task
                except asyncio.CancelledError:
                    pass  # Expected
            if location_timeout_monitor is not None:
                location_timeout_monitor.cancel()
                try:
                    await location_timeout_monitor
                except asyncio.CancelledError:
                    pass  # Expected
            
            # Ensure job status is updated before removing from current_jobs
            # This is a safety net, but we need to be careful not to mark incomplete jobs as completed
            try:
                current_job = await db.get_job(job.job_id)
                
                if not current_job:
                    # Job doesn't exist, nothing to do
                    if job.job_id in self.current_jobs:
                        del self.current_jobs[job.job_id]
                elif current_job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                    # If status is already final, just remove from current_jobs
                    if job.job_id in self.current_jobs:
                        del self.current_jobs[job.job_id]
                        # Clean up job run flags
                        self.job_run_flags.pop(job.job_id, None)
                        status_str = current_job.status.value if hasattr(current_job.status, 'value') else str(current_job.status)
                        logger.debug(f"Removed job {job.job_id} from current_jobs (status: {status_str})")
                elif cancel_flag.get("cancelled", False):
                    # If status is still RUNNING, check if it was cancelled
                    # Job was cancelled - status should already be CANCELLED, but if not, don't change it here
                    # The cancellation monitor should have handled this
                    logger.debug(f"Job {job.job_id} was cancelled, keeping in current_jobs until status is updated")
                else:
                    # If status is RUNNING, check if all locations are actually done
                    # Only update to COMPLETED if we're certain all locations finished
                    progress_logs = current_job.progress_logs or {}
                    job_completed = progress_logs.get("job_completed")
                    completed_locations = current_job.completed_locations or 0
                    total_locations = current_job.total_locations or len(job.locations)
                    
                    # Only mark as COMPLETED/FAILED if we have explicit completion signal
                    if job_completed and completed_locations >= total_locations and total_locations > 0:
                        # Check job_completed data to determine if it should be FAILED
                        job_completed_data = progress_logs.get("job_completed", {})
                        has_errors = job_completed_data.get("has_errors", False)
                        error_count = job_completed_data.get("error_count", 0)
                        total_props = job_completed_data.get("total_properties", current_job.properties_scraped or 0)
                        saved_props = job_completed_data.get("saved_properties", current_job.properties_saved or 0)
                        
                        # Determine status: FAILED if zero properties with errors, otherwise COMPLETED
                        final_status_safety = JobStatus.COMPLETED
                        error_message_safety = None
                        if (total_props == 0 and saved_props == 0 and error_count > 0) or (completed_locations == 0 and total_locations > 0):
                            final_status_safety = JobStatus.FAILED
                            error_message_safety = f"Job failed: 0 properties scraped with {error_count} error(s). {completed_locations}/{total_locations} locations processed."
                        
                        status_label_safety = "COMPLETED" if final_status_safety == JobStatus.COMPLETED else "FAILED"
                        logger.warning(
                            f"Job {job.job_id} was still in RUNNING status but has completion signal. "
                            f"Updating status to {status_label_safety} in finally block (safety net)."
                        )
                        # Retry the status update with verification
                        # Get completed_locations from progress_logs if not available
                        if completed_locations == 0:
                            # Try to get from progress_logs summary
                            summary = progress_logs.get("summary", {})
                            completed_locations = summary.get("completed_locations", 0)
                            total_locations = summary.get("total_locations", total_locations)
                        
                        for retry in range(3):
                            update_success = await db.update_job_status(
                                job.job_id,
                                final_status_safety,  # Use determined status (COMPLETED or FAILED)
                                completed_locations=completed_locations,
                                total_locations=total_locations,
                                progress_logs=progress_logs,
                                error_message=error_message_safety
                            )
                            if update_success:
                                verify_job = await db.get_job(job.job_id)
                                if verify_job and verify_job.status == final_status_safety:
                                    current_job = verify_job
                                    logger.info(f"Successfully updated job {job.job_id} to {status_label_safety} in finally block (attempt {retry + 1})")
                                    break
                                else:
                                    await asyncio.sleep(0.5)
                            else:
                                await asyncio.sleep(0.5)
                        else:
                            logger.error(f"Failed to update job {job.job_id} to {status_label_safety} in finally block after 3 attempts")
                    
                    # Now remove from current_jobs only if status is final
                    if current_job and current_job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                        if job.job_id in self.current_jobs:
                            del self.current_jobs[job.job_id]
                            status_str = current_job.status.value if hasattr(current_job.status, 'value') else str(current_job.status)
                            logger.debug(f"Removed job {job.job_id} from current_jobs (status: {status_str})")
                    else:
                        # Keep in current_jobs - job is still running or status is unclear
                        if current_job:
                            status_str = current_job.status.value if hasattr(current_job.status, 'value') else str(current_job.status)
                        else:
                            status_str = 'unknown'
                        logger.debug(
                            f"Keeping job {job.job_id} in current_jobs "
                            f"(status: {status_str})"
                        )
            except Exception as e:
                logger.warning(f"Error in finally block while updating job status: {e}")
                # Don't remove from current_jobs if we can't verify/update status
    
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
                # Note: 'off_market' is included but will be inferred from status, not passed to homeharvest
                listing_types_to_scrape = ["for_sale", "sold", "pending", "for_rent", "off_market"]
                logger.debug(f"   [TARGET] Scraping ALL property types in {location} (default, including off_market detection)")
            
            # Enforce consistent order: for_sale, sold, pending, for_rent, off_market
            # Note: off_market properties are detected from status, not scraped directly
            preferred_order = ["for_sale", "sold", "pending", "for_rent", "off_market"]
            listing_types_to_scrape = sorted(
                listing_types_to_scrape,
                key=lambda x: preferred_order.index(x) if x in preferred_order else 999
            )
            
            # Filter out 'off_market' from listing_types passed to homeharvest (not supported by API)
            # but keep it in listing_types_to_scrape for status inference
            listing_types_for_homeharvest = [lt for lt in listing_types_to_scrape if lt != "off_market"]
            
            logger.debug(f"   [DEBUG] Listing types to scrape (ordered): {listing_types_to_scrape}")
            logger.debug(f"   [DEBUG] Listing types for homeharvest API: {listing_types_for_homeharvest}")
            logger.debug(f"   [NOTE] 'off_market' will be inferred from status field, not passed to homeharvest library")
            
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
                "rentcast": {
                    "status": "pending",
                    "total": 0,
                    "completed": 0,
                    "failed": 0,
                    "skipped": 0
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
                    "rentcast": 0,
                    "rentcast_skipped": 0,
                    "off_market": 0
                }
            
            # Find or create location entry in progress_logs
            location_idx = None
            original_location_index = None
            for idx, loc_entry in enumerate(progress_logs.get("locations", [])):
                if loc_entry.get("location") == location:
                    location_idx = idx
                    # Preserve the original location_index if this is a retry
                    original_location_index = loc_entry.get("location_index")
                    break
            
            if location_idx is not None:
                # Update existing entry (this is a retry)
                # Preserve the original location_index to maintain proper ordering
                if original_location_index is not None:
                    location_entry["location_index"] = original_location_index
                progress_logs["locations"][location_idx] = location_entry
            else:
                # Add new location entry
                progress_logs["locations"].append(location_entry)
                location_idx = len(progress_logs["locations"]) - 1
            
            # Update summary
            progress_logs["summary"]["in_progress_locations"] = sum(
                1 for loc in progress_logs["locations"] if loc.get("status") == "in_progress"
            )
            
            # Make a single API call with all listing types instead of 4 separate calls
            # This is more efficient and reduces proxy usage
            if cancel_flag.get("cancelled", False):
                logger.info(f"   [CANCELLED] Job was cancelled, stopping fetch")
            else:
                try:
                    # Use the proxy that was fetched at the start of this location
                    # Proxy is rotated once per location (not per listing type) to balance rate limiting and quota
                    if job.proxy_config:
                        # Job has specific proxy config, use it
                        current_proxy_config = proxy_config
                    else:
                        # Reuse the proxy that was fetched at the start of this location
                        current_proxy_config = proxy_config
                        if not current_proxy_config:
                            # No proxy available, try to get one
                            current_proxy_config = await self.get_proxy_config(job)
                            if current_proxy_config:
                                proxy_config = current_proxy_config  # Update for next location
                    # Proxy enforcement + sanitized logging
                    proxy_url = (current_proxy_config or {}).get("proxy_url")
                    proxy_host = (current_proxy_config or {}).get("proxy_host")
                    proxy_port = (current_proxy_config or {}).get("proxy_port")
                    proxy_username = (current_proxy_config or {}).get("proxy_username")
                    proxy_used = bool(proxy_url)

                    if getattr(settings, "USE_DATAIMPULSE", False) and not proxy_used:
                        # Don't crash the job; mark this location/types as failed and continue.
                        error_msg = "USE_DATAIMPULSE=true but no proxy_url could be resolved for this location."
                        error_type = "ProxyMissing"
                        logger.error(f"   [PROXY ERROR] {error_msg}")

                        if location_idx is not None and location_idx < len(progress_logs.get("locations", [])):
                            location_entry = progress_logs["locations"][location_idx]
                            location_entry["status"] = "failed"
                            location_entry["error"] = error_msg
                            location_entry["error_type"] = error_type
                            location_entry["proxy_used"] = False
                            location_entry["proxy_username"] = proxy_username
                            location_entry["timestamp"] = datetime.utcnow().isoformat()
                            for lt in listing_types_to_scrape:
                                if lt in location_entry.get("listing_types", {}):
                                    location_entry["listing_types"][lt]["error"] = error_msg
                                    location_entry["listing_types"][lt]["error_type"] = error_type

                        await db.update_job_status(job.job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                        return {
                            "inserted": 0,
                            "updated": 0,
                            "skipped": 0,
                            "enriched": 0,
                            "off_market": 0,
                            "failed": True,
                            "error": error_msg,
                        }

                    if proxy_used:
                        logger.info(f"   [PROXY] Using proxy {proxy_host}:{proxy_port} username={proxy_username}")

                    
                    start_time = datetime.utcnow()
                    # Show both the requested types and what's being sent to homeharvest
                    requested_types_str = ', '.join(listing_types_to_scrape)
                    homeharvest_types_str = ', '.join([lt for lt in listing_types_to_scrape if lt != "off_market"])
                    if "off_market" in listing_types_to_scrape:
                        logger.info(f"   [FETCH] Fetching listing types ({homeharvest_types_str}) from {location} in a single request (off_market will be inferred from status)...")
                    else:
                        logger.info(f"   [FETCH] Fetching all listing types ({requested_types_str}) from {location} in a single request...")
                    
                    # Update location_last_update when scraping starts (for timeout detection)
                    if location_last_update is not None:
                        location_last_update[location] = datetime.utcnow()
                    
                    # Update location entry to show we're fetching
                    if location_idx is not None and location_idx < len(progress_logs["locations"]):
                        progress_logs["locations"][location_idx]["timestamp"] = start_time.isoformat()
                    
                    # Push to database immediately for real-time UI update
                    await db.update_job_status(job.job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                    
                    # Use job's limit (or None to get all properties)
                    # Wrap in timeout to prevent getting stuck
                    scrape_timeout = min(settings.LOCATION_TIMEOUT_MINUTES * 60, 600)  # Max 10 minutes per location
                    scrape_error_info = None
                    try:
                        # Make a single call with all listing types
                        await self._increment_homeharvest_inflight()
                        try:
                            all_properties_by_type, scrape_error_info = await asyncio.wait_for(
                            self._scrape_all_listing_types(
                                location, 
                                job, 
                                current_proxy_config, 
                                listing_types_to_scrape,  # Pass full list including off_market for status inference
                                limit=job.limit if job.limit else None,
                                past_days=job.past_days if job.past_days else 90,
                                cancel_flag=cancel_flag,
                                location_last_update=location_last_update
                            ),
                                timeout=scrape_timeout
                            )
                        finally:
                            await self._decrement_homeharvest_inflight()
                    except asyncio.TimeoutError:
                        logger.warning(f"   [TIMEOUT] Scraping all listing types in {location} exceeded {scrape_timeout}s timeout")
                        all_properties_by_type = {lt: [] for lt in listing_types_to_scrape}  # Empty results for all types
                        scrape_error_info = f"Timeout after {scrape_timeout}s"
                    
                    end_time = datetime.utcnow()
                    location_total_found = sum(len(v) for v in all_properties_by_type.values())
                    was_full_scrape = self.job_run_flags.get(job.job_id, {}).get("was_full_scrape")
                    if was_full_scrape is True and location_total_found == 0:
                        # Check if there were 403 errors that caused the zero results
                        has_403_error = False
                        error_text_to_check = ""
                        
                        # First check the error returned from _scrape_all_listing_types
                        if scrape_error_info:
                            error_text_lower = str(scrape_error_info).lower()
                            error_text_original = str(scrape_error_info)
                            logger.info(f"   [403-CHECK] Checking scrape_error_info for 403 patterns: {scrape_error_info[:200]}")
                            # Check for various 403 error patterns
                            if any(pattern in error_text_lower for pattern in ["403", "forbidden", "received 403", "blocked", "anti-bot", "anti bot", "access denied"]):
                                has_403_error = True
                                logger.warning(f"   [403-DETECT] ✅ Found 403 error in scrape_error_info: {scrape_error_info[:200]}")
                            # Also check the original case-sensitive version for "Received 403" (common homeharvest error)
                            elif "Received 403" in error_text_original or "403 Forbidden" in error_text_original:
                                has_403_error = True
                                logger.warning(f"   [403-DETECT] ✅ Found 403 error (case-sensitive check) in scrape_error_info: {scrape_error_info[:200]}")
                        
                        # Also check progress_logs for errors (in case they were stored there)
                        if location_idx is not None and location_idx < len(progress_logs.get("locations", [])):
                            le = progress_logs["locations"][location_idx]
                            # Check location-level error
                            location_error = le.get("error", "")
                            if location_error:
                                error_text_lower = str(location_error).lower()
                                error_text_original = str(location_error)
                                logger.info(f"   [403-CHECK] Checking location error for 403 patterns: {location_error[:200]}")
                                if any(pattern in error_text_lower for pattern in ["403", "forbidden", "received 403", "blocked", "anti-bot", "anti bot", "access denied"]):
                                    has_403_error = True
                                    logger.warning(f"   [403-DETECT] ✅ Found 403 error in location error: {location_error[:200]}")
                                elif "Received 403" in error_text_original or "403 Forbidden" in error_text_original:
                                    has_403_error = True
                                    logger.warning(f"   [403-DETECT] ✅ Found 403 error (case-sensitive) in location error: {location_error[:200]}")
                            # Check listing_type-level errors
                            for lt in listing_types_to_scrape:
                                if lt in le.get("listing_types", {}):
                                    lt_error = le["listing_types"][lt].get("error", "")
                                    if lt_error:
                                        error_text_lower = str(lt_error).lower()
                                        error_text_original = str(lt_error)
                                        if any(pattern in error_text_lower for pattern in ["403", "forbidden", "received 403", "blocked", "anti-bot", "anti bot", "access denied"]):
                                            has_403_error = True
                                            logger.warning(f"   [403-DETECT] ✅ Found 403 error in listing_type {lt} error: {lt_error[:200]}")
                                        elif "Received 403" in error_text_original or "403 Forbidden" in error_text_original:
                                            has_403_error = True
                                            logger.warning(f"   [403-DETECT] ✅ Found 403 error (case-sensitive) in listing_type {lt} error: {lt_error[:200]}")
                        
                        logger.info(f"   [403-CHECK] Final result: has_403_error={has_403_error}, scrape_error_info={'present' if scrape_error_info else 'None'}")
                        
                        # Also check if curl_cffi is available to provide better diagnostics
                        curl_cffi_info = f"curl_cffi: {'available' if _curl_cffi_available else 'NOT available'}"
                        
                        if has_403_error:
                            error_msg = f"COMPLETED_WITH_ERRORS: Full scrape returned 0 properties due to 403 Forbidden (anti-bot blocking). Realtor.com is blocking requests. {curl_cffi_info}. Check proxy configuration and ensure homeharvest is using TLS fingerprinting."
                            error_type = "BlockedByAntiBot"
                        else:
                            error_msg = f"COMPLETED_WITH_ERRORS: Full scrape returned 0 properties for this location. {curl_cffi_info}"
                            error_type = "ZeroResultsFullScrape"
                        
                        logger.warning(f"   [ZERO] {error_msg}")
                        if location_idx is not None and location_idx < len(progress_logs.get("locations", [])):
                            le = progress_logs["locations"][location_idx]
                            le["status"] = "failed"
                            le["error"] = error_msg
                            le["error_type"] = error_type
                            le["timestamp"] = datetime.utcnow().isoformat()
                            for lt in listing_types_to_scrape:
                                if lt in le.get("listing_types", {}):
                                    le["listing_types"][lt]["error"] = error_msg
                                    le["listing_types"][lt]["error_type"] = error_type
                        await db.update_job_status(job.job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                    duration = (end_time - start_time).total_seconds()
                    
                    # Process each listing type's results
                    for listing_type in listing_types_to_scrape:
                        properties = all_properties_by_type.get(listing_type, [])
                        logger.info(f"   [OK] Found {len(properties)} {listing_type} properties in {duration:.1f}s (from single request)")
                        
                        # Initialize save_results for use later
                        save_results = {"inserted": 0, "updated": 0, "skipped": 0, "errors": 0}
                        
                        # Save properties immediately after each listing type fetch
                        if properties:
                            logger.debug(f"   [DEBUG] Entering save block for {listing_type}...")
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
                            
                            logger.debug(f"   [SAVED] {listing_type}: {save_results['inserted']} inserted, {save_results['updated']} updated, {save_results['skipped']} skipped")
                        
                        # Update location entry's listing_types with completion stats
                        if location_idx is not None and location_idx < len(progress_logs["locations"]):
                            location_entry = progress_logs["locations"][location_idx]
                            if listing_type in location_entry.get("listing_types", {}):
                                location_entry["listing_types"][listing_type]["found"] = len(properties)
                                location_entry["listing_types"][listing_type]["inserted"] = save_results.get("inserted", 0) if properties else 0
                                location_entry["listing_types"][listing_type]["updated"] = save_results.get("updated", 0) if properties else 0
                                location_entry["listing_types"][listing_type]["skipped"] = save_results.get("skipped", 0) if properties else 0
                            location_entry["timestamp"] = end_time.isoformat()
                    
                    # Push to database immediately with updated job totals after processing all listing types
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
                    
                    # Anti-blocking: Increased delay after processing location to avoid rapid requests
                    post_location_delay = random.uniform(1.0, 2.0)  # 1-2 seconds
                    logger.debug(f"   [THROTTLE] Waiting {post_location_delay:.1f}s after processing location")
                    await asyncio.sleep(post_location_delay)
                    
                except Exception as e:
                    error_msg = str(e)
                    error_type = type(e).__name__
                    blocked_by_realtor = _is_realtor_block_exception(e)
                    logger.warning(f"   [WARNING] Error scraping all listing types in {location}: {error_msg}")
                    try:
                        import traceback as tb
                        logger.debug(f"   [WARNING] Full traceback: {tb.format_exc()}")
                    except Exception:
                        logger.debug(f"   [WARNING] Could not format traceback")
                    # Log the error for all listing types
                    for listing_type in listing_types_to_scrape:
                        error_log = {
                            "listing_type": listing_type,
                            "status": "error",
                            "properties_found": 0,
                            "error": error_msg,
                            "error_type": error_type,
                            "blocked_by_realtor": blocked_by_realtor,
                            "timestamp": datetime.utcnow().isoformat()
                        }
                        listing_type_logs.append(error_log)
                        
                        # Update location entry with error for this listing type
                        if location_idx is not None and location_idx < len(progress_logs.get("locations", [])):
                            location_entry = progress_logs["locations"][location_idx]
                            if listing_type in location_entry.get("listing_types", {}):
                                location_entry["listing_types"][listing_type]["found"] = 0
                                location_entry["listing_types"][listing_type]["error"] = error_msg
                                location_entry["listing_types"][listing_type]["error_type"] = error_type
                            # Also store error at location level
                            location_entry["error"] = error_msg
                            location_entry["error_type"] = error_type
                            if location_entry.get("status") != "failed":
                                location_entry["status"] = "failed"
                            location_entry["timestamp"] = datetime.utcnow().isoformat()
                    
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
                    # Off-market detection should only run for FULL scrapes
                    was_full_scrape = self.job_run_flags.get(job.job_id, {}).get("was_full_scrape")
                    
                    # Detect location-level errors to avoid marking properties off-market on failed scrapes
                    location_entry = None
                    listing_type_errors = False
                    location_error = False
                    if location_idx is not None and location_idx < len(progress_logs.get("locations", [])):
                        location_entry = progress_logs["locations"][location_idx]
                        location_error = bool(location_entry.get("error"))
                        for lt in listing_types_to_scrape:
                            lt_error = location_entry.get("listing_types", {}).get(lt, {}).get("error")
                            if lt_error:
                                listing_type_errors = True
                                break
                    
                    location_had_errors = location_total_errors > 0 or location_error or listing_type_errors
                    
                    if was_full_scrape is True and location_total_found > 0 and not location_had_errors:
                        # Use job start time (not end time) to ensure we only check properties
                        # that weren't scraped in THIS job run
                        job_start = job_start_time or datetime.utcnow()
                        
                        # Run off-market detection in background (non-blocking)
                        # This allows the location to be marked as complete immediately
                        # Only check properties for THIS specific location (zip code), not all properties in the job
                        logger.info(
                            "   [OFF-MARKET] Starting background off-market detection for location %s "
                            "(found_ids=%s, listing_types=%s)",
                            location,
                            len(found_property_ids) if found_property_ids else 0,
                            listing_types_to_scrape
                        )
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
                    else:
                        logger.info(
                            "   [OFF-MARKET] Skipping background off-market detection for location %s "
                            "(full_scrape=%s, found=%s, errors=%s, listing_types=%s, found_ids=%s)",
                            location,
                            was_full_scrape,
                            location_total_found,
                            location_had_errors,
                            listing_types_to_scrape,
                            len(found_property_ids) if found_property_ids else 0
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
            
            # Queue RentCast for for_sale and pending properties (non-blocking, async tasks, parallel with enrichment)
            if enrichment_queue and getattr(settings, "RENTCAST_ENABLED", True):
                # Filter enrichment_queue for for_sale and pending properties
                rentcast_queue = [
                    item for item in enrichment_queue
                    if (item.get("listing_type") or "").lower() in ("for_sale", "pending")
                ]
                
                if rentcast_queue:
                    rentcast_count = len(rentcast_queue)
                    logger.info(f"   [RENTCAST] Starting RentCast for {rentcast_count} properties from {location}")
                    
                    # Initialize RentCast status in location entry
                    if location_idx is not None and location_idx < len(progress_logs["locations"]):
                        location_entry = progress_logs["locations"][location_idx]
                        location_entry["rentcast"]["status"] = "in_progress"
                        location_entry["rentcast"]["total"] = rentcast_count
                        location_entry["rentcast"]["completed"] = 0
                        location_entry["rentcast"]["failed"] = 0
                        location_entry["rentcast"]["skipped"] = 0
                    
                    # Process RentCast in batches if configured (reuse ENRICHMENT_BATCH_SIZE)
                    batch_size = settings.ENRICHMENT_BATCH_SIZE
                    if batch_size and isinstance(batch_size, str):
                        try:
                            batch_size = int(batch_size)
                        except ValueError:
                            batch_size = None
                    
                    if batch_size and batch_size > 0:
                        # Process in batches
                        for i in range(0, len(rentcast_queue), batch_size):
                            batch = rentcast_queue[i:i + batch_size]
                            for rentcast_item in batch:
                                # Create async task with semaphore for concurrency control
                                asyncio.create_task(
                                    self._rentcast_property_async(
                                        rentcast_item["property_id"],
                                        rentcast_item["property_dict"],
                                        rentcast_item["job_id"],
                                        location=location,
                                        location_idx=location_idx,
                                        listing_type=rentcast_item.get("listing_type"),
                                        progress_logs=progress_logs
                                    )
                                )
                    else:
                        # Process all at once
                        for rentcast_item in rentcast_queue:
                            # Create async task with semaphore for concurrency control
                            asyncio.create_task(
                                self._rentcast_property_async(
                                    rentcast_item["property_id"],
                                    rentcast_item["property_dict"],
                                    rentcast_item["job_id"],
                                    location=location,
                                    location_idx=location_idx,
                                    listing_type=rentcast_item.get("listing_type"),
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
                    # CRITICAL: Only update if job is still RUNNING (don't overwrite COMPLETED status)
                    if (completed + failed) % 10 == 0 or (completed + failed) >= total:
                        try:
                            # Check current job status before updating
                            current_job = await db.get_job(job_id)
                            if current_job and current_job.status == JobStatus.RUNNING:
                                await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                            else:
                                # Job is already COMPLETED/FAILED/CANCELLED, only update progress_logs without changing status
                                if current_job and current_job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                                    logger.debug(f"Job {job_id} is {getattr(current_job.status, 'value', current_job.status)}, updating only progress_logs for enrichment")
                                    await db.jobs_collection.update_one(
                                        {"job_id": job_id},
                                        {"$set": {"progress_logs": progress_logs, "updated_at": datetime.utcnow()}}
                                    )
                        except Exception as e:
                            logger.error(f"Error updating job status with enrichment progress: {e}")
    
    async def _rentcast_property_async(
        self, 
        property_id: str, 
        property_dict: Dict[str, Any], 
        job_id: Optional[str],
        location: Optional[str] = None,
        location_idx: Optional[int] = None,
        listing_type: Optional[str] = None,
        progress_logs: Optional[Dict[str, Any]] = None
    ):
        """Async RentCast task with semaphore for concurrency control"""
        # Only process for_sale and pending properties
        lt = listing_type or property_dict.get("listing_type") or ""
        if not getattr(settings, "RENTCAST_ENABLED", True) or lt not in ("for_sale", "pending"):
            return

        success = False
        acquired = False
        skipped = False
        skip_reason = None

        # Best-effort mode: yield to HomeHarvest and skip if scraping is active.
        if await self._is_homeharvest_busy():
            skipped = True
            skip_reason = "homeharvest_busy"
        else:
            try:
                # Best-effort: only run if we can acquire immediately.
                await asyncio.wait_for(self.rentcast_semaphore.acquire(), timeout=0.01)
                acquired = True
            except asyncio.TimeoutError:
                skipped = True
                skip_reason = "rentcast_busy"

        try:
            if skipped:
                logger.debug(f"Rentcast skipped for {property_id}: {skip_reason}")
                await self._enqueue_rentcast_retry(job_id, property_id, property_dict)
            else:
                from services.rentcast_service import RentcastService
                if not hasattr(self, "_rentcast_service"):
                    self._rentcast_service = RentcastService(db)
                await self._rentcast_service.fetch_and_save_rent_estimate(property_id, property_dict)
                success = True
        except Exception as e:
            logger.warning(f"Rentcast rent estimate failed for {property_id}: {e}")
        finally:
            if acquired:
                self.rentcast_semaphore.release()

            # Update progress logs if provided
            if progress_logs and location_idx is not None and location_idx < len(progress_logs.get("locations", [])):
                location_entry = progress_logs["locations"][location_idx]
                rentcast_status = location_entry.get("rentcast", {})
                
                if success:
                    rentcast_status["completed"] = rentcast_status.get("completed", 0) + 1
                    # Update per-listing-type rentcast counter
                    if listing_type and listing_type in location_entry.get("listing_types", {}):
                        location_entry["listing_types"][listing_type]["rentcast"] = \
                            location_entry["listing_types"][listing_type].get("rentcast", 0) + 1
                elif skipped:
                    rentcast_status["skipped"] = rentcast_status.get("skipped", 0) + 1
                    # Update per-listing-type rentcast skipped counter
                    if listing_type and listing_type in location_entry.get("listing_types", {}):
                        location_entry["listing_types"][listing_type]["rentcast_skipped"] = \
                            location_entry["listing_types"][listing_type].get("rentcast_skipped", 0) + 1
                else:
                    rentcast_status["failed"] = rentcast_status.get("failed", 0) + 1
                
                # Check if RentCast is complete
                total = rentcast_status.get("total", 0)
                completed = rentcast_status.get("completed", 0)
                failed = rentcast_status.get("failed", 0)
                skipped_count = rentcast_status.get("skipped", 0)
                
                if completed + failed + skipped_count >= total and total > 0:
                    rentcast_status["status"] = "completed"
                
                # Update job status periodically (every 10 completions to avoid too many DB writes)
                # CRITICAL: Only update if job is still RUNNING (don't overwrite COMPLETED status)
                if (completed + failed) % 10 == 0 or (completed + failed) >= total:
                    try:
                        # Check current job status before updating
                        current_job = await db.get_job(job_id)
                        if current_job and current_job.status == JobStatus.RUNNING:
                            await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                        else:
                            # Job is already COMPLETED/FAILED/CANCELLED, only update progress_logs without changing status
                            if current_job and current_job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                                logger.debug(f"Job {job_id} is {getattr(current_job.status, 'value', current_job.status)}, updating only progress_logs for RentCast")
                                await db.jobs_collection.update_one(
                                    {"job_id": job_id},
                                    {"$set": {"progress_logs": progress_logs, "updated_at": datetime.utcnow()}}
                                )
                    except Exception as e:
                        logger.error(f"Error updating job status with RentCast progress: {e}")
    
    async def _scrape_all_listing_types(self, location: str, job: ScrapingJob, proxy_config: Optional[Dict[str, Any]], listing_types: List[str], limit: Optional[int] = None, past_days: Optional[int] = None, cancel_flag: Optional[dict] = None, location_last_update: Optional[dict] = None) -> Tuple[Dict[str, List[Property]], Optional[str]]:
        """Scrape properties for all listing types in a single request. Returns a tuple of (dict mapping listing_type to properties, error_message if any)."""
        try:
            # Helper to check for cancellation/timeout
            def check_cancelled():
                if cancel_flag:
                    if cancel_flag.get("cancelled", False):
                        return True, "Job cancelled"
                    timed_out = cancel_flag.get("timed_out_locations", [])
                    if location in timed_out:
                        return True, f"Location {location} timed out"
                return False, None
            logger.debug(f"   [DEBUG] _scrape_all_listing_types called with listing_types: {listing_types}")
            
            # Filter out 'off_market' from listing_types passed to homeharvest (not supported by API)
            # but keep it in listing_types for status inference later
            listing_types_for_homeharvest = [lt for lt in listing_types if lt != "off_market"]
            if "off_market" in listing_types and "off_market" not in listing_types_for_homeharvest:
                logger.debug(f"   [DEBUG] 'off_market' filtered out from homeharvest API call (will be inferred from status)")
            
            # Prepare scraping parameters - use the original location as-is (no automatic zip code extraction)
            # Zip code extraction should only happen per user request or for broad area searches
            location_to_use = location
            logger.debug(f"   [LOCATION] Using location as-is: '{location_to_use}'")
            
            scrape_params = {
                "location": location_to_use,
                "listing_type": listing_types_for_homeharvest,  # Pass filtered list (without off_market) to homeharvest
                "mls_only": False,
                "limit": limit or job.limit or 10000
            }
            
            # Add optional parameters if specified
            if job.radius:
                scrape_params["radius"] = job.radius
                logger.warning(f"   [WARNING] Radius={job.radius} is set - this may cause overlap between nearby locations!")
            
            # Check if we should only get properties updated since last run (incremental logic)
            use_updated_since_last_run = False
            should_do_full_scrape = False
            
            # First, check if force_full_scrape was set when triggering the job manually
            force_full_scrape = self.job_run_flags.get(job.job_id, {}).get("force_full_scrape")
            if force_full_scrape is not None:
                should_do_full_scrape = force_full_scrape
                if force_full_scrape:
                    logger.info(f"   [FULL SCRAPE] Forced full scrape (user selected when triggering job)")
                else:
                    logger.info(f"   [INCREMENTAL] Forced incremental scrape (user selected when triggering job)")
                    if job.scheduled_job_id:
                        try:
                            scheduled_job = await db.get_scheduled_job(job.scheduled_job_id)
                            if scheduled_job and scheduled_job.last_run_at:
                                time_since_last_run = datetime.utcnow() - scheduled_job.last_run_at
                                hours_since_last_run = time_since_last_run.total_seconds() / 3600
                                
                                if 0 < hours_since_last_run <= (30 * 24):
                                    scrape_params["updated_in_past_hours"] = max(1, int(hours_since_last_run) + 1)
                                    use_updated_since_last_run = True
                                    logger.info(f"   [INCREMENTAL] Only fetching properties updated in past {scrape_params['updated_in_past_hours']} hours (since last run at {scheduled_job.last_run_at})")
                                elif hours_since_last_run > (30 * 24):
                                    # If last run was more than 30 days ago, do a full scrape instead of using date_from
                                    # date_from may not work correctly with homeharvest API for very old dates
                                    should_do_full_scrape = True
                                    use_updated_since_last_run = False
                                    logger.info(f"   [FULL SCRAPE] Last run was {hours_since_last_run/24:.1f} days ago (>30 days) - doing full scrape instead of incremental")
                                else:
                                    logger.warning(f"   [INCREMENTAL] No valid last_run_at found, falling back to past_days")
                            else:
                                logger.warning(f"   [INCREMENTAL] No scheduled job or last_run_at found, falling back to past_days")
                        except Exception as e:
                            logger.warning(f"   [INCREMENTAL] Could not fetch scheduled job for incremental update: {e}, falling back to past_days")
                    else:
                        logger.warning(f"   [INCREMENTAL] No scheduled_job_id found, cannot determine last_run_at, falling back to past_days")
            elif job.scheduled_job_id:
                try:
                    scheduled_job = await db.get_scheduled_job(job.scheduled_job_id)
                    if scheduled_job:
                        incremental_config = scheduled_job.incremental_runs_before_full
                        incremental_count = scheduled_job.incremental_runs_count or 0
                        
                        if incremental_config is None:
                            should_do_full_scrape = False
                        elif incremental_config == 0:
                            should_do_full_scrape = True
                        else:
                            if incremental_count >= incremental_config:
                                should_do_full_scrape = True
                            else:
                                should_do_full_scrape = False
                        
                        if should_do_full_scrape:
                            logger.info(f"   [FULL SCRAPE] Doing full scrape (incremental count: {incremental_count}/{incremental_config if incremental_config else 'N/A'})")
                        elif scheduled_job.last_run_at:
                            time_since_last_run = datetime.utcnow() - scheduled_job.last_run_at
                            hours_since_last_run = time_since_last_run.total_seconds() / 3600
                            
                            if 0 < hours_since_last_run <= (30 * 24):
                                scrape_params["updated_in_past_hours"] = max(1, int(hours_since_last_run) + 1)
                                use_updated_since_last_run = True
                                logger.info(f"   [INCREMENTAL] Only fetching properties updated in past {scrape_params['updated_in_past_hours']} hours (since last run at {scheduled_job.last_run_at}, count: {incremental_count}/{incremental_config if incremental_config else '∞'})")
                            elif hours_since_last_run > (30 * 24):
                                # If last run was more than 30 days ago, do a full scrape instead of using date_from
                                # date_from may not work correctly with homeharvest API for very old dates
                                should_do_full_scrape = True
                                use_updated_since_last_run = False
                                logger.info(f"   [FULL SCRAPE] Last run was {hours_since_last_run/24:.1f} days ago (>30 days) - doing full scrape instead of incremental (count: {incremental_count}/{incremental_config if incremental_config else '∞'})")
                        else:
                            should_do_full_scrape = True
                            logger.info(f"   [FULL SCRAPE] First run for scheduled job {job.scheduled_job_id} - fetching all properties (no last_run_at)")
                except Exception as e:
                    logger.warning(f"   [WARNING] Could not fetch scheduled job for incremental update: {e}")
            
            # Set the flag for this job run (only set once)
            if job.job_id not in self.job_run_flags:
                self.job_run_flags[job.job_id] = {"was_full_scrape": None}
            if self.job_run_flags[job.job_id]["was_full_scrape"] is None:
                self.job_run_flags[job.job_id]["was_full_scrape"] = should_do_full_scrape
            
            # Use provided past_days or job past_days (only if not using incremental update)
            # If neither is set, use default of 90 days for full scrapes to ensure we get properties
            if not use_updated_since_last_run:
                if past_days:
                    scrape_params["past_days"] = past_days
                elif job.past_days:
                    scrape_params["past_days"] = job.past_days
                else:
                    # Default to 90 days if no past_days is specified for full scrapes
                    scrape_params["past_days"] = 90
                    logger.info(f"   [FULL SCRAPE] No past_days specified, using default of 90 days")
            
            # Add sorting by last_update_date for better results
            if use_updated_since_last_run:
                scrape_params["sort_by"] = "last_update_date"
                scrape_params["sort_direction"] = "desc"
            
            # Add proxy configuration if available
            if proxy_config:
                scrape_params["proxy"] = proxy_config.get("proxy_url")
            
            # Log the exact parameters being passed to homeharvest for debugging
            log_params = {k: v for k, v in scrape_params.items() if k != "proxy"}
            proxy_enabled = bool(scrape_params.get("proxy"))
            proxy_host = (proxy_config or {}).get("proxy_host") if proxy_enabled else None
            proxy_port = (proxy_config or {}).get("proxy_port") if proxy_enabled else None
            curl_cffi_status = "✅ enabled" if _curl_cffi_available else "❌ not available"
            logger.info(f"   [HOMEHARVEST] Calling scrape_property proxy_enabled={proxy_enabled} proxy_host={proxy_host} proxy_port={proxy_port} curl_cffi={curl_cffi_status} params={log_params}")
            
            # Serialization: Acquire semaphore to ensure only 1 request at a time
            # This prevents overwhelming the gateway with concurrent requests
            # Gateway manages rate limiting, so we don't need client-side delays
            async with self.global_request_semaphore:
                
                # Scrape properties - Run blocking call in thread pool
                timeout_seconds = 600  # 10 minutes timeout for all listing types combined
                loop = asyncio.get_event_loop()
                
                properties_df = None
                scrape_error = None
                
                try:
                    properties_df = await asyncio.wait_for(
                        loop.run_in_executor(
                            self.executor,
                            lambda: scrape_property(**scrape_params)
                        ),
                        timeout=timeout_seconds
                    )
                    # Gateway manages rate limiting - no need to track request time
                    
                    num_properties = len(properties_df) if properties_df is not None and not properties_df.empty else 0
                    logger.info(f"   [HOMEHARVEST] Received {num_properties} total properties for location='{location}', listing_types={listing_types}")
                    self._dump_homeharvest_df(
                        properties_df,
                        context="all_listing_types",
                        location=location,
                        listing_type=None,
                        listing_types=listing_types_for_homeharvest,
                        job_id=job.job_id,
                        scheduled_job_id=job.scheduled_job_id,
                    )
                    
                    # Update location_last_update when properties are actually found
                    if num_properties > 0 and location_last_update is not None:
                        location_last_update[location] = datetime.utcnow()
                    
                    # No post-request delay needed - gateway manages rate limiting
                    
                    # Validate location if zip code was used
                    zip_match = re.search(r'\b(\d{5})\b', location)
                    expected_zip = zip_match.group(1) if zip_match else None
                    
                    if expected_zip and num_properties > 0:
                        zip_codes_in_results = set()
                        if 'zip_code' in properties_df.columns:
                            zip_codes_in_results = set(properties_df['zip_code'].dropna().astype(str).str.zfill(5).unique())
                        elif 'address' in properties_df.columns:
                            for addr in properties_df['address'].dropna():
                                if isinstance(addr, str):
                                    zip_match = re.search(r'\b(\d{5})\b', addr)
                                    if zip_match:
                                        zip_codes_in_results.add(zip_match.group(1).zfill(5))
                        
                        if zip_codes_in_results:
                            matching_zips = [z for z in zip_codes_in_results if z == expected_zip.zfill(5)]
                            if not matching_zips:
                                logger.warning(
                                    f"   [WARNING] Location mismatch! Requested zip '{expected_zip}' but got zips: {sorted(zip_codes_in_results)[:10]}"
                                    f" (showing first 10 of {len(zip_codes_in_results)} unique zips)"
                                )
                            else:
                                logger.debug(f"   [VALIDATION] All properties match requested zip code '{expected_zip}'")
                except asyncio.TimeoutError:
                    error_msg = f"Scraping all listing types in {location} timed out after {timeout_seconds} seconds"
                    logger.warning(f"   [TIMEOUT] {error_msg}")
                    scrape_error = error_msg
                except Exception as e:
                    # Catch all other exceptions from homeharvest/realtor (API errors, network errors, etc.)
                    error_msg = f"HomeHarvest/Realtor error for all listing types in {location}: {str(e)}"
                    error_type = type(e).__name__
                    logger.error(f"   [HOMEHARVEST ERROR] {error_msg} (Type: {error_type})")
                    try:
                        import traceback as tb
                        logger.debug(f"   [HOMEHARVEST ERROR] Full traceback: {tb.format_exc()}")
                    except Exception:
                        # Fallback if traceback import fails
                        logger.debug(f"   [HOMEHARVEST ERROR] Could not format traceback")
                    scrape_error = error_msg
                    properties_df = None
            
            # If the combined call failed or returned empty, try individual calls as fallback
            num_properties_from_combined = len(properties_df) if properties_df is not None and not properties_df.empty else 0
            should_try_fallback = (properties_df is None or properties_df.empty or num_properties_from_combined == 0)
            
            if should_try_fallback:
                fallback_reason = scrape_error if scrape_error else "returned 0 properties"
                logger.warning(f"   [FALLBACK] Combined listing_types call {fallback_reason}. Trying individual calls for each listing type...")
                
                # Anti-blocking: Rotate proxy on fallback retry to get a fresh session
                if proxy_config:
                    logger.info(f"   [FALLBACK] Rotating proxy for fallback retry...")
                    proxy_config = await self.get_proxy_config(job)
                    if proxy_config:
                        logger.info(f"   [FALLBACK] Using new proxy: {proxy_config.get('proxy_host')}:{proxy_config.get('proxy_port')} username={proxy_config.get('proxy_username')}")
                
                properties_by_type_fallback: Dict[str, List[Property]] = {lt: [] for lt in listing_types}
                
                # Filter out 'off_market' from fallback calls since homeharvest doesn't support it
                listing_types_for_fallback = [lt for lt in listing_types if lt != "off_market"]
                
                for idx, listing_type in enumerate(listing_types_for_fallback):
                    try:
                        # No delay needed - gateway manages rate limiting
                        # Semaphore ensures serialization (one request at a time)
                        
                        # Create params for individual listing type call
                        individual_params = scrape_params.copy()
                        individual_params["listing_type"] = listing_type  # Single string, not list
                        # Update proxy if we rotated it
                        if proxy_config:
                            individual_params["proxy"] = proxy_config.get("proxy_url")
                        # Remove parameters that might not be needed for individual calls
                        if "sort_by" in individual_params:
                            del individual_params["sort_by"]
                        if "sort_direction" in individual_params:
                            del individual_params["sort_direction"]
                        
                        logger.info(f"   [FALLBACK] Trying individual call for listing_type='{listing_type}' with params: {[k for k in individual_params.keys() if k != 'proxy']}")
                        
                        # Serialization: Acquire semaphore for fallback calls too
                        # Gateway manages rate limiting, so we just ensure one request at a time
                        async with self.global_request_semaphore:
                            # Make the request while holding the semaphore
                            individual_df = await asyncio.wait_for(
                                loop.run_in_executor(
                                    self.executor,
                                    lambda lt=listing_type, params=individual_params: scrape_property(**params)
                                ),
                                timeout=timeout_seconds // max(1, len(listing_types_for_fallback))  # Divide timeout among types (excluding off_market)
                            )
                            
                            # No delay needed - gateway manages rate limiting
                        
                        if individual_df is not None and not individual_df.empty:
                            num_individual = len(individual_df)
                            logger.info(f"   [FALLBACK] Individual call for '{listing_type}' returned {num_individual} properties")
                            self._dump_homeharvest_df(
                                individual_df,
                                context="listing_type_fallback",
                                location=location,
                                listing_type=listing_type,
                                listing_types=None,
                                job_id=job.job_id,
                                scheduled_job_id=job.scheduled_job_id,
                            )
                            
                            # Update location_last_update when properties are actually found
                            if location_last_update is not None:
                                location_last_update[location] = datetime.utcnow()
                            
                            # No post-request delay needed - gateway manages rate limiting
                            
                            # Convert to Property objects
                            conversion_errors = 0
                            for index, row in individual_df.iterrows():
                                try:
                                    property_obj = self.convert_to_property_model(row, job.job_id, listing_type, job.scheduled_job_id)
                                    if listing_type == "sold":
                                        property_obj.is_comp = True
                                    properties_by_type_fallback[listing_type].append(property_obj)
                                except Exception as e:
                                    conversion_errors += 1
                                    logger.error(f"Error converting property for {listing_type} in fallback: {e}")
                                    continue
                        else:
                            logger.warning(f"   [FALLBACK] Individual call for '{listing_type}' returned empty result")
                    except Exception as e:
                        error_type = type(e).__name__
                        blocked_by_realtor = _is_realtor_block_exception(e)
                        logger.error(f"   [FALLBACK] Error in individual call for '{listing_type}': {e} (Type: {error_type}, Blocked: {blocked_by_realtor})")
                        try:
                            import traceback as tb
                            logger.debug(f"   [FALLBACK] Full traceback: {tb.format_exc()}")
                        except Exception:
                            logger.debug(f"   [FALLBACK] Could not format traceback")
                        
                        # Anti-blocking: If blocked, add exponential backoff before next fallback call
                        # Drastically increased delays to avoid persistent blocking
                        if blocked_by_realtor and idx < len(listing_types) - 1:
                            base_delay = 30.0 * (2 ** idx)  # Start at 30s, then 60s, 120s, 240s
                            backoff_delay = min(base_delay, 300.0)  # Cap at 300s (5 minutes)
                            # Add randomization to avoid predictable patterns
                            randomized_delay = backoff_delay + random.uniform(-5.0, 10.0)
                            randomized_delay = max(30.0, randomized_delay)  # Minimum 30s
                            logger.warning(f"   [THROTTLE] Blocked by Realtor.com, waiting {randomized_delay:.1f}s before next fallback call")
                            # Check for cancellation during backoff (split into chunks)
                            chunk_size = 5.0  # Check every 5 seconds
                            waited = 0.0
                            while waited < randomized_delay:
                                cancelled, reason = check_cancelled()
                                if cancelled:
                                    return {}, reason
                                sleep_time = min(chunk_size, randomized_delay - waited)
                                await asyncio.sleep(sleep_time)
                                waited += sleep_time
                        
                        continue
                
                # If fallback got results, use them
                total_fallback = sum(len(props) for props in properties_by_type_fallback.values())
                if total_fallback > 0:
                    logger.info(f"   [FALLBACK] Fallback individual calls succeeded! Got {total_fallback} total properties across all types")
                    return properties_by_type_fallback, None  # No error, got results
                else:
                    logger.error(f"   [FALLBACK] All individual calls also failed or returned empty. Original error: {scrape_error}")
                    # Return the error so caller can detect 403 blocking
                    return properties_by_type_fallback, scrape_error
            
            # Split properties by listing_type from the DataFrame
            # HomeHarvest should include a 'listing_type' column in the DataFrame when multiple types are requested
            properties_by_type: Dict[str, List[Property]] = {lt: [] for lt in listing_types}
            
            # If properties_df is None or empty and we haven't already tried fallback, return empty
            if properties_df is None:
                logger.warning(f"   [WARNING] properties_df is None for location='{location}'. Returning empty results.")
                # Fallback was already attempted above if scrape_error exists
                return properties_by_type, scrape_error
            
            if properties_df is not None and not properties_df.empty:
                # Log DataFrame columns for debugging
                logger.debug(f"   [DEBUG] DataFrame columns: {list(properties_df.columns)}")
                
                # Check if DataFrame has a listing_type column
                if 'listing_type' in properties_df.columns:
                    # Group by listing_type
                    logger.debug(f"   [SPLIT] Splitting {len(properties_df)} properties by listing_type column")
                    
                    # Check for unexpected listing types in the DataFrame
                    unique_listing_types = properties_df['listing_type'].dropna().unique().tolist()
                    unexpected_types = [lt for lt in unique_listing_types if lt not in listing_types]
                    
                    if unexpected_types:
                        unexpected_count = len(properties_df[properties_df['listing_type'].isin(unexpected_types)])
                        logger.warning(
                            f"   [UNEXPECTED] Found {unexpected_count} properties with unexpected listing types: {unexpected_types} "
                            f"(expected: {listing_types}). These will be skipped."
                        )
                        # Log examples of unexpected types
                        for unexpected_type in unexpected_types:
                            unexpected_df = properties_df[properties_df['listing_type'] == unexpected_type]
                            logger.warning(f"   [UNEXPECTED]   - {len(unexpected_df)} properties with listing_type='{unexpected_type}'")
                            # Show status values for these unexpected types
                            if 'status' in unexpected_df.columns:
                                status_counts = unexpected_df['status'].value_counts().head(5)
                                logger.warning(f"   [UNEXPECTED]     Status breakdown: {dict(status_counts)}")
                    
                    for listing_type in listing_types:
                        type_df = properties_df[properties_df['listing_type'] == listing_type]
                        logger.debug(f"   [SPLIT] Found {len(type_df)} properties for listing_type '{listing_type}'")
                        for index, row in type_df.iterrows():
                            try:
                                property_obj = self.convert_to_property_model(row, job.job_id, listing_type, job.scheduled_job_id)
                                if listing_type == "sold":
                                    property_obj.is_comp = True
                                properties_by_type[listing_type].append(property_obj)
                            except Exception as e:
                                logger.error(f"Error converting property for {listing_type}: {e}")
                                continue
                    
                    # Log summary of split
                    total_split = sum(len(props) for props in properties_by_type.values())
                    total_expected = len(properties_df[properties_df['listing_type'].isin(listing_types)])
                    logger.info(
                        f"   [SPLIT] Split {len(properties_df)} total properties into: "
                        f"{', '.join([f'{lt}={len(properties_by_type[lt])}' for lt in listing_types])} "
                        f"(total after split: {total_split}, expected: {total_expected}, "
                        f"unexpected/skipped: {len(properties_df) - total_expected})"
                    )
                else:
                    # No listing_type column - infer listing type from status field
                    logger.warning(f"   [WARNING] DataFrame doesn't have 'listing_type' column! Inferring listing type from 'status' field.")
                    logger.debug(f"   [SPLIT] Attempting to split {len(properties_df)} properties by inferring listing_type from status field")
                    
                    
                    def infer_listing_type_from_status(status_value: Any, mls_status_value: Any = None) -> Optional[str]:
                        """Infer listing type from status and mls_status fields"""
                        # Safely check for None and pandas NA
                        status_is_valid = status_value is not None
                        if status_is_valid:
                            try:
                                if pd.isna(status_value):
                                    status_is_valid = False
                            except (TypeError, ValueError):
                                pass
                        
                        if not status_is_valid:
                            return None
                        
                        # Safely convert status to string
                        status_str = ""
                        try:
                            if status_is_valid:
                                status_str = str(status_value).upper()
                        except:
                            status_str = ""
                        
                        # Safely check for mls_status None and pandas NA
                        mls_status_is_valid = mls_status_value is not None
                        if mls_status_is_valid:
                            try:
                                if pd.isna(mls_status_value):
                                    mls_status_is_valid = False
                            except (TypeError, ValueError):
                                pass
                        
                        # Safely convert mls_status to string
                        mls_status_str = ""
                        try:
                            if mls_status_is_valid:
                                mls_status_str = str(mls_status_value).upper()
                        except:
                            mls_status_str = ""
                        
                        # Check for off-market properties (EXPIRED, WITHDRAWN, CANCELLED, etc.)
                        # NOTE: OFF_MARKET is requested in scraping (force_rescrape=true) but not in ListingType enum
                        # Properties with OFF_MARKET status should be handled if "off_market" is in listing_types
                        off_market_indicators = ['EXPIRED', 'WITHDRAWN', 'CANCELLED', 'INACTIVE', 'DELISTED', 
                                                'TEMPORARILY OFF MARKET', 'TEMPORARILY_OFF_MARKET', 'OFF_MARKET']
                        if status_str == 'OFF_MARKET' or any(indicator in mls_status_str for indicator in off_market_indicators):
                            # Return "off_market" if it's in the requested listing_types, otherwise fallback to "sold"
                            if "off_market" in listing_types:
                                return "off_market"
                            # If off_market not in listing_types, fallback to sold (legacy behavior)
                            return "sold"
                        
                        # Check for sold properties
                        if "SOLD" in status_str or "SOLD" in mls_status_str:
                            return "sold"
                        
                        # Check for pending properties
                        if "PENDING" in status_str or "PENDING" in mls_status_str or "CONTINGENT" in status_str:
                            return "pending"
                        
                        # Check for rental properties
                        if "RENT" in status_str or "FOR_RENT" in status_str:
                            return "for_rent"
                        
                        # Check for for_sale properties (default for active listings)
                        if "FOR_SALE" in status_str or "ACTIVE" in status_str or "LISTED" in status_str:
                            return "for_sale"
                        
                        # Default fallback - return None to use first listing type
                        return None
                    
                    # Split properties by inferred listing type
                    inferred_count = {lt: 0 for lt in listing_types}
                    unassigned_count = 0
                    unassigned_properties = []  # Track unassigned properties for logging
                    conversion_failure_count = 0
                    
                    for index, row in properties_df.iterrows():
                        try:
                            # Access DataFrame row values (pandas Series supports dict-style access)
                            status_value = row.get('status') if 'status' in row.index else None
                            mls_status_value = row.get('mls_status') if 'mls_status' in row.index else None
                            
                            # Try to get property address for logging
                            address_value = None
                            if 'address' in row.index:
                                addr = row.get('address')
                                if isinstance(addr, str):
                                    address_value = addr
                                elif isinstance(addr, dict):
                                    address_value = addr.get('formatted_address') or addr.get('street') or str(addr)
                            elif 'street' in row.index:
                                address_value = row.get('street')
                            elif 'formatted_address' in row.index:
                                address_value = row.get('formatted_address')
                            
                            inferred_type = infer_listing_type_from_status(status_value, mls_status_value)
                            
                            # Safely check inferred_type for pandas NA before using in boolean context
                            inferred_type_is_valid = inferred_type is not None
                            if inferred_type_is_valid:
                                try:
                                    if pd.isna(inferred_type):
                                        inferred_type_is_valid = False
                                except (TypeError, ValueError):
                                    pass
                            
                            # Use inferred type if it's in our requested listing types, otherwise use first type as fallback
                            if inferred_type_is_valid and inferred_type in listing_types:
                                listing_type = inferred_type
                            else:
                                listing_type = listing_types[0]  # Fallback to first type
                                unassigned_count += 1
                                # Track unassigned property details for logging
                                # Safely convert address_value to string, checking for pandas NA first
                                address_str_safe = 'Unknown'
                                if address_value is not None:
                                    try:
                                        if not pd.isna(address_value):
                                            address_str_safe = str(address_value)
                                    except (TypeError, ValueError):
                                        try:
                                            address_str_safe = str(address_value) if address_value is not None else 'Unknown'
                                        except:
                                            address_str_safe = 'Unknown'
                                unassigned_properties.append({
                                    'status': str(status_value) if (status_value is not None and not pd.isna(status_value)) else 'None',
                                    'mls_status': str(mls_status_value) if (mls_status_value is not None and not pd.isna(mls_status_value)) else 'None',
                                    'inferred_type': inferred_type if inferred_type else 'None',
                                    'address': address_str_safe,
                                    'assigned_to': listing_type  # Show which type it was assigned to as fallback
                                })
                            
                            property_obj = self.convert_to_property_model(row, job.job_id, listing_type, job.scheduled_job_id)
                            if listing_type == "sold":
                                property_obj.is_comp = True
                            properties_by_type[listing_type].append(property_obj)
                            inferred_count[listing_type] += 1
                        except Exception as e:
                            conversion_failure_count += 1
                            logger.error(f"Error converting property: {e}")
                            continue
                    
                    # Log summary of inferred split
                    total_split = sum(len(props) for props in properties_by_type.values())
                    split_summary = ', '.join([f'{lt}={len(properties_by_type[lt])}' for lt in listing_types])
                    logger.info(f"   [SPLIT] Inferred listing types from status field: {split_summary} (total: {total_split}, unassigned: {unassigned_count})")
                    
                    # Log details of unassigned properties
                    # NOTE: The ListingType enum in models.py only defines 4 types (for_sale, for_rent, sold, pending)
                    # However, "off_market" is requested in scraping when force_rescrape=true (see main.py line 1015)
                    # Properties with OFF_MARKET status (status='OFF_MARKET' or mls_status='EXPIRED', etc.) 
                    # are now handled by infer_listing_type_from_status() if "off_market" is in listing_types
                    # If "off_market" is not in listing_types, they fallback to 'sold' (legacy behavior)
                    expected_types_list = ', '.join(listing_types)
                    if unassigned_count > 0:
                        logger.warning(f"   [UNASSIGNED] Found {unassigned_count} properties that don't match any of the expected listing types ({expected_types_list}):")
                        # Group by status for cleaner logging
                        status_groups = {}
                        for prop in unassigned_properties:
                            status_key = f"status='{prop['status']}', mls_status='{prop['mls_status']}'"
                            if status_key not in status_groups:
                                status_groups[status_key] = []
                            status_groups[status_key].append(prop)
                        
                        # Log each unique status combination
                        for status_key, props in status_groups.items():
                            logger.warning(f"   [UNASSIGNED]   - {len(props)} property/properties with {status_key} (assigned to '{props[0]['assigned_to']}' as fallback)")
                            # Log first few addresses as examples
                            example_addresses = [p['address'] for p in props[:3]]
                            if example_addresses:
                                logger.warning(f"   [UNASSIGNED]     Examples: {', '.join(example_addresses)}")
                                if len(props) > 3:
                                    logger.warning(f"   [UNASSIGNED]     ... and {len(props) - 3} more with same status")
            
            # Deduplicate by property_id (keep first) to avoid E11000 when the API
            # returns the same property in multiple rows for the same listing_type
            for lt in properties_by_type:
                seen = set()
                deduped = []
                for p in properties_by_type[lt]:
                    pid = getattr(p, 'property_id', None)
                    if pid is None:
                        deduped.append(p)
                        continue
                    if pid in seen:
                        logger.debug(f"   [DEDUPE] Dropped duplicate property_id={pid} in {lt}")
                        continue
                    seen.add(pid)
                    deduped.append(p)
                properties_by_type[lt] = deduped

            return properties_by_type, None  # No error, got results
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error scraping all listing types in {location}: {e}")
            try:
                import traceback as tb
                logger.error(f"Full traceback: {tb.format_exc()}")
            except Exception:
                logger.error(f"Could not format traceback")
            return {lt: [] for lt in listing_types}, error_msg
    
    async def _scrape_listing_type(self, location: str, job: ScrapingJob, proxy_config: Optional[Dict[str, Any]], listing_type: str, limit: Optional[int] = None, past_days: Optional[int] = None) -> List[Property]:
        """Scrape properties for a specific listing type"""
        try:
            logger.debug(f"   [DEBUG] _scrape_listing_type called with listing_type: '{listing_type}' (type: {type(listing_type)})")
            
            # Prepare scraping parameters - use the original location as-is (no automatic zip code extraction)
            # Zip code extraction should only happen per user request or for broad area searches
            # According to homeharvest docs, location accepts: zip code, city, "city, state", or full address
            location_to_use = location
            logger.debug(f"   [LOCATION] Using location as-is: '{location_to_use}'")
            
            scrape_params = {
                "location": location_to_use,  # Use location as-is (no automatic zip code extraction)
                "listing_type": listing_type,
                "mls_only": False,  # Always use all sources for maximum data
                "limit": limit or job.limit or 10000  # Use high limit for comprehensive scraping
            }
            
            # Add optional parameters if specified
            # NOTE: According to docs, radius is for searching within radius of an address
            # If radius is set, it might cause overlap between nearby zip codes
            if job.radius:
                scrape_params["radius"] = job.radius
                logger.warning(f"   [WARNING] Radius={job.radius} is set - this may cause overlap between nearby locations!")
            
            # Check if we should only get properties updated since last run
            # This uses homeharvest's updated_in_past_hours or date_from parameter
            use_updated_since_last_run = False
            should_do_full_scrape = False
            
            # First, check if force_full_scrape was set when triggering the job manually
            force_full_scrape = self.job_run_flags.get(job.job_id, {}).get("force_full_scrape")
            if force_full_scrape is not None:
                # User explicitly chose full or incremental when triggering
                should_do_full_scrape = force_full_scrape
                if force_full_scrape:
                    logger.info(f"   [FULL SCRAPE] Forced full scrape (user selected when triggering job)")
                else:
                    # Forced incremental - need to get last_run_at from scheduled job to calculate time window
                    logger.info(f"   [INCREMENTAL] Forced incremental scrape (user selected when triggering job)")
                    if job.scheduled_job_id:
                        try:
                            scheduled_job = await db.get_scheduled_job(job.scheduled_job_id)
                            if scheduled_job and scheduled_job.last_run_at:
                                # Calculate hours since last run
                                time_since_last_run = datetime.utcnow() - scheduled_job.last_run_at
                                hours_since_last_run = time_since_last_run.total_seconds() / 3600
                                
                                # Only use updated_in_past_hours if last run was within reasonable time (not more than 30 days)
                                if 0 < hours_since_last_run <= (30 * 24):
                                    scrape_params["updated_in_past_hours"] = max(1, int(hours_since_last_run) + 1)  # Add 1 hour buffer
                                    use_updated_since_last_run = True
                                    logger.info(f"   [INCREMENTAL] Only fetching properties updated in past {scrape_params['updated_in_past_hours']} hours (since last run at {scheduled_job.last_run_at})")
                                elif hours_since_last_run > (30 * 24):
                                    # If last run was more than 30 days ago, do a full scrape instead of using date_from
                                    # date_from may not work correctly with homeharvest API for very old dates
                                    should_do_full_scrape = True
                                    use_updated_since_last_run = False
                                    logger.info(f"   [FULL SCRAPE] Last run was {hours_since_last_run/24:.1f} days ago (>30 days) - doing full scrape instead of incremental")
                                else:
                                    logger.warning(f"   [INCREMENTAL] No valid last_run_at found, falling back to past_days")
                            else:
                                logger.warning(f"   [INCREMENTAL] No scheduled job or last_run_at found, falling back to past_days")
                        except Exception as e:
                            logger.warning(f"   [INCREMENTAL] Could not fetch scheduled job for incremental update: {e}, falling back to past_days")
                    else:
                        logger.warning(f"   [INCREMENTAL] No scheduled_job_id found, cannot determine last_run_at, falling back to past_days")
            elif job.scheduled_job_id:
                try:
                    scheduled_job = await db.get_scheduled_job(job.scheduled_job_id)
                    if scheduled_job:
                        # Determine if we should do full or incremental scrape based on scheduled job config
                        incremental_config = scheduled_job.incremental_runs_before_full
                        incremental_count = scheduled_job.incremental_runs_count or 0
                        
                        if incremental_config is None:
                            # None = always incremental (if last_run_at exists)
                            should_do_full_scrape = False
                        elif incremental_config == 0:
                            # 0 = always full scrape
                            should_do_full_scrape = True
                        else:
                            # > 0 = do incremental until count reaches config, then full scrape
                            if incremental_count >= incremental_config:
                                should_do_full_scrape = True
                            else:
                                should_do_full_scrape = False
                        
                        # If doing full scrape, skip incremental logic
                        if should_do_full_scrape:
                            logger.info(f"   [FULL SCRAPE] Doing full scrape (incremental count: {incremental_count}/{incremental_config if incremental_config else 'N/A'})")
                        elif scheduled_job.last_run_at:
                            # Do incremental scrape - calculate hours since last run
                            time_since_last_run = datetime.utcnow() - scheduled_job.last_run_at
                            hours_since_last_run = time_since_last_run.total_seconds() / 3600
                            
                            # Only use updated_in_past_hours if last run was within reasonable time (not more than 30 days)
                            if 0 < hours_since_last_run <= (30 * 24):
                                scrape_params["updated_in_past_hours"] = max(1, int(hours_since_last_run) + 1)  # Add 1 hour buffer
                                use_updated_since_last_run = True
                                logger.info(f"   [INCREMENTAL] Only fetching properties updated in past {scrape_params['updated_in_past_hours']} hours (since last run at {scheduled_job.last_run_at}, count: {incremental_count}/{incremental_config if incremental_config else '∞'})")
                            elif hours_since_last_run > (30 * 24):
                                # If last run was more than 30 days ago, do a full scrape instead of using date_from
                                # date_from may not work correctly with homeharvest API for very old dates
                                should_do_full_scrape = True
                                use_updated_since_last_run = False
                                logger.info(f"   [FULL SCRAPE] Last run was {hours_since_last_run/24:.1f} days ago (>30 days) - doing full scrape instead of incremental (count: {incremental_count}/{incremental_config if incremental_config else '∞'})")
                        else:
                            # First run - no last_run_at, do full scrape
                            should_do_full_scrape = True
                            logger.info(f"   [FULL SCRAPE] First run for scheduled job {job.scheduled_job_id} - fetching all properties (no last_run_at)")
                except Exception as e:
                    logger.warning(f"   [WARNING] Could not fetch scheduled job for incremental update: {e}")
            
            # Set the flag for this job run (only set once, on first listing type)
            if job.job_id not in self.job_run_flags:
                self.job_run_flags[job.job_id] = {"was_full_scrape": None}
            if self.job_run_flags[job.job_id]["was_full_scrape"] is None:
                self.job_run_flags[job.job_id]["was_full_scrape"] = should_do_full_scrape
            
            # Use provided past_days or job past_days (only if not using incremental update)
            # If neither is set, use default of 90 days for full scrapes to ensure we get properties
            if not use_updated_since_last_run:
                if past_days:
                    scrape_params["past_days"] = past_days
                elif job.past_days:
                    scrape_params["past_days"] = job.past_days
                else:
                    # Default to 90 days if no past_days is specified for full scrapes
                    scrape_params["past_days"] = 90
                    logger.info(f"   [FULL SCRAPE] No past_days specified, using default of 90 days")
            
            # Add sorting by last_update_date for better results (homeharvest 0.8.7+ feature)
            # This ensures we get the most recently updated properties first
            if use_updated_since_last_run:
                scrape_params["sort_by"] = "last_update_date"
                scrape_params["sort_direction"] = "desc"
            
            # Add proxy configuration if available
            if proxy_config:
                scrape_params["proxy"] = proxy_config.get("proxy_url")
            
            # Log the exact parameters being passed to homeharvest for debugging
            log_params = {k: v for k, v in scrape_params.items() if k != "proxy"}  # Don't log proxy URL
            proxy_enabled = bool(scrape_params.get("proxy"))
            proxy_host = (proxy_config or {}).get("proxy_host") if proxy_enabled else None
            proxy_port = (proxy_config or {}).get("proxy_port") if proxy_enabled else None
            curl_cffi_status = "✅ enabled" if _curl_cffi_available else "❌ not available"
            logger.info(f"   [HOMEHARVEST] Calling scrape_property proxy_enabled={proxy_enabled} proxy_host={proxy_host} proxy_port={proxy_port} curl_cffi={curl_cffi_status} params={log_params}")
            
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
                
                # Log the number of properties returned by homeharvest
                num_properties = len(properties_df) if properties_df is not None and not properties_df.empty else 0
                logger.info(f"   [HOMEHARVEST] Received {num_properties} properties for location='{location}', listing_type='{listing_type}'")
                self._dump_homeharvest_df(
                    properties_df,
                    context="listing_type",
                    location=location,
                    listing_type=listing_type,
                    listing_types=None,
                    job_id=job.job_id,
                    scheduled_job_id=job.scheduled_job_id,
                )
                
                # Extract expected zip code from location string (e.g., "Indianapolis, IN 46201" -> "46201")
                zip_match = re.search(r'\b(\d{5})\b', location)
                expected_zip = zip_match.group(1) if zip_match else None
                
                # Validate that returned properties match the requested location
                if expected_zip and num_properties > 0:
                    # Check zip codes in the returned data
                    zip_codes_in_results = set()
                    if 'zip_code' in properties_df.columns:
                        zip_codes_in_results = set(properties_df['zip_code'].dropna().astype(str).str.zfill(5).unique())
                    elif 'address' in properties_df.columns:
                        # Try to extract from address column if it exists
                        for addr in properties_df['address'].dropna():
                            if isinstance(addr, str):
                                zip_match = re.search(r'\b(\d{5})\b', addr)
                                if zip_match:
                                    zip_codes_in_results.add(zip_match.group(1).zfill(5))
                    
                    if zip_codes_in_results:
                        matching_zips = [z for z in zip_codes_in_results if z == expected_zip.zfill(5)]
                        if not matching_zips:
                            logger.warning(
                                f"   [WARNING] Location mismatch! Requested zip '{expected_zip}' but got zips: {sorted(zip_codes_in_results)[:10]}"
                                f" (showing first 10 of {len(zip_codes_in_results)} unique zips)"
                            )
                        else:
                            logger.debug(f"   [VALIDATION] All properties match requested zip code '{expected_zip}'")
            except asyncio.TimeoutError:
                error_msg = f"Scraping {listing_type} properties in {location} timed out after {timeout_seconds} seconds"
                logger.warning(f"   [TIMEOUT] {error_msg}")
                raise TimeoutError(error_msg)
            except Exception as e:
                # Catch all other exceptions from homeharvest/realtor (API errors, network errors, etc.)
                error_msg = f"HomeHarvest/Realtor error for {listing_type} in {location}: {str(e)}"
                error_type = type(e).__name__
                logger.error(f"   [HOMEHARVEST ERROR] {error_msg} (Type: {error_type})")
                try:
                    import traceback as tb
                    logger.debug(f"   [HOMEHARVEST ERROR] Full traceback: {tb.format_exc()}")
                except Exception:
                    logger.debug(f"   [HOMEHARVEST ERROR] Could not format traceback")
                # Re-raise with more context
                raise Exception(f"HomeHarvest API error: {error_msg}") from e
            
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
            error_msg = str(e)
            error_type = type(e).__name__
            logger.error(f"Error scraping {listing_type} properties in {location}: {error_msg}")
            try:
                import traceback as tb
                logger.debug(f"Full traceback: {tb.format_exc()}")
            except Exception:
                logger.debug(f"Could not format traceback")
            
            # Store error in progress_logs if available (note: progress_logs is handled at caller level)
            # This method doesn't have access to progress_logs, so errors are logged but not stored here
            # The caller (scrape_location) will handle error storage in progress_logs
            
            # Return empty list but error is now logged
            return []
    
    def convert_to_property_model(self, prop_data: Any, job_id: str, listing_type: str = None, scheduled_job_id: Optional[str] = None) -> Property:
        """Convert HomeHarvest property data (pandas Series) to our Property model"""
        try:
            # Helper function to safely get values from pandas Series
            def safe_get(key, default=None):
                try:
                    value = prop_data.get(key, default)
                    # Convert pandas NaN/NA to None - check multiple ways to be safe
                    if value is not None:
                        try:
                            # Check for pandas NA types (NaN, NA, NaT)
                            if pd.isna(value):
                                return None
                            # Also check for pandas.NA singleton explicitly
                            if hasattr(pd, 'NA') and value is pd.NA:
                                return None
                        except (TypeError, ValueError):
                            # If pd.isna fails, continue with value
                            pass
                    # Handle None values
                    if value is None:
                        return None
                    return value
                except Exception as e:
                    # Log the error for debugging
                    logger.debug(f"Error in safe_get for key '{key}': {e}")
                    return default
            
            # Helper function to safely get boolean values (handles pandas NA properly)
            def safe_get_bool(key, default=None):
                try:
                    value = prop_data.get(key, default)
                    # Check for pandas NA types first (before any boolean operations)
                    # This prevents "boolean value of NA is ambiguous" errors
                    # Use pd.isna() which handles all pandas NA types (NaN, NA, NaT)
                    if pd.isna(value):
                        return None
                    # Handle None values
                    if value is None:
                        return None
                    # Check for pandas NA singleton (pandas 1.0+)
                    if hasattr(pd, 'NA') and value is pd.NA:
                        return None
                    # Convert to boolean, handling string representations
                    if isinstance(value, str):
                        return value.lower() in ('true', '1', 'yes', 'on')
                    # Use explicit boolean conversion that handles pandas/numpy types
                    if isinstance(value, (bool, np.bool_)):
                        return bool(value)
                    # For numeric values, convert to bool (0/1, etc.)
                    try:
                        return bool(value) if value is not None else None
                    except (ValueError, TypeError):
                        return None
                except Exception:
                    # Catch all exceptions (including "boolean value of NA is ambiguous")
                    # to prevent property conversion from failing
                    return None
            
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
                new_construction=safe_get_bool('new_construction'),
                monthly_fees=safe_get('monthly_fees'),
                one_time_fees=safe_get('one_time_fees'),
                tax_history=safe_get('tax_history'),
                nearby_schools=nearby_schools_list,
                job_id=job_id,
                scheduled_job_id=scheduled_job_id,
                last_scraped=datetime.utcnow()  # Set last_scraped timestamp
            )
            
            # Normalize/override status based on listing_type and raw status fields.
            raw_status = safe_get('status')
            raw_mls_status = safe_get('mls_status')
            # Ensure raw_mls_status is not pandas NA before using in boolean context
            # Double-check in case safe_get didn't catch it
            try:
                if raw_mls_status is not None and pd.isna(raw_mls_status):
                    raw_mls_status = None
            except (TypeError, ValueError):
                # If pd.isna fails, treat as None
                raw_mls_status = None
            
            listing_type_value = str(property_obj.listing_type).lower() if property_obj.listing_type else ""
            if listing_type_value == "sold":
                # Listing type should win for SOLD to avoid OFF_MARKET misclassification.
                property_obj.status = "SOLD"
                if not property_obj.mls_status:
                    property_obj.mls_status = "SOLD"
            elif self.is_sold_status(raw_status, raw_mls_status):
                property_obj.status = "SOLD"
                if not property_obj.mls_status:
                    property_obj.mls_status = raw_mls_status or "SOLD"
            elif self.is_off_market_status(raw_status, raw_mls_status):
                # Property is off-market - update status accordingly
                property_obj.status = "OFF_MARKET"
                # If mls_status indicates off-market but isn't already set, set it
                # Use safe check to avoid pandas NA boolean ambiguity
                if raw_mls_status is not None:
                    try:
                        # Double-check it's not pandas NA
                        if not pd.isna(raw_mls_status):
                            property_obj.mls_status = raw_mls_status
                    except (TypeError, ValueError):
                        # If check fails, skip setting mls_status
                        pass
                if not property_obj.mls_status:
                    # If no mls_status but status indicates off-market, set a default
                    property_obj.mls_status = "DELISTED"
            
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
            # Build proxy URL
            proxy_username = proxy.username
            # DataImpulse: add a random session per location/run so successive requests don't share the same session.
            # This is best-effort rotation and helps reduce Realtor.com blocking.
            if proxy_username and 'session.' not in proxy_username:
                proxy_username = f"{proxy_username};session.{secrets.randbelow(1_000_000)}"

            if proxy_username and proxy.password:
                proxy_url = f"http://{proxy_username}:{proxy.password}@{proxy.host}:{proxy.port}"
            else:
                proxy_url = f"http://{proxy.host}:{proxy.port}"
            return {
                "proxy_url": proxy_url,
                "proxy_host": proxy.host,
                "proxy_port": proxy.port,
                "proxy_username": proxy_username,
            }
            
        except Exception as e:
            logger.error(f"Error getting proxy config: {e}")
            return None
    
    def is_off_market_status(self, status: Optional[str], mls_status: Optional[str]) -> bool:
        """Check if a property status indicates it's off-market"""
        # Safely check for None/NA values to avoid pandas NA boolean ambiguity
        status_is_valid = False
        mls_status_is_valid = False
        
        try:
            if status is not None:
                # Check if it's pandas NA
                if not pd.isna(status):
                    status_is_valid = True
        except (TypeError, ValueError):
            # If pd.isna fails, assume it's valid if not None
            status_is_valid = (status is not None)
        
        try:
            if mls_status is not None:
                # Check if it's pandas NA
                if not pd.isna(mls_status):
                    mls_status_is_valid = True
        except (TypeError, ValueError):
            # If pd.isna fails, assume it's valid if not None
            mls_status_is_valid = (mls_status is not None)
        
        if not status_is_valid and not mls_status_is_valid:
            return False
        
        # If MLS status indicates SOLD, it should override OFF_MARKET
        if self.is_sold_status(status, mls_status):
            return False

        # Check status field
        if status_is_valid and str(status).upper() == 'OFF_MARKET':
            return True
        
        # Check mls_status field for off-market indicators
        if mls_status_is_valid:
            mls_status_upper = mls_status.upper()
            off_market_indicators = [
                'EXPIRED', 'WITHDRAWN', 'CANCELLED', 'INACTIVE', 'DELISTED',
                'TEMPORARILY OFF MARKET', 'TEMPORARILY_OFF_MARKET'
            ]
            if mls_status_upper in off_market_indicators:
                return True
        
        return False

    def is_sold_status(self, status: Optional[str], mls_status: Optional[str]) -> bool:
        """Check if a property status indicates it's sold"""
        status_is_valid = False
        mls_status_is_valid = False

        try:
            if status is not None:
                if not pd.isna(status):
                    status_is_valid = True
        except (TypeError, ValueError):
            status_is_valid = (status is not None)

        try:
            if mls_status is not None:
                if not pd.isna(mls_status):
                    mls_status_is_valid = True
        except (TypeError, ValueError):
            mls_status_is_valid = (mls_status is not None)

        if not status_is_valid and not mls_status_is_valid:
            return False

        status_upper = str(status).upper() if status_is_valid else ""
        mls_status_upper = str(mls_status).upper() if mls_status_is_valid else ""

        sold_indicators = ["SOLD", "CLOSED", "SETTLED"]
        if any(indicator in status_upper for indicator in sold_indicators):
            return True
        if any(indicator in mls_status_upper for indicator in sold_indicators):
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
            
            self._dump_homeharvest_df(
                properties_df,
                context="address_query",
                location=formatted_address,
                listing_type=None,
                listing_types=None,
                job_id=None,
                scheduled_job_id=None,
            )

            # Convert DataFrame to Property model
            if properties_df is not None and not properties_df.empty:
                # Take the first property found
                row = properties_df.iloc[0]
                property_obj = self.convert_to_property_model(row, "off_market_check", None, None)
                return property_obj

            # Fallback: try sold listing_type explicitly (address lookups often miss sold)
            scrape_params_sold = dict(scrape_params)
            scrape_params_sold["listing_type"] = "sold"
            try:
                sold_df = await asyncio.wait_for(
                    loop.run_in_executor(
                        self.executor,
                        lambda: scrape_property(**scrape_params_sold)
                    ),
                    timeout=timeout_seconds
                )
            except asyncio.TimeoutError:
                print(f"   [TIMEOUT] Querying SOLD for {formatted_address} timed out")
                return None

            self._dump_homeharvest_df(
                sold_df,
                context="address_query_sold",
                location=formatted_address,
                listing_type="sold",
                listing_types=None,
                job_id=None,
                scheduled_job_id=None,
            )

            if sold_df is not None and not sold_df.empty:
                row = sold_df.iloc[0]
                property_obj = self.convert_to_property_model(row, "off_market_check", "sold", None)
                return property_obj
            
            return None
            
        except Exception as e:
            print(f"Error querying property by address {formatted_address}: {e}")
            return None

    def _is_dev_env(self) -> bool:
        env = (settings.NODE_ENV or "").lower()
        return env in ("dev", "development", "local")

    def _dump_homeharvest_df(
        self,
        df: Optional[pd.DataFrame],
        *,
        context: str,
        location: Optional[str],
        listing_type: Optional[str],
        listing_types: Optional[List[str]],
        job_id: Optional[str],
        scheduled_job_id: Optional[str],
    ) -> None:
        if not self._is_dev_env() or df is None:
            return
        try:
            base_dir = os.path.join(os.getcwd(), "temp", "homeharvest_raw")
            os.makedirs(base_dir, exist_ok=True)
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            safe_location = re.sub(r"[^A-Za-z0-9._-]+", "_", location or "unknown")
            if listing_types:
                listing_types_str = "-".join([str(lt) for lt in listing_types])
            else:
                listing_types_str = listing_type or "unknown"
            safe_listing_types = re.sub(r"[^A-Za-z0-9._-]+", "_", listing_types_str)
            filename = f"{ts}_{context}_{safe_location}_{safe_listing_types}.json"
            path = os.path.join(base_dir, filename)

            cleaned_df = df.where(pd.notna(df), None)
            records = cleaned_df.to_dict(orient="records")
            payload = {
                "meta": {
                    "context": context,
                    "location": location,
                    "listing_type": listing_type,
                    "listing_types": listing_types,
                    "job_id": job_id,
                    "scheduled_job_id": scheduled_job_id,
                    "row_count": len(df),
                    "columns": list(df.columns),
                    "saved_at": datetime.utcnow().isoformat() + "Z",
                },
                "records": records,
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=True)
            logger.info(f"   [DEV] Saved raw HomeHarvest data: {path}")
        except Exception as e:
            logger.warning(f"   [DEV] Failed to save raw HomeHarvest data: {e}")
    
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
        
        Properties that are not found when queried by address are marked as OFF_MARKET since
        they have been completely removed from listing sites.
        """
        # Initialize variables at function start so they're always defined in exception handler
        total_checked = 0
        off_market_count = 0
        error_count = 0
        batch_number = 0
        total_missing = 0
        
        try:
            logger.info(
                "   [OFF-MARKET] Start check_missing_properties_for_off_market "
                "(scheduled_job_id=%s, job_id=%s, location=%s, listing_types_scraped=%s, found_ids=%s, "
                "job_start_time=%s, batch_size=%s, has_progress_logs=%s, cancel_flag=%s)",
                scheduled_job_id,
                job_id,
                location,
                listing_types_scraped,
                len(found_property_ids) if found_property_ids else 0,
                job_start_time.isoformat() if job_start_time else None,
                batch_size,
                bool(progress_logs),
                bool(cancel_flag)
            )
            # Only check for for_sale and pending properties
            listing_types_to_check = ['for_sale', 'pending']
            
            location_info = f" for location {location}" if location else ""
            logger.debug(f"   [OFF-MARKET] Checking for missing properties for scheduled job {scheduled_job_id}{location_info}")
            logger.debug(f"   [OFF-MARKET] Job start time: {job_start_time.isoformat()}")
            
            # Query database for properties with this scheduled_job_id that weren't scraped in THIS job run
            # Properties that have last_scraped < job_start_time (weren't updated in this scrape)
            # OR have last_scraped = null (never been scraped by this job)
            # Also check scraped_at field as fallback, and handle missing scheduled_job_id by checking job_id pattern
            import re
            # Extract base scheduled_job_id from pattern (e.g., "scheduled_for_sale_1760461902" from "scheduled_scheduled_for_sale_1760461902_...")
            base_scheduled_job_id = scheduled_job_id
            if scheduled_job_id.startswith("scheduled_"):
                # Remove leading "scheduled_" if present
                base_scheduled_job_id = scheduled_job_id.replace("scheduled_", "", 1)
            
            query = {
                "$or": [
                    {"scheduled_job_id": scheduled_job_id},
                    {"scheduled_job_id": base_scheduled_job_id},
                    # Fallback: match job_id pattern if scheduled_job_id field is missing
                    {"job_id": {"$regex": f"^scheduled_{re.escape(base_scheduled_job_id)}"}},
                    {"job_id": {"$regex": f"^scheduled_scheduled_{re.escape(base_scheduled_job_id)}"}}
                ],
                "listing_type": {"$in": listing_types_to_check},
                "$or": [
                    {"last_scraped": {"$lt": job_start_time}},
                    {"last_scraped": None},
                    {"last_scraped": {"$exists": False}},
                    # Also check scraped_at field as fallback
                    {"scraped_at": {"$lt": job_start_time}},
                    {"scraped_at": None},
                    {"scraped_at": {"$exists": False}}
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
            
            logger.debug(
                "   [OFF-MARKET] Querying missing properties (scheduled_job_id=%s, base_id=%s, location=%s)",
                scheduled_job_id,
                base_scheduled_job_id,
                location
            )
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
                logger.info(
                    "   [OFF-MARKET] No missing properties (scheduled_job_id=%s, job_id=%s, location=%s). "
                    "Will mark off_market_check completed in progress logs if possible.",
                    scheduled_job_id,
                    job_id,
                    location
                )
                # Still mark the check as completed even if there are no missing properties
                if location and job_id:
                    try:
                        current_job = await db.get_job(job_id)
                        if current_job:
                            latest_progress_logs = current_job.progress_logs or {"locations": []}
                            location_entry = None
                            for loc_entry in latest_progress_logs.get("locations", []):
                                if loc_entry.get("location") == location:
                                    location_entry = loc_entry
                                    break
                            
                            if location_entry:
                                if "off_market_check" not in location_entry:
                                    location_entry["off_market_check"] = {}
                                location_entry["off_market_check"]["status"] = "completed"
                                location_entry["off_market_check"]["checked"] = 0
                                location_entry["off_market_check"]["found"] = 0
                                location_entry["off_market_check"]["errors"] = 0
                                
                                if current_job.status == JobStatus.RUNNING:
                                    await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=latest_progress_logs)
                                elif current_job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                                    await db.jobs_collection.update_one(
                                        {"job_id": job_id},
                                        {"$set": {"progress_logs": latest_progress_logs, "updated_at": datetime.utcnow()}}
                                    )
                                logger.info(
                                    "   [OFF-MARKET] Marked off_market_check completed (job_id=%s, location=%s, status=%s)",
                                    job_id,
                                    location,
                                    getattr(current_job.status, "value", current_job.status)
                                )
                            else:
                                logger.warning(
                                    "   [OFF-MARKET] Location entry not found when marking completed "
                                    "(job_id=%s, location=%s).",
                                    job_id,
                                    location
                                )
                    except Exception as e:
                        logger.error(f"   [OFF-MARKET] Error updating progress logs for no missing properties: {e}")
                
                return {
                    "missing_checked": 0,
                    "off_market_found": 0,
                    "errors": 0,
                    "skipped": False
                }
            
            total_missing = len(missing_properties)
            logger.info(f"   [OFF-MARKET] Found {total_missing} missing properties, processing in batches of {batch_size}")
            if total_missing > 0:
                sample_ids = [p.get("property_id") for p in missing_properties[:5]]
                logger.debug(
                    "   [OFF-MARKET] Sample missing property IDs (first %s): %s",
                    len(sample_ids),
                    sample_ids
                )
            if total_missing > 0:
                sample_ids = [p.get("property_id") for p in missing_properties[:5]]
                logger.debug(
                    "   [OFF-MARKET] Sample missing property IDs (first %s): %s",
                    len(sample_ids),
                    sample_ids
                )
            
            off_market_count = 0
            error_count = 0
            total_checked = 0
            
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
                            # CRITICAL: Only update if job is still RUNNING (don't overwrite COMPLETED status)
                            current_job = await db.get_job(job_id)
                            if current_job and current_job.status == JobStatus.RUNNING:
                                await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                            elif current_job and current_job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                                # Job is already final, only update progress_logs without changing status
                                logger.debug(f"Job {job_id} is {getattr(current_job.status, 'value', current_job.status)}, updating only progress_logs for off-market check")
                                await db.jobs_collection.update_one(
                                    {"job_id": job_id},
                                    {"$set": {"progress_logs": progress_logs, "updated_at": datetime.utcnow()}}
                                )
                            logger.debug(
                                "   [OFF-MARKET] Updated progress logs (job_id=%s, location=%s, status=%s, checked=%s, found=%s, errors=%s)",
                                job_id,
                                location,
                                getattr(current_job.status, "value", current_job.status) if current_job else None,
                                total_checked,
                                off_market_count,
                                error_count
                            )
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
                            # Check if property is sold first (avoid misclassifying as off-market)
                            if self.is_sold_status(queried_property.status, queried_property.mls_status):
                                logger.info(
                                    f"   [OFF-MARKET] [SOLD] Property {property_id} is SOLD "
                                    f"(status={queried_property.status}, mls_status={queried_property.mls_status})"
                                )
                                old_status = prop_data.get("status") or 'UNKNOWN'
                                change_entries = []
                                if old_status != "SOLD":
                                    change_entries.append({
                                        "field": "status",
                                        "old_value": old_status,
                                        "new_value": "SOLD",
                                        "change_type": "modified",
                                        "timestamp": datetime.utcnow()
                                    })
                                await db.update_property_with_hash_and_logs(
                                    property_id=property_id,
                                    update_fields={
                                        "status": "SOLD",
                                        "mls_status": queried_property.mls_status or prop_data.get("mls_status") or "SOLD",
                                        "listing_type": "sold",
                                        "scraped_at": datetime.utcnow(),
                                        "last_scraped": datetime.utcnow()
                                    },
                                    change_entries=change_entries,
                                    job_id="off_market_detection"
                                )
                            # Check if property is off-market
                            elif self.is_off_market_status(queried_property.status, queried_property.mls_status):
                                logger.info(f"   [OFF-MARKET] [OK] Property {property_id} is OFF_MARKET (status={queried_property.status}, mls_status={queried_property.mls_status})")
                                
                                # Update property status and keep hash/change_logs consistent
                                old_status = prop_data.get("status") or 'UNKNOWN'
                                change_entries = []
                                if old_status != "OFF_MARKET":
                                    change_entries.append({
                                        "field": "status",
                                        "old_value": old_status,
                                        "new_value": "OFF_MARKET",
                                        "change_type": "modified",
                                        "timestamp": datetime.utcnow()
                                    })
                                await db.update_property_with_hash_and_logs(
                                    property_id=property_id,
                                    update_fields={
                                        "status": "OFF_MARKET",
                                        "mls_status": queried_property.mls_status or prop_data.get("mls_status"),
                                        "scraped_at": datetime.utcnow(),
                                        "last_scraped": datetime.utcnow()
                                    },
                                    change_entries=change_entries,
                                    job_id="off_market_detection"
                                )
                                
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
                            # Property not found - mark as OFF_MARKET since it's been completely removed
                            logger.info(f"   [OFF-MARKET] Property {property_id} not found in HomeHarvest - marking as OFF_MARKET")
                            
                            # Update property status to OFF_MARKET since it's no longer available
                            old_status = prop_data.get("status") or 'UNKNOWN'
                            change_entries = []
                            if old_status != "OFF_MARKET":
                                change_entries.append({
                                    "field": "status",
                                    "old_value": old_status,
                                    "new_value": "OFF_MARKET",
                                    "change_type": "modified",
                                    "timestamp": datetime.utcnow()
                                })
                            await db.update_property_with_hash_and_logs(
                                property_id=property_id,
                                update_fields={
                                    "status": "OFF_MARKET",
                                    "mls_status": "DELISTED",
                                    "scraped_at": datetime.utcnow(),
                                    "last_scraped": datetime.utcnow()
                                },
                                change_entries=change_entries,
                                job_id="off_market_detection"
                            )
                            
                            off_market_count += 1
                            # Track per listing type (only for for_sale and pending)
                            if listing_type in ['for_sale', 'pending']:
                                off_market_by_listing_type[listing_type] = off_market_by_listing_type.get(listing_type, 0) + 1
                                
                                # Update progress_logs per listing type
                                if location_entry and listing_type in location_entry.get("listing_types", {}):
                                    location_entry["listing_types"][listing_type]["off_market"] = \
                                        location_entry["listing_types"][listing_type].get("off_market", 0) + 1
                        
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
            if location and job_id:
                try:
                    # Re-fetch the latest job and progress_logs from database to avoid stale data
                    current_job = await db.get_job(job_id)
                    if not current_job:
                        logger.warning(f"   [OFF-MARKET] Job {job_id} not found, cannot update progress logs")
                    else:
                        # Get latest progress_logs from job
                        latest_progress_logs = current_job.progress_logs or {"locations": []}
                        
                        # Find location_entry in latest progress_logs
                        location_entry = None
                        for loc_entry in latest_progress_logs.get("locations", []):
                            if loc_entry.get("location") == location:
                                location_entry = loc_entry
                                break
                        
                        if location_entry:
                            # Initialize off_market_check if it doesn't exist
                            if "off_market_check" not in location_entry:
                                location_entry["off_market_check"] = {}
                            
                            # Update off-market check status
                            location_entry["off_market_check"]["status"] = "completed"
                            location_entry["off_market_check"]["checked"] = total_checked
                            location_entry["off_market_check"]["found"] = off_market_count
                            location_entry["off_market_check"]["errors"] = error_count
                            
                            # CRITICAL: Only update if job is still RUNNING (don't overwrite COMPLETED status)
                            if current_job.status == JobStatus.RUNNING:
                                await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=latest_progress_logs)
                            elif current_job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                                # Job is already final, only update progress_logs without changing status
                                logger.debug(f"Job {job_id} is {getattr(current_job.status, 'value', current_job.status)}, updating only progress_logs for off-market check completion")
                                await db.jobs_collection.update_one(
                                    {"job_id": job_id},
                                    {"$set": {"progress_logs": latest_progress_logs, "updated_at": datetime.utcnow()}}
                                )
                            logger.info(
                                "   [OFF-MARKET] Completed off-market check (job_id=%s, location=%s, status=%s, checked=%s, found=%s, errors=%s)",
                                job_id,
                                location,
                                getattr(current_job.status, "value", current_job.status),
                                total_checked,
                                off_market_count,
                                error_count
                            )
                        else:
                            logger.warning(f"   [OFF-MARKET] Location entry not found in progress_logs for location {location}")
                except Exception as e:
                    logger.error(f"   [OFF-MARKET] Error updating final progress logs: {e}")
                    try:
                        import traceback as tb
                        logger.debug(f"   [OFF-MARKET] Traceback: {tb.format_exc()}")
                    except Exception:
                        pass
            
            result = {
                "missing_checked": total_checked,
                "missing_total": total_missing,
                "missing_skipped": 0,  # No longer skipping - all are checked
                "off_market_found": off_market_count,
                "errors": error_count,
                "batches_processed": batch_number,
                "skipped": False
            }
            
            logger.info(
                f"   [OFF-MARKET] Check complete: {total_checked} checked, {off_market_count} off-market, "
                f"{error_count} errors, {batch_number} batches (job_id={job_id}, location={location})"
            )
            return result
            
        except Exception as e:
            logger.error(
                "   [OFF-MARKET] Error in off-market detection (scheduled job) "
                "(job_id=%s, scheduled_job_id=%s, location=%s): %s",
                job_id,
                scheduled_job_id,
                location,
                e
            )
            try:
                import traceback as tb
                logger.debug(f"Traceback: {tb.format_exc()}")
            except Exception:
                logger.debug(f"Could not format traceback")
            
            # CRITICAL: Always mark as completed (even on error) so UI doesn't show "in_progress" forever
            if location and job_id:
                try:
                    current_job = await db.get_job(job_id)
                    if current_job:
                        latest_progress_logs = current_job.progress_logs or {"locations": []}
                        location_entry = None
                        for loc_entry in latest_progress_logs.get("locations", []):
                            if loc_entry.get("location") == location:
                                location_entry = loc_entry
                                break
                        
                        if location_entry:
                            if "off_market_check" not in location_entry:
                                location_entry["off_market_check"] = {}
                            # Mark as failed if no properties were checked, completed if some were checked
                            location_entry["off_market_check"]["status"] = "failed" if total_checked == 0 else "completed"
                            location_entry["off_market_check"]["checked"] = total_checked
                            location_entry["off_market_check"]["found"] = off_market_count
                            location_entry["off_market_check"]["errors"] = error_count + 1
                            location_entry["off_market_check"]["error_message"] = str(e)
                            
                            # Update even if job is completed
                            await db.jobs_collection.update_one(
                                {"job_id": job_id},
                                {"$set": {"progress_logs": latest_progress_logs, "updated_at": datetime.utcnow()}}
                            )
                            logger.info(f"   [OFF-MARKET] Updated progress logs to mark off-market check as failed/completed after error")
                except Exception as update_error:
                    logger.error(f"   [OFF-MARKET] Error updating progress logs on exception: {update_error}")
            
            return {
                "missing_checked": total_checked,
                "off_market_found": off_market_count,
                "errors": error_count + 1,
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
            logger.info(
                "   [OFF-MARKET] Start check_missing_properties_for_off_market_by_location "
                "(job_id=%s, location=%s, listing_types_scraped=%s, found_ids=%s, "
                "job_start_time=%s, batch_size=%s, has_progress_logs=%s, cancel_flag=%s)",
                job_id,
                location,
                listing_types_scraped,
                len(found_property_ids) if found_property_ids else 0,
                job_start_time.isoformat() if job_start_time else None,
                batch_size,
                bool(progress_logs),
                bool(cancel_flag)
            )
            # Only check for for_sale and pending properties
            listing_types_to_check = ['for_sale', 'pending']
            
            # Parse city and state from location
            city, state = self._parse_city_state_from_location(location)
            if not city or not state:
                logger.warning(f"   [OFF-MARKET] Could not parse city/state from location '{location}', skipping check")
                logger.info(
                    "   [OFF-MARKET] Skipping location-based off-market check due to parse failure "
                    "(job_id=%s, location=%s)",
                    job_id,
                    location
                )
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
            
            logger.debug(
                "   [OFF-MARKET] Querying missing properties (city=%s, state=%s, location=%s, job_id=%s)",
                city,
                state,
                location,
                job_id
            )
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
                logger.info(
                    "   [OFF-MARKET] No missing properties for location-based check "
                    "(job_id=%s, location=%s). Will mark completed if possible.",
                    job_id,
                    location
                )
                # Still mark the check as completed even if there are no missing properties
                if location and job_id:
                    try:
                        current_job = await db.get_job(job_id)
                        if current_job:
                            latest_progress_logs = current_job.progress_logs or {"locations": []}
                            location_entry = None
                            for loc_entry in latest_progress_logs.get("locations", []):
                                if loc_entry.get("location") == location:
                                    location_entry = loc_entry
                                    break
                            
                            if location_entry:
                                if "off_market_check" not in location_entry:
                                    location_entry["off_market_check"] = {}
                                location_entry["off_market_check"]["status"] = "completed"
                                location_entry["off_market_check"]["checked"] = 0
                                location_entry["off_market_check"]["found"] = 0
                                location_entry["off_market_check"]["errors"] = 0
                                
                                if current_job.status == JobStatus.RUNNING:
                                    await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=latest_progress_logs)
                                elif current_job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                                    await db.jobs_collection.update_one(
                                        {"job_id": job_id},
                                        {"$set": {"progress_logs": latest_progress_logs, "updated_at": datetime.utcnow()}}
                                    )
                                logger.info(
                                    "   [OFF-MARKET] Marked off_market_check completed (job_id=%s, location=%s, status=%s)",
                                    job_id,
                                    location,
                                    getattr(current_job.status, "value", current_job.status)
                                )
                            else:
                                logger.warning(
                                    "   [OFF-MARKET] Location entry not found when marking completed "
                                    "(job_id=%s, location=%s).",
                                    job_id,
                                    location
                                )
                    except Exception as e:
                        logger.error(f"   [OFF-MARKET] Error updating progress logs for no missing properties: {e}")
                
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
                        # CRITICAL: Only update if job is still RUNNING (don't overwrite COMPLETED status)
                        current_job = await db.get_job(job_id)
                        if current_job and current_job.status == JobStatus.RUNNING:
                            await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                        elif current_job and current_job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                            # Job is already final, only update progress_logs without changing status
                            logger.debug(f"Job {job_id} is {getattr(current_job.status, 'value', current_job.status)}, updating only progress_logs for off-market check")
                            await db.jobs_collection.update_one(
                                {"job_id": job_id},
                                {"$set": {"progress_logs": progress_logs, "updated_at": datetime.utcnow()}}
                            )
                        logger.debug(
                            "   [OFF-MARKET] Updated progress logs (job_id=%s, location=%s, status=%s, checked=%s, found=%s, errors=%s)",
                            job_id,
                            location,
                            getattr(current_job.status, "value", current_job.status) if current_job else None,
                            total_checked,
                            off_market_count,
                            error_count
                        )
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
                            # Check if property is sold first (avoid misclassifying as off-market)
                            if self.is_sold_status(queried_property.status, queried_property.mls_status):
                                logger.info(
                                    f"   [OFF-MARKET] [SOLD] Property {property_id} is SOLD "
                                    f"(status={queried_property.status}, mls_status={queried_property.mls_status})"
                                )
                                old_status = prop_data.get("status") or 'UNKNOWN'
                                change_entries = []
                                if old_status != "SOLD":
                                    change_entries.append({
                                        "field": "status",
                                        "old_value": old_status,
                                        "new_value": "SOLD",
                                        "change_type": "modified",
                                        "timestamp": datetime.utcnow()
                                    })
                                await db.update_property_with_hash_and_logs(
                                    property_id=property_id,
                                    update_fields={
                                        "status": "SOLD",
                                        "mls_status": queried_property.mls_status or prop_data.get("mls_status") or "SOLD",
                                        "listing_type": "sold",
                                        "scraped_at": datetime.utcnow(),
                                        "last_scraped": datetime.utcnow()
                                    },
                                    change_entries=change_entries,
                                    job_id="off_market_detection"
                                )
                            # Check if property is off-market
                            elif self.is_off_market_status(queried_property.status, queried_property.mls_status):
                                logger.info(f"   [OFF-MARKET] [OK] Property {property_id} is OFF_MARKET (status={queried_property.status}, mls_status={queried_property.mls_status})")
                                
                                # Update property status and keep hash/change_logs consistent
                                old_status = prop_data.get("status") or 'UNKNOWN'
                                change_entries = []
                                if old_status != "OFF_MARKET":
                                    change_entries.append({
                                        "field": "status",
                                        "old_value": old_status,
                                        "new_value": "OFF_MARKET",
                                        "change_type": "modified",
                                        "timestamp": datetime.utcnow()
                                    })
                                await db.update_property_with_hash_and_logs(
                                    property_id=property_id,
                                    update_fields={
                                        "status": "OFF_MARKET",
                                        "mls_status": queried_property.mls_status or prop_data.get("mls_status"),
                                        "scraped_at": datetime.utcnow(),
                                        "last_scraped": datetime.utcnow()
                                    },
                                    change_entries=change_entries,
                                    job_id="off_market_detection"
                                )
                                
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
                            # Property not found - mark as OFF_MARKET since it's been completely removed
                            logger.info(f"   [OFF-MARKET] Property {property_id} not found in HomeHarvest - marking as OFF_MARKET")
                            
                            # Update property status to OFF_MARKET since it's no longer available
                            old_status = prop_data.get("status") or 'UNKNOWN'
                            change_entries = []
                            if old_status != "OFF_MARKET":
                                change_entries.append({
                                    "field": "status",
                                    "old_value": old_status,
                                    "new_value": "OFF_MARKET",
                                    "change_type": "modified",
                                    "timestamp": datetime.utcnow()
                                })
                            await db.update_property_with_hash_and_logs(
                                property_id=property_id,
                                update_fields={
                                    "status": "OFF_MARKET",
                                    "mls_status": "DELISTED",
                                    "scraped_at": datetime.utcnow(),
                                    "last_scraped": datetime.utcnow()
                                },
                                change_entries=change_entries,
                                job_id="off_market_detection"
                            )
                            
                            off_market_count += 1
                            # Track per listing type (only for for_sale and pending)
                            if listing_type in ['for_sale', 'pending']:
                                off_market_by_listing_type[listing_type] = off_market_by_listing_type.get(listing_type, 0) + 1
                                
                                # Update progress_logs per listing type
                                if location_entry and listing_type in location_entry.get("listing_types", {}):
                                    location_entry["listing_types"][listing_type]["off_market"] = \
                                        location_entry["listing_types"][listing_type].get("off_market", 0) + 1
                        
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
            if location and job_id:
                try:
                    # Re-fetch the latest job and progress_logs from database to avoid stale data
                    current_job = await db.get_job(job_id)
                    if not current_job:
                        logger.warning(f"   [OFF-MARKET] Job {job_id} not found, cannot update progress logs")
                    else:
                        # Get latest progress_logs from job
                        latest_progress_logs = current_job.progress_logs or {"locations": []}
                        
                        # Find location_entry in latest progress_logs
                        location_entry = None
                        for loc_entry in latest_progress_logs.get("locations", []):
                            if loc_entry.get("location") == location:
                                location_entry = loc_entry
                                break
                        
                        if location_entry:
                            # Initialize off_market_check if it doesn't exist
                            if "off_market_check" not in location_entry:
                                location_entry["off_market_check"] = {}
                            
                            # Update off-market check status
                            location_entry["off_market_check"]["status"] = "completed"
                            location_entry["off_market_check"]["checked"] = total_checked
                            location_entry["off_market_check"]["found"] = off_market_count
                            location_entry["off_market_check"]["errors"] = error_count
                            
                            # CRITICAL: Only update if job is still RUNNING (don't overwrite COMPLETED status)
                            if current_job.status == JobStatus.RUNNING:
                                await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=latest_progress_logs)
                            elif current_job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                                # Job is already final, only update progress_logs without changing status
                                logger.debug(f"Job {job_id} is {getattr(current_job.status, 'value', current_job.status)}, updating only progress_logs for off-market check completion")
                                await db.jobs_collection.update_one(
                                    {"job_id": job_id},
                                    {"$set": {"progress_logs": latest_progress_logs, "updated_at": datetime.utcnow()}}
                                )
                            logger.info(
                                "   [OFF-MARKET] Completed off-market check (job_id=%s, location=%s, status=%s, checked=%s, found=%s, errors=%s)",
                                job_id,
                                location,
                                getattr(current_job.status, "value", current_job.status),
                                total_checked,
                                off_market_count,
                                error_count
                            )
                        else:
                            logger.warning(f"   [OFF-MARKET] Location entry not found in progress_logs for location {location}")
                except Exception as e:
                    logger.error(f"   [OFF-MARKET] Error updating final progress logs: {e}")
                    try:
                        import traceback as tb
                        logger.debug(f"   [OFF-MARKET] Traceback: {tb.format_exc()}")
                    except Exception:
                        pass
            
            result = {
                "missing_checked": total_checked,
                "missing_total": total_missing,
                "missing_skipped": 0,  # No longer skipping - all are checked
                "off_market_found": off_market_count,
                "errors": error_count,
                "batches_processed": batch_number,
                "skipped": False
            }
            
            logger.info(
                f"   [OFF-MARKET] Check complete: {total_checked} checked, {off_market_count} off-market, "
                f"{error_count} errors, {batch_number} batches (job_id={job_id}, location={location})"
            )
            return result
            
        except Exception as e:
            logger.error(
                "   [OFF-MARKET] Error in off-market detection (location-based) "
                "(job_id=%s, location=%s): %s",
                job_id,
                location,
                e
            )
            try:
                import traceback as tb
                logger.debug(tb.format_exc())
            except Exception:
                logger.debug(f"Could not format traceback")
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
            logger.info(
                "   [OFF-MARKET] Background start (job_id=%s, scheduled_job_id=%s, location=%s, listing_types=%s, found_ids=%s)",
                job.job_id,
                job.scheduled_job_id,
                location,
                listing_types_to_scrape,
                len(found_property_ids) if found_property_ids else 0
            )
            if job.scheduled_job_id:
                # Scheduled job: use scheduled_job_id with location filter
                result = await self.check_missing_properties_for_off_market(
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
                result = await self.check_missing_properties_for_off_market_by_location(
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
            logger.info(
                "   [OFF-MARKET] Background complete (job_id=%s, location=%s, result=%s)",
                job.job_id,
                location,
                result
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
