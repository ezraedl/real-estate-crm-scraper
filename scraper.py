import asyncio
import random
import re
import time
import logging
import traceback
import secrets

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
        # Track if job runs were full scrapes (keyed by job_id)
        self.job_run_flags: Dict[str, Dict[str, Any]] = {}
        # Store cookies per proxy session to maintain session state (key: proxy_username, value: cookies dict)
        self.session_cookies: Dict[str, Dict[str, str]] = {}
    
    def _get_browser_headers(self) -> Dict[str, str]:
        """Get random browser headers to avoid anti-bot detection"""
        from proxy_manager import proxy_manager
        random_headers = proxy_manager.get_random_headers()
        # Add Referer to make it look like navigation from Realtor.com
        random_headers["Referer"] = "https://www.realtor.com/"
        
        # Ensure Sec-Fetch headers are present for modern browsers (Chrome/Edge)
        if "Chrome" in random_headers.get("User-Agent", "") and "Sec-Fetch-Dest" not in random_headers:
            random_headers["Sec-Fetch-Dest"] = "document"
            random_headers["Sec-Fetch-Mode"] = "navigate"
            random_headers["Sec-Fetch-Site"] = "same-origin"
            random_headers["Sec-Fetch-User"] = "?1"
        
        # Add viewport-related headers for better mobile/desktop consistency
        if "Mobile" not in random_headers.get("User-Agent", ""):
            # Desktop browser - add viewport width hint
            random_headers["Viewport-Width"] = str(random.choice([1920, 2560, 1440, 1366, 1536]))
        
        return random_headers
    
    def _patch_requests_with_headers(self, headers: Dict[str, str], cookies: Optional[Dict[str, str]] = None):
        """Monkey-patch requests library to add browser headers, cookies, and TLS fingerprinting"""
        try:
            # Try to use curl_cffi first for TLS fingerprinting (mimics real browser TLS)
            # curl_cffi provides requests-compatible API with real browser TLS fingerprints
            try:
                from curl_cffi import requests as curl_requests
                import requests
                
                # curl_cffi has requests-compatible API with TLS fingerprinting
                # Use it to replace requests for better anti-bot evasion
                original_get = requests.get
                original_post = requests.post
                original_request = requests.request
                
                # Determine browser type from User-Agent for TLS fingerprint
                user_agent = headers.get("User-Agent", "")
                if "Chrome" in user_agent:
                    impersonate = "chrome120"  # Chrome 120 TLS fingerprint
                elif "Firefox" in user_agent:
                    impersonate = "firefox120"  # Firefox 120 TLS fingerprint
                elif "Safari" in user_agent:
                    impersonate = "safari17_0"  # Safari 17 TLS fingerprint
                else:
                    impersonate = "chrome120"  # Default to Chrome
                
                # Create a session-like cookie jar for this request
                cookie_jar = requests.cookies.RequestsCookieJar()
                if cookies:
                    for name, value in cookies.items():
                        cookie_jar.set(name, value, domain='.realtor.com', path='/')
                
                def patched_get(*args, **kwargs):
                    if 'headers' not in kwargs:
                        kwargs['headers'] = {}
                    kwargs['headers'].update(headers)
                    # Merge cookies if not already provided
                    if 'cookies' not in kwargs and cookie_jar:
                        if isinstance(kwargs.get('cookies'), dict):
                            kwargs['cookies'].update(dict(cookie_jar))
                        else:
                            kwargs['cookies'] = cookie_jar
                    # Use curl_cffi with TLS fingerprinting
                    kwargs['impersonate'] = impersonate
                    return curl_requests.get(*args, **kwargs)
                
                def patched_post(*args, **kwargs):
                    if 'headers' not in kwargs:
                        kwargs['headers'] = {}
                    kwargs['headers'].update(headers)
                    # Merge cookies if not already provided
                    if 'cookies' not in kwargs and cookie_jar:
                        if isinstance(kwargs.get('cookies'), dict):
                            kwargs['cookies'].update(dict(cookie_jar))
                        else:
                            kwargs['cookies'] = cookie_jar
                    # Use curl_cffi with TLS fingerprinting
                    kwargs['impersonate'] = impersonate
                    return curl_requests.post(*args, **kwargs)
                
                def patched_request(*args, **kwargs):
                    if 'headers' not in kwargs:
                        kwargs['headers'] = {}
                    kwargs['headers'].update(headers)
                    # Merge cookies if not already provided
                    if 'cookies' not in kwargs and cookie_jar:
                        if isinstance(kwargs.get('cookies'), dict):
                            kwargs['cookies'].update(dict(cookie_jar))
                        else:
                            kwargs['cookies'] = cookie_jar
                    # Use curl_cffi with TLS fingerprinting
                    kwargs['impersonate'] = impersonate
                    return curl_requests.request(*args, **kwargs)
                
                # Apply patches
                requests.get = patched_get
                requests.post = patched_post
                requests.request = patched_request
                
                logger.debug(f"   [TLS] Using curl_cffi with {impersonate} TLS fingerprint for better anti-bot evasion")
                return (original_get, original_post, original_request)
                
            except ImportError:
                # Fallback to regular requests if curl_cffi not available
                import requests
                from http.cookiejar import CookieJar
                
                original_get = requests.get
                original_post = requests.post
                original_request = requests.request
                
                # Create a session-like cookie jar for this request
                cookie_jar = requests.cookies.RequestsCookieJar()
                if cookies:
                    for name, value in cookies.items():
                        cookie_jar.set(name, value, domain='.realtor.com', path='/')
                
                def patched_get(*args, **kwargs):
                    if 'headers' not in kwargs:
                        kwargs['headers'] = {}
                    kwargs['headers'].update(headers)
                    # Merge cookies if not already provided
                    if 'cookies' not in kwargs and cookie_jar:
                        if isinstance(kwargs.get('cookies'), dict):
                            kwargs['cookies'].update(dict(cookie_jar))
                        else:
                            kwargs['cookies'] = cookie_jar
                    return original_get(*args, **kwargs)
                
                def patched_post(*args, **kwargs):
                    if 'headers' not in kwargs:
                        kwargs['headers'] = {}
                    kwargs['headers'].update(headers)
                    # Merge cookies if not already provided
                    if 'cookies' not in kwargs and cookie_jar:
                        if isinstance(kwargs.get('cookies'), dict):
                            kwargs['cookies'].update(dict(cookie_jar))
                        else:
                            kwargs['cookies'] = cookie_jar
                    return original_post(*args, **kwargs)
                
                def patched_request(*args, **kwargs):
                    if 'headers' not in kwargs:
                        kwargs['headers'] = {}
                    kwargs['headers'].update(headers)
                    # Merge cookies if not already provided
                    if 'cookies' not in kwargs and cookie_jar:
                        if isinstance(kwargs.get('cookies'), dict):
                            kwargs['cookies'].update(dict(cookie_jar))
                        else:
                            kwargs['cookies'] = cookie_jar
                    return original_request(*args, **kwargs)
                
                # Apply patches
                requests.get = patched_get
                requests.post = patched_post
                requests.request = patched_request
                
                logger.warning(f"   [TLS] curl_cffi not available, using regular requests (install curl-cffi for TLS fingerprinting)")
                return (original_get, original_post, original_request)
        except ImportError:
            return None
    
    def _restore_requests(self, original_funcs):
        """Restore original requests functions"""
        if original_funcs:
            import requests
            requests.get, requests.post, requests.request = original_funcs
    
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
        try:
            # Update status to RUNNING immediately to prevent jobs from staying in PENDING
            await db.update_job_status(job.job_id, JobStatus.RUNNING)
            logger.info(f"Starting job {job.job_id} - status updated to RUNNING")
        except Exception as e:
            logger.error(f"Failed to update job {job.job_id} status to RUNNING: {e}")
            # Continue anyway - we'll try again below
        
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
                    "listing_types": job.listing_types or (job.listing_type and [job.listing_type]) or ["for_sale", "sold", "for_rent", "pending"],
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
            
            # Anti-blocking: Randomize location order to avoid predictable patterns
            # This makes the scraper look less like an automated bot
            locations_to_process = job.locations.copy()
            if len(locations_to_process) > 1:
                random.shuffle(locations_to_process)
                logger.info(f"[ANTI-BOT] Randomized order of {len(locations_to_process)} locations to avoid detection")
            
            # Process each location
            for i, location in enumerate(locations_to_process):
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
                
                # Random delay between locations with human-like variation
                # Increased delays significantly to reduce blocking: 10-20 seconds between locations
                # Add variation: sometimes user "reads" results (longer), sometimes quick navigation (shorter)
                if not location_failed:
                    if random.random() < 0.2:  # 20% chance of "reading" delay
                        delay = random.uniform(15.0, 25.0)  # User reviewing results
                    else:
                        delay = max(job.request_delay, 10.0) + random.uniform(0, 5)  # 10-15 seconds normal
                    logger.info(f"   [THROTTLE] Waiting {delay:.1f}s before next location (human-like pattern)")
                    await asyncio.sleep(delay)
                else:
                    # Longer delay after failures to avoid immediate retry with same proxy
                    # Add jitter to make it less predictable
                    delay = 5.0 + random.uniform(0, 5)  # 5-10 seconds (increased from 5-8)
                    logger.info(f"   [THROTTLE] Waiting {delay:.1f}s after failure before next location")
                    await asyncio.sleep(delay)
            
            # Retry failed locations once if there are any
            if failed_locations:
                logger.info(f"[RETRY] Retrying {len(failed_locations)} failed location(s)...")
                retry_failed_locations = []
                
                for idx, failed_location_entry in enumerate(failed_locations):
                    location = failed_location_entry["location"]
                    failed_location_entry["retry_count"] = failed_location_entry.get("retry_count", 0) + 1
                    
                    # Anti-blocking: Add delay before retry (exponential backoff)
                    # Increased delays significantly for better blocking resistance
                    if idx > 0:
                        # Retry with exponential backoff and human-like variation
                        # Add variation to make retries less predictable
                        if random.random() < 0.3:  # 30% chance of longer "cooldown" delay
                            retry_delay = random.uniform(20.0, 30.0)  # Extended cooldown
                        else:
                            retry_delay = random.uniform(15.0, 25.0)  # 15-25 seconds between retries
                        logger.info(f"[RETRY] Waiting {retry_delay:.1f}s before retrying location {location} (human-like backoff)")
                        await asyncio.sleep(retry_delay)
                    else:
                        # First retry: longer delay to let proxy/IP cool down with variation
                        if random.random() < 0.25:  # 25% chance of longer initial cooldown
                            retry_delay = random.uniform(12.0, 18.0)  # Extended initial cooldown
                        else:
                            retry_delay = random.uniform(10.0, 15.0)  # 10-15 seconds normal
                        logger.info(f"[RETRY] Waiting {retry_delay:.1f}s before first retry (human-like)")
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
            
            # Mark job as completed (even if some locations failed)
            final_status = JobStatus.COMPLETED
            completion_message = f"Job completed successfully. {successful_locations}/{len(job.locations)} locations processed."
            
            if failed_locations:
                completion_message += f" {len(failed_locations)} location(s) failed after retry."
                logger.info(f"[SUMMARY] Job {job.job_id} completed with {len(failed_locations)} failed location(s) out of {len(job.locations)} total")
            
            # Add completion info to summary
            # Get the scrape type from job_started (or determine it again if not set)
            scrape_type = progress_logs.get("job_started", {}).get("scrape_type", "full")
            scrape_type_details = progress_logs.get("job_started", {}).get("scrape_type_details")
            
            # Collect all errors from locations for error summary
            all_errors = []
            for loc_entry in progress_logs.get("locations", []):
                if loc_entry.get("error"):
                    all_errors.append({
                        "location": loc_entry.get("location"),
                        "error": loc_entry.get("error"),
                        "error_type": loc_entry.get("error_type", "Unknown")
                    })
                # Also check listing_types for errors
                for listing_type, data in loc_entry.get("listing_types", {}).items():
                    if data.get("error"):
                        all_errors.append({
                            "location": loc_entry.get("location"),
                            "listing_type": listing_type,
                            "error": data.get("error"),
                            "error_type": data.get("error_type", "Unknown")
                        })
            
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
                "errors": all_errors if all_errors else None
            }
            
            # CRITICAL: Update job status to COMPLETED with ALL completion data in ONE atomic operation
            # This ensures completed_at, status, and all location counts are set together
            logger.info(f"[COMPLETION] All {len(job.locations)} locations processed. Updating job {job.job_id} status to COMPLETED...")
            status_update_success = False
            max_retries = 5  # Increased retries for reliability
            for retry in range(max_retries):
                try:
                    update_success = await db.update_job_status(
                        job.job_id,
                        final_status,  # COMPLETED
                        completed_locations=successful_locations,
                        total_locations=len(job.locations),
                        properties_scraped=total_properties,
                        properties_saved=saved_properties,
                        properties_inserted=total_inserted,
                        properties_updated=total_updated,
                        properties_skipped=total_skipped,
                        failed_locations=failed_locations,
                        progress_logs=progress_logs,
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
                            
                            if verify_status_value == JobStatus.COMPLETED.value:
                                status_update_success = True
                                logger.info(
                                    f"[COMPLETION] ✅ Successfully updated and verified job {job.job_id} status to COMPLETED "
                                    f"(attempt {retry + 1}/{max_retries}). "
                                    f"completed_locations={successful_locations}, total_locations={len(job.locations)}"
                                )
                                break
                            else:
                                logger.warning(
                                    f"[COMPLETION] ⚠️ Job {job.job_id} status update reported success but verification failed "
                                    f"(attempt {retry + 1}/{max_retries}). "
                                    f"Expected COMPLETED, got {verify_status_value}. Retrying..."
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
                logger.error(f"[COMPLETION] ❌ CRITICAL: Failed to update job {job.job_id} status to COMPLETED after {max_retries} attempts!")
                # Still try to update in finally block as last resort
                # But also try one more direct update here
                try:
                    logger.warning(f"[COMPLETION] Attempting direct MongoDB update as last resort...")
                    await db.jobs_collection.update_one(
                        {"job_id": job.job_id},
                        {
                            "$set": {
                                "status": JobStatus.COMPLETED.value,
                                "completed_at": datetime.utcnow(),
                                "completed_locations": successful_locations,
                                "total_locations": len(job.locations),
                                "updated_at": datetime.utcnow()
                            }
                        }
                    )
                    # Verify one more time
                    final_verify = await db.get_job(job.job_id)
                    if final_verify and (isinstance(final_verify.status, JobStatus) and final_verify.status == JobStatus.COMPLETED or str(final_verify.status).lower() == "completed"):
                        logger.info(f"[COMPLETION] ✅ Direct MongoDB update succeeded for job {job.job_id}")
                        status_update_success = True
                    else:
                        logger.error(f"[COMPLETION] ❌ Direct MongoDB update also failed for job {job.job_id}")
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
                            JobStatus.COMPLETED,
                            next_run_at=next_run,
                            was_full_scrape=was_full_scrape
                        )
                        logger.debug(f"Updated run history for scheduled job: {job.scheduled_job_id} (was_full_scrape={was_full_scrape})")
                
                # Legacy: Update run history for old recurring jobs
                elif job.original_job_id:
                    await db.update_recurring_job_run_history(
                        job.original_job_id,
                        job.job_id,
                        JobStatus.COMPLETED
                    )
                    logger.debug(f"Updated run history for legacy recurring job: {job.original_job_id}")
            except Exception as run_history_error:
                # Log the error but don't fail the job - the job itself completed successfully
                logger.warning(
                    f"Failed to update scheduled job run history for job {job.job_id}, "
                    f"but job completed successfully: {run_history_error}"
                )
            
            logger.info(f"Job {job.job_id} completed: {saved_properties} properties saved")
            
            # FINAL SAFETY CHECK: Ensure status is COMPLETED before exiting try block
            # This is the last chance to fix it if something went wrong
            final_check = await db.get_job(job.job_id)
            if final_check and final_check.status != JobStatus.COMPLETED:
                logger.error(
                    f"CRITICAL: Job {job.job_id} status is {final_check.status.value} instead of COMPLETED! "
                    f"Force updating to COMPLETED..."
                )
                await db.update_job_status(
                    job.job_id, 
                    JobStatus.COMPLETED, 
                    completed_locations=successful_locations,
                    total_locations=len(job.locations),
                    progress_logs=progress_logs
                )
                # Verify one more time
                verify_final = await db.get_job(job.job_id)
                if verify_final and verify_final.status == JobStatus.COMPLETED:
                    logger.info(f"Successfully force-updated job {job.job_id} to COMPLETED")
                else:
                    logger.error(f"FAILED to force-update job {job.job_id} to COMPLETED! Status: {verify_final.status.value if verify_final else 'NOT FOUND'}")
            
        except Exception as e:
            logger.error(f"Error processing job {job.job_id}: {e}")
            
            # Check if job was already marked as COMPLETED before overwriting with FAILED
            # This prevents successful jobs from being marked as failed due to post-completion errors
            current_job = await db.get_job(job.job_id)
            if current_job and current_job.status == JobStatus.COMPLETED:
                logger.warning(
                    f"Job {job.job_id} encountered an error after completion, "
                    f"but keeping status as COMPLETED since all locations finished successfully. Error: {e}"
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
                            await db.update_scheduled_job_run_history(
                                job.scheduled_job_id,
                                job.job_id,
                                JobStatus.COMPLETED,
                                next_run_at=next_run,
                                was_full_scrape=was_full_scrape
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
                            await db.update_scheduled_job_run_history(
                                job.scheduled_job_id,
                                job.job_id,
                                JobStatus.FAILED,
                                next_run_at=next_run,
                                was_full_scrape=was_full_scrape
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
                    
                    # Only mark as COMPLETED if we have explicit completion signal
                    if job_completed and completed_locations >= total_locations and total_locations > 0:
                        logger.warning(
                            f"Job {job.job_id} was still in RUNNING status but has completion signal. "
                            f"Updating status to COMPLETED in finally block (safety net)."
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
                                JobStatus.COMPLETED,
                                completed_locations=completed_locations,
                                total_locations=total_locations,
                                progress_logs=progress_logs
                            )
                            if update_success:
                                verify_job = await db.get_job(job.job_id)
                                if verify_job and verify_job.status == JobStatus.COMPLETED:
                                    current_job = verify_job
                                    logger.info(f"Successfully updated job {job.job_id} to COMPLETED in finally block (attempt {retry + 1})")
                                    break
                                else:
                                    await asyncio.sleep(0.5)
                            else:
                                await asyncio.sleep(0.5)
                        else:
                            logger.error(f"Failed to update job {job.job_id} to COMPLETED in finally block after 3 attempts")
                    
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
                    from config import settings
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
                    logger.info(f"   [FETCH] Fetching all listing types ({', '.join(listing_types_to_scrape)}) from {location} in a single request...")
                    
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
                    from config import settings
                    scrape_timeout = min(settings.LOCATION_TIMEOUT_MINUTES * 60, 600)  # Max 10 minutes per location
                    try:
                        # Make a single call with all listing types
                        all_properties_by_type = await asyncio.wait_for(
                            self._scrape_all_listing_types(
                                location, 
                                job, 
                                current_proxy_config, 
                                listing_types_to_scrape, 
                                limit=job.limit if job.limit else None,
                                past_days=job.past_days if job.past_days else 90
                            ),
                            timeout=scrape_timeout
                        )
                    except asyncio.TimeoutError:
                        logger.warning(f"   [TIMEOUT] Scraping all listing types in {location} exceeded {scrape_timeout}s timeout")
                        all_properties_by_type = {lt: [] for lt in listing_types_to_scrape}  # Empty results for all types
                    
                    end_time = datetime.utcnow()
                    location_total_found = sum(len(v) for v in all_properties_by_type.values())
                    was_full_scrape = self.job_run_flags.get(job.job_id, {}).get("was_full_scrape")
                    if was_full_scrape is True and location_total_found == 0:
                        error_msg = "COMPLETED_WITH_ERRORS: Full scrape returned 0 properties for this location."
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
                    
                    # Anti-blocking: Increased delay after processing location with human-like variation
                    # Add variation: sometimes user "processes" results (longer), sometimes quick (shorter)
                    if random.random() < 0.15:  # 15% chance of longer "processing" delay
                        post_location_delay = random.uniform(6.0, 10.0)  # User "processing" results
                    else:
                        post_location_delay = random.uniform(3.0, 6.0)  # Normal post-processing
                    logger.info(f"   [THROTTLE] Waiting {post_location_delay:.1f}s after processing location (human-like)")
                    await asyncio.sleep(post_location_delay)
                    
                except Exception as e:
                    error_msg = str(e)
                    error_type = type(e).__name__
                    blocked_by_realtor = _is_realtor_block_exception(e)
                    logger.warning(f"   [WARNING] Error scraping all listing types in {location}: {error_msg}")
                    logger.debug(f"   [WARNING] Full traceback: {traceback.format_exc()}")
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
                                    logger.debug(f"Job {job_id} is {current_job.status.value}, updating only progress_logs for enrichment")
                                    await db.jobs_collection.update_one(
                                        {"job_id": job_id},
                                        {"$set": {"progress_logs": progress_logs, "updated_at": datetime.utcnow()}}
                                    )
                        except Exception as e:
                            logger.error(f"Error updating job status with enrichment progress: {e}")
    
    async def _scrape_all_listing_types(self, location: str, job: ScrapingJob, proxy_config: Optional[Dict[str, Any]], listing_types: List[str], limit: Optional[int] = None, past_days: Optional[int] = None) -> Dict[str, List[Property]]:
        """Scrape properties for all listing types in a single request. Returns a dict mapping listing_type to properties."""
        try:
            logger.debug(f"   [DEBUG] _scrape_all_listing_types called with listing_types: {listing_types}")
            
            # Prepare scraping parameters - same logic as _scrape_listing_type but for all types
            zip_match = re.search(r'\b(\d{5})\b', location)
            if zip_match:
                location_to_use = zip_match.group(1)
                logger.debug(f"   [LOCATION] Using zip code '{location_to_use}' from location '{location}' for precise matching")
            else:
                location_to_use = location
                logger.debug(f"   [LOCATION] Using full location format '{location_to_use}' (no zip code found)")
            
            scrape_params = {
                "location": location_to_use,
                "listing_type": listing_types,  # Pass list of listing types for single request
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
            
            # Anti-blocking: Add realistic browser headers to avoid detection
            # Realtor.com uses Kasada anti-bot protection that checks headers
            browser_headers = self._get_browser_headers()
            
            # Log the exact parameters being passed to homeharvest for debugging
            log_params = {k: v for k, v in scrape_params.items() if k != "proxy"}
            proxy_enabled = bool(scrape_params.get("proxy"))
            proxy_host = (proxy_config or {}).get("proxy_host") if proxy_enabled else None
            proxy_port = (proxy_config or {}).get("proxy_port") if proxy_enabled else None
            user_agent = browser_headers.get("User-Agent", "N/A")[:50]
            logger.info(f"   [HOMEHARVEST] Calling scrape_property proxy_enabled={proxy_enabled} proxy_host={proxy_host} proxy_port={proxy_port} user_agent={user_agent}... params={log_params}")
            
            # Anti-blocking: Add random delay with human-like variation before scraping
            # Use a more realistic delay pattern: sometimes faster (3-7s), sometimes slower (8-15s)
            # This mimics real user behavior better than uniform delays
            if random.random() < 0.3:  # 30% chance of slower "reading" delay
                pre_scrape_delay = random.uniform(8.0, 15.0)  # User "reading" the page
            else:
                pre_scrape_delay = random.uniform(3.0, 7.0)  # Quick navigation
            logger.info(f"   [THROTTLE] Waiting {pre_scrape_delay:.1f}s before scraping (human-like delay)")
            await asyncio.sleep(pre_scrape_delay)
            
            # Scrape properties - Run blocking call in thread pool
            timeout_seconds = 600  # 10 minutes timeout for all listing types combined
            loop = asyncio.get_event_loop()
            
            properties_df = None
            scrape_error = None
            
            # Get or create session cookies for this proxy to maintain session state
            proxy_username = (proxy_config or {}).get("proxy_username", "default")
            session_cookies = self.session_cookies.get(proxy_username, {})
            
            # Monkey-patch requests library to add browser headers and cookies (homeharvest uses requests internally)
            # This makes requests look like they come from a real browser with session cookies
            original_funcs = self._patch_requests_with_headers(browser_headers, cookies=session_cookies if session_cookies else None)
            if original_funcs:
                logger.debug(f"   [HEADERS] Patched requests library with browser headers and cookies")
            
            try:
                properties_df = await asyncio.wait_for(
                    loop.run_in_executor(
                        self.executor,
                        lambda: scrape_property(**scrape_params)
                    ),
                    timeout=timeout_seconds
                )
                
                num_properties = len(properties_df) if properties_df is not None and not properties_df.empty else 0
                logger.info(f"   [HOMEHARVEST] Received {num_properties} total properties for location='{location}', listing_types={listing_types}")
                
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
                logger.debug(f"   [HOMEHARVEST ERROR] Full traceback: {traceback.format_exc()}")
                scrape_error = error_msg
                properties_df = None
            finally:
                # Restore original functions
                if original_funcs:
                    self._restore_requests(original_funcs)
            
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
                
                for idx, listing_type in enumerate(listing_types):
                    try:
                        # Anti-blocking: Add delay between fallback calls with human-like variation
                        # Increased from 3-6s to 8-15s for better blocking resistance
                        # Add variation to mimic user behavior
                        if idx > 0:
                            if random.random() < 0.25:  # 25% chance of longer delay
                                fallback_delay = random.uniform(12.0, 18.0)  # User "thinking" or "reading"
                            else:
                                fallback_delay = random.uniform(8.0, 12.0)  # Normal navigation
                            logger.info(f"   [THROTTLE] Waiting {fallback_delay:.1f}s before fallback call for '{listing_type}' (human-like)")
                            await asyncio.sleep(fallback_delay)
                        
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
                        
                        individual_df = await asyncio.wait_for(
                            loop.run_in_executor(
                                self.executor,
                                lambda lt=listing_type, params=individual_params: scrape_property(**params)
                            ),
                            timeout=timeout_seconds // len(listing_types)  # Divide timeout among types
                        )
                        
                        if individual_df is not None and not individual_df.empty:
                            num_individual = len(individual_df)
                            logger.info(f"   [FALLBACK] Individual call for '{listing_type}' returned {num_individual} properties")
                            
                            # Convert to Property objects
                            for index, row in individual_df.iterrows():
                                try:
                                    property_obj = self.convert_to_property_model(row, job.job_id, listing_type, job.scheduled_job_id)
                                    if listing_type == "sold":
                                        property_obj.is_comp = True
                                    properties_by_type_fallback[listing_type].append(property_obj)
                                except Exception as e:
                                    logger.error(f"Error converting property for {listing_type} in fallback: {e}")
                                    continue
                        else:
                            logger.warning(f"   [FALLBACK] Individual call for '{listing_type}' returned empty result")
                    except Exception as e:
                        error_type = type(e).__name__
                        blocked_by_realtor = _is_realtor_block_exception(e)
                        logger.error(f"   [FALLBACK] Error in individual call for '{listing_type}': {e} (Type: {error_type}, Blocked: {blocked_by_realtor})")
                        logger.debug(f"   [FALLBACK] Full traceback: {traceback.format_exc()}")
                        
                        # Anti-blocking: If blocked, add exponential backoff with human-like jitter before next fallback call
                        if blocked_by_realtor and idx < len(listing_types) - 1:
                            base_backoff = min(15.0 * (2 ** idx), 120.0)  # Exponential backoff, max 120s (increased from 60s)
                            # Add jitter (±20%) to make backoff less predictable
                            jitter = base_backoff * 0.2 * (random.random() * 2 - 1)  # ±20% variation
                            backoff_delay = max(5.0, base_backoff + jitter)  # Ensure minimum 5s
                            logger.warning(f"   [THROTTLE] Blocked by Realtor.com, waiting {backoff_delay:.1f}s before next fallback call (exponential backoff with jitter)")
                            await asyncio.sleep(backoff_delay)
                        
                        continue
                
                # If fallback got results, use them
                total_fallback = sum(len(props) for props in properties_by_type_fallback.values())
                if total_fallback > 0:
                    logger.info(f"   [FALLBACK] Fallback individual calls succeeded! Got {total_fallback} total properties across all types")
                    return properties_by_type_fallback
                else:
                    logger.error(f"   [FALLBACK] All individual calls also failed or returned empty. Original error: {scrape_error}")
                    # Store the error for display in UI - this will be handled by the caller
                    if scrape_error:
                        # The error will be caught and stored in progress_logs by the caller
                        pass
            
            # Split properties by listing_type from the DataFrame
            # HomeHarvest should include a 'listing_type' column in the DataFrame when multiple types are requested
            properties_by_type: Dict[str, List[Property]] = {lt: [] for lt in listing_types}
            
            # If properties_df is None or empty and we haven't already tried fallback, return empty
            if properties_df is None:
                logger.warning(f"   [WARNING] properties_df is None for location='{location}'. Returning empty results.")
                # Fallback was already attempted above if scrape_error exists
                return properties_by_type
            
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
                        if status_value is None:
                            return None
                        
                        status_str = str(status_value).upper() if status_value else ""
                        mls_status_str = str(mls_status_value).upper() if mls_status_value else ""
                        
                        # Check for sold properties first (most specific)
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
                            
                            inferred_type = infer_listing_type_from_status(status_value, mls_status_value)
                            
                            # Use inferred type if it's in our requested listing types, otherwise use first type as fallback
                            if inferred_type and inferred_type in listing_types:
                                listing_type = inferred_type
                            else:
                                listing_type = listing_types[0]  # Fallback to first type
                                unassigned_count += 1
                                # Track unassigned property details for logging
                                unassigned_properties.append({
                                    'status': str(status_value) if status_value is not None else 'None',
                                    'mls_status': str(mls_status_value) if mls_status_value is not None else 'None',
                                    'inferred_type': inferred_type if inferred_type else 'None',
                                    'address': address_value if address_value else 'Unknown',
                                    'assigned_to': listing_type  # Show which type it was assigned to as fallback
                                })
                            
                            property_obj = self.convert_to_property_model(row, job.job_id, listing_type, job.scheduled_job_id)
                            if listing_type == "sold":
                                property_obj.is_comp = True
                            properties_by_type[listing_type].append(property_obj)
                            inferred_count[listing_type] += 1
                        except Exception as e:
                            logger.error(f"Error converting property: {e}")
                            continue
                    
                    # Log summary of inferred split
                    total_split = sum(len(props) for props in properties_by_type.values())
                    split_summary = ', '.join([f'{lt}={len(properties_by_type[lt])}' for lt in listing_types])
                    logger.info(f"   [SPLIT] Inferred listing types from status field: {split_summary} (total: {total_split}, unassigned: {unassigned_count})")
                    
                    # Log details of unassigned properties
                    if unassigned_count > 0:
                        logger.warning(f"   [UNASSIGNED] Found {unassigned_count} properties that don't match any of the 4 expected listing types:")
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
            
            return properties_by_type
            
        except Exception as e:
            logger.error(f"Error scraping all listing types in {location}: {e}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return {lt: [] for lt in listing_types}
    
    async def _scrape_listing_type(self, location: str, job: ScrapingJob, proxy_config: Optional[Dict[str, Any]], listing_type: str, limit: Optional[int] = None, past_days: Optional[int] = None) -> List[Property]:
        """Scrape properties for a specific listing type"""
        try:
            logger.debug(f"   [DEBUG] _scrape_listing_type called with listing_type: '{listing_type}' (type: {type(listing_type)})")
            
            # Prepare scraping parameters - remove all filtering for comprehensive data
            # According to homeharvest docs, location accepts: zip code, city, "city, state", or full address
            # For best precision, use just the zip code when available (e.g., "46201" instead of "Indianapolis, IN 46201")
            # Extract zip code if present for more precise matching
            zip_match = re.search(r'\b(\d{5})\b', location)
            if zip_match:
                # Use just the zip code for maximum precision (per homeharvest documentation)
                location_to_use = zip_match.group(1)
                logger.debug(f"   [LOCATION] Using zip code '{location_to_use}' from location '{location}' for precise matching")
            else:
                # No zip code found, use the original location format (city, state, or full address)
                location_to_use = location
                logger.debug(f"   [LOCATION] Using full location format '{location_to_use}' (no zip code found)")
            
            scrape_params = {
                "location": location_to_use,  # Use zip code if available, otherwise original format
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
            
            # Anti-blocking: Add realistic browser headers to avoid detection
            browser_headers = self._get_browser_headers()
            
            # Log the exact parameters being passed to homeharvest for debugging
            log_params = {k: v for k, v in scrape_params.items() if k != "proxy"}  # Don't log proxy URL
            proxy_enabled = bool(scrape_params.get("proxy"))
            proxy_host = (proxy_config or {}).get("proxy_host") if proxy_enabled else None
            proxy_port = (proxy_config or {}).get("proxy_port") if proxy_enabled else None
            user_agent = browser_headers.get("User-Agent", "N/A")[:50]
            logger.info(f"   [HOMEHARVEST] Calling scrape_property proxy_enabled={proxy_enabled} proxy_host={proxy_host} proxy_port={proxy_port} user_agent={user_agent}... params={log_params}")
            
            # Remove all filtering parameters to get ALL properties
            # Note: We're not setting foreclosure=False, exclude_pending=False, etc.
            # This allows the scraper to get off-market, foreclosures, and all other property types
            
            # Scrape properties - Run blocking call in thread pool to avoid blocking event loop
            # Add timeout to prevent locations from getting stuck (default 5 minutes per listing type)
            timeout_seconds = 300  # 5 minutes timeout per listing type
            loop = asyncio.get_event_loop()
            
            # Get or create session cookies for this proxy to maintain session state
            proxy_username = (proxy_config or {}).get("proxy_username", "default")
            session_cookies = self.session_cookies.get(proxy_username, {})
            
            # Patch requests with browser headers and cookies
            original_funcs = self._patch_requests_with_headers(browser_headers, cookies=session_cookies if session_cookies else None)
            
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
                logger.debug(f"   [HOMEHARVEST ERROR] Full traceback: {traceback.format_exc()}")
                # Re-raise with more context
                raise Exception(f"HomeHarvest API error: {error_msg}") from e
            finally:
                # Restore original functions
                if original_funcs:
                    self._restore_requests(original_funcs)
            
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
            logger.debug(f"Full traceback: {traceback.format_exc()}")
            
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
            
            # Anti-blocking: Add realistic browser headers to avoid detection
            browser_headers = self._get_browser_headers()
            
            # Get or create session cookies for this proxy to maintain session state
            proxy_username = (proxy_config or {}).get("proxy_username", "default")
            session_cookies = self.session_cookies.get(proxy_username, {})
            
            # Scrape property - Run blocking call in thread pool
            loop = asyncio.get_event_loop()
            timeout_seconds = 30  # 30 second timeout for direct address query
            
            # Patch requests with browser headers and cookies
            original_funcs = self._patch_requests_with_headers(browser_headers, cookies=session_cookies if session_cookies else None)
            
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
            finally:
                # Restore original functions
                if original_funcs:
                    self._restore_requests(original_funcs)
            
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
                            # CRITICAL: Only update if job is still RUNNING (don't overwrite COMPLETED status)
                            current_job = await db.get_job(job_id)
                            if current_job and current_job.status == JobStatus.RUNNING:
                                await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                            elif current_job and current_job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                                # Job is already final, only update progress_logs without changing status
                                logger.debug(f"Job {job_id} is {current_job.status.value}, updating only progress_logs for off-market check")
                                await db.jobs_collection.update_one(
                                    {"job_id": job_id},
                                    {"$set": {"progress_logs": progress_logs, "updated_at": datetime.utcnow()}}
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
                        # CRITICAL: Only update if job is still RUNNING (don't overwrite COMPLETED status)
                        current_job = await db.get_job(job_id)
                        if current_job and current_job.status == JobStatus.RUNNING:
                            await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                        elif current_job and current_job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                            # Job is already final, only update progress_logs without changing status
                            logger.debug(f"Job {job_id} is {current_job.status.value}, updating only progress_logs for off-market check completion")
                            await db.jobs_collection.update_one(
                                {"job_id": job_id},
                                {"$set": {"progress_logs": progress_logs, "updated_at": datetime.utcnow()}}
                            )
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
                        # CRITICAL: Only update if job is still RUNNING (don't overwrite COMPLETED status)
                        current_job = await db.get_job(job_id)
                        if current_job and current_job.status == JobStatus.RUNNING:
                            await db.update_job_status(job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                        elif current_job and current_job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                            # Job is already final, only update progress_logs without changing status
                            logger.debug(f"Job {job_id} is {current_job.status.value}, updating only progress_logs for off-market check")
                            await db.jobs_collection.update_one(
                                {"job_id": job_id},
                                {"$set": {"progress_logs": progress_logs, "updated_at": datetime.utcnow()}}
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
