import asyncio
import random
import time
from typing import List, Dict, Any, Optional
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

class MLSScraper:
    def __init__(self):
        self.is_running = False
        self.current_jobs = {}
        self.executor = ThreadPoolExecutor(max_workers=3)  # Thread pool for blocking operations
    
    async def start(self):
        """Start the scraper service"""
        self.is_running = True
        print("MLS Scraper started")
        
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
                print(f"Error in scraper main loop: {e}")
                await asyncio.sleep(10)
    
    async def stop(self):
        """Stop the scraper service"""
        self.is_running = False
        print("MLS Scraper stopped")
    
    async def check_cancellation_loop(self, job_id: str, cancel_flag: dict):
        """Background task that checks for job cancellation every 2 seconds"""
        try:
            while not cancel_flag.get("cancelled", False):
                await asyncio.sleep(2)  # Check every 2 seconds
                current_job_status = await db.get_job(job_id)
                if current_job_status and current_job_status.status == JobStatus.CANCELLED:
                    print(f"[MONITOR] Cancellation detected for job {job_id}")
                    cancel_flag["cancelled"] = True
                    break
        except Exception as e:
            print(f"[MONITOR] Error in cancellation monitor: {e}")

    async def process_job(self, job: ScrapingJob):
        """Process a single scraping job"""
        self.current_jobs[job.job_id] = job
        
        # Cancellation flag shared between main task and monitor
        cancel_flag = {"cancelled": False}
        
        # Start background cancellation monitor
        monitor_task = asyncio.create_task(self.check_cancellation_loop(job.job_id, cancel_flag))
        
        try:
            # Initialize progress logs with job start
            progress_logs = [{
                "timestamp": datetime.utcnow().isoformat(),
                "event": "job_started",
                "message": f"Job started - Processing {len(job.locations)} location(s)",
                "listing_types": job.listing_types or (job.listing_type and [job.listing_type]) or ["for_sale", "sold", "for_rent", "pending"],
                "total_locations": len(job.locations)
            }]
            
            # Update job status to running with initial log
            await db.update_job_status(job.job_id, JobStatus.RUNNING, progress_logs=progress_logs)
            
            print(f"Processing job {job.job_id} for {len(job.locations)} locations")
            
            total_properties = 0
            saved_properties = 0
            total_inserted = 0
            total_updated = 0
            total_skipped = 0
            
            # Process each location
            for i, location in enumerate(job.locations):
                # Check if job has been cancelled (via background monitor)
                if cancel_flag.get("cancelled", False):
                    print(f"Job {job.job_id} was cancelled, stopping execution")
                    return  # Exit immediately
                
                # Log location start
                progress_logs.append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "event": "location_started",
                    "location": location,
                    "location_index": i + 1,
                    "message": f"Starting location {i+1}/{len(job.locations)}: {location}"
                })
                await db.update_job_status(job.job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                try:
                    print(f"Scraping location {i+1}/{len(job.locations)}: {location}")
                    
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
                        total_locations=len(job.locations)
                    )
                    
                    # Update totals from running_totals (modified by scrape_location)
                    total_properties = running_totals["total_properties"]
                    saved_properties = running_totals["saved_properties"]
                    total_inserted = running_totals["total_inserted"]
                    total_updated = running_totals["total_updated"]
                    total_skipped = running_totals["total_skipped"]
                    
                    if location_summary:
                        progress_logs.append(location_summary)
                        print(f"Location {location} complete: {location_summary.get('inserted', 0)} inserted, {location_summary.get('updated', 0)} updated, {location_summary.get('skipped', 0)} skipped")
                    
                    # Update job progress with detailed breakdown and logs
                    await db.update_job_status(
                        job.job_id,
                        JobStatus.RUNNING,
                        completed_locations=i + 1,
                        properties_scraped=total_properties,
                        properties_saved=saved_properties,
                        properties_inserted=total_inserted,
                        properties_updated=total_updated,
                        properties_skipped=total_skipped,
                        progress_logs=progress_logs
                    )
                    
                    # Random delay between locations
                    delay = job.request_delay + random.uniform(0, 1)
                    await asyncio.sleep(delay)
                    
                except Exception as e:
                    print(f"Error scraping location {location}: {e}")
                    continue
            
            # Mark job as completed
            await db.update_job_status(
                job.job_id,
                JobStatus.COMPLETED,
                properties_scraped=total_properties,
                properties_saved=saved_properties
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
                    print(f"Updated run history for scheduled job: {job.scheduled_job_id}")
            
            # Legacy: Update run history for old recurring jobs
            elif job.original_job_id:
                await db.update_recurring_job_run_history(
                    job.original_job_id,
                    job.job_id,
                    JobStatus.COMPLETED
                )
                print(f"Updated run history for legacy recurring job: {job.original_job_id}")
            
            print(f"Job {job.job_id} completed: {saved_properties} properties saved")
            
        except Exception as e:
            print(f"Error processing job {job.job_id}: {e}")
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
                    print(f"Updated run history for scheduled job (failed): {job.scheduled_job_id}")
            
            # Legacy: Update run history for old recurring jobs (failed)
            elif job.original_job_id:
                await db.update_recurring_job_run_history(
                    job.original_job_id,
                    job.job_id,
                    JobStatus.FAILED
                )
                print(f"Updated run history for legacy recurring job (failed): {job.original_job_id}")
        
        finally:
            # Stop the cancellation monitor
            cancel_flag["cancelled"] = True  # Signal monitor to stop
            if 'monitor_task' in locals():
                monitor_task.cancel()
                try:
                    await monitor_task
                except asyncio.CancelledError:
                    pass  # Expected
            
            # Remove from current jobs
            if job.job_id in self.current_jobs:
                del self.current_jobs[job.job_id]
    
    async def scrape_location(self, location: str, job: ScrapingJob, proxy_config: Optional[Dict[str, Any]] = None, cancel_flag: dict = None, progress_logs: list = None, running_totals: dict = None, location_index: int = 1, total_locations: int = 1):
        """Scrape properties for a specific location based on job's listing_types. Saves after each type and returns summary."""
        if cancel_flag is None:
            cancel_flag = {"cancelled": False}
        if progress_logs is None:
            progress_logs = []
        if running_totals is None:
            running_totals = {
                "total_properties": 0,
                "saved_properties": 0,
                "total_inserted": 0,
                "total_updated": 0,
                "total_skipped": 0
            }
        try:
            listing_type_logs = []
            location_total_found = 0
            location_total_inserted = 0
            location_total_updated = 0
            location_total_skipped = 0
            location_total_errors = 0
            
            # Determine which listing types to scrape
            if job.listing_types and len(job.listing_types) > 0:
                # Use specified listing types
                listing_types_to_scrape = job.listing_types
                print(f"   [TARGET] Scraping specified types in {location}: {listing_types_to_scrape}")
            elif job.listing_type:
                # Backward compatibility: use single listing_type
                listing_types_to_scrape = [job.listing_type]
                print(f"   [TARGET] Scraping single type in {location}: {job.listing_type}")
            else:
                # Default: scrape all types for comprehensive data
                listing_types_to_scrape = ["for_sale", "sold", "for_rent", "pending"]
                print(f"   [TARGET] Scraping ALL property types in {location} (default)")
            
            print(f"   [DEBUG] Listing types to scrape: {listing_types_to_scrape}")
            print(f"   [NOTE] 'off_market' not supported by homeharvest library")
            
            for listing_type in listing_types_to_scrape:
                # Check if job was cancelled (checked every 2 seconds by background monitor)
                if cancel_flag.get("cancelled", False):
                    print(f"   [CANCELLED] Job was cancelled, stopping listing type fetch")
                    break  # Exit the listing type loop
                
                try:
                    start_time = datetime.utcnow()
                    print(f"   [FETCH] Fetching {listing_type} properties...")
                    
                    # Log API call START with "fetching" status
                    fetching_log = {
                        "listing_type": listing_type,
                        "status": "fetching",
                        "message": f"Fetching {listing_type} properties...",
                        "timestamp": start_time.isoformat()
                    }
                    listing_type_logs.append(fetching_log)
                    
                    # Update progress logs immediately for real-time UI feedback
                    # Create a summary log entry for this location showing what we're fetching
                    temp_summary = {
                        "timestamp": start_time.isoformat(),
                        "location": location,
                        "location_index": location_index,
                        "total_locations": total_locations,
                        "status": "in_progress",
                        "message": f"Fetching {listing_type} properties from {location}...",
                        "listing_type_breakdown": listing_type_logs.copy(),
                        # Current running totals for this location
                        "properties_found": location_total_found,
                        "inserted": location_total_inserted,
                        "updated": location_total_updated,
                        "skipped": location_total_skipped
                    }
                    # Find if we already have a temp entry for this location
                    temp_idx = None
                    for idx, log in enumerate(progress_logs):
                        if isinstance(log, dict) and log.get("location") == location and log.get("status") == "in_progress":
                            temp_idx = idx
                            break
                    
                    if temp_idx is not None:
                        # Update existing temp entry
                        progress_logs[temp_idx] = temp_summary
                    else:
                        # Add new temp entry
                        progress_logs.append(temp_summary)
                    
                    # Push to database immediately for real-time UI update
                    await db.update_job_status(job.job_id, JobStatus.RUNNING, progress_logs=progress_logs)
                    
                    # Use job's limit (or None to get all properties)
                    properties = await self._scrape_listing_type(
                        location, 
                        job, 
                        proxy_config, 
                        listing_type, 
                        limit=job.limit if job.limit else None,
                        past_days=job.past_days if job.past_days else 90
                    )
                    
                    end_time = datetime.utcnow()
                    duration = (end_time - start_time).total_seconds()
                    
                    print(f"   [OK] Found {len(properties)} {listing_type} properties in {duration:.1f}s")
                    
                    # Save properties immediately after each listing type fetch
                    if properties:
                        save_results = await db.save_properties_batch(properties)
                        location_total_found += len(properties)
                        location_total_inserted += save_results["inserted"]
                        location_total_updated += save_results["updated"]
                        location_total_skipped += save_results["skipped"]
                        location_total_errors += save_results["errors"]
                        
                        # Update running totals
                        running_totals["total_properties"] += len(properties)
                        running_totals["saved_properties"] += save_results["inserted"] + save_results["updated"]
                        running_totals["total_inserted"] += save_results["inserted"]
                        running_totals["total_updated"] += save_results["updated"]
                        running_totals["total_skipped"] += save_results["skipped"]
                        
                        print(f"   [SAVED] {save_results['inserted']} inserted, {save_results['updated']} updated, {save_results['skipped']} skipped")
                    
                    # Update log with completion and save stats
                    listing_type_logs[-1] = {
                        "listing_type": listing_type,
                        "status": "completed",
                        "properties_found": len(properties),
                        "inserted": save_results.get("inserted", 0) if properties else 0,
                        "updated": save_results.get("updated", 0) if properties else 0,
                        "skipped": save_results.get("skipped", 0) if properties else 0,
                        "duration_seconds": round(duration, 2),
                        "timestamp": start_time.isoformat()
                    }
                    
                    # Update the temp summary with completed status and running totals
                    if temp_idx is not None:
                        progress_logs[temp_idx] = {
                            "timestamp": end_time.isoformat(),
                            "location": location,
                            "location_index": location_index,
                            "total_locations": total_locations,
                            "status": "in_progress",
                            "message": f"Completed {listing_type}: {len(properties)} found, {save_results.get('inserted', 0) if properties else 0} inserted, {save_results.get('updated', 0) if properties else 0} updated",
                            "listing_type_breakdown": listing_type_logs.copy(),
                            # Running totals for this location so far
                            "properties_found": location_total_found,
                            "inserted": location_total_inserted,
                            "updated": location_total_updated,
                            "skipped": location_total_skipped
                        }
                        # Push to database immediately with updated job totals
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
                    
                    # Reduced delay for faster response
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    print(f"   [WARNING] Error scraping {listing_type} properties: {e}")
                    # Log the error
                    error_log = {
                        "listing_type": listing_type,
                        "status": "error",
                        "properties_found": 0,
                        "error": str(e),
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    listing_type_logs.append(error_log)
                    
                    # Update progress logs with error
                    if temp_idx is not None:
                        progress_logs[temp_idx] = {
                            "timestamp": datetime.utcnow().isoformat(),
                            "location": location,
                            "location_index": location_index,
                            "total_locations": total_locations,
                            "status": "in_progress",
                            "message": f"Error fetching {listing_type}: {str(e)}",
                            "listing_type_breakdown": listing_type_logs.copy(),
                            # Running totals for this location so far (with error)
                            "properties_found": location_total_found,
                            "inserted": location_total_inserted,
                            "updated": location_total_updated,
                            "skipped": location_total_skipped
                        }
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
            
            print(f"   [TOTAL] Location complete: {location_total_found} found, {location_total_inserted} inserted, {location_total_updated} updated, {location_total_skipped} skipped")
            
            # Return final summary for this location
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "location": location,
                "location_index": location_index,
                "total_locations": total_locations,
                "properties_found": location_total_found,
                "inserted": location_total_inserted,
                "updated": location_total_updated,
                "skipped": location_total_skipped,
                "errors": location_total_errors,
                "listing_type_breakdown": listing_type_logs
            }
            
        except Exception as e:
            print(f"Error scraping location {location}: {e}")
            return None
    
    async def _scrape_listing_type(self, location: str, job: ScrapingJob, proxy_config: Optional[Dict[str, Any]], listing_type: str, limit: Optional[int] = None, past_days: Optional[int] = None) -> List[Property]:
        """Scrape properties for a specific listing type"""
        try:
            print(f"   [DEBUG] _scrape_listing_type called with listing_type: '{listing_type}' (type: {type(listing_type)})")
            
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
            loop = asyncio.get_event_loop()
            properties_df = await loop.run_in_executor(
                self.executor,
                lambda: scrape_property(**scrape_params)
            )
            
            # Convert DataFrame to our Property models
            properties = []
            for index, row in properties_df.iterrows():
                try:
                    property_obj = self.convert_to_property_model(row, job.job_id, listing_type)
                    # Mark sold properties as comps
                    if listing_type == "sold":
                        property_obj.is_comp = True
                    properties.append(property_obj)
                except Exception as e:
                    print(f"Error converting property: {e}")
                    continue
            
            return properties
            
        except Exception as e:
            print(f"Error scraping {listing_type} properties in {location}: {e}")
            return []
    
    def convert_to_property_model(self, prop_data: Any, job_id: str, listing_type: str = None) -> Property:
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
                job_id=job_id
            )
            
            # Generate property_id from formatted address if not provided
            if not property_obj.property_id and property_obj.address and property_obj.address.formatted_address:
                property_obj.property_id = property_obj.generate_property_id()
            
            # Update content tracking (generate hash and check for changes)
            property_obj.update_content_tracking()
            
            return property_obj
            
        except Exception as e:
            print(f"Error converting property data: {e}")
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
            print(f"Error getting proxy config: {e}")
            return None
    
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
