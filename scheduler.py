import asyncio
import croniter
from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
from models import ScrapingJob, JobPriority, JobStatus
from database import db
from scraper import scraper

logger = logging.getLogger(__name__)

class JobScheduler:
    def __init__(self):
        self.is_running = False
        self.scheduled_jobs = {}
    
    async def start(self):
        """Start the scheduler service"""
        self.is_running = True
        logger.info("Job Scheduler started")
        
        while self.is_running:
            try:
                # Check for scheduled jobs
                await self.check_scheduled_jobs()
                
                # Check for recurring jobs
                await self.check_recurring_jobs()
                
                # Wait before next check
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in scheduler main loop: {e}")
                await asyncio.sleep(60)
    
    async def stop(self):
        """Stop the scheduler service"""
        self.is_running = False
        logger.info("Job Scheduler stopped")
    
    async def check_scheduled_jobs(self):
        """Check for one-time scheduled jobs that are ready to run"""
        try:
            # Get jobs scheduled for now or in the past
            now = datetime.utcnow()
            
            # Query for scheduled jobs that are ready
            cursor = db.jobs_collection.find({
                "scheduled_at": {"$lte": now},
                "status": JobStatus.PENDING.value,
                "cron_expression": {"$exists": False}  # Only one-time jobs
            })
            
            jobs_to_run = []
            async for job_data in cursor:
                job_data["_id"] = str(job_data["_id"])
                job = ScrapingJob(**job_data)
                jobs_to_run.append(job)
            
            # Process ready jobs
            for job in jobs_to_run:
                logger.info(f"Running scheduled job: {job.job_id}")
                asyncio.create_task(scraper.process_job(job))
                
        except Exception as e:
            logger.error(f"Error checking scheduled jobs: {e}")
    
    async def check_recurring_jobs(self):
        """Check for recurring jobs based on cron expressions"""
        try:
            now = datetime.utcnow()
            
            # Get all jobs with cron expressions
            cursor = db.jobs_collection.find({
                "cron_expression": {"$exists": True},
                "status": {"$in": [JobStatus.PENDING.value, JobStatus.COMPLETED.value]}
            })
            
            async for job_data in cursor:
                try:
                    job_data["_id"] = str(job_data["_id"])
                    job = ScrapingJob(**job_data)
                    
                    # Check if it's time to run this recurring job
                    if await self.should_run_recurring_job(job, now):
                        # Create a new instance of the job for this run
                        new_job = await self.create_recurring_job_instance(job)
                        
                        logger.info(f"Running recurring job: {new_job.job_id}")
                        asyncio.create_task(scraper.process_job(new_job))
                        
                except Exception as e:
                    logger.error(f"Error processing recurring job: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error checking recurring jobs: {e}")
    
    async def should_run_recurring_job(self, job: ScrapingJob, now: datetime) -> bool:
        """Check if a recurring job should run now"""
        try:
            if not job.cron_expression:
                return False
            
            # Determine base time: use last_run if available, otherwise use a recent time
            # Don't use created_at as it could be weeks/months ago
            if job.last_run:
                base_time = job.last_run
            else:
                # If never run, check if it's time to run now by going back 1 day
                base_time = now - timedelta(days=1)
            
            # Parse cron expression starting from base_time
            cron = croniter.croniter(job.cron_expression, base_time)
            
            # Get next scheduled run time after base_time
            next_run = cron.get_next(datetime)
            
            # If next_run is in the past (we missed it), the job should run now
            if next_run <= now:
                logger.info(f"Job {job.job_id} is overdue (scheduled for {next_run}, now is {now})")
                return True
            
            # Otherwise, check if next_run is within the next minute (upcoming)
            time_until = (next_run - now).total_seconds()
            if 0 < time_until <= 60:
                logger.info(f"Job {job.job_id} will run in {time_until:.2f} seconds")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking cron expression for job {job.job_id}: {e}")
            return False
    
    async def create_recurring_job_instance(self, original_job: ScrapingJob) -> ScrapingJob:
        """Create a new instance of a recurring job"""
        try:
            # Generate new job ID
            import uuid
            new_job_id = f"recurring_{int(datetime.utcnow().timestamp())}_{uuid.uuid4().hex[:8]}"
            
            # Create new job instance
            new_job = ScrapingJob(
                job_id=new_job_id,
                priority=original_job.priority,
                locations=original_job.locations,
                listing_type=original_job.listing_type,
                property_types=original_job.property_types,
                past_days=original_job.past_days,
                date_from=original_job.date_from,
                date_to=original_job.date_to,
                radius=original_job.radius,
                mls_only=original_job.mls_only,
                foreclosure=original_job.foreclosure,
                exclude_pending=original_job.exclude_pending,
                limit=original_job.limit,
                proxy_config=original_job.proxy_config,
                user_agent=original_job.user_agent,
                request_delay=original_job.request_delay,
                total_locations=len(original_job.locations),
                original_job_id=original_job.job_id  # Track the original job
            )
            
            # Save to database
            await db.create_job(new_job)
            
            return new_job
            
        except Exception as e:
            logger.error(f"Error creating recurring job instance: {e}")
            raise
    
    async def create_scheduled_job(
        self,
        locations: List[str],
        listing_type: str,
        scheduled_at: datetime,
        **kwargs
    ) -> str:
        """Create a one-time scheduled job"""
        try:
            import uuid
            job_id = f"scheduled_{int(datetime.utcnow().timestamp())}_{uuid.uuid4().hex[:8]}"
            
            job = ScrapingJob(
                job_id=job_id,
                priority=kwargs.get('priority', JobPriority.NORMAL),
                locations=locations,
                listing_type=listing_type,
                scheduled_at=scheduled_at,
                total_locations=len(locations),
                **{k: v for k, v in kwargs.items() if k != 'priority'}
            )
            
            await db.create_job(job)
            logger.info(f"Created scheduled job: {job_id} for {scheduled_at}")
            
            return job_id
            
        except Exception as e:
            logger.error(f"Error creating scheduled job: {e}")
            raise
    
    async def create_recurring_job(
        self,
        locations: List[str],
        listing_type: str,
        cron_expression: str,
        **kwargs
    ) -> str:
        """Create a recurring job with cron expression"""
        try:
            import uuid
            job_id = f"recurring_{int(datetime.utcnow().timestamp())}_{uuid.uuid4().hex[:8]}"
            
            job = ScrapingJob(
                job_id=job_id,
                priority=kwargs.get('priority', JobPriority.NORMAL),
                locations=locations,
                listing_type=listing_type,
                cron_expression=cron_expression,
                total_locations=len(locations),
                **{k: v for k, v in kwargs.items() if k != 'priority'}
            )
            
            await db.create_job(job)
            logger.info(f"Created recurring job: {job_id} with cron: {cron_expression}")
            
            return job_id
            
        except Exception as e:
            logger.error(f"Error creating recurring job: {e}")
            raise
    
    async def get_next_run_time(self, cron_expression: str) -> datetime:
        """Get the next run time for a cron expression"""
        try:
            cron = croniter.croniter(cron_expression, datetime.utcnow())
            return cron.get_next(datetime)
        except Exception as e:
            logger.error(f"Error calculating next run time: {e}")
            return datetime.utcnow() + timedelta(hours=1)

# Global scheduler instance
scheduler = JobScheduler()
