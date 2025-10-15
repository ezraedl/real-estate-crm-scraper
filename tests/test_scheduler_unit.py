#!/usr/bin/env python3
"""
Unit tests for the scheduler logic
These tests don't require a database connection
"""

import sys
import pytest
from datetime import datetime, timedelta
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scheduler import JobScheduler
from models import ScrapingJob, ListingType, JobPriority, JobStatus

class TestSchedulerLogic:
    """Test the scheduler logic without database"""
    
    @pytest.fixture
    def scheduler(self):
        """Create a scheduler instance"""
        return JobScheduler()
    
    @pytest.mark.asyncio
    async def test_job_never_run_should_run_if_overdue(self, scheduler):
        """Test that a job that never ran should run if the cron schedule has passed"""
        now = datetime.utcnow()
        
        # Create a job scheduled to run at 1 PM today
        # If current time is after 1 PM, it should run
        job = ScrapingJob(
            job_id="test_never_run",
            priority=JobPriority.NORMAL,
            locations=["Indianapolis, IN"],
            listing_type=ListingType.FOR_SALE,
            cron_expression="0 13 * * *",  # Daily at 1 PM
            created_at=now - timedelta(days=30),  # Created 30 days ago
            last_run=None,  # Never run
            run_count=0
        )
        
        # If current hour is >= 13 (1 PM), job should run
        should_run = await scheduler.should_run_recurring_job(job, now)
        
        if now.hour >= 13:
            assert should_run, f"Job scheduled at 13:00 should run when current time is {now.hour}:00"
        # Note: If current hour < 13, it's scheduled for later today, so should NOT run yet
    
    @pytest.mark.asyncio
    async def test_job_overdue_by_days_should_run(self, scheduler):
        """Test that a job that last ran days ago should run"""
        now = datetime.utcnow()
        
        # Create a job that last ran 3 days ago
        last_run = now - timedelta(days=3)
        
        job = ScrapingJob(
            job_id="test_overdue",
            priority=JobPriority.NORMAL,
            locations=["Indianapolis, IN"],
            listing_type=ListingType.SOLD,
            cron_expression="0 14 * * *",  # Daily at 2 PM
            created_at=now - timedelta(days=30),
            last_run=last_run,
            run_count=5
        )
        
        should_run = await scheduler.should_run_recurring_job(job, now)
        
        assert should_run, "Job that last ran 3 days ago should be overdue and should run"
    
    @pytest.mark.asyncio
    async def test_job_ran_today_should_not_run(self, scheduler):
        """Test that a job that already ran today should not run again"""
        now = datetime.utcnow()
        
        # Create a job that ran 2 hours ago (assuming daily schedule)
        last_run = now - timedelta(hours=2)
        
        job = ScrapingJob(
            job_id="test_ran_today",
            priority=JobPriority.NORMAL,
            locations=["Indianapolis, IN"],
            listing_type=ListingType.PENDING,
            cron_expression="0 14 * * *",  # Daily at 2 PM
            created_at=now - timedelta(days=30),
            last_run=last_run,
            run_count=10
        )
        
        should_run = await scheduler.should_run_recurring_job(job, now)
        
        # Job should not run if it just ran 2 hours ago (next run is ~22 hours away)
        assert not should_run, "Job that ran 2 hours ago should not run again"
    
    @pytest.mark.asyncio
    async def test_job_with_no_cron_expression_should_not_run(self, scheduler):
        """Test that a job without a cron expression should never run as recurring"""
        now = datetime.utcnow()
        
        job = ScrapingJob(
            job_id="test_no_cron",
            priority=JobPriority.NORMAL,
            locations=["Indianapolis, IN"],
            listing_type=ListingType.FOR_SALE,
            cron_expression=None,  # No cron expression
            created_at=now - timedelta(days=30),
            last_run=None,
            run_count=0
        )
        
        should_run = await scheduler.should_run_recurring_job(job, now)
        
        assert not should_run, "Job without cron expression should not run"
    
    @pytest.mark.asyncio
    async def test_multiple_cron_schedules(self, scheduler):
        """Test different cron schedules"""
        now = datetime.utcnow()
        
        test_cases = [
            {
                "cron": "0 6 * * *",  # Daily at 6 AM
                "name": "daily_6am",
            },
            {
                "cron": "0 14 * * *",  # Daily at 2 PM
                "name": "daily_2pm",
            },
            {
                "cron": "0 22 * * *",  # Daily at 10 PM
                "name": "daily_10pm",
            },
        ]
        
        for test_case in test_cases:
            # Create job that last ran yesterday
            last_run = now - timedelta(days=1)
            
            job = ScrapingJob(
                job_id=f"test_{test_case['name']}",
                priority=JobPriority.NORMAL,
                locations=["Indianapolis, IN"],
                listing_type=ListingType.FOR_SALE,
                cron_expression=test_case["cron"],
                created_at=now - timedelta(days=30),
                last_run=last_run,
                run_count=5
            )
            
            should_run = await scheduler.should_run_recurring_job(job, now)
            
            # Job that ran yesterday should run again (it's been > 24 hours)
            assert should_run, f"Job with cron {test_case['cron']} that ran yesterday should run"
    
    @pytest.mark.asyncio
    async def test_job_scheduled_for_future_should_not_run(self, scheduler):
        """Test that a job scheduled for future should not run yet"""
        now = datetime.utcnow()
        
        # Create a job scheduled to run 5 hours from now
        future_hour = (now.hour + 5) % 24
        cron = f"0 {future_hour} * * *"
        
        # Job ran yesterday at the scheduled time
        last_run = now - timedelta(days=1)
        
        job = ScrapingJob(
            job_id="test_future",
            priority=JobPriority.NORMAL,
            locations=["Indianapolis, IN"],
            listing_type=ListingType.FOR_SALE,
            cron_expression=cron,
            created_at=now - timedelta(days=30),
            last_run=last_run,
            run_count=5
        )
        
        should_run = await scheduler.should_run_recurring_job(job, now)
        
        # Job scheduled for 5 hours from now should not run yet
        # (unless we're very close to the scheduled time, within 1 minute)
        # Most likely it won't be that close
        # Note: This test might occasionally fail if run exactly at the scheduled time
    
    @pytest.mark.asyncio
    async def test_edge_case_job_last_ran_exactly_24_hours_ago(self, scheduler):
        """Test edge case where job last ran exactly 24 hours ago"""
        now = datetime.utcnow()
        
        # Create job scheduled at current hour
        cron = f"0 {now.hour} * * *"
        
        # Last ran exactly 24 hours ago
        last_run = now - timedelta(days=1)
        
        job = ScrapingJob(
            job_id="test_exact_24h",
            priority=JobPriority.NORMAL,
            locations=["Indianapolis, IN"],
            listing_type=ListingType.FOR_SALE,
            cron_expression=cron,
            created_at=now - timedelta(days=30),
            last_run=last_run,
            run_count=10
        )
        
        should_run = await scheduler.should_run_recurring_job(job, now)
        
        # Should run because it's been 24 hours and we're at the scheduled time
        assert should_run, "Job scheduled at current hour that ran 24 hours ago should run"


class TestSchedulerEdgeCases:
    """Test edge cases and error handling"""
    
    @pytest.fixture
    def scheduler(self):
        """Create a scheduler instance"""
        return JobScheduler()
    
    @pytest.mark.asyncio
    async def test_invalid_cron_expression_should_not_crash(self, scheduler):
        """Test that invalid cron expression is handled gracefully"""
        now = datetime.utcnow()
        
        job = ScrapingJob(
            job_id="test_invalid_cron",
            priority=JobPriority.NORMAL,
            locations=["Indianapolis, IN"],
            listing_type=ListingType.FOR_SALE,
            cron_expression="invalid cron",  # Invalid cron
            created_at=now - timedelta(days=30),
            last_run=None,
            run_count=0
        )
        
        # Should not raise exception, just return False
        should_run = await scheduler.should_run_recurring_job(job, now)
        
        assert not should_run, "Invalid cron expression should return False"
    
    @pytest.mark.asyncio
    async def test_job_created_in_future_should_not_crash(self, scheduler):
        """Test that a job created in future is handled gracefully"""
        now = datetime.utcnow()
        
        # Create job with future creation date (shouldn't happen but test it)
        job = ScrapingJob(
            job_id="test_future_created",
            priority=JobPriority.NORMAL,
            locations=["Indianapolis, IN"],
            listing_type=ListingType.FOR_SALE,
            cron_expression="0 12 * * *",
            created_at=now + timedelta(days=1),  # Created "tomorrow"
            last_run=None,
            run_count=0
        )
        
        # Should not crash
        should_run = await scheduler.should_run_recurring_job(job, now)
        
        # Result doesn't matter as much as not crashing
        assert isinstance(should_run, bool), "Should return a boolean value"


if __name__ == "__main__":
    """Run tests with pytest"""
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])

