#!/usr/bin/env python3
"""
Create multiple SOLD scraping jobs to get a full year of data
Since homeharvest seems to have limitations on date ranges, we'll create multiple jobs
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from database import Database
from models import ScrapingJob, ListingType, JobPriority

async def create_full_year_sold_jobs():
    """Create multiple SOLD jobs to cover a full year"""
    
    db = Database()
    
    try:
        await db.connect()
        print("✅ Connected to MongoDB")
        
        # Define locations
        locations = [
            "Indianapolis, IN",
            "Marion County, IN"
        ]
        
        print(f"\n🏠 Creating FULL YEAR SOLD properties jobs...")
        print(f"📍 Locations: {locations}")
        print(f"📅 Strategy: Multiple jobs to cover full year")
        
        # Create jobs for different time periods
        # Since homeharvest seems to limit date ranges, we'll create overlapping jobs
        jobs_config = [
            {
                "name": "Recent (0-3 months)",
                "past_days": 90,
                "limit": 2000,
                "priority": JobPriority.HIGH
            },
            {
                "name": "Mid-range (3-6 months)", 
                "past_days": 180,
                "limit": 2000,
                "priority": JobPriority.NORMAL
            },
            {
                "name": "Older (6-12 months)",
                "past_days": 365,
                "limit": 3000,
                "priority": JobPriority.NORMAL
            }
        ]
        
        created_jobs = []
        
        for i, config in enumerate(jobs_config, 1):
            print(f"\n{i}. Creating {config['name']} job...")
            
            job = ScrapingJob(
                job_id=f"indianapolis_sold_{config['name'].lower().replace(' ', '_').replace('(', '').replace(')', '')}_{int(datetime.utcnow().timestamp())}",
                priority=config['priority'],
                locations=locations,
                listing_type=ListingType.SOLD,
                limit=config['limit'],
                past_days=config['past_days'],
                mls_only=True,
                foreclosure=False,
                exclude_pending=False
            )
            
            job_id = await db.create_job(job)
            created_jobs.append((job.job_id, config['name']))
            print(f"   ✅ Created: {job.job_id}")
            print(f"   📅 Past Days: {config['past_days']}")
            print(f"   📊 Limit: {config['limit']}")
        
        print(f"\n🎯 Created {len(created_jobs)} jobs:")
        for job_id, name in created_jobs:
            print(f"   • {name}: {job_id}")
        
        print(f"\n📝 Next Steps:")
        print(f"   1. All jobs are in the database")
        print(f"   2. Trigger them via API: POST /scrape/trigger")
        print(f"   3. Or trigger from frontend when ready")
        print(f"   4. This should give you comprehensive SOLD data")
        
        # Also create a direct immediate job for testing
        print(f"\n🚀 Creating immediate test job...")
        immediate_job = ScrapingJob(
            job_id=f"immediate_sold_test_{int(datetime.utcnow().timestamp())}",
            priority=JobPriority.IMMEDIATE,
            locations=["Indianapolis, IN"],  # Just Indianapolis for testing
            listing_type=ListingType.SOLD,
            limit=1000,
            past_days=180,  # 6 months
            mls_only=True,
            foreclosure=False,
            exclude_pending=False
        )
        
        immediate_job_id = await db.create_job(immediate_job)
        print(f"   ✅ Immediate test job: {immediate_job.job_id}")
        
        return created_jobs + [(immediate_job.job_id, "Immediate Test")]
        
    except Exception as e:
        print(f"❌ Error creating jobs: {e}")
        return []
    finally:
        await db.disconnect()
        print(f"\n🔌 Disconnected from MongoDB")

if __name__ == "__main__":
    asyncio.run(create_full_year_sold_jobs())

