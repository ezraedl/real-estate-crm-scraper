"""
Start the scheduler service to run recurring Indianapolis scraping jobs.
This will automatically execute the scheduled jobs at their designated times.
"""

import asyncio
import sys
import os
from datetime import datetime

# Add current directory to Python path
sys.path.insert(0, str(os.path.dirname(__file__)))

from database import db
from scraper import scraper
from scheduler import scheduler
from proxy_manager import proxy_manager
from config import settings

async def start_scheduler_service():
    """Start the scheduler service"""
    print("=== Starting Scheduler Service ===\n")
    
    try:
        # Connect to database
        print("1. Connecting to MongoDB...")
        await db.connect()
        print("   ✅ Connected to MongoDB successfully!")
        
        # Initialize proxy manager
        print("\n2. Initializing proxy manager...")
        if settings.DATAIMPULSE_API_KEY:
            await proxy_manager.initialize_dataimpulse_proxies(settings.DATAIMPULSE_API_KEY)
            print("   ✅ DataImpulse proxies initialized")
        else:
            print("   ⚠️  No DataImpulse API key provided - running without proxy support")
        
        # Start scraper service
        print("\n3. Starting scraper service...")
        asyncio.create_task(scraper.start())
        print("   ✅ Scraper service started")
        
        # Start scheduler service
        print("\n4. Starting scheduler service...")
        asyncio.create_task(scheduler.start())
        print("   ✅ Scheduler service started")
        
        # Show current time and next scheduled runs
        print("\n5. Schedule Information:")
        current_time = datetime.utcnow()
        print(f"   🕐 Current time: {current_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"   📅 FOR_SALE Properties: Daily at 6:00 AM UTC")
        print(f"   📅 SOLD Properties: Daily at 7:00 AM UTC")
        print(f"   📅 PENDING Properties: Daily at 8:00 AM UTC")
        
        # Show what the scheduler will do
        print("\n6. What the Scheduler Will Do:")
        print("   🔄 Automatically run scheduled jobs at designated times")
        print("   📊 Use hash-based diff system for efficient updates")
        print("   📈 Provide detailed reporting of scraping results")
        print("   🛡️  Handle errors and retry failed jobs")
        print("   📝 Log all job activities and results")
        
        print("\n=== Scheduler Service Started Successfully ===")
        print("\n✅ The scheduler is now running and will automatically execute jobs!")
        print("✅ Indianapolis properties will be scraped daily")
        print("✅ Only changed properties will be updated in database")
        print("✅ Detailed change reporting will be provided")
        
        print("\nTo check job status:")
        print("1. Check the API at http://localhost:8000/docs")
        print("2. Use endpoint: GET /jobs to see all jobs")
        print("3. Use endpoint: GET /stats to see scraping statistics")
        
        print("\nTo stop the scheduler:")
        print("Press Ctrl+C")
        
        # Keep the service running
        print(f"\n🔄 Scheduler service is running...")
        print(f"Press Ctrl+C to stop")
        
        # Wait indefinitely
        while True:
            await asyncio.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        print("\n🛑 Received shutdown signal...")
        print("Stopping scheduler service...")
        
    except Exception as e:
        print(f"❌ Error in scheduler service: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        print("\n🧹 Cleaning up...")
        await scraper.stop()
        await scheduler.stop()
        await db.disconnect()
        print("✅ Scheduler service stopped gracefully")

if __name__ == "__main__":
    print("🚀 Starting Indianapolis Property Scraping Scheduler")
    print("=" * 60)
    asyncio.run(start_scheduler_service())
