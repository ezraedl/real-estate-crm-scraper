"""
Short test for basic scraper functionality
"""
import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper import MLSScraper
from models import ScrapingJob, JobPriority, ListingType

async def test_scraper_basic():
    """Test basic scraper functionality with minimal data"""
    try:
        print("Testing basic scraper functionality...")
        
        # Initialize scraper
        scraper = MLSScraper()
        print("✅ Scraper initialized")
        
        # Create a minimal test job
        test_job = ScrapingJob(
            job_id="test_job_123",
            priority=JobPriority.IMMEDIATE,
            locations=["Indianapolis, IN"],
            listing_type=ListingType.FOR_SALE,
            limit=1  # Only 1 property for quick test
        )
        
        print("✅ Test job created")
        
        # Test immediate scrape (should be fast with limit=1)
        job_id = await scraper.immediate_scrape(
            locations=["Indianapolis, IN"],
            listing_type=ListingType.FOR_SALE,
            limit=1
        )
        
        print(f"✅ Immediate scrape job created: {job_id}")
        
        return True
        
    except Exception as e:
        print(f"❌ Scraper test failed: {e}")
        return False

if __name__ == "__main__":
    result = asyncio.run(test_scraper_basic())
    print(f"\nTest result: {'PASSED' if result else 'FAILED'}")
