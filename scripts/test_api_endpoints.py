"""
Comprehensive API endpoint testing script.

Tests all scraper and backend API endpoints to ensure consolidation changes work correctly.
"""

import asyncio
import aiohttp
import json
from typing import Dict, List, Any
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
SCRAPER_API_URL = "http://localhost:8000"
BACKEND_API_URL = "http://localhost:3000"

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

def print_pass(msg):
    print(f"{Colors.GREEN}✅ PASS: {msg}{Colors.RESET}")

def print_fail(msg):
    print(f"{Colors.RED}❌ FAIL: {msg}{Colors.RESET}")

def print_warn(msg):
    print(f"{Colors.YELLOW}⚠️  WARN: {msg}{Colors.RESET}")

def print_info(msg):
    print(f"{Colors.BLUE}ℹ️  INFO: {msg}{Colors.RESET}")


async def test_scraper_endpoints(session: aiohttp.ClientSession, property_id: str) -> Dict[str, bool]:
    """Test scraper API endpoints"""
    results = {}
    
    print_info(f"Testing scraper API endpoints with property: {property_id}")
    
    # Test 1: GET /properties/{id}/enrichment
    try:
        url = f"{SCRAPER_API_URL}/properties/{property_id}/enrichment"
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                enrichment = data.get("enrichment", {})
                
                # Check for removed fields
                if "price_history_summary" not in enrichment:
                    print_pass("enrichment.price_history_summary removed")
                    results["no_price_summary"] = True
                else:
                    print_fail("enrichment.price_history_summary still exists")
                    results["no_price_summary"] = False
                
                if "quick_access_flags" not in enrichment:
                    print_pass("enrichment.quick_access_flags removed")
                    results["no_quick_flags"] = True
                else:
                    print_warn("enrichment.quick_access_flags still exists (old data)")
                    results["no_quick_flags"] = False
                
                # Check version
                version = enrichment.get("enrichment_version", "1.0")
                if version == "2.0":
                    print_pass(f"enrichment_version is 2.0")
                    results["version_2"] = True
                else:
                    print_warn(f"enrichment_version is {version} (may be old data)")
                    results["version_2"] = False
                
                results["enrichment_endpoint"] = True
            else:
                print_fail(f"enrichment endpoint returned {response.status}")
                results["enrichment_endpoint"] = False
    except Exception as e:
        print_fail(f"enrichment endpoint error: {e}")
        results["enrichment_endpoint"] = False
    
    # Test 2: GET /properties/{id}/history
    try:
        url = f"{SCRAPER_API_URL}/properties/{property_id}/history"
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                history = data.get("history", [])
                print_pass(f"history endpoint returned {len(history)} entries")
                results["history_endpoint"] = True
            else:
                print_fail(f"history endpoint returned {response.status}")
                results["history_endpoint"] = False
    except Exception as e:
        print_fail(f"history endpoint error: {e}")
        results["history_endpoint"] = False
    
    # Test 3: GET /properties/{id}/changes
    try:
        url = f"{SCRAPER_API_URL}/properties/{property_id}/changes"
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                change_logs = data.get("change_logs", [])
                print_pass(f"changes endpoint returned {len(change_logs)} entries")
                results["changes_endpoint"] = True
            else:
                print_fail(f"changes endpoint returned {response.status}")
                results["changes_endpoint"] = False
    except Exception as e:
        print_fail(f"changes endpoint error: {e}")
        results["changes_endpoint"] = False
    
    return results


async def test_backend_endpoints(session: aiohttp.ClientSession) -> Dict[str, bool]:
    """Test backend API endpoints"""
    results = {}
    
    print_info("Testing backend API endpoints")
    
    # Test 1: Price reduction filter with amount
    try:
        url = f"{BACKEND_API_URL}/api/mlsproperties"
        params = {"priceReductionMin": "1000", "limit": 10}
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                properties = data.get("properties", [])
                print_pass(f"price reduction filter (amount) returned {len(properties)} properties")
                results["price_reduction_amount"] = True
            else:
                print_fail(f"price reduction filter returned {response.status}")
                results["price_reduction_amount"] = False
    except Exception as e:
        print_fail(f"price reduction filter error: {e}")
        results["price_reduction_amount"] = False
    
    # Test 2: Price reduction filter with time window
    try:
        url = f"{BACKEND_API_URL}/api/mlsproperties"
        params = {"priceReductionTimeWindow": "30", "limit": 10}
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                properties = data.get("properties", [])
                print_pass(f"price reduction filter (time window) returned {len(properties)} properties")
                results["price_reduction_time"] = True
            else:
                print_fail(f"price reduction time filter returned {response.status}")
                results["price_reduction_time"] = False
    except Exception as e:
        print_fail(f"price reduction time filter error: {e}")
        results["price_reduction_time"] = False
    
    # Test 3: Combined filter
    try:
        url = f"{BACKEND_API_URL}/api/mlsproperties"
        params = {"priceReductionMin": "500", "priceReductionTimeWindow": "30", "limit": 10}
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                properties = data.get("properties", [])
                print_pass(f"combined price reduction filter returned {len(properties)} properties")
                results["combined_filter"] = True
            else:
                print_fail(f"combined filter returned {response.status}")
                results["combined_filter"] = False
    except Exception as e:
        print_fail(f"combined filter error: {e}")
        results["combined_filter"] = False
    
    # Test 4: Motivated seller filter
    try:
        url = f"{BACKEND_API_URL}/api/mlsproperties"
        params = {"motivatedSellerMin": "40", "limit": 10}
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                properties = data.get("properties", [])
                print_pass(f"motivated seller filter returned {len(properties)} properties")
                results["motivated_seller"] = True
            else:
                print_fail(f"motivated seller filter returned {response.status}")
                results["motivated_seller"] = False
    except Exception as e:
        print_fail(f"motivated seller filter error: {e}")
        results["motivated_seller"] = False
    
    return results


async def get_test_property_id(session: aiohttp.ClientSession) -> str:
    """Get a property ID for testing"""
    try:
        url = f"{SCRAPER_API_URL}/properties"
        params = {"limit": 1}
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                properties = data.get("properties", [])
                if properties:
                    return properties[0].get("property_id")
    except:
        pass
    
    # Fallback
    return "4615498238"


async def main():
    """Run all API tests"""
    print("=" * 60)
    print("API Endpoint Testing")
    print("=" * 60)
    print(f"Scraper API: {SCRAPER_API_URL}")
    print(f"Backend API: {BACKEND_API_URL}")
    print("")
    
    async with aiohttp.ClientSession() as session:
        # Get test property
        property_id = await get_test_property_id(session)
        print_info(f"Using property ID: {property_id}")
        print("")
        
        # Test scraper endpoints
        print("=" * 60)
        print("SCRAPER API TESTS")
        print("=" * 60)
        scraper_results = await test_scraper_endpoints(session, property_id)
        print("")
        
        # Test backend endpoints
        print("=" * 60)
        print("BACKEND API TESTS")
        print("=" * 60)
        backend_results = await test_backend_endpoints(session)
        print("")
        
        # Summary
        print("=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)
        
        all_results = {**scraper_results, **backend_results}
        passed = sum(1 for v in all_results.values() if v)
        total = len(all_results)
        
        for test_name, result in all_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{status}: {test_name}")
        
        print("")
        print(f"Total: {passed}/{total} tests passed")
        
        if passed == total:
            print_pass("All tests passed!")
            return 0
        else:
            print_fail(f"{total - passed} test(s) failed")
            return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)

