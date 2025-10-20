"""
Comprehensive test for all API endpoints (requires server to be running)
"""
import asyncio
import sys
import os
import httpx
import json

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def test_health_endpoint(client, base_url):
    """Test health endpoint"""
    try:
        response = await client.get(f"{base_url}/health")
        if response.status_code == 200:
            data = response.json()
            print("✅ Health endpoint - Server is healthy")
            return True
        else:
            print(f"❌ Health endpoint failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Health endpoint error: {e}")
        return False

async def test_immediate_scrape_endpoint(client, base_url):
    """Test immediate scrape endpoint"""
    try:
        payload = {
            "locations": ["Indianapolis, IN"],
            "listing_type": "for_sale",
            "limit": 1
        }
        response = await client.post(f"{base_url}/scrape/immediate", json=payload)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Immediate scrape endpoint - Job created: {data.get('job_id', 'N/A')}")
            return data.get('job_id')
        else:
            print(f"❌ Immediate scrape endpoint failed: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Immediate scrape endpoint error: {e}")
        return None

async def test_immediate_sync_endpoint(client, base_url):
    """Test immediate sync scrape endpoint"""
    try:
        payload = {
            "locations": ["Indianapolis, IN"],
            "listing_type": "for_sale",
            "limit": 1
        }
        response = await client.post(f"{base_url}/scrape/immediate/sync", json=payload)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Immediate sync endpoint - Properties scraped: {data.get('properties_scraped', 0)}")
            return True
        else:
            print(f"❌ Immediate sync endpoint failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Immediate sync endpoint error: {e}")
        return False

async def test_scheduled_scrape_endpoint(client, base_url):
    """Test scheduled scrape endpoint"""
    try:
        payload = {
            "locations": ["Indianapolis, IN"],
            "listing_type": "for_sale",
            "scheduled_at": "2025-12-31T23:59:59",
            "limit": 1
        }
        response = await client.post(f"{base_url}/scrape/scheduled", json=payload)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Scheduled scrape endpoint - Job created: {data.get('job_id', 'N/A')}")
            return data.get('job_id')
        else:
            print(f"❌ Scheduled scrape endpoint failed: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Scheduled scrape endpoint error: {e}")
        return None

async def test_job_status_endpoint(client, base_url, job_id):
    """Test job status endpoint"""
    try:
        response = await client.get(f"{base_url}/jobs/{job_id}")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Job status endpoint - Status: {data.get('status', 'N/A')}")
            return True
        else:
            print(f"❌ Job status endpoint failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Job status endpoint error: {e}")
        return False

async def test_jobs_list_endpoint(client, base_url):
    """Test jobs list endpoint"""
    try:
        response = await client.get(f"{base_url}/jobs")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Jobs list endpoint - Total jobs: {len(data)}")
            return True
        else:
            print(f"❌ Jobs list endpoint failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Jobs list endpoint error: {e}")
        return False

async def test_jobs_list_with_params_endpoint(client, base_url):
    """Test jobs list endpoint with query parameters"""
    try:
        response = await client.get(f"{base_url}/jobs?status=completed&limit=10&offset=0")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Jobs list with params endpoint - Jobs found: {len(data)}")
            return True
        else:
            print(f"❌ Jobs list with params endpoint failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Jobs list with params endpoint error: {e}")
        return False

async def test_cancel_job_endpoint(client, base_url, job_id):
    """Test cancel job endpoint"""
    try:
        response = await client.post(f"{base_url}/jobs/{job_id}/cancel")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Cancel job endpoint - Job cancelled: {data.get('message', 'N/A')}")
            return True
        else:
            print(f"❌ Cancel job endpoint failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Cancel job endpoint error: {e}")
        return False

async def test_stats_endpoint(client, base_url):
    """Test stats endpoint"""
    try:
        response = await client.get(f"{base_url}/stats")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Stats endpoint - Properties: {data.get('total_properties', 0)}")
            return True
        else:
            print(f"❌ Stats endpoint failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Stats endpoint error: {e}")
        return False

async def test_proxy_stats_endpoint(client, base_url):
    """Test proxy stats endpoint"""
    try:
        response = await client.get(f"{base_url}/proxies/stats")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Proxy stats endpoint - Active proxies: {data.get('active_proxies', 0)}")
            return True
        else:
            print(f"❌ Proxy stats endpoint failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Proxy stats endpoint error: {e}")
        return False

async def test_reset_proxies_endpoint(client, base_url):
    """Test reset proxies endpoint"""
    try:
        response = await client.post(f"{base_url}/proxies/reset")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Reset proxies endpoint - Proxies reset: {data.get('message', 'N/A')}")
            return True
        else:
            print(f"❌ Reset proxies endpoint failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Reset proxies endpoint error: {e}")
        return False

async def test_api_endpoints():
    """Test all API endpoints"""
    try:
        print("Testing all API endpoints...")
        
        base_url = "http://localhost:8000"
        results = {}
        job_ids = []
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Test all endpoints
            results['health'] = await test_health_endpoint(client, base_url)
            results['immediate_sync'] = await test_immediate_sync_endpoint(client, base_url)
            
            # Test job creation endpoints
            immediate_job_id = await test_immediate_scrape_endpoint(client, base_url)
            results['immediate_scrape'] = immediate_job_id is not None
            if immediate_job_id:
                job_ids.append(immediate_job_id)
            
            scheduled_job_id = await test_scheduled_scrape_endpoint(client, base_url)
            results['scheduled_scrape'] = scheduled_job_id is not None
            if scheduled_job_id:
                job_ids.append(scheduled_job_id)
            
            # Test job management endpoints
            results['jobs_list'] = await test_jobs_list_endpoint(client, base_url)
            results['jobs_list_params'] = await test_jobs_list_with_params_endpoint(client, base_url)
            
            if job_ids:
                results['job_status'] = await test_job_status_endpoint(client, base_url, job_ids[0])
                results['cancel_job'] = await test_cancel_job_endpoint(client, base_url, job_ids[0])
            
            # Test stats endpoints
            results['stats'] = await test_stats_endpoint(client, base_url)
            results['proxy_stats'] = await test_proxy_stats_endpoint(client, base_url)
            results['reset_proxies'] = await test_reset_proxies_endpoint(client, base_url)
        
        # Calculate results
        passed = sum(1 for result in results.values() if result)
        total = len(results)
        
        print(f"\n📊 API Test Results: {passed}/{total} endpoints passed")
        
        for endpoint, result in results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"  {status} {endpoint}")
        
        return passed == total
        
    except Exception as e:
        print(f"❌ API test suite failed: {e}")
        print("Note: Make sure the server is running with: python start_server.py")
        return False

if __name__ == "__main__":
    result = asyncio.run(test_api_endpoints())
    print(f"\nTest result: {'PASSED' if result else 'FAILED'}")
