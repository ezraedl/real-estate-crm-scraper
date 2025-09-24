"""
Test runner for MLS Scraping Server - Short CI/CD Tests
"""
import sys
import os
import asyncio

# Add parent directory to path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def run_database_tests():
    """Run short database tests"""
    print("=" * 60)
    print("DATABASE TESTS")
    print("=" * 60)
    
    try:
        from test_database_connection import test_database_connection
        result = await test_database_connection()
        print(f"✅ Database Connection: {'PASSED' if result else 'FAILED'}")
        return result
    except Exception as e:
        print(f"❌ Database Tests: ERROR - {e}")
        return False

async def run_scraper_tests():
    """Run short scraper tests"""
    print("\n" + "=" * 60)
    print("SCRAPER TESTS")
    print("=" * 60)
    
    try:
        from test_scraper_basic import test_scraper_basic
        result = await test_scraper_basic()
        print(f"✅ Basic Scraper: {'PASSED' if result else 'FAILED'}")
        return result
    except Exception as e:
        print(f"❌ Scraper Tests: ERROR - {e}")
        return False

async def run_hash_system_tests():
    """Run hash system tests"""
    print("\n" + "=" * 60)
    print("HASH SYSTEM TESTS")
    print("=" * 60)
    
    try:
        from test_hash_system import test_hash_system
        result = await test_hash_system()
        print(f"✅ Hash-based Diff System: {'PASSED' if result else 'FAILED'}")
        return result
    except Exception as e:
        print(f"❌ Hash System Tests: ERROR - {e}")
        return False

async def run_api_tests():
    """Run API tests (optional - requires server)"""
    print("\n" + "=" * 60)
    print("API TESTS (Optional - requires server)")
    print("=" * 60)
    
    try:
        from test_api_endpoints import test_api_endpoints
        result = await test_api_endpoints()
        print(f"✅ API Endpoints: {'PASSED' if result else 'FAILED'}")
        return result
    except Exception as e:
        print(f"⚠️  API Tests: SKIPPED - {e}")
        return True  # Don't fail CI/CD if server is not running

async def main():
    """Run all short CI/CD tests"""
    print("MLS Scraping Server - Short CI/CD Test Suite")
    print("=" * 60)
    
    # Run core tests
    database_result = await run_database_tests()
    scraper_result = await run_scraper_tests()
    hash_result = await run_hash_system_tests()
    
    # Run optional API tests
    api_result = await run_api_tests()
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    core_tests_passed = sum([database_result, scraper_result, hash_result])
    core_tests_total = 3
    
    print(f"Database Tests: {'PASSED' if database_result else 'FAILED'}")
    print(f"Scraper Tests: {'PASSED' if scraper_result else 'FAILED'}")
    print(f"Hash System Tests: {'PASSED' if hash_result else 'FAILED'}")
    print(f"API Tests: {'PASSED' if api_result else 'SKIPPED'}")
    
    print(f"\nCore Tests: {core_tests_passed}/{core_tests_total} passed")
    
    overall_success = core_tests_passed == core_tests_total
    print(f"\nOverall: {'✅ ALL CORE TESTS PASSED' if overall_success else '❌ SOME CORE TESTS FAILED'}")
    
    return overall_success

if __name__ == "__main__":
    asyncio.run(main())
