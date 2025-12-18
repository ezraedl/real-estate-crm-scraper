"""
Direct test script for homeharvest library
This script tests homeharvest independently to verify the library is working correctly.
Run this script to diagnose if the issue is with homeharvest or with the scraper implementation.

NOTE: If you get 403 errors, Realtor.com is blocking requests. Try using a proxy.
"""

import sys
import os
from datetime import datetime
from pathlib import Path

# Fix Windows console encoding for Unicode characters
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add current directory to path
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

try:
    from homeharvest import scrape_property
    print("[OK] Successfully imported homeharvest")
except ImportError as e:
    print(f"[ERROR] Failed to import homeharvest: {e}")
    print("Make sure homeharvest is installed: pip install homeharvest")
    sys.exit(1)

# Try to load proxy configuration from environment or config
def get_proxy_url():
    """Get proxy URL from environment or config"""
    # Check environment variable first
    proxy_url = os.getenv("PROXY_URL") or os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")
    if proxy_url:
        return proxy_url
    
    # Try to load from config (if available)
    try:
        from config import settings
        if hasattr(settings, 'DATAIMPULSE_ENDPOINT') and settings.DATAIMPULSE_ENDPOINT:
            # Construct DataImpulse proxy URL
            login = settings.DATAIMPULSE_LOGIN
            password = settings.DATAIMPULSE_PASSWORD
            endpoint = settings.DATAIMPULSE_ENDPOINT
            
            if login and password and endpoint:
                # Remove protocol if present
                if endpoint.startswith('http://'):
                    endpoint = endpoint[7:]
                elif endpoint.startswith('https://'):
                    endpoint = endpoint[8:]
                
                # Format: http://username:password@host:port
                if ':' in endpoint:
                    host, port = endpoint.split(':')
                else:
                    host = endpoint
                    port = "823"
                
                # DataImpulse format: username__cr.us:password@host:port
                proxy_url = f"http://{login}__cr.us:{password}@{host}:{port}"
                return proxy_url
    except:
        pass
    
    return None

def test_basic_scrape(location: str = "Indianapolis, IN", listing_type: str = "for_sale", limit: int = 10, use_proxy: bool = False):
    """Test basic property scraping"""
    print(f"\n{'='*80}")
    print(f"TEST 1: Basic Scrape")
    print(f"{'='*80}")
    print(f"Location: {location}")
    print(f"Listing Type: {listing_type}")
    print(f"Limit: {limit}")
    print(f"Time: {datetime.now()}")
    
    # Get proxy if requested
    proxy_url = None
    if use_proxy:
        proxy_url = get_proxy_url()
        if proxy_url:
            print(f"Using proxy: {proxy_url.split('@')[-1] if '@' in proxy_url else 'configured'}")
        else:
            print("[WARNING] Proxy requested but not configured. Trying without proxy...")
    
    try:
        print("\nCalling scrape_property...")
        params = {
            "location": location,
            "listing_type": [listing_type],
            "limit": limit
        }
        if proxy_url:
            params["proxy"] = proxy_url
        
        df = scrape_property(**params)
        
        if df is None:
            print("[ERROR] scrape_property returned None")
            return False
        
        if df.empty:
            print(f"[WARNING] scrape_property returned empty DataFrame (0 properties)")
            print("This might indicate:")
            print("  - No properties match the criteria")
            print("  - API rate limiting")
            print("  - Network/connection issues")
            print("  - homeharvest API changes")
            return False
        
        num_properties = len(df)
        print(f"[SUCCESS] Got {num_properties} properties")
        
        # Show basic info
        print(f"\nDataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)[:10]}...")  # Show first 10 columns
        
        # Show sample data
        if num_properties > 0:
            print(f"\nFirst property sample:")
            first_prop = df.iloc[0]
            for col in ['address', 'city', 'state', 'zip_code', 'price', 'status', 'property_type']:
                if col in df.columns:
                    print(f"  {col}: {first_prop[col]}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Error during scraping: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_with_past_days(location: str = "Indianapolis, IN", past_days: int = 90, use_proxy: bool = False):
    """Test scraping with past_days parameter"""
    print(f"\n{'='*80}")
    print(f"TEST 2: Scrape with past_days")
    print(f"{'='*80}")
    print(f"Location: {location}")
    print(f"Past Days: {past_days}")
    
    proxy_url = get_proxy_url() if use_proxy else None
    
    try:
        print("\nCalling scrape_property with past_days...")
        params = {
            "location": location,
            "listing_type": ["for_sale"],
            "past_days": past_days,
            "limit": 10
        }
        if proxy_url:
            params["proxy"] = proxy_url
        
        df = scrape_property(**params)
        
        if df is None or df.empty:
            print(f"[WARNING] Got 0 properties with past_days={past_days}")
            return False
        
        print(f"[SUCCESS] Got {len(df)} properties with past_days={past_days}")
        return True
        
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_multiple_listing_types(location: str = "Indianapolis, IN", use_proxy: bool = False):
    """Test scraping multiple listing types"""
    print(f"\n{'='*80}")
    print(f"TEST 3: Multiple Listing Types")
    print(f"{'='*80}")
    print(f"Location: {location}")
    print(f"Listing Types: ['for_sale', 'sold', 'for_rent']")
    
    proxy_url = get_proxy_url() if use_proxy else None
    
    try:
        print("\nCalling scrape_property with multiple listing types...")
        params = {
            "location": location,
            "listing_type": ["for_sale", "sold", "for_rent"],
            "limit": 20
        }
        if proxy_url:
            params["proxy"] = proxy_url
        
        df = scrape_property(**params)
        
        if df is None or df.empty:
            print(f"[WARNING] Got 0 properties with multiple listing types")
            return False
        
        print(f"[SUCCESS] Got {len(df)} properties")
        
        # Show breakdown by listing type if available
        if 'status' in df.columns:
            status_counts = df['status'].value_counts()
            print(f"\nProperties by status:")
            for status, count in status_counts.items():
                print(f"  {status}: {count}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_zip_code(location: str = "46201", use_proxy: bool = False):
    """Test scraping with zip code"""
    print(f"\n{'='*80}")
    print(f"TEST 4: Zip Code Scrape")
    print(f"{'='*80}")
    print(f"Location (Zip): {location}")
    
    proxy_url = get_proxy_url() if use_proxy else None
    
    try:
        print("\nCalling scrape_property with zip code...")
        params = {
            "location": location,
            "listing_type": ["for_sale"],
            "limit": 10
        }
        if proxy_url:
            params["proxy"] = proxy_url
        
        df = scrape_property(**params)
        
        if df is None or df.empty:
            print(f"[WARNING] Got 0 properties for zip code {location}")
            return False
        
        print(f"✅ Success! Got {len(df)} properties for zip code {location}")
        return True
        
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests"""
    print("\n" + "="*80)
    print("HOMEHARVEST DIRECT TEST SCRIPT")
    print("="*80)
    print(f"Test started at: {datetime.now()}")
    print(f"Python version: {sys.version}")
    
    # Check for proxy
    proxy_url = get_proxy_url()
    use_proxy = proxy_url is not None
    
    if use_proxy:
        print(f"[OK] Proxy detected: {proxy_url.split('@')[-1] if '@' in proxy_url else 'configured'}")
        print("   Tests will use proxy to avoid 403 errors")
    else:
        print("[WARNING] No proxy configured")
        print("   If you get 403 errors, configure a proxy in your .env file:")
        print("   - DATAIMPULSE_LOGIN=your_login")
        print("   - DATAIMPULSE_PASSWORD=your_password")
        print("   - DATAIMPULSE_ENDPOINT=gw.dataimpulse.com:823")
        print("   Or set PROXY_URL environment variable")
    
    # Test location - you can change this
    test_location = "Indianapolis, IN"
    
    results = []
    
    # Run tests
    results.append(("Basic Scrape", test_basic_scrape(test_location, use_proxy=use_proxy)))
    results.append(("Past Days (90)", test_with_past_days(test_location, 90, use_proxy=use_proxy)))
    results.append(("Multiple Listing Types", test_multiple_listing_types(test_location, use_proxy=use_proxy)))
    results.append(("Zip Code (46201)", test_zip_code("46201", use_proxy=use_proxy)))
    
    # Summary
    print(f"\n{'='*80}")
    print("TEST SUMMARY")
    print(f"{'='*80}")
    for test_name, result in results:
        status = "[PASSED]" if result else "[FAILED]"
        print(f"{test_name}: {status}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == 0:
        print("\n[WARNING] All tests failed. This suggests:")
        print("  1. Realtor.com API is blocking requests (403 errors) - MOST LIKELY")
        print("  2. Network/API connectivity problems")
        print("  3. API rate limiting or blocking")
        print("  4. homeharvest API changes")
        print("\nSolutions:")
        print("  1. USE A PROXY - This is the most common solution for 403 errors")
        print("     - Configure DataImpulse proxy in .env file")
        print("     - Or set PROXY_URL environment variable")
        print("  2. Check your internet connection")
        print("  3. Update homeharvest: pip install --upgrade homeharvest")
        print("  4. Wait and retry (might be temporary rate limiting)")
        print("  5. Check homeharvest GitHub for known issues")
        print("\nNOTE: The scraper has proxy support built-in. Make sure your .env file")
        print("   has DATAIMPULSE credentials configured if you're using DataImpulse.")
    elif passed < total:
        print(f"\n[WARNING] Some tests failed. Check which specific test failed above.")
    else:
        print("\n[SUCCESS] All tests passed! homeharvest library is working correctly.")
        print("   If the scraper still doesn't work, the issue is likely in the scraper implementation.")

if __name__ == "__main__":
    main()

