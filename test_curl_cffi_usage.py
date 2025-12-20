"""
Test script to verify if homeharvest is using curl_cffi for TLS fingerprinting.

This script checks:
1. If curl_cffi is installed and importable
2. If homeharvest is using curl_cffi internally
3. Network behavior differences with/without curl_cffi
4. Environment variables that might affect curl_cffi usage
"""

import os
import sys
from pathlib import Path

# Fix Windows console encoding for emoji characters
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

import logging
import inspect

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_curl_cffi_installation():
    """Test if curl_cffi is installed and can be imported"""
    print("\n" + "="*70)
    print("TEST 1: curl_cffi Installation Check")
    print("="*70)
    
    try:
        import curl_cffi
        print("✅ curl_cffi is installed")
        print(f"   Version: {curl_cffi.__version__ if hasattr(curl_cffi, '__version__') else 'Unknown'}")
        
        # Check if curl_cffi.requests is available
        try:
            from curl_cffi import requests as curl_requests
            print("✅ curl_cffi.requests is available")
            return True, curl_cffi
        except ImportError as e:
            print(f"⚠️  curl_cffi.requests not available: {e}")
            return True, curl_cffi
    except ImportError as e:
        print(f"❌ curl_cffi is NOT installed: {e}")
        print("   Install with: pip install curl-cffi")
        return False, None

def check_environment_variables():
    """Check environment variables that might affect curl_cffi usage"""
    print("\n" + "="*70)
    print("TEST 2: Environment Variables Check")
    print("="*70)
    
    relevant_vars = [
        "HOMEHARVEST_USE_CURL_CFFI",
        "CURL_CFFI_IMPS",
        "CURL_CFFI_CA_BUNDLE",
    ]
    
    for var in relevant_vars:
        value = os.environ.get(var)
        if value:
            print(f"✅ {var} = {value}")
        else:
            print(f"   {var} = (not set)")
    
    # Check if we set HOMEHARVEST_USE_CURL_CFFI
    if not os.environ.get("HOMEHARVEST_USE_CURL_CFFI"):
        print("\n⚠️  HOMEHARVEST_USE_CURL_CFFI is not set")
        print("   Setting it to 'true'...")
        os.environ["HOMEHARVEST_USE_CURL_CFFI"] = "true"
        print("   ✅ Set HOMEHARVEST_USE_CURL_CFFI=true")

def inspect_homeharvest_internals():
    """Inspect homeharvest source code to see if it uses curl_cffi"""
    print("\n" + "="*70)
    print("TEST 3: HomeHarvest Internal Inspection")
    print("="*70)
    
    try:
        import homeharvest
        print(f"✅ homeharvest is installed")
        print(f"   Version: {homeharvest.__version__ if hasattr(homeharvest, '__version__') else 'Unknown'}")
        print(f"   Location: {homeharvest.__file__ if hasattr(homeharvest, '__file__') else 'Unknown'}")
        
        # Try to find the scrape_property function
        from homeharvest import scrape_property
        print(f"✅ scrape_property function found")
        
        # Get the source file
        try:
            source_file = inspect.getfile(scrape_property)
            print(f"   Source file: {source_file}")
        except:
            print("   Source file: (could not determine)")
        
        # Check if we can inspect the function's code
        try:
            source_code = inspect.getsource(scrape_property)
            # Look for curl_cffi references
            if "curl_cffi" in source_code.lower():
                print("✅ Found 'curl_cffi' reference in scrape_property source code")
                # Count occurrences
                count = source_code.lower().count("curl_cffi")
                print(f"   Found {count} occurrence(s) of 'curl_cffi'")
            else:
                print("⚠️  No 'curl_cffi' reference found in scrape_property source code")
            
            # Look for requests library usage
            if "import requests" in source_code or "from requests" in source_code:
                print("⚠️  Found 'requests' library usage (might not use curl_cffi)")
            if "import httpx" in source_code or "from httpx" in source_code:
                print("ℹ️  Found 'httpx' library usage")
            if "curl" in source_code.lower():
                print("ℹ️  Found 'curl' reference in source code")
        except Exception as e:
            print(f"⚠️  Could not inspect source code: {e}")
        
        # Try to find the homeharvest module directory
        try:
            homeharvest_path = Path(homeharvest.__file__).parent
            print(f"\n   Searching for curl_cffi references in homeharvest package...")
            
            # Search Python files in homeharvest package
            python_files = list(homeharvest_path.rglob("*.py"))
            curl_cffi_files = []
            for py_file in python_files:
                try:
                    content = py_file.read_text(encoding='utf-8', errors='ignore')
                    if "curl_cffi" in content.lower():
                        curl_cffi_files.append(py_file)
                except:
                    pass
            
            if curl_cffi_files:
                print(f"✅ Found {len(curl_cffi_files)} file(s) with curl_cffi references:")
                for f in curl_cffi_files[:5]:  # Show first 5
                    rel_path = f.relative_to(homeharvest_path)
                    print(f"   - {rel_path}")
            else:
                print("⚠️  No files found with curl_cffi references in homeharvest package")
        except Exception as e:
            print(f"⚠️  Could not search homeharvest package: {e}")
        
        return True
    except ImportError as e:
        print(f"❌ Could not import homeharvest: {e}")
        return False

def test_http_client_type():
    """Test what HTTP client homeharvest is actually using"""
    print("\n" + "="*70)
    print("TEST 4: HTTP Client Type Detection")
    print("="*70)
    
    try:
        from homeharvest import scrape_property
        import traceback
        
        # Monkey patch to intercept HTTP calls
        original_import = __builtins__.__import__
        http_clients_used = []
        
        def intercept_import(name, *args, **kwargs):
            if name in ["requests", "httpx", "curl_cffi", "curl_cffi.requests"]:
                http_clients_used.append(name)
                print(f"   📦 Detected import: {name}")
            return original_import(name, *args, **kwargs)
        
        # This is tricky - we can't easily intercept imports at runtime
        # Instead, let's check what's actually imported in the homeharvest namespace
        import homeharvest
        
        # Check what HTTP-related modules are imported
        http_modules = []
        for attr_name in dir(homeharvest):
            attr = getattr(homeharvest, attr_name)
            if inspect.ismodule(attr):
                module_name = getattr(attr, '__name__', '')
                if any(x in module_name.lower() for x in ['request', 'http', 'curl']):
                    http_modules.append(module_name)
        
        if http_modules:
            print(f"✅ Found HTTP-related modules in homeharvest namespace:")
            for mod in http_modules:
                print(f"   - {mod}")
        else:
            print("⚠️  Could not detect HTTP modules in homeharvest namespace")
        
        return True
    except Exception as e:
        print(f"❌ Error testing HTTP client: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_actual_scrape_with_monitoring():
    """Test actual scraping and monitor for 403 errors"""
    print("\n" + "="*70)
    print("TEST 5: Actual Scrape Test (with 403 monitoring)")
    print("="*70)
    
    try:
        from homeharvest import scrape_property
        
        print("   Testing with a simple location (Indianapolis, IN)...")
        print("   This will make an actual HTTP request to Realtor.com")
        print("   Monitoring for 403 errors...")
        
        # Set environment variable if not set
        if not os.environ.get("HOMEHARVEST_USE_CURL_CFFI"):
            os.environ["HOMEHARVEST_USE_CURL_CFFI"] = "true"
        
        try:
            # Try a minimal scrape
            df = scrape_property(
                location="Indianapolis, IN",
                listing_type=["for_sale"],
                limit=1
            )
            
            if df is not None and not df.empty:
                print(f"✅ Scrape succeeded! Got {len(df)} property(ies)")
                print("   This suggests curl_cffi might be working, OR")
                print("   the request succeeded for other reasons (proxy, etc.)")
                return True
            else:
                print("⚠️  Scrape returned empty result (0 properties)")
                print("   This could indicate blocking or no properties available")
                return False
                
        except Exception as e:
            error_msg = str(e)
            error_lower = error_msg.lower()
            
            print(f"❌ Scrape failed with error:")
            print(f"   Error: {error_msg}")
            print(f"   Type: {type(e).__name__}")
            
            # Check for 403
            if "403" in error_msg or "forbidden" in error_lower:
                print("\n   🚨 403 FORBIDDEN ERROR DETECTED!")
                print("   This indicates anti-bot blocking.")
                print("   curl_cffi might not be working, or Realtor.com")
                print("   is using more sophisticated detection.")
            elif "timeout" in error_lower:
                print("\n   ⚠️  Timeout error (might be network issue)")
            else:
                print("\n   ⚠️  Other error (not necessarily blocking)")
            
            return False
            
    except ImportError as e:
        print(f"❌ Could not import homeharvest: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_curl_cffi_directly():
    """Test curl_cffi directly to verify it works"""
    print("\n" + "="*70)
    print("TEST 6: Direct curl_cffi Test")
    print("="*70)
    
    try:
        from curl_cffi import requests as curl_requests
        
        print("   Testing curl_cffi.requests directly...")
        print("   Making a test request to httpbin.org to verify TLS fingerprinting...")
        
        try:
            # Test with curl_cffi directly
            response = curl_requests.get(
                "https://httpbin.org/get",
                timeout=10,
                impersonate="chrome110"  # Use Chrome impersonation
            )
            
            if response.status_code == 200:
                print("✅ curl_cffi.requests works correctly!")
                print(f"   Status code: {response.status_code}")
                print("   TLS fingerprinting should be active")
                return True
            else:
                print(f"⚠️  curl_cffi.requests returned status {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ curl_cffi.requests test failed: {e}")
            return False
            
    except ImportError as e:
        print(f"❌ curl_cffi.requests not available: {e}")
        return False

def main():
    """Run all tests"""
    print("\n" + "="*70)
    print("HOMEHARVEST curl_cffi USAGE TEST")
    print("="*70)
    print("\nThis script tests if homeharvest is using curl_cffi for TLS fingerprinting.")
    print("This is important for bypassing Realtor.com's anti-bot measures.\n")
    
    results = {}
    
    # Test 1: curl_cffi installation
    curl_cffi_installed, curl_cffi_module = test_curl_cffi_installation()
    results['curl_cffi_installed'] = curl_cffi_installed
    
    # Test 2: Environment variables
    check_environment_variables()
    
    # Test 3: HomeHarvest internals
    results['homeharvest_inspection'] = inspect_homeharvest_internals()
    
    # Test 4: HTTP client type
    results['http_client_test'] = test_http_client_type()
    
    # Test 6: Direct curl_cffi test (before actual scrape)
    if curl_cffi_installed:
        results['curl_cffi_direct'] = test_curl_cffi_directly()
    
    # Test 5: Actual scrape (last, as it makes real requests)
    print("\n" + "⚠️ " * 35)
    print("WARNING: The next test will make actual HTTP requests to Realtor.com")
    print("This may trigger rate limiting or blocking if done too frequently.")
    print("⚠️ " * 35)
    
    # Check for --skip-scrape flag or non-interactive mode
    skip_scrape = '--skip-scrape' in sys.argv or not sys.stdin.isatty()
    
    if skip_scrape:
        print("\n   Skipping actual scrape test (non-interactive mode or --skip-scrape flag)")
        print("   To run the scrape test, run: python test_curl_cffi_usage.py (without --skip-scrape)")
        results['actual_scrape'] = None
    else:
        try:
            user_input = input("\nProceed with actual scrape test? (y/n): ").strip().lower()
            if user_input == 'y':
                results['actual_scrape'] = test_actual_scrape_with_monitoring()
            else:
                print("   Skipping actual scrape test (user declined)")
                results['actual_scrape'] = None
        except (EOFError, KeyboardInterrupt):
            print("\n   Skipping actual scrape test (interrupted or non-interactive)")
            results['actual_scrape'] = None
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    for test_name, result in results.items():
        if result is True:
            print(f"✅ {test_name}: PASSED")
        elif result is False:
            print(f"❌ {test_name}: FAILED")
        elif result is None:
            print(f"⏭️  {test_name}: SKIPPED")
        else:
            print(f"⚠️  {test_name}: {result}")
    
    print("\n" + "="*70)
    print("RECOMMENDATIONS")
    print("="*70)
    
    if not results.get('curl_cffi_installed'):
        print("1. ❌ Install curl_cffi: pip install curl-cffi")
    else:
        print("1. ✅ curl_cffi is installed")
    
    if not results.get('homeharvest_inspection'):
        print("2. ⚠️  Could not inspect homeharvest internals")
    else:
        print("2. ✅ HomeHarvest inspection completed")
    
    if results.get('actual_scrape') is False:
        print("3. 🚨 Actual scrape failed - likely 403 blocking")
        print("   - Verify homeharvest is using curl_cffi")
        print("   - Check if homeharvest needs to be updated")
        print("   - Consider using proxies")
        print("   - Check Realtor.com's current anti-bot measures")
    elif results.get('actual_scrape') is True:
        print("3. ✅ Actual scrape succeeded")
    else:
        print("3. ⏭️  Actual scrape test was skipped")
    
    print("\n" + "="*70)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

