"""
Check DataImpulse Proxy Configuration
This script verifies that DataImpulse proxy is properly configured and can be initialized.
"""

import sys
import os
from pathlib import Path

# Add current directory to path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

try:
    from config import settings
    from proxy_manager import proxy_manager
    import asyncio
except ImportError as e:
    print(f"[ERR] Failed to import required modules: {e}")
    sys.exit(1)

async def check_proxy_config():
    """Check DataImpulse proxy configuration"""
    print("\n" + "="*80)
    print("DATAIMPULSE PROXY CONFIGURATION CHECK")
    print("="*80)
    
    # Check USE_DATAIMPULSE setting
    use_dataimpulse = getattr(settings, 'USE_DATAIMPULSE', True)
    print(f"\n1. USE_DATAIMPULSE: {use_dataimpulse}")
    if not use_dataimpulse:
        print("   ⚠️  WARNING: DataImpulse is DISABLED!")
        print("   Set USE_DATAIMPULSE=true in your .env file to enable proxy support")
        return False
    
    # Check credentials
    login = getattr(settings, 'DATAIMPULSE_LOGIN', '')
    password = getattr(settings, 'DATAIMPULSE_PASSWORD', '')
    endpoint = getattr(settings, 'DATAIMPULSE_ENDPOINT', '')
    
    print(f"\n2. DATAIMPULSE_LOGIN: {'[OK] Set' if login else '[ERR] Missing'}")
    if login:
        print(f"   Value: {login[:10]}..." if len(login) > 10 else f"   Value: {login}")
    else:
        print("   ⚠️  Set DATAIMPULSE_LOGIN in your .env file")
    
    print(f"\n3. DATAIMPULSE_PASSWORD: {'[OK] Set' if password else '[ERR] Missing'}")
    if password:
        print(f"   Value: {'*' * min(len(password), 10)}...")
    else:
        print("   ⚠️  Set DATAIMPULSE_PASSWORD in your .env file")
    
    print(f"\n4. DATAIMPULSE_ENDPOINT: {'[OK] Set' if endpoint else '[ERR] Missing'}")
    if endpoint:
        print(f"   Value: {endpoint}")
    else:
        print("   ⚠️  Set DATAIMPULSE_ENDPOINT in your .env file (e.g., gw.dataimpulse.com:823)")
    
    # Check if all required fields are set
    if not login or not password or not endpoint:
        print("\n[ERR] Configuration incomplete. Please set all required DataImpulse variables:")
        print("   DATAIMPULSE_LOGIN=your_login")
        print("   DATAIMPULSE_PASSWORD=your_password")
        print("   DATAIMPULSE_ENDPOINT=gw.dataimpulse.com:823")
        print("   USE_DATAIMPULSE=true")
        return False
    
    # Try to initialize proxies
    print("\n5. Initializing DataImpulse proxies...")
    try:
        success = await proxy_manager.initialize_dataimpulse_proxies(login)
        if success:
            proxy_count = len(proxy_manager.proxies)
            print(f"   [OK] Successfully initialized {proxy_count} proxy configurations")
            
            # Show proxy details
            print("\n   Proxy configurations:")
            for i, proxy in enumerate(proxy_manager.proxies, 1):
                print(f"   {i}. {proxy.username}@{proxy.host}:{proxy.port}")
            
            # Test getting a proxy
            print("\n6. Testing proxy retrieval...")
            test_proxy = proxy_manager.get_next_proxy()
            if test_proxy:
                print(f"   [OK] Successfully retrieved proxy: {test_proxy.username}@{test_proxy.host}:{test_proxy.port}")
                
                # Build proxy URL
                proxy_url = f"http://{test_proxy.username}:{test_proxy.password}@{test_proxy.host}:{test_proxy.port}"
                print(f"   Proxy URL format: http://{test_proxy.username}:****@{test_proxy.host}:{test_proxy.port}")
            else:
                print("   [ERR] Failed to retrieve proxy")
                return False
            
            print("\n" + "="*80)
            print("[OK] DataImpulse proxy is properly configured and ready to use!")
            print("="*80)
            return True
        else:
            print("   [ERR] Failed to initialize proxies")
            return False
    except Exception as e:
        print(f"   [ERR] Error initializing proxies: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function"""
    try:
        result = asyncio.run(check_proxy_config())
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

