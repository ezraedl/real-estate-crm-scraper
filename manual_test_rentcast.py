
import asyncio
import logging
import sys
import os

# Configure logging to stdout
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

from dotenv import load_dotenv
load_dotenv()
d_login = os.getenv("DATAIMPULSE_LOGIN")
print(f"DEBUG: DATAIMPULSE_LOGIN loaded: {bool(d_login)} (len={len(d_login) if d_login else 0})")

# Helper to suppress other loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

from services.rentcast_service import RentcastService
from proxy_manager import proxy_manager

from config import settings

async def main():
    print("--- Starting Rentcast Test ---")
    
    # Initialize proxies if not already done (since this is a standalone script)
    if not proxy_manager.proxies and settings.USE_DATAIMPULSE and settings.DATAIMPULSE_LOGIN:
        print("Initializing DataImpulse proxies...")
        await proxy_manager.initialize_dataimpulse_proxies(
            login=settings.DATAIMPULSE_LOGIN,
            password=settings.DATAIMPULSE_PASSWORD,
            endpoint=settings.DATAIMPULSE_ENDPOINT
        )
    
    print(f"Proxy configured: {bool(proxy_manager.get_next_proxy())}")
    rs = RentcastService()
    
    # Test address from user's logs
    prop = {
        'address': {
            'formatted_address': '5534 S 980 E, Indianapolis, IN, 46795',
            'street': '5534 S 980 E',
            'city': 'Indianapolis',
            'state': 'IN',
            'zip_code': '46795'
        },
        'description': {
            'property_type': 'single_family',
            'beds': 3,
            'full_baths': 1,
            'half_baths': 1, # 1.5 baths
            'sqft': 1932,
            'year_built': 1955
        }
    }
    
    print(f"Fetching for: {prop['address']['formatted_address']}")
    
    # We use fetch_rent_estimate (no db save required)
    result = await rs.fetch_rent_estimate(prop)
    
    if result:
        print("\n✅ SUCCESS!")
        print(f"Rent: {result.get('rent')}")
        print(f"Range: {result.get('rent_range_low')} - {result.get('rent_range_high')}")
        comps = result.get('comparables', [])
        print(f"Comps found: {len(comps)}")
        if comps:
            print(f"First comp: {comps[0]}")
    else:
        print("\n❌ FAILED to get data.")

if __name__ == "__main__":
    # Ensure current directory is in path
    sys.path.append(os.getcwd())
    asyncio.run(main())
