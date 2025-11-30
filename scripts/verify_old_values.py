"""Quick script to verify _old values were set correctly"""
import asyncio
import os
import sys
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import settings

async def verify():
    parsed_uri = urlparse(settings.MONGODB_URI)
    database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
    client = AsyncIOMotorClient(settings.MONGODB_URI)
    db = client[database_name]
    
    prop = await db.properties.find_one({'property_id': '4615498238'})
    if prop:
        print('Property ID:', prop.get('property_id'))
        print('Current list_price:', prop.get('financial', {}).get('list_price'))
        print('_old list_price:', prop.get('financial', {}).get('list_price_old'))
        print('Current original_list_price:', prop.get('financial', {}).get('original_list_price'))
        print('_old original_list_price:', prop.get('financial', {}).get('original_list_price_old'))
        print('Current status:', prop.get('status'))
        print('_old status:', prop.get('status_old'))
        print('Current mls_status:', prop.get('mls_status'))
        print('_old mls_status:', prop.get('mls_status_old'))
        print('Current listing_type:', prop.get('listing_type'))
        print('_old listing_type:', prop.get('listing_type_old'))
        print('_old_values_scraped_at:', prop.get('_old_values_scraped_at'))
    else:
        print('Property not found')
    
    client.close()

asyncio.run(verify())

