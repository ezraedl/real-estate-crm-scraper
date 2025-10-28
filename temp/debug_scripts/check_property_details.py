#!/usr/bin/env python
"""Check property enrichment details"""
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from config import settings
import json

async def check_property(property_id):
    """Check enrichment details for a property"""
    
    client = AsyncIOMotorClient(settings.MONGODB_URI)
    db = client['mls_scraper']
    
    prop = await db.properties.find_one({'property_id': property_id})
    
    if prop:
        enrichment = prop.get('enrichment', {})
        print(f"Property ID: {property_id}")
        print(f"Address: {prop.get('address', {}).get('formatted_address', 'Unknown')}")
        print(f"\nEnrichment Data:")
        print(f"  Motivated Seller Score: {enrichment.get('motivated_seller', {}).get('score', 'N/A')}")
        print(f"  Reasoning: {enrichment.get('motivated_seller', {}).get('reasoning', [])}")
        
        # Price history
        price_history = enrichment.get('price_history_summary', {})
        if price_history:
            print(f"\nPrice History:")
            print(f"  Number of reductions: {price_history.get('number_of_reductions', 0)}")
            print(f"  Total reduced: ${price_history.get('total_reductions', 0):,.0f}")
            print(f"  Average reduction: ${price_history.get('average_reduction_amount', 0):,.0f}")
        
        # Big ticket items
        big_ticket = enrichment.get('big_ticket_items', {})
        print(f"\nBig Ticket Items:")
        for item in ['roof', 'hvac', 'kitchen', 'bathroom']:
            if big_ticket.get(item, {}).get('found'):
                print(f"  {item.capitalize()}: Found")
        
        # Distress signals
        distress = enrichment.get('distress_signals', {}).get('signals_found', [])
        if distress:
            print(f"\nDistress Signals: {distress}")
        
        # Special sale types
        special = enrichment.get('special_sale_types', {})
        special_types = [k for k, v in special.items() if v and k.startswith('is_')]
        if special_types:
            print(f"\nSpecial Sale Types: {special_types}")
    else:
        print(f"Property {property_id} not found")
    
    client.close()

if __name__ == "__main__":
    import sys
    prop_id = sys.argv[1] if len(sys.argv) > 1 else "3645735789"
    asyncio.run(check_property(prop_id))

