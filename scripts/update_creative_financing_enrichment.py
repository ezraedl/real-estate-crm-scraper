"""
Script to analyze all properties for creative financing keywords and update enrichment data.

This script:
1. Queries all properties from the database
2. Analyzes their descriptions for creative financing keywords
3. Updates the enrichment.creative_financing field in the database
"""

import asyncio
import os
import sys
from datetime import datetime
from typing import Dict, Any
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import settings
from services.text_analyzer import TextAnalyzer

# Initialize text analyzer
text_analyzer = TextAnalyzer()


def extract_description(property_dict: Dict[str, Any]) -> str:
    """Extract description text from property data"""
    description_fields = [
        'description.text',
        'description.full_description',
        'description.summary',
        'description',
        'remarks',
        'public_remarks'
    ]
    
    for field in description_fields:
        if '.' in field:
            # Nested field
            parts = field.split('.')
            value = property_dict
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    value = None
                    break
        else:
            # Direct field
            value = property_dict.get(field)
        
        if value and isinstance(value, str) and value.strip():
            return value.strip()
    
    return ""


async def update_creative_financing_enrichment(
    batch_size: int = 100,
    dry_run: bool = False,
    limit: int = None
):
    """
    Analyze all properties and update creative financing enrichment
    
    Args:
        batch_size: Number of properties to process in each batch
        dry_run: If True, only analyze without updating database
        limit: Maximum number of properties to process (None for all)
    """
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        print("=" * 80)
        print("CREATIVE FINANCING ENRICHMENT UPDATE")
        print("=" * 80)
        print(f"Database: {database_name}")
        print(f"Dry run: {dry_run}")
        if limit:
            print(f"Limit: {limit} properties")
        print(f"Batch size: {batch_size}")
        print("")
        
        # Get total count of properties
        total_count = await db.properties.count_documents({})
        print(f"Total properties in database: {total_count}")
        
        if limit:
            total_count = min(total_count, limit)
            print(f"Processing limit: {total_count} properties")
        print("")
        
        # Statistics
        stats = {
            'total_processed': 0,
            'properties_with_descriptions': 0,
            'properties_with_creative_financing': 0,
            'properties_updated': 0,
            'properties_skipped': 0,
            'properties_errors': 0,
            'categories_found': {
                'owner_finance': 0,
                'subject_to': 0,
                'lease_option': 0,
                'wrap_mortgage': 0,
                'distressed': 0,
                'partnership': 0
            }
        }
        
        # Process properties in batches
        skip = 0
        batch_num = 0
        
        while skip < total_count:
            batch_num += 1
            batch_limit = min(batch_size, total_count - skip)
            
            print(f"Processing batch {batch_num} (properties {skip + 1} to {skip + batch_limit})...")
            
            # Fetch batch of properties
            cursor = db.properties.find(
                {},
                {
                    "property_id": 1,
                    "mls_id": 1,
                    "description": 1,
                    "remarks": 1,
                    "public_remarks": 1,
                    "enrichment": 1
                }
            ).skip(skip).limit(batch_limit)
            
            properties = await cursor.to_list(length=batch_limit)
            
            if not properties:
                print(f"No more properties to process. Stopping.")
                break
            
            # Process each property in the batch
            for prop in properties:
                stats['total_processed'] += 1
                property_id = prop.get('property_id') or prop.get('_id')
                
                try:
                    # Extract description
                    description = extract_description(prop)
                    
                    if not description:
                        stats['properties_skipped'] += 1
                        if stats['total_processed'] % 100 == 0:
                            print(f"  Processed {stats['total_processed']} properties...")
                        continue
                    
                    stats['properties_with_descriptions'] += 1
                    
                    # Analyze for creative financing
                    text_analysis = text_analyzer.analyze_property_description(description)
                    creative_financing = text_analysis.get('creative_financing', {})
                    has_creative_financing = text_analysis.get('summary', {}).get('has_creative_financing', False)
                    
                    if has_creative_financing:
                        stats['properties_with_creative_financing'] += 1
                        
                        # Count categories found
                        for category, data in creative_financing.items():
                            if data.get('found', False):
                                if category in stats['categories_found']:
                                    stats['categories_found'][category] += 1
                    
                    # Prepare enrichment update
                    enrichment_update = {
                        'categories_found': [cat for cat, data in creative_financing.items() if data.get('found', False)],
                        'details': creative_financing,
                        'has_creative_financing': has_creative_financing
                    }
                    
                    # Update database if not dry run
                    if not dry_run:
                        await db.properties.update_one(
                            {"property_id": property_id},
                            {
                                "$set": {
                                    "enrichment.creative_financing": enrichment_update,
                                    "enrichment.enriched_at": datetime.utcnow(),
                                    "last_enriched_at": datetime.utcnow()
                                }
                            }
                        )
                        stats['properties_updated'] += 1
                    else:
                        # In dry run, just track stats
                        if has_creative_financing:
                            stats['properties_updated'] += 1
                    
                    # Progress update every 100 properties
                    if stats['total_processed'] % 100 == 0:
                        print(f"  Processed {stats['total_processed']} properties... "
                              f"({stats['properties_with_creative_financing']} with creative financing)")
                
                except Exception as e:
                    stats['properties_errors'] += 1
                    print(f"  Error processing property {property_id}: {e}")
                    continue
            
            skip += batch_size
            
            # Print batch summary
            print(f"Batch {batch_num} complete. "
                  f"Total processed: {stats['total_processed']}, "
                  f"With creative financing: {stats['properties_with_creative_financing']}")
            print("")
        
        # Print final summary
        print("=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total properties processed: {stats['total_processed']}")
        print(f"Properties with descriptions: {stats['properties_with_descriptions']}")
        print(f"Properties with creative financing: {stats['properties_with_creative_financing']}")
        print(f"Properties {'would be ' if dry_run else ''}updated: {stats['properties_updated']}")
        print(f"Properties skipped (no description): {stats['properties_skipped']}")
        print(f"Properties with errors: {stats['properties_errors']}")
        print("")
        print("Categories found:")
        for category, count in stats['categories_found'].items():
            if count > 0:
                print(f"  {category}: {count} properties")
        print("")
        
        if dry_run:
            print("DRY RUN - No changes were made to the database.")
        else:
            print(f"Successfully updated {stats['properties_updated']} properties with creative financing enrichment.")
        
        print("=" * 80)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Update creative financing enrichment for all properties')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing (default: 100)')
    parser.add_argument('--dry-run', action='store_true', help='Analyze without updating database')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of properties to process (default: all)')
    
    args = parser.parse_args()
    
    asyncio.run(update_creative_financing_enrichment(
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        limit=args.limit
    ))

