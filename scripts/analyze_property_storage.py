"""
Analyze property documents to identify:
1. Duplicate/redundant fields
2. Unused fields
3. Large fields that could be optimized
4. Storage optimization opportunities

Usage:
    python scripts/analyze_property_storage.py [--sample N]
"""

import asyncio
import os
import sys
import json
from collections import defaultdict, Counter
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse
from config import settings

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def analyze_property_storage(sample_size: int = 100):
    """Analyze property documents for storage optimization opportunities"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        logger.info(f"Analyzing property documents (sample size: {sample_size})...")
        
        # Get sample of properties
        cursor = db.properties.find({}).limit(sample_size)
        
        # Statistics
        field_usage = Counter()  # How many properties have each field
        field_sizes = defaultdict(list)  # Size of each field
        field_null_count = Counter()  # How many have null/empty values
        total_sizes = []
        enrichment_sizes = []
        description_sizes = []
        contact_info_sizes = []
        
        # Potentially unused fields based on codebase analysis
        potentially_unused_fields = {
            'formatted_address',  # If we have full_street_line, city, state, zip
            'full_street_line',  # If we have formatted_address
            'description.full_description',  # If we have description.text
            'description.summary',  # If we have description.text
            'listing_id',  # If we have mls_id
            'permalink',  # If we have property_url
            'agent.agent_mls_set',  # Rarely used
            'agent.agent_nrds_id',  # Rarely used
            'office.office_mls_set',  # Rarely used
            'monthly_fees',  # Potentially large, rarely used
            'one_time_fees',  # Potentially large, rarely used
            'tax_history',  # Potentially large, rarely used
            'nearby_schools',  # Potentially large, rarely used
            'alt_photos',  # Potentially large list of URLs
            'is_comp',  # Default False, mostly unused
            'builder',  # Rarely populated
        }
        
        async for prop in cursor:
            # Calculate document size (approximate)
            doc_json = json.dumps(prop, default=str)
            doc_size = len(doc_json.encode('utf-8'))
            total_sizes.append(doc_size)
            
            # Analyze each field
            analyze_field(prop, '', field_usage, field_sizes, field_null_count)
            
            # Analyze specific large sections
            if 'enrichment' in prop:
                enrichment_json = json.dumps(prop['enrichment'], default=str)
                enrichment_sizes.append(len(enrichment_json.encode('utf-8')))
            
            if 'description' in prop and prop['description']:
                desc_json = json.dumps(prop['description'], default=str)
                description_sizes.append(len(desc_json.encode('utf-8')))
            
            # Contact info (agent, broker, builder, office)
            contact_info = {}
            for key in ['agent', 'broker', 'builder', 'office']:
                if key in prop and prop[key]:
                    contact_info[key] = prop[key]
            if contact_info:
                contact_json = json.dumps(contact_info, default=str)
                contact_info_sizes.append(len(contact_json.encode('utf-8')))
        
        # Calculate statistics
        total_properties = len(total_sizes)
        avg_doc_size = sum(total_sizes) / total_properties if total_sizes else 0
        avg_enrichment_size = sum(enrichment_sizes) / len(enrichment_sizes) if enrichment_sizes else 0
        avg_description_size = sum(description_sizes) / len(description_sizes) if description_sizes else 0
        avg_contact_size = sum(contact_info_sizes) / len(contact_info_sizes) if contact_info_sizes else 0
        
        # Print analysis
        logger.info("\n" + "="*80)
        logger.info("PROPERTY STORAGE ANALYSIS")
        logger.info("="*80)
        logger.info(f"\nSample Size: {total_properties} properties")
        logger.info(f"Average document size: {avg_doc_size / 1024:.2f} KB")
        logger.info(f"Average enrichment size: {avg_enrichment_size / 1024:.2f} KB ({avg_enrichment_size/avg_doc_size*100:.1f}% of doc)")
        logger.info(f"Average description size: {avg_description_size / 1024:.2f} KB ({avg_description_size/avg_doc_size*100:.1f}% of doc)")
        logger.info(f"Average contact info size: {avg_contact_size / 1024:.2f} KB ({avg_contact_size/avg_doc_size*100:.1f}% of doc)")
        
        # Find fields that are rarely used
        logger.info("\n" + "-"*80)
        logger.info("RARELY USED FIELDS (could be removed)")
        logger.info("-"*80)
        for field in sorted(field_usage.keys()):
            usage_pct = (field_usage[field] / total_properties) * 100
            null_pct = (field_null_count[field] / total_properties) * 100 if field_null_count[field] > 0 else 0
            
            # Calculate average size
            avg_size = sum(field_sizes[field]) / len(field_sizes[field]) if field_sizes[field] else 0
            
            if usage_pct < 20 or (field in potentially_unused_fields and null_pct > 50):
                logger.info(f"  {field:50s} - Used: {usage_pct:5.1f}% | Null: {null_pct:5.1f}% | Avg Size: {avg_size:.0f} bytes")
        
        # Find large fields
        logger.info("\n" + "-"*80)
        logger.info("LARGEST FIELDS (could be optimized)")
        logger.info("-"*80)
        field_avg_sizes = [(field, sum(field_sizes[field]) / len(field_sizes[field])) for field in field_sizes if field_sizes[field]]
        field_avg_sizes.sort(key=lambda x: x[1], reverse=True)
        
        for field, avg_size in field_avg_sizes[:20]:  # Top 20 largest
            usage_pct = (field_usage[field] / total_properties) * 100
            logger.info(f"  {field:50s} - Avg Size: {avg_size/1024:6.2f} KB | Used: {usage_pct:5.1f}%")
        
        # Find duplicate/redundant fields
        logger.info("\n" + "-"*80)
        logger.info("POTENTIALLY DUPLICATE/REDUNDANT FIELDS")
        logger.info("-"*80)
        
        # Check for address duplicates
        address_fields = ['address.formatted_address', 'address.full_street_line', 
                         'address.street', 'address.city', 'address.state', 'address.zip_code']
        address_usage = {f: field_usage[f] for f in address_fields if f in field_usage}
        if address_usage:
            logger.info("  Address fields:")
            for field, count in sorted(address_usage.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"    {field}: {count}/{total_properties} ({count/total_properties*100:.1f}%)")
        
        # Check for description duplicates
        desc_fields = ['description.text', 'description.full_description', 'description.summary']
        desc_usage = {f: field_usage[f] for f in desc_fields if f in field_usage}
        if desc_usage:
            logger.info("  Description fields:")
            for field, count in sorted(desc_usage.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"    {field}: {count}/{total_properties} ({count/total_properties*100:.1f}%)")
        
        # Check for URL duplicates
        url_fields = ['property_url', 'permalink', 'listing_id', 'mls_id']
        url_usage = {f: field_usage[f] for f in url_fields if f in field_usage}
        if url_usage:
            logger.info("  URL/ID fields:")
            for field, count in sorted(url_usage.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"    {field}: {count}/{total_properties} ({count/total_properties*100:.1f}%)")
        
        # Recommendations
        logger.info("\n" + "="*80)
        logger.info("OPTIMIZATION RECOMMENDATIONS")
        logger.info("="*80)
        
        # Check alt_photos
        if 'alt_photos' in field_usage and field_usage['alt_photos'] > 0:
            avg_photos_size = sum(field_sizes['alt_photos']) / len(field_sizes['alt_photos']) if field_sizes['alt_photos'] else 0
            if avg_photos_size > 1000:  # More than 1KB
                logger.info(f"\n1. alt_photos: Average {avg_photos_size/1024:.2f} KB per property")
                logger.info("   → Consider: Store only primary_photo, fetch alt_photos on-demand from source")
        
        # Check description text
        if 'description.text' in field_usage:
            desc_text_size = sum(field_sizes['description.text']) / len(field_sizes['description.text']) if field_sizes['description.text'] else 0
            if desc_text_size > 2000:  # More than 2KB
                logger.info(f"\n2. description.text: Average {desc_text_size/1024:.2f} KB per property")
                logger.info("   → Consider: Truncate to first 500 characters, store full in separate collection if needed")
        
        # Check large arrays
        large_array_fields = ['monthly_fees', 'one_time_fees', 'tax_history', 'nearby_schools', 'alt_photos']
        for field in large_array_fields:
            if field in field_usage and field_usage[field] > 0:
                avg_size = sum(field_sizes[field]) / len(field_sizes[field]) if field_sizes[field] else 0
                if avg_size > 500:  # More than 500 bytes
                    logger.info(f"\n3. {field}: Average {avg_size:.0f} bytes per property")
                    logger.info(f"   → Consider: Remove if not used, or move to separate collection")
        
        # Check duplicate address fields
        if 'address.formatted_address' in field_usage and 'address.full_street_line' in field_usage:
            if field_usage['address.formatted_address'] > 0.8 * total_properties and field_usage['address.full_street_line'] > 0.8 * total_properties:
                logger.info(f"\n4. Address fields: Both formatted_address and full_street_line present")
                logger.info("   → Consider: Keep only formatted_address, derive full_street_line if needed")
        
        logger.info("\n" + "="*80)
        
        # Calculate potential savings
        logger.info("\nPOTENTIAL STORAGE SAVINGS")
        logger.info("="*80)
        
        potential_savings = 0
        
        # If we remove alt_photos (keep only primary_photo)
        if 'alt_photos' in field_usage:
            avg_photos_size = sum(field_sizes['alt_photos']) / len(field_sizes['alt_photos']) if field_sizes['alt_photos'] else 0
            potential_savings += avg_photos_size * total_properties
        
        # If we truncate description.text to 500 chars
        if 'description.text' in field_usage:
            desc_text_size = sum(field_sizes['description.text']) / len(field_sizes['description.text']) if field_sizes['description.text'] else 0
            if desc_text_size > 500:
                potential_savings += (desc_text_size - 500) * total_properties
        
        # If we remove rarely used large fields
        for field in ['monthly_fees', 'one_time_fees', 'tax_history', 'nearby_schools']:
            if field in field_usage and field_usage[field] < 0.1 * total_properties:
                avg_size = sum(field_sizes[field]) / len(field_sizes[field]) if field_sizes[field] else 0
                potential_savings += avg_size * field_usage[field]
        
        if potential_savings > 0:
            potential_savings_mb = (potential_savings / (1024 * 1024)) * (await db.properties.count_documents({}) / sample_size)
            logger.info(f"\nEstimated potential savings: {potential_savings_mb:.2f} MB")
            logger.info("(Based on sample, extrapolated to full collection)")
        
        return {
            'total_properties': total_properties,
            'avg_doc_size': avg_doc_size,
            'avg_enrichment_size': avg_enrichment_size,
            'potential_savings_mb': potential_savings_mb if potential_savings > 0 else 0
        }
        
    except Exception as e:
        logger.error(f"Error analyzing property storage: {e}")
        raise
    finally:
        client.close()


def analyze_field(obj, prefix, field_usage, field_sizes, field_null_count):
    """Recursively analyze all fields in a document"""
    if isinstance(obj, dict):
        for key, value in obj.items():
            field_name = f"{prefix}.{key}" if prefix else key
            
            # Count usage
            field_usage[field_name] += 1
            
            # Count null/empty
            if value is None or value == "" or (isinstance(value, list) and len(value) == 0) or (isinstance(value, dict) and len(value) == 0):
                field_null_count[field_name] += 1
            
            # Calculate size
            try:
                field_json = json.dumps(value, default=str)
                field_size = len(field_json.encode('utf-8'))
                field_sizes[field_name].append(field_size)
            except:
                pass
            
            # Recurse
            if isinstance(value, dict):
                analyze_field(value, field_name, field_usage, field_sizes, field_null_count)
            elif isinstance(value, list):
                # For lists, analyze first few items only
                for i, item in enumerate(value[:5]):  # Limit to first 5 items
                    analyze_field(item, f"{field_name}[{i}]", field_usage, field_sizes, field_null_count)
    
    elif isinstance(obj, list):
        for i, item in enumerate(obj[:5]):  # Limit to first 5 items
            analyze_field(item, f"{prefix}[{i}]", field_usage, field_sizes, field_null_count)


async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Analyze property storage for optimization opportunities")
    parser.add_argument("--sample", type=int, default=100, help="Number of properties to sample (default: 100)")
    
    args = parser.parse_args()
    
    try:
        result = await analyze_property_storage(args.sample)
        logger.info("\n✓ Analysis complete!")
        logger.info(f"\nEstimated potential savings: {result.get('potential_savings_mb', 0):.2f} MB")
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'client' in locals():
            client.close()


if __name__ == "__main__":
    asyncio.run(main())

