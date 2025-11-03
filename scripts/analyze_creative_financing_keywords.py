"""
Script to analyze property descriptions for creative financing keywords.
Queries the database for properties matching existing keywords and extracts
additional keywords from real property descriptions.
"""

import asyncio
import os
import sys
import json
import re
from collections import defaultdict, Counter
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse
from typing import Dict, List, Set, Any

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import settings

# Initial creative financing keywords from ChatGPT
creative_financing_keywords = {
    "owner_finance": ["owner financing", "seller will carry", "contract for deed", "no bank qualifying"],
    "subject_to": ["subject to existing mortgage", "take over payments", "assumable loan"],
    "lease_option": ["rent to own", "lease option", "lease purchase"],
    "wrap_mortgage": ["wrap", "wraparound mortgage"],
    "distressed": ["motivated seller", "must sell", "as is", "foreclosure"],
    "partnership": ["open to JV", "joint venture", "partner wanted"]
}


def extract_description_fields(property_dict: Dict[str, Any]) -> str:
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


def build_keyword_patterns(keywords: List[str]) -> List[str]:
    """Build regex patterns for keywords (case-insensitive, word boundaries)"""
    patterns = []
    for keyword in keywords:
        # Escape special regex characters
        escaped = re.escape(keyword)
        # Create pattern with word boundaries for whole word matching
        pattern = rf'\b{escaped}\b'
        patterns.append(pattern)
    return patterns


def find_matches_in_text(text: str, patterns: List[str]) -> bool:
    """Check if any pattern matches in the text"""
    text_lower = text.lower()
    for pattern in patterns:
        if re.search(pattern, text_lower, re.IGNORECASE):
            return True
    return False


def extract_additional_keywords(text: str, context_words: int = 5) -> Set[str]:
    """
    Extract potential financing-related keywords from text.
    Looks for phrases near known financing keywords.
    """
    additional_keywords = set()
    text_lower = text.lower()
    
    # Common financing-related patterns
    financing_patterns = [
        r'\b(?:owner|seller)\s+(?:will|can|offers?|provides?)\s+\w+\s+\w+',
        r'\b(?:financing|finance|mortgage|loan|payment|down payment|terms?)\s+(?:available|offered|provided)',
        r'\b(?:assum|take over|assume)\s+(?:loan|mortgage|payments?)',
        r'\b(?:lease|rent)\s+(?:with|to|for)\s+(?:option|purchase|buy)',
        r'\b(?:carry|holding)\s+(?:back|note|paper)',
        r'\b(?:creative|flexible|alternative)\s+(?:financing|terms?|options?)',
        r'\b(?:no|without)\s+(?:bank|qualifying|approval|credit check)',
        r'\b(?:terms|conditions|negotiable|flexible)',
        r'\b(?:joint|partner|JV)\s+(?:venture|partnership|opportunity)',
        r'\b(?:subject\s+to|subject-to|sub2)',
        r'\b(?:contract|land|installment)\s+(?:for\s+deed|deed)',
        r'\b(?:wraparound|wrap-around|wrap)\s+(?:mortgage|loan)',
        r'\b(?:seller|owner)\s+(?:carry|financing|assist)',
        r'\b(?:investor|wholesale|deal)\s+(?:friendly|welcome|opportunity)',
        r'\b(?:terms?|financing)\s+(?:available|negotiable|flexible)',
    ]
    
    for pattern in financing_patterns:
        matches = re.findall(pattern, text_lower, re.IGNORECASE)
        for match in matches:
            # Clean up the match
            cleaned = match.strip()
            if len(cleaned) >= 3:  # Minimum length for a meaningful keyword
                additional_keywords.add(cleaned)
    
    return additional_keywords


async def analyze_creative_financing_keywords():
    """Analyze property descriptions for creative financing keywords"""
    try:
        # Connect to MongoDB
        parsed_uri = urlparse(settings.MONGODB_URI)
        database_name = parsed_uri.path.lstrip('/').split('?')[0] if parsed_uri.path else 'mls_scraper'
        
        client = AsyncIOMotorClient(settings.MONGODB_URI)
        db = client[database_name]
        
        print("=" * 80)
        print("ANALYZING CREATIVE FINANCING KEYWORDS")
        print("=" * 80)
        print("")
        
        # Results storage
        category_results = {}
        all_additional_keywords = defaultdict(list)
        
        # Process each category
        for category, keywords in creative_financing_keywords.items():
            print(f"Processing category: {category}")
            print(f"  Keywords: {', '.join(keywords)}")
            print("")
            
            # Build regex patterns for this category
            patterns = build_keyword_patterns(keywords)
            
            # Query for properties matching these keywords
            # We'll search in description fields
            query = {
                "$or": [
                    {"description.text": {"$regex": pattern, "$options": "i"}} for pattern in patterns
                ] + [
                    {"description.full_description": {"$regex": pattern, "$options": "i"}} for pattern in patterns
                ] + [
                    {"description.summary": {"$regex": pattern, "$options": "i"}} for pattern in patterns
                ] + [
                    {"remarks": {"$regex": pattern, "$options": "i"}} for pattern in patterns
                ] + [
                    {"public_remarks": {"$regex": pattern, "$options": "i"}} for pattern in patterns
                ]
            }
            
            # Get properties matching this category
            cursor = db['properties'].find(
                query,
                {
                    "property_id": 1,
                    "mls_id": 1,
                    "address": 1,
                    "description": 1,
                    "remarks": 1,
                    "public_remarks": 1
                }
            ).limit(50)  # Limit to 50 properties per category for analysis
            
            properties = await cursor.to_list(length=50)
            
            print(f"  Found {len(properties)} properties matching this category")
            
            # Extract descriptions and analyze
            descriptions = []
            for prop in properties:
                desc = extract_description_fields(prop)
                if desc:
                    descriptions.append(desc)
            
            print(f"  Found {len(descriptions)} properties with descriptions")
            
            # Analyze descriptions for additional keywords
            category_additional_keywords = set()
            for desc in descriptions:
                additional = extract_additional_keywords(desc)
                category_additional_keywords.update(additional)
                # Store individual examples
                for kw in additional:
                    if kw not in all_additional_keywords:
                        all_additional_keywords[kw] = []
                    # Store a snippet containing the keyword
                    snippet = desc[:200] if len(desc) > 200 else desc
                    all_additional_keywords[kw].append(snippet)
            
            # Store results
            category_results[category] = {
                "matched_properties": len(properties),
                "descriptions_found": len(descriptions),
                "additional_keywords": sorted(list(category_additional_keywords)),
                "sample_descriptions": descriptions[:5]  # Store first 5 for review
            }
            
            print(f"  Found {len(category_additional_keywords)} additional potential keywords")
            print("")
        
        # Print summary
        print("=" * 80)
        print("SUMMARY BY CATEGORY")
        print("=" * 80)
        print("")
        
        for category, results in category_results.items():
            print(f"{category.upper()}:")
            print(f"  Matched Properties: {results['matched_properties']}")
            print(f"  Descriptions Found: {results['descriptions_found']}")
            print(f"  Additional Keywords Found: {len(results['additional_keywords'])}")
            if results['additional_keywords']:
                print(f"  Additional Keywords: {', '.join(results['additional_keywords'][:10])}")
                if len(results['additional_keywords']) > 10:
                    print(f"    ... and {len(results['additional_keywords']) - 10} more")
            print("")
        
        # Analyze all additional keywords and find most common
        print("=" * 80)
        print("MOST COMMON ADDITIONAL KEYWORDS (across all categories)")
        print("=" * 80)
        print("")
        
        keyword_counts = Counter()
        for kw, examples in all_additional_keywords.items():
            keyword_counts[kw] = len(examples)
        
        top_keywords = keyword_counts.most_common(20)
        for kw, count in top_keywords:
            print(f"  '{kw}': found in {count} properties")
        
        print("")
        print("=" * 80)
        print("EXPANDED KEYWORDS DICTIONARY")
        print("=" * 80)
        print("")
        
        # Create expanded dictionary
        expanded_keywords = {}
        for category, keywords in creative_financing_keywords.items():
            expanded_keywords[category] = keywords.copy()
            # Add additional keywords from this category (if they seem relevant)
            additional = category_results[category]['additional_keywords']
            # Filter additional keywords - only add if they appear in multiple properties
            for kw in additional:
                if kw not in expanded_keywords[category]:
                    # Check if it's relevant (appears in at least 2 examples for this category)
                    if len([ex for ex in all_additional_keywords.get(kw, [])]) >= 2:
                        expanded_keywords[category].append(kw)
        
        # Print expanded dictionary
        print("creative_financing_keywords = {")
        for category, keywords in expanded_keywords.items():
            keywords_str = json.dumps(keywords, indent=12)
            print(f'    "{category}": {keywords_str},')
        print("}")
        print("")
        
        # Save detailed results to file
        output_file = os.path.join(project_root, "scripts", "creative_financing_analysis.json")
        output_data = {
            "analysis_date": str(asyncio.get_event_loop().time()),
            "original_keywords": creative_financing_keywords,
            "expanded_keywords": expanded_keywords,
            "category_results": {
                cat: {
                    "matched_properties": res["matched_properties"],
                    "descriptions_found": res["descriptions_found"],
                    "additional_keywords": res["additional_keywords"],
                    "sample_descriptions": res["sample_descriptions"][:3]  # Only save first 3
                }
                for cat, res in category_results.items()
            },
            "all_additional_keywords": dict(all_additional_keywords)
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"Detailed analysis saved to: {output_file}")
        print("")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(analyze_creative_financing_keywords())

