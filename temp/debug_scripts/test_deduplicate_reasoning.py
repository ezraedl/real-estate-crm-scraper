"""
Test that motivated seller reasoning doesn't have duplicates.
"""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from services.motivated_seller_scorer import MotivatedSellerScorer

# Create scorer
scorer = MotivatedSellerScorer()

# Simulate price history with duplicate entries (same percentage)
history_data = {
    'price_history': [
        {
            'change_type': 'price_change',
            'data': {
                'old_price': 99000,
                'new_price': 89000,
                'percent_change': -10.10
            },
            'timestamp': datetime.utcnow()
        },
        {
            'change_type': 'price_change',
            'data': {
                'old_price': 99000,
                'new_price': 89000,
                'percent_change': -10.10  # Same percentage
            },
            'timestamp': datetime.utcnow()
        },
        {
            'change_type': 'price_change',
            'data': {
                'old_price': 99000,
                'new_price': 89000,
                'percent_change': -10.10  # Same percentage
            },
            'timestamp': datetime.utcnow()
        }
    ]
}

property_data = {
    'days_on_mls': 103,
    'scraped_at': datetime.utcnow()
}

analysis_data = {
    'distress_signals': [],
    'motivated_keywords': [],
    'special_sale_types': []
}

print("="*80)
print("TESTING DEDUPLICATION OF REASONING")
print("="*80)

# Calculate score
result = scorer.calculate_motivated_score(property_data, analysis_data, history_data)

print(f"\nScore: {result['score']}")
print(f"\nReasoning ({len(result['reasoning'])} items):")
for i, reason in enumerate(result['reasoning'], 1):
    print(f"  {i}. {reason}")

print(f"\nPrice Reduction Details ({len(result['components']['price_reductions']['details'])} items):")
for i, detail in enumerate(result['components']['price_reductions']['details'], 1):
    print(f"  {i}. {detail}")

# Check for duplicates
reasoning_duplicates = len(result['reasoning']) != len(set(result['reasoning']))
details_duplicates = len(result['components']['price_reductions']['details']) != len(set(result['components']['price_reductions']['details']))

print("\n" + "="*80)
print("RESULTS")
print("="*80)
print(f"[OK] Reasoning has duplicates: {reasoning_duplicates}")
print(f"[OK] Price reduction details have duplicates: {details_duplicates}")

if not reasoning_duplicates and not details_duplicates:
    print("\n[SUCCESS] No duplicates found!")
else:
    if reasoning_duplicates:
        print("\n[ERROR] Reasoning has duplicates!")
    if details_duplicates:
        print("\n[ERROR] Price reduction details have duplicates!")

