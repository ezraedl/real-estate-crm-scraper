"""
Test that price changes are tracked when status changes within the same listing_type.
Example: FOR_SALE -> PENDING -> FOR_SALE (all have listing_type="for_sale")
"""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from services.property_differ import PropertyDiffer

# Create PropertyDiffer
differ = PropertyDiffer()

print("="*80)
print("TESTING PRICE CHANGE WITH STATUS CHANGE (SAME LISTING_TYPE)")
print("="*80)

# Test Case: Price change from FOR_SALE to PENDING (both listing_type="for_sale")
print("\n[TEST 1] Price reduction: FOR_SALE ($99,000) -> PENDING ($89,000)")
print("         Both have listing_type='for_sale' - should be tracked")
print("-" * 80)

old_property = {
    "property_id": "123",
    "status": "FOR_SALE",
    "listing_type": "for_sale",
    "financial": {
        "list_price": 99000
    }
}

new_property = {
    "property_id": "123",
    "status": "PENDING",
    "listing_type": "for_sale",  # Same listing_type!
    "financial": {
        "list_price": 89000
    }
}

changes = differ.detect_changes(old_property, new_property)
price_summary = differ.get_price_change_summary(changes, old_property, new_property)
status_summary = differ.get_status_change_summary(changes)

print(f"Changes detected: {changes['has_changes']}")
print(f"Price changes count: {changes['summary']['price_changes_count']}")
print(f"Status changes count: {changes['summary']['status_changes_count']}")
print(f"Price summary returned: {price_summary is not None}")
print(f"Status summary returned: {status_summary is not None}")

if price_summary:
    print(f"[OK] Price change correctly detected:")
    print(f"     Old: ${price_summary['old_price']:,} ({old_property['status']})")
    print(f"     New: ${price_summary['new_price']:,} ({new_property['status']})")
    print(f"     Change: {price_summary['percent_change']:.2f}%")
    print(f"     Listing type: {old_property['listing_type']} -> {new_property['listing_type']} (same)")
else:
    print("[ERROR] Price change should have been detected (same listing_type)!")

if status_summary:
    print(f"[OK] Status change also detected: {status_summary['old_status']} -> {status_summary['new_status']}")

# Test Case: Price change from PENDING back to FOR_SALE
print("\n[TEST 2] Price change: PENDING ($89,000) -> FOR_SALE ($85,000)")
print("         Both have listing_type='for_sale' - should be tracked")
print("-" * 80)

old_property_2 = {
    "property_id": "123",
    "status": "PENDING",
    "listing_type": "for_sale",
    "financial": {
        "list_price": 89000
    }
}

new_property_2 = {
    "property_id": "123",
    "status": "FOR_SALE",
    "listing_type": "for_sale",  # Same listing_type!
    "financial": {
        "list_price": 85000
    }
}

changes_2 = differ.detect_changes(old_property_2, new_property_2)
price_summary_2 = differ.get_price_change_summary(changes_2, old_property_2, new_property_2)

if price_summary_2:
    print(f"[OK] Price change correctly detected:")
    print(f"     Old: ${price_summary_2['old_price']:,} ({old_property_2['status']})")
    print(f"     New: ${price_summary_2['new_price']:,} ({new_property_2['status']})")
    print(f"     Change: {price_summary_2['percent_change']:.2f}%")
else:
    print("[ERROR] Price change should have been detected (same listing_type)!")

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
all_passed = price_summary is not None and price_summary_2 is not None

if all_passed:
    print("[SUCCESS] Price changes are correctly tracked when status changes")
    print("          within the same listing_type (FOR_SALE <-> PENDING)")
else:
    print("[ERROR] Some tests failed. Please review the output above.")

