"""
Test that price changes are ignored when status/listing_type changes.
"""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from services.property_differ import PropertyDiffer

# Create PropertyDiffer
differ = PropertyDiffer()

print("="*80)
print("TESTING PRICE CHANGE DETECTION WITH STATUS/LISTING_TYPE FILTERING")
print("="*80)

# Test Case 1: Price change with status/listing_type change (should be ignored)
print("\n[TEST 1] Price change from for_rent ($1,500) to for_sale ($250,000)")
print("-" * 80)

old_property_1 = {
    "property_id": "123",
    "status": "FOR_RENT",
    "listing_type": "for_rent",
    "financial": {
        "list_price": 1500
    }
}

new_property_1 = {
    "property_id": "123",
    "status": "FOR_SALE",
    "listing_type": "for_sale",
    "financial": {
        "list_price": 250000
    }
}

changes_1 = differ.detect_changes(old_property_1, new_property_1)
price_summary_1 = differ.get_price_change_summary(changes_1, old_property_1, new_property_1)

print(f"Changes detected: {changes_1['has_changes']}")
print(f"Price changes count: {changes_1['summary']['price_changes_count']}")
print(f"Status changes count: {changes_1['summary']['status_changes_count']}")
print(f"Price summary returned: {price_summary_1 is not None}")

if price_summary_1 is None:
    print("[OK] Price change correctly ignored (status/listing_type changed)")
else:
    print("[ERROR] Price change should have been ignored!")

# Test Case 2: Price change with same status (should be recorded)
print("\n[TEST 2] Price reduction from $99,000 to $89,000 (for_sale -> for_sale)")
print("-" * 80)

old_property_2 = {
    "property_id": "456",
    "status": "FOR_SALE",
    "listing_type": "for_sale",
    "financial": {
        "list_price": 99000
    }
}

new_property_2 = {
    "property_id": "456",
    "status": "FOR_SALE",
    "listing_type": "for_sale",
    "financial": {
        "list_price": 89000
    }
}

changes_2 = differ.detect_changes(old_property_2, new_property_2)
price_summary_2 = differ.get_price_change_summary(changes_2, old_property_2, new_property_2)

print(f"Changes detected: {changes_2['has_changes']}")
print(f"Price changes count: {changes_2['summary']['price_changes_count']}")
print(f"Price summary returned: {price_summary_2 is not None}")

if price_summary_2:
    print(f"[OK] Price change correctly detected:")
    print(f"     Old: ${price_summary_2['old_price']:,}")
    print(f"     New: ${price_summary_2['new_price']:,}")
    print(f"     Change: {price_summary_2['percent_change']:.2f}%")
else:
    print("[ERROR] Price change should have been detected!")

# Test Case 3: Status change only (no price change recorded, but status change is)
print("\n[TEST 3] Status change from for_sale to SOLD (same price)")
print("-" * 80)

old_property_3 = {
    "property_id": "789",
    "status": "FOR_SALE",
    "listing_type": "for_sale",
    "financial": {
        "list_price": 100000
    }
}

new_property_3 = {
    "property_id": "789",
    "status": "SOLD",
    "listing_type": "sold",
    "financial": {
        "list_price": 100000
    }
}

changes_3 = differ.detect_changes(old_property_3, new_property_3)
price_summary_3 = differ.get_price_change_summary(changes_3, old_property_3, new_property_3)
status_summary_3 = differ.get_status_change_summary(changes_3)

print(f"Changes detected: {changes_3['has_changes']}")
print(f"Price changes count: {changes_3['summary']['price_changes_count']}")
print(f"Status changes count: {changes_3['summary']['status_changes_count']}")
print(f"Price summary returned: {price_summary_3 is not None}")
print(f"Status summary returned: {status_summary_3 is not None}")

if price_summary_3 is None:
    print("[OK] No price change recorded (status changed)")
else:
    print("[ERROR] Price change should not be recorded when status changes!")

if status_summary_3:
    print(f"[OK] Status change correctly detected: {status_summary_3['old_status']} -> {status_summary_3['new_status']}")

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
all_passed = (
    price_summary_1 is None and
    price_summary_2 is not None and
    price_summary_3 is None and
    status_summary_3 is not None
)

if all_passed:
    print("[SUCCESS] All tests passed! Price changes are correctly filtered by status.")
else:
    print("[ERROR] Some tests failed. Please review the output above.")

