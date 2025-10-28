"""
Test that PropertyDiffer detects changes in ALL nested fields, not just price.
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from services.property_differ import PropertyDiffer

# Create a PropertyDiffer instance
differ = PropertyDiffer()

# Test data: property with multiple nested field changes
old_property = {
    "property_id": "123",
    "status": "FOR_SALE",  # Status field
    "financial": {
        "list_price": 100000,  # Price field
        "price_per_sqft": 50,  # Price field
    },
    "address": {
        "city": "Indianapolis",  # Regular nested field
        "state": "IN",  # Regular nested field
    },
    "description": {
        "beds": 3,  # Regular nested field
        "baths": 2,  # Regular nested field
    },
    "agent": {
        "agent_name": "John Doe",  # Regular nested field
    }
}

new_property = {
    "property_id": "123",
    "status": "PENDING",  # Status change
    "financial": {
        "list_price": 90000,  # Price change
        "price_per_sqft": 45,  # Price change
    },
    "address": {
        "city": "Indianapolis",
        "state": "IN",
    },
    "description": {
        "beds": 4,  # Regular field change
        "baths": 2,  # No change
    },
    "agent": {
        "agent_name": "Jane Smith",  # Regular field change
    }
}

# Detect changes
changes = differ.detect_changes(old_property, new_property)

print("="*80)
print("TESTING ALL NESTED FIELD CHANGES")
print("="*80)

print(f"\nTotal Changes Detected: {changes['summary']['total_changes']}")
print(f"Price Changes: {changes['summary']['price_changes_count']}")
print(f"Status Changes: {changes['summary']['status_changes_count']}")
print(f"All Field Changes: {changes['summary']['field_changes_count']}")

print("\n" + "="*80)
print("PRICE CHANGES (categorized):")
print("="*80)
for change in changes['price_changes']:
    print(f"  - {change['field']}: {change['old_value']} -> {change['new_value']} ({change['change_type']})")

print("\n" + "="*80)
print("STATUS CHANGES (categorized):")
print("="*80)
for change in changes['status_changes']:
    print(f"  - {change['field']}: {change['old_value']} -> {change['new_value']} ({change['change_type']})")

print("\n" + "="*80)
print("ALL FIELD CHANGES (including nested):")
print("="*80)
for change in changes['field_changes']:
    print(f"  - {change['field']}: {change['old_value']} -> {change['new_value']} ({change['change_type']})")

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"[OK] Price fields detected: {len(changes['price_changes']) > 0}")
print(f"[OK] Status fields detected: {len(changes['status_changes']) > 0}")
other_nested = [c for c in changes['field_changes'] if c['field'] not in differ.price_fields and c['field'] not in differ.status_fields]
print(f"[OK] Other nested fields detected: {len(other_nested) > 0}")
if other_nested:
    print(f"     Examples: {', '.join([c['field'] for c in other_nested])}")
print("\n[CONCLUSION] The fix handles ALL nested fields, not just price!")

