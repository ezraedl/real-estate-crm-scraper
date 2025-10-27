"""
Property Differ Service

Detects changes between two property dictionaries and categorizes them
for history tracking and change logging.
"""

from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class PropertyDiffer:
    """Detects and categorizes changes between property versions"""
    
    def __init__(self):
        # Fields that are considered "price changes"
        self.price_fields = {
            'financial.list_price',
            'financial.original_list_price',
            'financial.price_per_sqft',
            'financial.monthly_fees',
            'financial.one_time_fees'
        }
        
        # Fields that are considered "status changes"
        self.status_fields = {
            'status',
            'mls_status',
            'listing_type'
        }
        
        # Fields to ignore in change detection (metadata, timestamps, etc.)
        self.ignore_fields = {
            '_id',
            'scraped_at',
            'last_content_updated',
            'content_hash',
            'job_id',
            'source',
            'days_on_mls'  # This is calculated, not a real change
        }
    
    def detect_changes(self, old_property: Optional[Dict[str, Any]], new_property: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect all changes between old and new property data
        
        Args:
            old_property: Previous property data (None for new properties)
            new_property: Current property data
            
        Returns:
            Dictionary containing categorized changes
        """
        if old_property is None:
            return self._detect_new_property_changes(new_property)
        
        changes = {
            'has_changes': False,
            'price_changes': [],
            'status_changes': [],
            'field_changes': [],
            'summary': {
                'total_changes': 0,
                'price_changes_count': 0,
                'status_changes_count': 0,
                'field_changes_count': 0
            }
        }
        
        # Compare all fields
        all_fields = set(old_property.keys()) | set(new_property.keys())
        
        for field in all_fields:
            if field in self.ignore_fields:
                continue
                
            old_value = old_property.get(field)
            new_value = new_property.get(field)
            
            # Skip if values are the same
            if old_value == new_value:
                continue
            
            change_entry = {
                'field': field,
                'old_value': old_value,
                'new_value': new_value,
                'timestamp': datetime.utcnow(),
                'change_type': self._categorize_change(field, old_value, new_value)
            }
            
            changes['has_changes'] = True
            changes['field_changes'].append(change_entry)
            
            # Categorize specific change types
            if field in self.price_fields:
                changes['price_changes'].append(change_entry)
            elif field in self.status_fields:
                changes['status_changes'].append(change_entry)
        
        # Update summary counts
        changes['summary']['total_changes'] = len(changes['field_changes'])
        changes['summary']['price_changes_count'] = len(changes['price_changes'])
        changes['summary']['status_changes_count'] = len(changes['status_changes'])
        changes['summary']['field_changes_count'] = len(changes['field_changes'])
        
        return changes
    
    def _detect_new_property_changes(self, new_property: Dict[str, Any]) -> Dict[str, Any]:
        """Handle case where property is completely new"""
        return {
            'has_changes': True,
            'price_changes': [],
            'status_changes': [],
            'field_changes': [],
            'summary': {
                'total_changes': 0,
                'price_changes_count': 0,
                'status_changes_count': 0,
                'field_changes_count': 0
            },
            'is_new_property': True
        }
    
    def _categorize_change(self, field: str, old_value: Any, new_value: Any) -> str:
        """Categorize the type of change"""
        if old_value is None and new_value is not None:
            return 'added'
        elif old_value is not None and new_value is None:
            return 'removed'
        elif isinstance(old_value, (int, float)) and isinstance(new_value, (int, float)):
            if new_value > old_value:
                return 'increased'
            else:
                return 'decreased'
        else:
            return 'modified'
    
    def get_price_change_summary(self, changes: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract price change summary from detected changes"""
        if not changes.get('price_changes'):
            return None
        
        price_changes = changes['price_changes']
        
        # Find the main list price change
        list_price_change = None
        for change in price_changes:
            if change['field'] == 'financial.list_price':
                list_price_change = change
                break
        
        if not list_price_change:
            return None
        
        old_price = list_price_change['old_value']
        new_price = list_price_change['new_value']
        
        if old_price is None or new_price is None:
            return None
        
        price_diff = new_price - old_price
        percent_change = (price_diff / old_price) * 100 if old_price > 0 else 0
        
        return {
            'old_price': old_price,
            'new_price': new_price,
            'price_difference': price_diff,
            'percent_change': round(percent_change, 2),
            'change_type': 'reduction' if price_diff < 0 else 'increase',
            'timestamp': list_price_change['timestamp']
        }
    
    def get_status_change_summary(self, changes: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract status change summary from detected changes"""
        if not changes.get('status_changes'):
            return None
        
        status_changes = changes['status_changes']
        
        # Find the main status change
        status_change = None
        for change in status_changes:
            if change['field'] == 'status':
                status_change = change
                break
        
        if not status_change:
            return None
        
        return {
            'old_status': status_change['old_value'],
            'new_status': status_change['new_value'],
            'timestamp': status_change['timestamp']
        }