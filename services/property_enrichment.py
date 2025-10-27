"""
Property Enrichment Pipeline Service

Main orchestrator that coordinates all enrichment services to analyze
property data and generate structured insights, history tracking, and
motivated seller scoring.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
import asyncio
from motor.motor_asyncio import AsyncIOMotorDatabase

from .property_differ import PropertyDiffer
from .history_tracker import HistoryTracker
from .text_analyzer import TextAnalyzer
from .motivated_seller_scorer import MotivatedSellerScorer

logger = logging.getLogger(__name__)

class PropertyEnrichmentPipeline:
    """Main enrichment pipeline orchestrator"""
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.property_differ = PropertyDiffer()
        self.history_tracker = HistoryTracker(db)
        self.text_analyzer = TextAnalyzer()
        self.motivated_seller_scorer = MotivatedSellerScorer()
        self._initialized = False
    
    async def initialize(self):
        """Initialize the enrichment pipeline"""
        if self._initialized:
            return
        
        try:
            await self.history_tracker.initialize()
            self._initialized = True
            logger.info("Property enrichment pipeline initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing enrichment pipeline: {e}")
            raise
    
    async def enrich_property(self, property_id: str, property_dict: Dict[str, Any], existing_property: Optional[Dict[str, Any]] = None, job_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Main enrichment method - analyzes property and generates enrichment data
        
        Args:
            property_id: Property identifier
            property_dict: Current property data
            existing_property: Previous property data (None for new properties)
            job_id: Scraping job ID for tracking
            
        Returns:
            Dictionary containing all enrichment data
        """
        try:
            if not self._initialized:
                await self.initialize()
            
            logger.info(f"Starting enrichment for property {property_id}")
            start_time = datetime.utcnow()
            
            # Step 1: Detect changes
            changes = self.property_differ.detect_changes(existing_property, property_dict)
            
            # Step 2: Record history and change logs
            await self._record_history_and_changes(property_id, changes, job_id)
            
            # Step 3: Analyze property description
            description = self._extract_description(property_dict)
            text_analysis = self.text_analyzer.analyze_property_description(description)
            
            # Step 4: Get history data for motivated seller scoring
            history_data = await self._get_history_data(property_id)
            
            # Step 5: Calculate motivated seller score
            motivated_seller = self.motivated_seller_scorer.calculate_motivated_score(
                property_dict, text_analysis, history_data
            )
            
            # Step 6: Assemble enrichment data
            enrichment_data = self._assemble_enrichment_data(
                changes, text_analysis, motivated_seller, property_dict
            )
            
            # Step 7: Update property with enrichment data
            await self._update_property_enrichment(property_id, enrichment_data)
            
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"Enrichment completed for property {property_id} in {processing_time:.2f}s")
            
            return enrichment_data
            
        except Exception as e:
            logger.error(f"Error enriching property {property_id}: {e}")
            return self._create_error_enrichment(str(e))
    
    async def _record_history_and_changes(self, property_id: str, changes: Dict[str, Any], job_id: Optional[str]):
        """Record history and change logs"""
        try:
            # Record price changes
            price_summary = self.property_differ.get_price_change_summary(changes)
            if price_summary:
                await self.history_tracker.record_price_change(property_id, price_summary, job_id)
            
            # Record status changes
            status_summary = self.property_differ.get_status_change_summary(changes)
            if status_summary:
                await self.history_tracker.record_status_change(property_id, status_summary, job_id)
            
            # Record detailed change logs
            if changes.get('field_changes'):
                await self.history_tracker.record_change_logs(property_id, changes['field_changes'], job_id)
                
        except Exception as e:
            logger.error(f"Error recording history for property {property_id}: {e}")
    
    async def _get_history_data(self, property_id: str) -> Dict[str, Any]:
        """Get history data for motivated seller scoring"""
        try:
            price_history = await self.history_tracker.get_price_history(property_id, limit=10)
            status_history = await self.history_tracker.get_status_history(property_id, limit=10)
            recent_changes = await self.history_tracker.get_recent_changes(property_id, days=30)
            
            return {
                'price_history': price_history,
                'status_history': status_history,
                'recent_changes': recent_changes
            }
        except Exception as e:
            logger.error(f"Error getting history data for property {property_id}: {e}")
            return {}
    
    def _extract_description(self, property_dict: Dict[str, Any]) -> str:
        """Extract description text from property data"""
        # Try multiple possible description fields
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
    
    def _assemble_enrichment_data(self, changes: Dict[str, Any], text_analysis: Dict[str, Any], motivated_seller: Dict[str, Any], property_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Assemble all enrichment data into final structure"""
        return {
            'motivated_seller': motivated_seller,
            'big_ticket_items': text_analysis.get('big_ticket_items', {}),
            'distress_signals': {
                'signals_found': text_analysis.get('distress_signals', []),
                'motivated_keywords': text_analysis.get('motivated_keywords', [])
            },
            'special_sale_types': {
                'is_auction': 'auction' in text_analysis.get('special_sale_types', []),
                'is_reo': 'reo' in text_analysis.get('special_sale_types', []),
                'is_probate': 'probate' in text_analysis.get('special_sale_types', []),
                'is_short_sale': 'short_sale' in text_analysis.get('special_sale_types', []),
                'is_as_is': 'as_is' in text_analysis.get('special_sale_types', []),
                'types_found': text_analysis.get('special_sale_types', [])
            },
            'text_analysis': {
                'keywords_found': text_analysis.get('keywords_found', []),
                'total_keywords': text_analysis.get('summary', {}).get('total_keywords', 0),
                'big_ticket_items_count': text_analysis.get('summary', {}).get('big_ticket_items_count', 0)
            },
            'change_summary': {
                'has_changes': changes.get('has_changes', False),
                'total_changes': changes.get('summary', {}).get('total_changes', 0),
                'price_changes_count': changes.get('summary', {}).get('price_changes_count', 0),
                'status_changes_count': changes.get('summary', {}).get('status_changes_count', 0),
                'is_new_property': changes.get('is_new_property', False)
            },
            'quick_access_flags': {
                'is_motivated_seller': motivated_seller.get('score', 0) >= 40,
                'has_price_reduction': changes.get('summary', {}).get('price_changes_count', 0) > 0,
                'has_distress_signals': text_analysis.get('summary', {}).get('has_distress_signals', False)
            },
            'enriched_at': datetime.utcnow(),
            'enrichment_version': '1.0'
        }
    
    async def _update_property_enrichment(self, property_id: str, enrichment_data: Dict[str, Any]):
        """Update property document with enrichment data"""
        try:
            # Update the property document with enrichment data
            await self.db.properties.update_one(
                {"property_id": property_id},
                {
                    "$set": {
                        "enrichment": enrichment_data,
                        "is_motivated_seller": enrichment_data['quick_access_flags']['is_motivated_seller'],
                        "has_price_reduction": enrichment_data['quick_access_flags']['has_price_reduction'],
                        "has_distress_signals": enrichment_data['quick_access_flags']['has_distress_signals'],
                        "last_enriched_at": datetime.utcnow()
                    }
                }
            )
            logger.info(f"Updated property {property_id} with enrichment data")
        except Exception as e:
            logger.error(f"Error updating property {property_id} with enrichment data: {e}")
    
    def _create_error_enrichment(self, error_message: str) -> Dict[str, Any]:
        """Create error enrichment data"""
        return {
            'motivated_seller': {
                'score': 0,
                'confidence': 'low',
                'reasoning': [f'Error: {error_message}'],
                'components': {},
                'calculated_at': datetime.utcnow()
            },
            'big_ticket_items': {},
            'distress_signals': {
                'signals_found': [],
                'motivated_keywords': []
            },
            'special_sale_types': {
                'is_auction': False,
                'is_reo': False,
                'is_probate': False,
                'is_short_sale': False,
                'is_as_is': False,
                'types_found': []
            },
            'text_analysis': {
                'keywords_found': [],
                'total_keywords': 0,
                'big_ticket_items_count': 0
            },
            'change_summary': {
                'has_changes': False,
                'total_changes': 0,
                'price_changes_count': 0,
                'status_changes_count': 0,
                'is_new_property': False
            },
            'quick_access_flags': {
                'is_motivated_seller': False,
                'has_price_reduction': False,
                'has_distress_signals': False
            },
            'enriched_at': datetime.utcnow(),
            'enrichment_version': '1.0',
            'error': error_message
        }
    
    async def get_property_enrichment(self, property_id: str) -> Optional[Dict[str, Any]]:
        """Get enrichment data for a property"""
        try:
            property_doc = await self.db.properties.find_one(
                {"property_id": property_id},
                {"enrichment": 1, "is_motivated_seller": 1, "has_price_reduction": 1, "has_distress_signals": 1}
            )
            
            if property_doc and 'enrichment' in property_doc:
                return property_doc['enrichment']
            
            return None
        except Exception as e:
            logger.error(f"Error getting enrichment for property {property_id}: {e}")
            return None
    
    async def get_property_history(self, property_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get property history"""
        return await self.history_tracker.get_property_history(property_id, limit)
    
    async def get_property_change_logs(self, property_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get property change logs"""
        return await self.history_tracker.get_property_change_logs(property_id, limit)
    
    async def get_motivated_sellers(self, min_score: int = 40, limit: int = 100) -> List[Dict[str, Any]]:
        """Get properties with motivated seller scores above threshold"""
        try:
            cursor = self.db.properties.find(
                {
                    "is_motivated_seller": True,
                    "enrichment.motivated_seller.score": {"$gte": min_score}
                },
                {
                    "property_id": 1,
                    "mls_id": 1,
                    "status": 1,
                    "address.formatted_address": 1,
                    "financial.list_price": 1,
                    "enrichment.motivated_seller": 1,
                    "enrichment.quick_access_flags": 1,
                    "days_on_mls": 1
                }
            ).sort("enrichment.motivated_seller.score", -1).limit(limit)
            
            motivated_sellers = []
            async for doc in cursor:
                doc["_id"] = str(doc["_id"])
                motivated_sellers.append(doc)
            
            return motivated_sellers
        except Exception as e:
            logger.error(f"Error getting motivated sellers: {e}")
            return []