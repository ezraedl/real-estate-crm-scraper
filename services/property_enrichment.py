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
            await self._record_history_and_changes(property_id, changes, existing_property, property_dict, job_id)
            
            # Step 3: Analyze property description
            description = self._extract_description(property_dict)
            text_analysis = self.text_analyzer.analyze_property_description(description)
            
            # Step 4: Get history data for motivated seller scoring
            history_data = await self._get_history_data(property_id)
            
            # Step 5: Calculate motivated seller score (Phase 1 + Phase 2)
            motivated_seller = self.motivated_seller_scorer.calculate_motivated_score(
                property_dict, text_analysis, history_data
            )
            
            # Step 6: Assemble enrichment data
            enrichment_data = self._assemble_enrichment_data(
                changes, text_analysis, motivated_seller, property_dict, history_data
            )
            
            # Step 7: Update property with enrichment data
            await self._update_property_enrichment(property_id, enrichment_data)
            
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"Enrichment completed for property {property_id} in {processing_time:.2f}s")
            
            return enrichment_data
            
        except Exception as e:
            logger.error(f"Error enriching property {property_id}: {e}")
            return self._create_error_enrichment(str(e))
    
    async def _record_history_and_changes(self, property_id: str, changes: Dict[str, Any], old_property: Optional[Dict[str, Any]], new_property: Dict[str, Any], job_id: Optional[str]):
        """Record history and change logs"""
        try:
            # Record price changes (only if status/listing_type didn't change)
            price_summary = self.property_differ.get_price_change_summary(changes, old_property, new_property)
            if price_summary:
                await self.history_tracker.record_price_change(property_id, price_summary, job_id)
            
            # Record status changes
            status_summary = self.property_differ.get_status_change_summary(changes)
            if status_summary:
                await self.history_tracker.record_status_change(property_id, status_summary, job_id)
            
            # Record detailed change logs - ONLY for price, status, and listing_type fields
            # This reduces storage by not tracking every field change (e.g., description text, images, etc.)
            if changes.get('field_changes'):
                # Fields we care about tracking in change logs
                tracked_fields = {
                    # Price fields
                    'financial.list_price',
                    'financial.original_list_price',
                    'financial.price_per_sqft',
                    # Status fields
                    'status',
                    'mls_status',
                    # Listing type
                    'listing_type'
                }
                
                # Filter to only tracked fields
                filtered_changes = [
                    change for change in changes['field_changes']
                    if change.get('field') in tracked_fields
                ]
                
                if filtered_changes:
                    await self.history_tracker.record_change_logs(property_id, filtered_changes, job_id)
                
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
    
    def _parse_timestamp(self, ts: Any) -> Optional[datetime]:
        """Parse timestamp from various formats"""
        if ts is None:
            return None
        if isinstance(ts, datetime):
            # Remove timezone info if present
            if ts.tzinfo:
                return ts.replace(tzinfo=None)
            return ts
        if isinstance(ts, str):
            try:
                # Try ISO format
                ts_clean = ts.replace('Z', '+00:00')
                dt = datetime.fromisoformat(ts_clean)
                if dt.tzinfo:
                    dt = dt.replace(tzinfo=None)
                return dt
            except (ValueError, AttributeError):
                return None
        return None
    
    def _build_price_history_summary(self, price_history: List[Dict[str, Any]], current_price: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """Build summary of price history"""
        if not price_history:
            return None
        
        try:
            # Sort by timestamp ascending (oldest first)
            def get_timestamp(entry):
                ts = entry.get('timestamp') or entry.get('data', {}).get('timestamp')
                parsed = self._parse_timestamp(ts)
                return parsed if parsed else datetime.min
            
            sorted_history = sorted(price_history, key=get_timestamp)
            
            if not sorted_history:
                return None
            
            # Get original price (from first entry's old_price)
            first_entry = sorted_history[0]
            data = first_entry.get('data', {})
            original_price = data.get('old_price') or current_price
            
            # Get current price (from last entry's new_price, or use provided)
            last_entry = sorted_history[-1]
            data = last_entry.get('data', {})
            current = current_price or data.get('new_price')
            
            if original_price is None or current is None:
                return None
            
            # Find all price reductions
            reductions = []
            for entry in sorted_history:
                entry_data = entry.get('data', {})
                if entry_data.get('change_type') == 'reduction':
                    price_diff = abs(entry_data.get('price_difference', 0))
                    if price_diff > 0:
                        ts = entry.get('timestamp') or entry_data.get('timestamp')
                        parsed_ts = self._parse_timestamp(ts)
                        reductions.append({
                            'amount': price_diff,
                            'percent': abs(entry_data.get('percent_change', 0)),
                            'timestamp': parsed_ts
                        })
            
            # Calculate totals
            total_reduction = sum(r['amount'] for r in reductions)
            total_reduction_percent = round((total_reduction / original_price) * 100, 2) if original_price > 0 else 0
            number_of_reductions = len(reductions)
            average_reduction = total_reduction / number_of_reductions if number_of_reductions > 0 else 0
            
            # Calculate days since last reduction
            days_since_last_reduction = None
            if reductions:
                # Filter out None timestamps and find most recent
                valid_reductions = [r for r in reductions if r['timestamp'] is not None]
                if valid_reductions:
                    last_reduction = max(valid_reductions, key=lambda x: x['timestamp'])
                    if last_reduction['timestamp']:
                        days_since_last_reduction = (datetime.utcnow() - last_reduction['timestamp']).days
            
            return {
                'original_price': original_price,
                'current_price': current,
                'total_reductions': total_reduction,
                'total_reduction_percent': total_reduction_percent,
                'number_of_reductions': number_of_reductions,
                'days_since_last_reduction': days_since_last_reduction,
                'average_reduction_amount': round(average_reduction, 2)
            }
        except Exception as e:
            logger.error(f"Error building price history summary: {e}")
            return None
    
    def _build_status_history_summary(self, status_history: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Build summary of status history"""
        if not status_history:
            return None
        
        try:
            # Count times relisted (FOR_SALE after PENDING or OFF_MARKET)
            times_relisted = sum(
                1 for entry in status_history
                if entry.get('data', {}).get('new_status') == 'FOR_SALE' 
                and entry.get('data', {}).get('old_status') in ['PENDING', 'OFF_MARKET']
            )
            
            # Get most recent status change
            most_recent = status_history[0] if status_history else None
            last_status_change = None
            if most_recent:
                ts = most_recent.get('timestamp')
                last_status_change = self._parse_timestamp(ts)
            
            return {
                'times_relisted': times_relisted,
                'last_status_change': last_status_change.isoformat() if last_status_change else None,
                'total_status_changes': len(status_history),
                'time_as_active': None,  # Could be calculated if needed
                'time_as_pending': None  # Could be calculated if needed
            }
        except Exception as e:
            logger.error(f"Error building status history summary: {e}")
            return None
    
    def _assemble_enrichment_data(self, changes: Dict[str, Any], text_analysis: Dict[str, Any], motivated_seller: Dict[str, Any], property_dict: Dict[str, Any], history_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Assemble all enrichment data into final structure"""
        # Build price and status history summaries
        price_history_summary = None
        status_history_summary = None
        
        if history_data:
            price_history = history_data.get('price_history', [])
            current_price = None
            if property_dict:
                financial = property_dict.get('financial', {})
                current_price = financial.get('list_price') or financial.get('original_list_price')
            
            price_history_summary = self._build_price_history_summary(price_history, current_price)
            
            status_history = history_data.get('status_history', [])
            status_history_summary = self._build_status_history_summary(status_history)
        
        # Update has_price_reduction flag based on summary
        has_price_reduction = False
        if price_history_summary and price_history_summary.get('total_reductions', 0) > 0:
            has_price_reduction = True
        elif changes.get('summary', {}).get('price_changes_count', 0) > 0:
            has_price_reduction = True
        
        # motivated_seller already includes findings from v2 scoring system
        enrichment_data = {
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
                'has_price_reduction': has_price_reduction,
                'has_distress_signals': text_analysis.get('summary', {}).get('has_distress_signals', False)
            },
            'enriched_at': datetime.utcnow(),
            'enrichment_version': '1.0'
        }
        
        # Add history summaries if available
        if price_history_summary:
            enrichment_data['price_history_summary'] = price_history_summary
        
        if status_history_summary:
            enrichment_data['status_history_summary'] = status_history_summary
        
        return enrichment_data
    
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
    
    async def recalculate_scores_only(self, property_id: str) -> Dict[str, Any]:
        """
        Recalculate scores from existing findings (Phase 2 only, skip detection)
        
        Args:
            property_id: Property identifier
            
        Returns:
            Updated motivated seller score data
        """
        try:
            # Get existing enrichment data
            property_doc = await self.db.properties.find_one(
                {"property_id": property_id},
                {"enrichment": 1}
            )
            
            if not property_doc or 'enrichment' not in property_doc:
                logger.warning(f"No enrichment found for property {property_id}")
                return None
            
            enrichment = property_doc['enrichment']
            motivated_seller = enrichment.get('motivated_seller', {})
            findings = motivated_seller.get('findings')
            
            if not findings:
                logger.warning(f"No findings found for property {property_id}, running full detection")
                # Fall back to full enrichment
                property_dict = await self._get_property_dict(property_id)
                if not property_dict:
                    return None
                description = self._extract_description(property_dict)
                text_analysis = self.text_analyzer.analyze_property_description(description)
                history_data = await self._get_history_data(property_id)
                motivated_seller = self.motivated_seller_scorer.calculate_motivated_score(
                    property_dict, text_analysis, history_data
                )
            else:
                # Recalculate from existing findings
                motivated_seller = self.motivated_seller_scorer.calculate_score_from_findings(findings)
            
            # Update only the motivated_seller part of enrichment
            await self._update_motivated_seller_score(property_id, motivated_seller)
            
            return motivated_seller
            
        except Exception as e:
            logger.error(f"Error recalculating scores for property {property_id}: {e}")
            return None
    
    async def detect_keywords_only(self, property_id: str) -> Dict[str, Any]:
        """
        Re-detect keywords and recalculate scores (re-run text analysis + recalculation)
        
        Args:
            property_id: Property identifier
            
        Returns:
            Updated motivated seller score data
        """
        try:
            # Get property data
            property_dict = await self._get_property_dict(property_id)
            if not property_dict:
                return None
            
            # Re-run text analysis
            description = self._extract_description(property_dict)
            text_analysis = self.text_analyzer.analyze_property_description(description)
            
            # Get existing history data
            history_data = await self._get_history_data(property_id)
            
            # Get property basic data
            property_basic = {
                'days_on_mls': property_dict.get('days_on_mls', 0),
                'scraped_at': property_dict.get('scraped_at')
            }
            
            # Re-detect signals (Phase 1)
            findings = self.motivated_seller_scorer.detect_signals(
                property_basic, text_analysis, history_data
            )
            
            # Recalculate scores (Phase 2)
            motivated_seller = self.motivated_seller_scorer.calculate_score_from_findings(findings)
            
            # Update enrichment
            await self._update_motivated_seller_score(property_id, motivated_seller)
            
            return motivated_seller
            
        except Exception as e:
            logger.error(f"Error re-detecting keywords for property {property_id}: {e}")
            return None
    
    async def update_dom_only(self, property_id: str) -> Dict[str, Any]:
        """
        Update only days on market finding and recalculate scores
        
        Args:
            property_id: Property identifier
            
        Returns:
            Updated motivated seller score data
        """
        try:
            # Get property data
            property_dict = await self._get_property_dict(property_id)
            if not property_dict:
                return None
            
            # Get existing findings
            property_doc = await self.db.properties.find_one(
                {"property_id": property_id},
                {"enrichment.motivated_seller.findings": 1}
            )
            
            if not property_doc or 'enrichment' not in property_doc:
                return None
            
            findings = property_doc.get('enrichment', {}).get('motivated_seller', {}).get('findings')
            
            if not findings:
                # No findings, run full detection
                description = self._extract_description(property_dict)
                text_analysis = self.text_analyzer.analyze_property_description(description)
                history_data = await self._get_history_data(property_id)
                motivated_seller = self.motivated_seller_scorer.calculate_motivated_score(
                    property_dict, text_analysis, history_data
                )
            else:
                # Update only DOM finding
                property_basic = {
                    'days_on_mls': property_dict.get('days_on_mls', 0)
                }
                dom_finding = self.motivated_seller_scorer._detect_days_on_market(property_basic)
                findings['days_on_market'] = dom_finding
                
                # Recalculate scores
                motivated_seller = self.motivated_seller_scorer.calculate_score_from_findings(findings)
            
            # Update enrichment
            await self._update_motivated_seller_score(property_id, motivated_seller)
            
            return motivated_seller
            
        except Exception as e:
            logger.error(f"Error updating DOM for property {property_id}: {e}")
            return None
    
    async def _get_property_dict(self, property_id: str) -> Optional[Dict[str, Any]]:
        """Get property data as dict"""
        try:
            property_doc = await self.db.properties.find_one(
                {"property_id": property_id}
            )
            if property_doc:
                property_doc["_id"] = str(property_doc["_id"])
            return property_doc
        except Exception as e:
            logger.error(f"Error getting property {property_id}: {e}")
            return None
    
    async def _update_motivated_seller_score(self, property_id: str, motivated_seller: Dict[str, Any]):
        """Update only the motivated_seller part of enrichment"""
        try:
            update_data = {
                "enrichment.motivated_seller": motivated_seller,
                "enrichment.enriched_at": datetime.utcnow(),
                "is_motivated_seller": motivated_seller.get('score', 0) >= 40,
                "last_enriched_at": datetime.utcnow()
            }
            
            await self.db.properties.update_one(
                {"property_id": property_id},
                {"$set": update_data}
            )
            logger.info(f"Updated motivated seller score for property {property_id}")
        except Exception as e:
            logger.error(f"Error updating motivated seller score for property {property_id}: {e}")
    
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