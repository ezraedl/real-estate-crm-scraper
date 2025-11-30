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
            existing_property: DEPRECATED - Previous property data (ignored, will fetch from DB)
            job_id: Scraping job ID for tracking
            
        Returns:
            Dictionary containing all enrichment data
        """
        try:
            if not self._initialized:
                await self.initialize()
            
            logger.info(f"Starting enrichment for property {property_id}")
            start_time = datetime.utcnow()
            
            # Step 1: Get property from DB and extract _old values for comparison
            property_doc = await self._get_property_dict(property_id)
            old_property = self._extract_old_values(property_doc) if property_doc else None
            
            # Step 2: Detect changes (compare current vs _old values)
            changes = self.property_differ.detect_changes(old_property, property_dict)
            
            # Step 3: Record history and change logs
            await self._record_history_and_changes(property_id, changes, old_property, property_dict, job_id)
            
            # Step 4: Analyze property description
            description = self._extract_description(property_dict)
            text_analysis = self.text_analyzer.analyze_property_description(description)
            
            # Step 5: Get history data for motivated seller scoring
            history_data = await self._get_history_data(property_id)
            
            # Step 6: Calculate motivated seller score (Phase 1 + Phase 2)
            motivated_seller = self.motivated_seller_scorer.calculate_motivated_score(
                property_dict, text_analysis, history_data
            )
            
            # Step 7: Assemble enrichment data
            enrichment_data = self._assemble_enrichment_data(
                changes, text_analysis, motivated_seller, property_dict, history_data
            )
            
            # Step 8: Update property with enrichment data
            await self._update_property_enrichment(property_id, enrichment_data)
            
            # Step 9: Move current values to _old after successful enrichment
            await self._move_current_to_old(property_id, property_dict)
            
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"Enrichment completed for property {property_id} in {processing_time:.2f}s")
            
            return enrichment_data
            
        except Exception as e:
            logger.error(f"Error enriching property {property_id}: {e}")
            return self._create_error_enrichment(str(e))
    
    async def _record_history_and_changes(self, property_id: str, changes: Dict[str, Any], old_property: Optional[Dict[str, Any]], new_property: Dict[str, Any], job_id: Optional[str]):
        """Record change logs - ONLY for price, status, and listing_type fields"""
        try:
            # Record detailed change logs - ONLY for price, status, and listing_type fields
            # Note: We no longer use property_history collection - only embedded change_logs
            # This reduces storage by using single source of truth
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
            logger.error(f"Error recording change logs for property {property_id}: {e}")
    
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
        # Calculate has_price_reduction flag from change_logs (dynamic calculation)
        # No longer storing price_history_summary - calculated on-demand from change_logs
        has_price_reduction = False
        if changes.get('summary', {}).get('price_changes_count', 0) > 0:
            # Check if any price change is a reduction
            for change in changes.get('field_changes', []):
                field = change.get('field', '')
                if 'financial.list_price' in field or 'financial.original_list_price' in field:
                    old_val = change.get('old_value')
                    new_val = change.get('new_value')
                    if old_val is not None and new_val is not None and new_val < old_val:
                        has_price_reduction = True
                        break
        
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
                'is_portfolio': 'portfolio' in text_analysis.get('special_sale_types', []),
                'types_found': text_analysis.get('special_sale_types', [])
            },
            'creative_financing': {
                'categories_found': [cat for cat, data in text_analysis.get('creative_financing', {}).items() if data.get('found', False)],
                'details': text_analysis.get('creative_financing', {}),
                'has_creative_financing': text_analysis.get('summary', {}).get('has_creative_financing', False)
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
            # Removed quick_access_flags - using root-level flags only to avoid duplication
            'enriched_at': datetime.utcnow(),
            'enrichment_version': '2.0'  # Version bump - removed price_history_summary and quick_access_flags
        }
        
        return enrichment_data
    
    async def _update_property_enrichment(self, property_id: str, enrichment_data: Dict[str, Any]):
        """Update property document with enrichment data"""
        try:
            # Calculate flags from enrichment data (no longer using quick_access_flags)
            is_motivated_seller = enrichment_data.get('motivated_seller', {}).get('score', 0) >= 40
            has_distress_signals = enrichment_data.get('text_analysis', {}).get('summary', {}).get('has_distress_signals', False) or len(enrichment_data.get('distress_signals', {}).get('signals_found', [])) > 0
            
            # Calculate has_price_reduction from change_logs
            # Check if property has price reductions in change_logs
            # Handles both simple format (single field) and consolidated format (multiple fields same date)
            property_doc = await self.db.properties.find_one(
                {"property_id": property_id},
                {"change_logs": 1}
            )
            has_price_reduction = False
            if property_doc and "change_logs" in property_doc:
                change_logs = property_doc.get("change_logs", [])
                price_fields = {'financial.list_price', 'financial.original_list_price'}
                for log in change_logs:
                    # Check simple format (single field change)
                    if log.get("field") in price_fields:
                        old_val = log.get("old_value")
                        new_val = log.get("new_value")
                        if old_val is not None and new_val is not None and new_val < old_val:
                            has_price_reduction = True
                            break
                    # Check consolidated format (multiple fields changed on same date)
                    elif log.get("field") is None and isinstance(log.get("field_changes"), dict):
                        field_changes = log.get("field_changes", {})
                        for field_name, change_data in field_changes.items():
                            if field_name in price_fields:
                                old_val = change_data.get("old_value")
                                new_val = change_data.get("new_value")
                                if old_val is not None and new_val is not None and new_val < old_val:
                                    has_price_reduction = True
                                    break
                        if has_price_reduction:
                            break
            
            # Update the property document with enrichment data
            await self.db.properties.update_one(
                {"property_id": property_id},
                {
                    "$set": {
                        "enrichment": enrichment_data,
                        "is_motivated_seller": is_motivated_seller,
                        "has_price_reduction": has_price_reduction,
                        "has_distress_signals": has_distress_signals,
                        "last_enriched_at": datetime.utcnow()
                    }
                }
            )
            logger.info(f"Updated property {property_id} with enrichment data")
        except Exception as e:
            logger.error(f"Error updating property {property_id} with enrichment data: {e}")
    
    def _create_error_enrichment(self, error_message: str) -> Dict[str, Any]:
        """Create error enrichment data"""
        enrichment_data = {
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
            'creative_financing': {
                'categories_found': [],
                'details': {},
                'has_creative_financing': False
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
            'enriched_at': datetime.utcnow(),
            'enrichment_version': '2.0',
            'error': error_message
        }
        return enrichment_data
    
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
            
            # Get existing enrichment data to merge with new text analysis
            property_doc = await self.db.properties.find_one(
                {"property_id": property_id},
                {"enrichment": 1}
            )
            
            existing_enrichment = property_doc.get('enrichment', {}) if property_doc else {}
            
            # Update full enrichment data with new text analysis including creative financing
            await self._update_enrichment_with_text_analysis(
                property_id, 
                motivated_seller, 
                text_analysis,
                existing_enrichment
            )
            
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
    
    def _extract_old_values(self, property_doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract _old values from property document for comparison.
        Returns old_property dict in format expected by PropertyDiffer.
        Returns None if no _old values exist (first scrape).
        """
        if not property_doc:
            return None
        
        # Check if _old values exist (check for list_price_old as indicator)
        financial = property_doc.get('financial', {})
        if not financial.get('list_price_old') and not property_doc.get('status_old'):
            # No _old values exist yet (first time scraping)
            return None
        
        # Build old_property dict matching PropertyDiffer expected format
        old_property = {
            'financial': {
                'list_price': financial.get('list_price_old'),
                'original_list_price': financial.get('original_list_price_old')
            },
            'status': property_doc.get('status_old'),
            'mls_status': property_doc.get('mls_status_old'),
            'listing_type': property_doc.get('listing_type_old')
        }
        
        return old_property
    
    async def _move_current_to_old(self, property_id: str, property_dict: Dict[str, Any]):
        """
        Move current tracked values to _old fields after enrichment completes.
        Sets _old_values_scraped_at to when the current values were scraped.
        """
        try:
            # Extract current values for the 5 tracked fields
            financial = property_dict.get('financial', {})
            
            # Get scraped_at timestamp (when these values were scraped)
            scraped_at = property_dict.get('scraped_at') or property_dict.get('last_scraped')
            if isinstance(scraped_at, str):
                try:
                    # Try parsing ISO format string
                    if 'T' in scraped_at or ' ' in scraped_at:
                        scraped_at = datetime.fromisoformat(scraped_at.replace('Z', '+00:00'))
                    else:
                        scraped_at = datetime.utcnow()
                except (ValueError, AttributeError):
                    scraped_at = datetime.utcnow()
            elif isinstance(scraped_at, datetime):
                # Already a datetime object
                pass
            elif not scraped_at:
                scraped_at = datetime.utcnow()
            
            # Prepare update fields
            update_fields = {
                # Copy current to _old
                'financial.list_price_old': financial.get('list_price'),
                'financial.original_list_price_old': financial.get('original_list_price'),
                'status_old': property_dict.get('status'),
                'mls_status_old': property_dict.get('mls_status'),
                'listing_type_old': property_dict.get('listing_type'),
                # Timestamp when these values were scraped
                '_old_values_scraped_at': scraped_at
            }
            
            # Update property document
            await self.db.properties.update_one(
                {"property_id": property_id},
                {"$set": update_fields}
            )
            
            logger.debug(f"Moved current values to _old for property {property_id}")
            
        except Exception as e:
            logger.error(f"Error moving current to _old for property {property_id}: {e}")
            # Don't raise - this is not critical, enrichment can continue
    
    async def _update_enrichment_with_text_analysis(
        self, 
        property_id: str, 
        motivated_seller: Dict[str, Any],
        text_analysis: Dict[str, Any],
        existing_enrichment: Dict[str, Any]
    ):
        """Update enrichment data with new text analysis including creative financing"""
        try:
            # Merge existing enrichment with new text analysis
            enrichment_updates = {
                "enrichment.motivated_seller": motivated_seller,
                "enrichment.big_ticket_items": text_analysis.get('big_ticket_items', {}),
                "enrichment.distress_signals": {
                    'signals_found': text_analysis.get('distress_signals', []),
                    'motivated_keywords': text_analysis.get('motivated_keywords', [])
                },
                "enrichment.special_sale_types": {
                    'is_auction': 'auction' in text_analysis.get('special_sale_types', []),
                    'is_reo': 'reo' in text_analysis.get('special_sale_types', []),
                    'is_probate': 'probate' in text_analysis.get('special_sale_types', []),
                    'is_short_sale': 'short_sale' in text_analysis.get('special_sale_types', []),
                    'is_as_is': 'as_is' in text_analysis.get('special_sale_types', []),
                    'is_portfolio': 'portfolio' in text_analysis.get('special_sale_types', []),
                    'types_found': text_analysis.get('special_sale_types', [])
                },
                "enrichment.creative_financing": {
                    'categories_found': [cat for cat, data in text_analysis.get('creative_financing', {}).items() if data.get('found', False)],
                    'details': text_analysis.get('creative_financing', {}),
                    'has_creative_financing': text_analysis.get('summary', {}).get('has_creative_financing', False)
                },
                "enrichment.text_analysis": {
                    'keywords_found': text_analysis.get('keywords_found', []),
                    'total_keywords': text_analysis.get('summary', {}).get('total_keywords', 0),
                    'big_ticket_items_count': text_analysis.get('summary', {}).get('big_ticket_items_count', 0)
                },
                "enrichment.enriched_at": datetime.utcnow(),
                "is_motivated_seller": motivated_seller.get('score', 0) >= 40,
                "has_distress_signals": text_analysis.get('summary', {}).get('has_distress_signals', False) or len(text_analysis.get('distress_signals', [])) > 0,
                "last_enriched_at": datetime.utcnow()
            }
            
            # Preserve existing change_summary if it exists
            if existing_enrichment.get('change_summary'):
                enrichment_updates["enrichment.change_summary"] = existing_enrichment['change_summary']
            
            await self.db.properties.update_one(
                {"property_id": property_id},
                {"$set": enrichment_updates}
            )
            logger.info(f"Updated enrichment with text analysis (including creative financing) for property {property_id}")
        except Exception as e:
            logger.error(f"Error updating enrichment with text analysis for property {property_id}: {e}")
    
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
                    "is_motivated_seller": 1,
                    "has_price_reduction": 1,
                    "has_distress_signals": 1,
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