"""
Motivated Seller Scorer Service

Calculates a motivated seller score (0-100) based on various signals
like price reductions, days on market, keywords, and sale type indicators.

V2: Separates raw signal detection from score calculation for flexible recalculation.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging
import os
import yaml
import hashlib
import json

logger = logging.getLogger(__name__)

class MotivatedSellerScorer:
    """Calculates motivated seller scores based on multiple signals"""
    
    def __init__(self, config_path: Optional[str] = None):
        # Load configuration from YAML file or use defaults
        self.config = self._load_config(config_path)
        self.config_hash = self._generate_config_hash()
        
        # Signal weights (total should be ~100)
        self.signal_weights = self.config.get('signal_weights', {
            'explicit_keywords': 50,
            'price_reductions': 30,
            'days_on_market': 15,
            'distress_keywords': 10,
            'special_sale_type': 5,
            'recent_activity': 0
        })
        
        # Explicit keywords (high-confidence motivated seller declarations)
        distress_config = self.config.get('distress_keywords', {})
        self.explicit_keywords = [
            "motivated seller",
            "must sell",
            "need to sell",
            "have to sell",
            "urgent sale",
            "quick sale",
            "desperate seller"
        ]
        # Also include high-level keywords from config
        high_keywords = distress_config.get('high', {}).get('keywords', [])
        self.explicit_keywords.extend([kw for kw in high_keywords if kw not in self.explicit_keywords])
        
        # Price reduction scoring thresholds
        self.price_reduction_thresholds = self.config.get('price_reduction_thresholds', {
            'major': {'percent': 10, 'score': 30},
            'significant': {'percent': 5, 'score': 20},
            'moderate': {'percent': 2, 'score': 10},
            'minor': {'percent': 0, 'score': 5}
        })
        
        # Days on market scoring thresholds
        self.dom_thresholds = self.config.get('days_on_market_thresholds', {
            'very_long': {'days': 180, 'score': 30},
            'long': {'days': 120, 'score': 28},
            'moderate': {'days': 90, 'score': 25},
            'short': {'days': 60, 'score': 20},
            'minimal': {'days': 30, 'score': 12},
            'very_short': {'days': 14, 'score': 6},
            'immediate': {'days': 0, 'score': 0}
        })
        
        # Days since pending bonus multipliers
        self.pending_bonus = self.config.get('days_since_pending_bonus', {
            'very_recent': {'days': 7, 'score_multiplier': 1.5},
            'recent': {'days': 14, 'score_multiplier': 1.3},
            'moderate': {'days': 30, 'score_multiplier': 1.2},
            'none': {'days': None, 'score_multiplier': 1.0}
        })
        
        # Distress keyword scoring
        self.distress_keyword_scores = {
            'high': distress_config.get('high', {}).get('keywords', []),
            'medium': distress_config.get('medium', {}).get('keywords', []),
            'low': distress_config.get('low', {}).get('keywords', []),
            'negative': distress_config.get('negative', {}).get('keywords', [])
        }
        
        # Distress keyword scores per level
        self.distress_score_per_keyword = {
            'high': distress_config.get('high', {}).get('score_per_keyword', 8),
            'medium': distress_config.get('medium', {}).get('score_per_keyword', 5),
            'low': distress_config.get('low', {}).get('score_per_keyword', 2),
            'negative': distress_config.get('negative', {}).get('score_per_keyword', -3)
        }
        
        # Special sale type scoring
        self.special_sale_scores = self.config.get('special_sale_types', {
            'auction': 15,
            'reo': 15,
            'probate': 12,
            'short_sale': 10,
            'as_is': 8
        })
        
        # Recent activity thresholds
        self.recent_activity_thresholds = self.config.get('recent_activity_thresholds', {
            'very_recent': {'days': 3, 'score': 2},
            'recent': {'days': 7, 'score': 1}
        })
        self.max_activity_score = self.config.get('recent_activity_thresholds', {}).get('max_score', 5)
    
    def _load_config(self, config_path: Optional[str] = None) -> Dict[str, Any]:
        """Load configuration from YAML file or return defaults"""
        if config_path is None:
            # Default config path
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                'config',
                'enrichment_scoring.yaml'
            )
        
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)
                    logger.info(f"Loaded scoring configuration from {config_path}")
                    return config
            else:
                logger.warning(f"Config file not found at {config_path}, using defaults")
                return {}
        except Exception as e:
            logger.error(f"Error loading config from {config_path}: {e}")
            return {}
    
    def _generate_config_hash(self) -> str:
        """Generate hash of current configuration to track config changes"""
        try:
            # Create hash from config keys that affect scoring
            config_data = {
                'signal_weights': self.config.get('signal_weights', {}),
                'price_reduction_thresholds': self.config.get('price_reduction_thresholds', {}),
                'days_on_market_thresholds': self.config.get('days_on_market_thresholds', {}),
                'days_since_pending_bonus': self.config.get('days_since_pending_bonus', {}),
                'distress_keywords': self.config.get('distress_keywords', {}),
                'special_sale_types': self.config.get('special_sale_types', {}),
                'config_version': self.config.get('config_version', '1.0')
            }
            
            config_json = json.dumps(config_data, sort_keys=True, default=str)
            return hashlib.md5(config_json.encode('utf-8')).hexdigest()[:8]
        except Exception as e:
            logger.error(f"Error generating config hash: {e}")
            return "unknown"
    
    def get_config_hash(self) -> str:
        """Get current config hash"""
        return self.config_hash
    
    def detect_signals(self, property_data: Dict[str, Any], analysis_data: Dict[str, Any], history_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Phase 1: Detect raw signals and store findings with absolute scores
        
        Args:
            property_data: Property data including price, status, days_on_mls
            analysis_data: Text analysis results
            history_data: Optional price/status history data
            
        Returns:
            Dictionary containing raw findings with absolute scores
        """
        try:
            findings = {}
            
            # 1. Explicit keywords detection
            findings['explicit_keywords'] = self._detect_explicit_keywords(property_data, analysis_data)
            
            # 2. Days on market detection (now includes pending bonus)
            findings['days_on_market'] = self._detect_days_on_market(property_data, history_data)
            
            # 3. Price reduction detection
            findings['price_reduction'] = self._detect_price_reductions(property_data, history_data)
            
            # 4. Distress keywords detection
            findings['distress_keywords'] = self._detect_distress_keywords(analysis_data)
            
            # 5. Special sale types detection
            findings['special_sale_types'] = self._detect_special_sale_types(analysis_data)
            
            # 6. Recent activity detection
            findings['recent_activity'] = self._detect_recent_activity(property_data, history_data)
            
            return findings
            
        except Exception as e:
            logger.error(f"Error detecting signals: {e}")
            return self._empty_findings()
    
    def calculate_score_from_findings(self, findings: Dict[str, Any]) -> Dict[str, Any]:
        """
        Phase 2: Calculate final score from raw findings using current config
        
        Args:
            findings: Raw findings from detect_signals()
            
        Returns:
            Dictionary containing score, confidence, reasoning, and metadata
        """
        try:
            # Calculate weighted total score
            total_score = 0.0
            
            # Explicit keywords: direct weight contribution (0-50 points)
            explicit_finding = findings.get('explicit_keywords', {})
            if explicit_finding.get('detected', False):
                explicit_raw = explicit_finding.get('raw_score', 0)
                weight = self.signal_weights.get('explicit_keywords', 50)
                # Explicit keywords get full weight if detected
                total_score += weight
            
            # Price reductions: weighted contribution (0-30 points)
            price_finding = findings.get('price_reduction', {})
            if price_finding.get('detected', False):
                price_raw = price_finding.get('raw_score', 0)
                price_max = self.price_reduction_thresholds['major']['score']
                weight = self.signal_weights.get('price_reductions', 30)
                contribution = (price_raw / price_max) * weight if price_max > 0 else 0
                total_score += contribution
            
            # Days on market: weighted contribution (0-15 points) with pending bonus multiplier
            dom_finding = findings.get('days_on_market', {})
            dom_raw = dom_finding.get('raw_score', 0)
            dom_max = self.dom_thresholds['very_long']['score']
            weight = self.signal_weights.get('days_on_market', 15)
            base_contribution = (dom_raw / dom_max) * weight if dom_max > 0 else 0
            
            # Apply pending multiplier (bonus for properties recently returned from pending)
            pending_multiplier = dom_finding.get('pending_multiplier', 1.0)
            contribution = base_contribution * pending_multiplier
            total_score += contribution
            
            # Distress keywords: weighted contribution (0-10 points)
            distress_finding = findings.get('distress_keywords', {})
            distress_raw = distress_finding.get('raw_score', 0)
            # Cap distress keywords raw score at reasonable max (e.g., 20)
            distress_max = 20
            weight = self.signal_weights.get('distress_keywords', 10)
            contribution = (min(distress_raw, distress_max) / distress_max) * weight if distress_max > 0 else 0
            total_score += contribution
            
            # Special sale types: weighted contribution (0-5 points)
            special_finding = findings.get('special_sale_types', {})
            special_raw = special_finding.get('raw_score', 0)
            special_max = max(self.special_sale_scores.values()) if self.special_sale_scores else 15
            weight = self.signal_weights.get('special_sale_type', 5)
            contribution = (special_raw / special_max) * weight if special_max > 0 else 0
            total_score += contribution
            
            # Recent activity: weighted contribution (0-5 points)
            activity_finding = findings.get('recent_activity', {})
            activity_raw = activity_finding.get('raw_score', 0)
            activity_max = self.max_activity_score
            weight = self.signal_weights.get('recent_activity', 0)
            contribution = (activity_raw / activity_max) * weight if activity_max > 0 else 0
            total_score += contribution
            
            # Store uncapped score
            score_uncapped = total_score
            
            # Cap at 100
            score_capped = min(100, max(0, total_score))
            
            # Calculate confidence
            confidence = self._calculate_confidence_from_findings(findings)
            
            # Generate reasoning
            reasoning = self._generate_reasoning_from_findings(findings, score_capped)
            
            return {
                'score': round(score_capped, 1),
                'score_uncapped': round(score_uncapped, 1),
                'confidence': confidence,
                'reasoning': reasoning,
                'findings': findings,
                'score_version': 'v2',
                'config_hash': self.config_hash,
                'last_calculated': datetime.utcnow(),
                # Keep legacy components for backward compatibility
                'components': self._findings_to_components(findings)
            }
            
        except Exception as e:
            logger.error(f"Error calculating score from findings: {e}")
            return {
                'score': 0,
                'score_uncapped': 0,
                'confidence': 'low',
                'reasoning': [f'Error calculating score: {str(e)}'],
                'findings': findings,
                'score_version': 'v2',
                'config_hash': self.config_hash,
                'last_calculated': datetime.utcnow(),
                'components': {}
            }
    
    def calculate_motivated_score(self, property_data: Dict[str, Any], analysis_data: Dict[str, Any], history_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Calculate motivated seller score (Phase 1 + Phase 2 combined)
        
        This is the main entry point that runs both detection and calculation.
        For recalculation only, use calculate_score_from_findings() directly.
        
        Args:
            property_data: Property data including price, status, days_on_mls
            analysis_data: Text analysis results
            history_data: Optional price/status history data
            
        Returns:
            Dictionary containing score, confidence, reasoning, and findings
        """
        # Phase 1: Detect signals
        findings = self.detect_signals(property_data, analysis_data, history_data)
        
        # Phase 2: Calculate score from findings
        result = self.calculate_score_from_findings(findings)
        
        return result
    
    # Detection methods (Phase 1)
    
    def _detect_explicit_keywords(self, property_data: Dict[str, Any], analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """Detect explicit motivated seller keywords"""
        description = self._extract_description(property_data)
        description_lower = description.lower() if description else ""
        
        keywords_found = []
        detected = False
        raw_score = 50  # Base score for explicit declaration
        
        for keyword in self.explicit_keywords:
            if keyword.lower() in description_lower:
                keywords_found.append(keyword)
                detected = True
        
        return {
            'detected': detected,
            'keywords_found': keywords_found,
            'count': len(keywords_found),
            'raw_score': raw_score if detected else 0
        }
    
    def _detect_days_on_market(self, property_data: Dict[str, Any], history_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Detect days on market signal and days since last pending"""
        days_on_market = property_data.get('days_on_mls', 0) or 0
        raw_score = 0
        threshold_met = None
        
        # Determine threshold based on DOM
        if days_on_market >= self.dom_thresholds['very_long']['days']:
            raw_score = self.dom_thresholds['very_long']['score']
            threshold_met = 'very_long'
        elif days_on_market >= self.dom_thresholds['long']['days']:
            raw_score = self.dom_thresholds['long']['score']
            threshold_met = 'long'
        elif days_on_market >= self.dom_thresholds['moderate']['days']:
            raw_score = self.dom_thresholds['moderate']['score']
            threshold_met = 'moderate'
        elif days_on_market >= self.dom_thresholds['short']['days']:
            raw_score = self.dom_thresholds['short']['score']
            threshold_met = 'short'
        elif days_on_market >= self.dom_thresholds['minimal']['days']:
            raw_score = self.dom_thresholds['minimal']['score']
            threshold_met = 'minimal'
        elif days_on_market >= self.dom_thresholds['very_short']['days']:
            raw_score = self.dom_thresholds['very_short']['score']
            threshold_met = 'very_short'
        else:
            raw_score = self.dom_thresholds['immediate']['score']
            threshold_met = 'immediate'
        
        # Detect days since last pending
        days_since_pending = self._calculate_days_since_pending(property_data, history_data)
        pending_multiplier = self._get_pending_multiplier(days_since_pending)
        
        return {
            'value': days_on_market,
            'threshold_met': threshold_met,
            'raw_score': raw_score,
            'days_since_pending': days_since_pending,
            'pending_multiplier': pending_multiplier
        }
    
    def _calculate_days_since_pending(self, property_data: Dict[str, Any], history_data: Optional[Dict[str, Any]] = None) -> Optional[int]:
        """Calculate days since property last returned from pending status"""
        if not history_data:
            return None
        
        # Check status history for most recent PENDING -> FOR_SALE transition
        status_history = history_data.get('status_history', [])
        
        # Current status
        current_status = property_data.get('status', '').upper()
        if current_status == 'PENDING':
            return None  # Currently pending, can't calculate
        
        # Look for most recent transition from PENDING to FOR_SALE
        for entry in status_history:
            if entry.get('change_type') == 'status_change':
                data = entry.get('data', {})
                old_status = data.get('old_status', '').upper()
                new_status = data.get('new_status', '').upper()
                
                # Found a transition from PENDING to FOR_SALE
                if old_status == 'PENDING' and new_status == 'FOR_SALE':
                    timestamp = entry.get('timestamp')
                    if timestamp:
                        try:
                            if isinstance(timestamp, str):
                                pending_end = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                            else:
                                pending_end = timestamp
                            
                            days_ago = (datetime.utcnow() - pending_end.replace(tzinfo=None)).days
                            return max(0, days_ago)
                        except Exception as e:
                            logger.debug(f"Error parsing pending timestamp: {e}")
                            continue
        
        return None  # Never was pending (or no history available)
    
    def _get_pending_multiplier(self, days_since_pending: Optional[int]) -> float:
        """Get multiplier based on days since last pending"""
        if days_since_pending is None:
            return self.pending_bonus['none']['score_multiplier']
        
        # Check thresholds in order (most recent first)
        if days_since_pending <= self.pending_bonus['very_recent']['days']:
            return self.pending_bonus['very_recent']['score_multiplier']
        elif days_since_pending <= self.pending_bonus['recent']['days']:
            return self.pending_bonus['recent']['score_multiplier']
        elif days_since_pending <= self.pending_bonus['moderate']['days']:
            return self.pending_bonus['moderate']['score_multiplier']
        else:
            return self.pending_bonus['none']['score_multiplier']
    
    def _detect_price_reductions(self, property_data: Dict[str, Any], history_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Detect price reduction signals
        
        Only considers price reductions where the listing_type remained the same.
        Filters out price changes that occurred when a property moved between listing types.
        
        Note: Status changes (e.g., FOR_SALE -> PENDING) within the same listing_type are OK.
        """
        detected = False
        reductions = []
        max_percent = 0
        raw_score = 0
        
        # Get current listing_type for filtering (not status - status can change within same listing_type)
        current_listing_type = property_data.get('listing_type')
        
        if history_data and 'price_history' in history_data:
            price_history = history_data['price_history']
            
            if len(price_history) > 1:
                # Look at recent price changes
                recent_changes = price_history[:3]  # Last 3 changes
                
                for change in recent_changes:
                    if change.get('change_type') == 'price_change':
                        data = change.get('data', {})
                        percent_change = data.get('percent_change', 0)
                        
                        # Check if listing_type changed during this price change
                        # Don't check status - status can change (FOR_SALE -> PENDING) within same listing_type
                        old_listing_type = data.get('old_listing_type')
                        new_listing_type = data.get('new_listing_type')
                        
                        # Skip if listing_type changed (e.g., for_rent -> for_sale, for_sale -> sold)
                        listing_type_changed = old_listing_type and new_listing_type and old_listing_type != new_listing_type
                        
                        if listing_type_changed:
                            logger.debug(f"Skipping price change due to listing_type change: {old_listing_type} -> {new_listing_type}")
                            continue
                        
                        # Only consider reductions where new_listing_type matches current
                        # This ensures we're looking at reductions within the same listing context
                        # Status doesn't matter - FOR_SALE -> PENDING -> FOR_SALE is fine
                        if new_listing_type and current_listing_type and new_listing_type != current_listing_type:
                            continue
                        
                        if percent_change < 0:  # Price reduction
                            detected = True
                            abs_percent = abs(percent_change)
                            max_percent = max(max_percent, abs_percent)
                            reductions.append({
                                'percent_change': percent_change,
                                'old_price': data.get('old_price'),
                                'new_price': data.get('new_price'),
                                'timestamp': change.get('timestamp')
                            })
                            
                            # Determine raw score based on reduction size
                            if abs_percent >= 10:
                                raw_score = max(raw_score, self.price_reduction_thresholds['major']['score'])
                            elif abs_percent >= 5:
                                raw_score = max(raw_score, self.price_reduction_thresholds['significant']['score'])
                            elif abs_percent >= 2:
                                raw_score = max(raw_score, self.price_reduction_thresholds['moderate']['score'])
                            else:
                                raw_score = max(raw_score, self.price_reduction_thresholds['minor']['score'])
        
        return {
            'detected': detected,
            'reductions': reductions,
            'max_percent': max_percent,
            'raw_score': raw_score
        }
    
    def _detect_distress_keywords(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """Detect distress keywords"""
        distress_signals = analysis_data.get('distress_signals', [])
        motivated_keywords = analysis_data.get('motivated_keywords', [])
        
        keywords_found = []
        count_by_level = {'high': 0, 'medium': 0, 'low': 0, 'negative': 0}
        raw_score = 0
        
        # Check distress signals
        for signal in distress_signals:
            signal_lower = signal.lower()
            found = False
            
            for level in ['high', 'medium', 'low', 'negative']:
                if any(keyword in signal_lower for keyword in self.distress_keyword_scores[level]):
                    keywords_found.append(signal)
                    count_by_level[level] += 1
                    raw_score += self.distress_score_per_keyword[level]
                    found = True
                    break
            
            if not found:
                keywords_found.append(signal)
        
        # Check motivated keywords
        for keyword in motivated_keywords:
            keyword_lower = keyword.lower()
            found = False
            
            for level in ['high', 'medium', 'low', 'negative']:
                if any(kw in keyword_lower for kw in self.distress_keyword_scores[level]):
                    if keyword not in keywords_found:
                        keywords_found.append(keyword)
                        count_by_level[level] += 1
                        # Motivated keywords get slightly less than distress signals
                        score_adjustment = -2 if level != 'negative' else 0
                        raw_score += self.distress_score_per_keyword[level] + score_adjustment
                        found = True
                        break
            
            if not found and keyword not in keywords_found:
                keywords_found.append(keyword)
        
        # Don't allow negative score from this component
        raw_score = max(0, raw_score)
        
        return {
            'keywords_found': keywords_found,
            'count_by_level': count_by_level,
            'raw_score': raw_score
        }
    
    def _detect_special_sale_types(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """Detect special sale types"""
        special_sale_types = analysis_data.get('special_sale_types', [])
        raw_score = 0
        
        for sale_type in special_sale_types:
            if sale_type in self.special_sale_scores:
                # Take max score (don't double-count)
                raw_score = max(raw_score, self.special_sale_scores[sale_type])
        
        return {
            'types_found': special_sale_types,
            'raw_score': raw_score
        }
    
    def _detect_recent_activity(self, property_data: Dict[str, Any], history_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Detect recent activity signals"""
        detected = False
        days_since_update = None
        raw_score = 0
        
        # Check if property was recently updated
        scraped_at = property_data.get('scraped_at')
        if scraped_at:
            if isinstance(scraped_at, str):
                try:
                    scraped_at = datetime.fromisoformat(scraped_at.replace('Z', '+00:00'))
                except:
                    scraped_at = None
            
            if scraped_at:
                days_since_update = (datetime.utcnow() - scraped_at.replace(tzinfo=None)).days
                
                very_recent_days = self.recent_activity_thresholds.get('very_recent', {}).get('days', 3)
                very_recent_score = self.recent_activity_thresholds.get('very_recent', {}).get('score', 2)
                recent_days = self.recent_activity_thresholds.get('recent', {}).get('days', 7)
                recent_score = self.recent_activity_thresholds.get('recent', {}).get('score', 1)
                
                if days_since_update <= very_recent_days:
                    raw_score += very_recent_score
                    detected = True
                elif days_since_update <= recent_days:
                    raw_score += recent_score
                    detected = True
        
        # Check for recent changes in history
        if history_data and 'recent_changes' in history_data:
            recent_changes = history_data['recent_changes']
            if recent_changes.get('summary', {}).get('history_count', 0) > 0:
                raw_score += 3
                detected = True
        
        # Cap at max score
        raw_score = min(raw_score, self.max_activity_score)
        
        return {
            'detected': detected,
            'days_since_update': days_since_update,
            'raw_score': raw_score
        }
    
    # Helper methods
    
    def _extract_description(self, property_data: Dict[str, Any]) -> str:
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
                value = property_data
                for part in parts:
                    if isinstance(value, dict) and part in value:
                        value = value[part]
                    else:
                        value = None
                        break
            else:
                # Direct field
                value = property_data.get(field)
            
            if value and isinstance(value, str) and value.strip():
                return value.strip()
        
        return ""
    
    def _empty_findings(self) -> Dict[str, Any]:
        """Return empty findings structure"""
        return {
            'explicit_keywords': {'detected': False, 'keywords_found': [], 'count': 0, 'raw_score': 0},
            'days_on_market': {'value': 0, 'threshold_met': 'immediate', 'raw_score': 0, 'days_since_pending': None, 'pending_multiplier': 1.0},
            'price_reduction': {'detected': False, 'reductions': [], 'max_percent': 0, 'raw_score': 0},
            'distress_keywords': {'keywords_found': [], 'count_by_level': {'high': 0, 'medium': 0, 'low': 0, 'negative': 0}, 'raw_score': 0},
            'special_sale_types': {'types_found': [], 'raw_score': 0},
            'recent_activity': {'detected': False, 'days_since_update': None, 'raw_score': 0}
        }
    
    def _calculate_confidence_from_findings(self, findings: Dict[str, Any]) -> str:
        """Calculate confidence level based on findings data availability"""
        data_points = 0
        max_data_points = 6
        
        # Check data availability
        if findings.get('explicit_keywords', {}).get('detected', False):
            data_points += 1
        if findings.get('days_on_market', {}).get('value', 0) is not None:
            data_points += 1
        if findings.get('price_reduction', {}).get('detected', False):
            data_points += 1
        if findings.get('distress_keywords', {}).get('keywords_found'):
            data_points += 1
        if findings.get('special_sale_types', {}).get('types_found'):
            data_points += 1
        if findings.get('recent_activity', {}).get('detected', False):
            data_points += 1
        
        confidence_ratio = data_points / max_data_points
        
        if confidence_ratio >= 0.8:
            return 'high'
        elif confidence_ratio >= 0.6:
            return 'medium'
        else:
            return 'low'
    
    def _generate_reasoning_from_findings(self, findings: Dict[str, Any], final_score: float) -> List[str]:
        """Generate human-readable reasoning from findings"""
        reasoning = []
        
        # Explicit keywords
        explicit = findings.get('explicit_keywords', {})
        if explicit.get('detected', False):
            keywords = explicit.get('keywords_found', [])
            reasoning.append(f"Explicit motivation: {', '.join(keywords)}")
        
        # Price reductions
        price = findings.get('price_reduction', {})
        if price.get('detected', False):
            max_percent = price.get('max_percent', 0)
            reductions = price.get('reductions', [])
            reasoning.append(f"Price reduction: {max_percent:.1f}% ({len(reductions)} reduction(s))")
        
        # Days on market
        dom = findings.get('days_on_market', {})
        dom_value = dom.get('value', 0)
        days_since_pending = dom.get('days_since_pending')
        pending_multiplier = dom.get('pending_multiplier', 1.0)
        
        if dom_value > 0:
            reasoning.append(f"Days on market: {dom_value} days")
        
        # Add pending bonus info if applicable
        if days_since_pending is not None and pending_multiplier > 1.0:
            bonus_pct = int((pending_multiplier - 1.0) * 100)
            reasoning.append(f"Returned from pending {days_since_pending} days ago ({bonus_pct}% DOM bonus)")
        
        # Distress keywords
        distress = findings.get('distress_keywords', {})
        keywords_found = distress.get('keywords_found', [])
        if keywords_found:
            count_by_level = distress.get('count_by_level', {})
            total = sum(count_by_level.values())
            reasoning.append(f"Distress signals: {total} keyword(s) found")
        
        # Special sale types
        special = findings.get('special_sale_types', {})
        types_found = special.get('types_found', [])
        if types_found:
            reasoning.append(f"Special sale type: {', '.join(types_found)}")
        
        # Overall assessment
        if final_score >= 70:
            reasoning.append("High motivated seller - strong indicators present")
        elif final_score >= 40:
            reasoning.append("Moderate motivated seller - some indicators present")
        elif final_score >= 20:
            reasoning.append("Low motivated seller - few indicators present")
        else:
            reasoning.append("Very low motivated seller - minimal indicators")
        
        return reasoning if reasoning else ["No motivation indicators found"]
    
    def _findings_to_components(self, findings: Dict[str, Any]) -> Dict[str, Any]:
        """Convert findings to legacy components format for backward compatibility"""
        return {
            'explicit_keywords': {
                'score': 50 if findings.get('explicit_keywords', {}).get('detected', False) else 0,
                'max_possible': 50,
                'details': [f"Explicit: {', '.join(findings.get('explicit_keywords', {}).get('keywords_found', []))}"] if findings.get('explicit_keywords', {}).get('detected', False) else []
            },
            'price_reductions': {
                'score': findings.get('price_reduction', {}).get('raw_score', 0),
                'max_possible': self.price_reduction_thresholds['major']['score'],
                'details': [f"Price reduction: {findings.get('price_reduction', {}).get('max_percent', 0):.1f}%"] if findings.get('price_reduction', {}).get('detected', False) else [],
                'data_available': findings.get('price_reduction', {}).get('detected', False)
            },
            'days_on_market': {
                'score': findings.get('days_on_market', {}).get('raw_score', 0),
                'max_possible': self.dom_thresholds['very_long']['score'],
                'details': [f"Days on market: {findings.get('days_on_market', {}).get('value', 0)} days"],
                'days_on_market': findings.get('days_on_market', {}).get('value', 0)
            },
            'distress_keywords': {
                'score': findings.get('distress_keywords', {}).get('raw_score', 0),
                'max_possible': 20,
                'details': [f"Distress signals: {len(findings.get('distress_keywords', {}).get('keywords_found', []))} found"],
                'signals_found': len(findings.get('distress_keywords', {}).get('keywords_found', [])),
                'keywords_found': len(findings.get('distress_keywords', {}).get('keywords_found', []))
            },
            'special_sale_type': {
                'score': findings.get('special_sale_types', {}).get('raw_score', 0),
                'max_possible': max(self.special_sale_scores.values()) if self.special_sale_scores else 15,
                'details': [f"Special sale: {', '.join(findings.get('special_sale_types', {}).get('types_found', []))}"] if findings.get('special_sale_types', {}).get('types_found', []) else [],
                'sale_types_found': findings.get('special_sale_types', {}).get('types_found', [])
            },
            'recent_activity': {
                'score': findings.get('recent_activity', {}).get('raw_score', 0),
                'max_possible': self.max_activity_score,
                'details': [f"Recent activity: {findings.get('recent_activity', {}).get('days_since_update', 'N/A')} days ago"] if findings.get('recent_activity', {}).get('detected', False) else [],
                'data_available': findings.get('recent_activity', {}).get('detected', False)
            }
        }
