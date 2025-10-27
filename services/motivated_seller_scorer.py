"""
Motivated Seller Scorer Service

Calculates a motivated seller score (0-100) based on various signals
like price reductions, days on market, keywords, and sale type indicators.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging
import os
import yaml

logger = logging.getLogger(__name__)

class MotivatedSellerScorer:
    """Calculates motivated seller scores based on multiple signals"""
    
    def __init__(self, config_path: Optional[str] = None):
        # Load configuration from YAML file or use defaults
        self.config = self._load_config(config_path)
        
        # Signal weights (total should be ~100)
        self.signal_weights = self.config.get('signal_weights', {
            'price_reductions': 40,
            'days_on_market': 30,
            'distress_keywords': 15,
            'special_sale_type': 10,
            'recent_activity': 5
        })
        
        # Price reduction scoring thresholds
        self.price_reduction_thresholds = self.config.get('price_reduction_thresholds', {
            'major': {'percent': 10, 'score': 30},
            'significant': {'percent': 5, 'score': 20},
            'moderate': {'percent': 2, 'score': 10},
            'minor': {'percent': 0, 'score': 5}
        })
        
        # Days on market scoring thresholds
        self.dom_thresholds = self.config.get('days_on_market_thresholds', {
            'very_long': {'days': 270, 'score': 30},
            'long': {'days': 180, 'score': 25},
            'moderate': {'days': 120, 'score': 18},
            'short': {'days': 90, 'score': 12},
            'minimal': {'days': 60, 'score': 6},
            'very_short': {'days': 0, 'score': 0}
        })
        
        # Distress keyword scoring
        distress_config = self.config.get('distress_keywords', {})
        self.distress_keyword_scores = {
            'high': distress_config.get('high', {}).get('keywords', ['must sell', 'quick sale', 'urgent', 'motivated seller', 'as-is', 'cash only']),
            'medium': distress_config.get('medium', {}).get('keywords', ['price reduced', 'bring offers', 'handyman', 'fixer upper']),
            'low': distress_config.get('low', {}).get('keywords', ['needs work', 'handy man', 'diy']),
            'negative': distress_config.get('negative', {}).get('keywords', [])  # Anti-motivation keywords
        }
        
        # Distress keyword scores per level
        self.distress_score_per_keyword = {
            'high': distress_config.get('high', {}).get('score_per_keyword', 8),
            'medium': distress_config.get('medium', {}).get('score_per_keyword', 5),
            'low': distress_config.get('low', {}).get('score_per_keyword', 2),
            'negative': distress_config.get('negative', {}).get('score_per_keyword', -3)  # Subtracts points
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
    
    def calculate_motivated_score(self, property_data: Dict[str, Any], analysis_data: Dict[str, Any], history_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Calculate motivated seller score for a property
        
        Args:
            property_data: Property data including price, status, days_on_mls
            analysis_data: Text analysis results
            history_data: Optional price/status history data
            
        Returns:
            Dictionary containing score, confidence, and reasoning
        """
        try:
            score_components = {
                'price_reductions': self._score_price_reductions(property_data, history_data),
                'days_on_market': self._score_days_on_market(property_data),
                'distress_keywords': self._score_distress_keywords(analysis_data),
                'special_sale_type': self._score_special_sale_type(analysis_data),
                'recent_activity': self._score_recent_activity(property_data, history_data)
            }
            
            # Calculate weighted total score
            # Each component score is already out of its max_possible points
            # We weight each component based on its importance
            total_weighted_score = 0
            total_max_possible_score = 0
            
            for signal, weight in self.signal_weights.items():
                component = score_components[signal]
                component_score = component['score']
                component_max = component.get('max_possible', 100)
                
                # Calculate weighted contribution
                # Weight represents how much of the total 100 points this component can contribute
                # So if weight is 40, this component can contribute up to 40 points to the final score
                contribution = (component_score / component_max) * weight if component_max > 0 else 0
                
                total_weighted_score += contribution
                total_max_possible_score += weight  # Each weight contributes to max
            
            # Final score is percentage of max possible (where max is sum of all weights)
            # Since weights are set to sum to ~100, we can divide by 100 to get percentage
            if total_max_possible_score > 0:
                final_score = (total_weighted_score / total_max_possible_score) * 100
            else:
                final_score = 0
            
            # Cap at 100
            final_score = min(100, max(0, final_score))
            
            # Calculate confidence based on data availability
            confidence = self._calculate_confidence(score_components, property_data, analysis_data)
            
            # Generate reasoning
            reasoning = self._generate_reasoning(score_components, final_score)
            
            return {
                'score': round(final_score, 1),
                'confidence': confidence,
                'reasoning': reasoning,
                'components': score_components,
                'calculated_at': datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Error calculating motivated score: {e}")
            return {
                'score': 0,
                'confidence': 'low',
                'reasoning': [f'Error calculating score'],
                'components': {},
                'calculated_at': datetime.utcnow()
            }
    
    def _score_price_reductions(self, property_data: Dict[str, Any], history_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Score based on price reductions"""
        score = 0
        details = []
        
        if history_data and 'price_history' in history_data:
            price_history = history_data['price_history']
            
            if len(price_history) > 1:
                # Look at recent price changes
                recent_changes = price_history[:3]  # Last 3 changes
                
                for change in recent_changes:
                    if change.get('change_type') == 'price_change':
                        data = change.get('data', {})
                        percent_change = data.get('percent_change', 0)
                        
                        if percent_change < 0:  # Price reduction
                            if percent_change <= -10:
                                score = max(score, self.price_reduction_thresholds['major']['score'])
                                details.append(f"Major price reduction: {abs(percent_change):.1f}%")
                            elif percent_change <= -5:
                                score = max(score, self.price_reduction_thresholds['significant']['score'])
                                details.append(f"Significant price reduction: {abs(percent_change):.1f}%")
                            elif percent_change <= -2:
                                score = max(score, self.price_reduction_thresholds['moderate']['score'])
                                details.append(f"Moderate price reduction: {abs(percent_change):.1f}%")
                            else:
                                score = max(score, self.price_reduction_thresholds['minor']['score'])
                                details.append(f"Minor price reduction: {abs(percent_change):.1f}%")
        
        return {
            'score': score,
            'max_possible': self.price_reduction_thresholds['major']['score'],
            'details': details,
            'data_available': history_data is not None
        }
    
    def _score_days_on_market(self, property_data: Dict[str, Any]) -> Dict[str, Any]:
        """Score based on days on market"""
        days_on_market = property_data.get('days_on_mls', 0)
        score = 0
        details = []
        
        if days_on_market >= self.dom_thresholds['very_long']['days']:
            score = self.dom_thresholds['very_long']['score']
            details.append(f"Very long time on market: {days_on_market} days")
        elif days_on_market >= self.dom_thresholds['long']['days']:
            score = self.dom_thresholds['long']['score']
            details.append(f"Long time on market: {days_on_market} days")
        elif days_on_market >= self.dom_thresholds['moderate']['days']:
            score = self.dom_thresholds['moderate']['score']
            details.append(f"Moderate time on market: {days_on_market} days")
        elif days_on_market >= self.dom_thresholds['short']['days']:
            score = self.dom_thresholds['short']['score']
            details.append(f"Short time on market: {days_on_market} days")
        else:
            score = self.dom_thresholds['very_short']['score']
            details.append(f"Recently listed: {days_on_market} days")
        
        return {
            'score': score,
            'max_possible': self.dom_thresholds['very_long']['score'],
            'details': details,
            'days_on_market': days_on_market
        }
    
    def _score_distress_keywords(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """Score based on distress keywords found in text analysis"""
        score = 0
        details = []
        
        distress_signals = analysis_data.get('distress_signals', [])
        motivated_keywords = analysis_data.get('motivated_keywords', [])
        
        # Score distress signals (positive motivators)
        for signal in distress_signals:
            signal_lower = signal.lower()
            if any(keyword in signal_lower for keyword in self.distress_keyword_scores['high']):
                score += self.distress_score_per_keyword['high']
                details.append(f"High distress signal: '{signal}'")
            elif any(keyword in signal_lower for keyword in self.distress_keyword_scores['medium']):
                score += self.distress_score_per_keyword['medium']
                details.append(f"Medium distress signal: '{signal}'")
            elif any(keyword in signal_lower for keyword in self.distress_keyword_scores['low']):
                score += self.distress_score_per_keyword['low']
                details.append(f"Low distress signal: '{signal}'")
            # Check for negative (anti-motivation) keywords
            elif any(keyword in signal_lower for keyword in self.distress_keyword_scores['negative']):
                score += self.distress_score_per_keyword['negative']  # This is negative, so it subtracts
                details.append(f"Anti-motivation signal: '{signal}'")
        
        # Score motivated keywords
        for keyword in motivated_keywords:
            keyword_lower = keyword.lower()
            if any(motivated in keyword_lower for motivated in self.distress_keyword_scores['high']):
                score += self.distress_score_per_keyword['high'] - 2  # Slightly less for motivated keywords
                details.append(f"Motivated keyword: '{keyword}'")
            elif any(motivated in keyword_lower for motivated in self.distress_keyword_scores['medium']):
                score += self.distress_score_per_keyword['medium'] - 2
                details.append(f"Motivated keyword: '{keyword}'")
            # Check for negative keywords
            elif any(keyword in keyword_lower for keyword in self.distress_keyword_scores['negative']):
                score += self.distress_score_per_keyword['negative']
                details.append(f"Anti-motivation keyword: '{keyword}'")
        
        # Cap at max possible (don't allow negative from this component)
        score = min(score, 20)
        
        return {
            'score': score,
            'max_possible': 20,
            'details': details,
            'signals_found': len(distress_signals),
            'keywords_found': len(motivated_keywords)
        }
    
    def _score_special_sale_type(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """Score based on special sale types"""
        score = 0
        details = []
        
        special_sale_types = analysis_data.get('special_sale_types', [])
        
        for sale_type in special_sale_types:
            if sale_type in self.special_sale_scores:
                type_score = self.special_sale_scores[sale_type]
                score = max(score, type_score)
                details.append(f"Special sale type: {sale_type} (+{type_score} points)")
        
        return {
            'score': score,
            'max_possible': max(self.special_sale_scores.values()),
            'details': details,
            'sale_types_found': special_sale_types
        }
    
    def _score_recent_activity(self, property_data: Dict[str, Any], history_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Score based on recent activity/changes"""
        score = 0
        details = []
        
        # Check if property was recently updated
        scraped_at = property_data.get('scraped_at')
        if scraped_at:
            if isinstance(scraped_at, str):
                scraped_at = datetime.fromisoformat(scraped_at.replace('Z', '+00:00'))
            
            days_since_update = (datetime.utcnow() - scraped_at).days
            
            # Use configured thresholds
            very_recent_days = self.recent_activity_thresholds.get('very_recent', {}).get('days', 3)
            very_recent_score = self.recent_activity_thresholds.get('very_recent', {}).get('score', 2)
            recent_days = self.recent_activity_thresholds.get('recent', {}).get('days', 7)
            recent_score = self.recent_activity_thresholds.get('recent', {}).get('score', 1)
            
            if days_since_update <= very_recent_days:
                score += very_recent_score
                details.append(f"Very recently updated: {days_since_update} days ago")
            elif days_since_update <= recent_days:
                score += recent_score
                details.append(f"Recently updated: {days_since_update} days ago")
        
        # Check for recent changes in history
        if history_data and 'recent_changes' in history_data:
            recent_changes = history_data['recent_changes']
            if recent_changes.get('summary', {}).get('history_count', 0) > 0:
                score += 3
                details.append("Recent activity detected")
        
        # Cap at configured max score
        score = min(score, self.max_activity_score)
        
        return {
            'score': score,
            'max_possible': self.max_activity_score,
            'details': details,
            'data_available': history_data is not None
        }
    
    def _calculate_confidence(self, score_components: Dict[str, Any], property_data: Dict[str, Any], analysis_data: Dict[str, Any]) -> str:
        """Calculate confidence level based on data availability"""
        data_points = 0
        max_data_points = 5
        
        # Check data availability
        if score_components['price_reductions']['data_available']:
            data_points += 1
        
        if property_data.get('days_on_mls') is not None:
            data_points += 1
        
        if analysis_data.get('distress_signals') or analysis_data.get('motivated_keywords'):
            data_points += 1
        
        if analysis_data.get('special_sale_types'):
            data_points += 1
        
        if score_components['recent_activity']['data_available']:
            data_points += 1
        
        confidence_ratio = data_points / max_data_points
        
        if confidence_ratio >= 0.8:
            return 'high'
        elif confidence_ratio >= 0.6:
            return 'medium'
        else:
            return 'low'
    
    def _generate_reasoning(self, score_components: Dict[str, Any], final_score: float) -> List[str]:
        """Generate human-readable reasoning for the score"""
        reasoning = []
        
        # Add reasoning for each component that contributed
        for component, data in score_components.items():
            if data['score'] > 0:
                if data['details']:
                    reasoning.extend(data['details'])
        
        # Add overall assessment
        if final_score >= 70:
            reasoning.append("High motivated seller score - strong indicators present")
        elif final_score >= 40:
            reasoning.append("Moderate motivated seller score - some indicators present")
        elif final_score >= 20:
            reasoning.append("Low motivated seller score - few indicators present")
        else:
            reasoning.append("Very low motivated seller score - minimal indicators")
        
        return reasoning