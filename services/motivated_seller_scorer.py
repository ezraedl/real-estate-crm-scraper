"""
Motivated Seller Scorer Service

Calculates a motivated seller score (0-100) based on various signals
like price reductions, days on market, keywords, and sale type indicators.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class MotivatedSellerScorer:
    """Calculates motivated seller scores based on multiple signals"""
    
    def __init__(self):
        # Signal weights (total should be ~100)
        self.signal_weights = {
            'price_reductions': 30,      # Price drops are strong signals
            'days_on_market': 25,        # Long time on market
            'distress_keywords': 20,     # Keywords like "as-is", "must sell"
            'special_sale_type': 15,     # Auction, REO, probate, etc.
            'recent_activity': 10        # Recent changes/updates
        }
        
        # Price reduction scoring thresholds
        self.price_reduction_thresholds = {
            'major': {'percent': 10, 'score': 30},      # 10%+ reduction
            'significant': {'percent': 5, 'score': 20}, # 5-10% reduction
            'moderate': {'percent': 2, 'score': 10},    # 2-5% reduction
            'minor': {'percent': 0, 'score': 5}         # Any reduction
        }
        
        # Days on market scoring thresholds
        self.dom_thresholds = {
            'very_long': {'days': 180, 'score': 25},    # 6+ months
            'long': {'days': 90, 'score': 20},          # 3-6 months
            'moderate': {'days': 60, 'score': 15},      # 2-3 months
            'short': {'days': 30, 'score': 10},         # 1-2 months
            'very_short': {'days': 0, 'score': 0}       # < 1 month
        }
        
        # Distress keyword scoring
        self.distress_keyword_scores = {
            'high': ['must sell', 'quick sale', 'urgent', 'motivated seller', 'as-is', 'cash only'],
            'medium': ['price reduced', 'bring offers', 'handyman', 'fixer upper'],
            'low': ['needs work', 'handy man', 'diy']
        }
        
        # Special sale type scoring
        self.special_sale_scores = {
            'auction': 15,
            'reo': 15,
            'probate': 12,
            'short_sale': 10,
            'as_is': 8
        }
    
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
            total_score = 0
            total_weight = 0
            
            for signal, weight in self.signal_weights.items():
                component_score = score_components[signal]['score']
                total_score += component_score * weight
                total_weight += weight
            
            # Normalize to 0-100 scale
            final_score = min(100, max(0, total_score / total_weight * 100))
            
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
        
        # Score distress signals
        for signal in distress_signals:
            signal_lower = signal.lower()
            if any(keyword in signal_lower for keyword in self.distress_keyword_scores['high']):
                score += 8
                details.append(f"High distress signal: '{signal}'")
            elif any(keyword in signal_lower for keyword in self.distress_keyword_scores['medium']):
                score += 5
                details.append(f"Medium distress signal: '{signal}'")
            elif any(keyword in signal_lower for keyword in self.distress_keyword_scores['low']):
                score += 2
                details.append(f"Low distress signal: '{signal}'")
        
        # Score motivated keywords
        for keyword in motivated_keywords:
            keyword_lower = keyword.lower()
            if any(motivated in keyword_lower for motivated in self.distress_keyword_scores['high']):
                score += 6
                details.append(f"Motivated keyword: '{keyword}'")
            elif any(motivated in keyword_lower for motivated in self.distress_keyword_scores['medium']):
                score += 3
                details.append(f"Motivated keyword: '{keyword}'")
        
        # Cap at max possible
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
            
            if days_since_update <= 7:
                score += 5
                details.append(f"Recently updated: {days_since_update} days ago")
            elif days_since_update <= 14:
                score += 3
                details.append(f"Recently updated: {days_since_update} days ago")
        
        # Check for recent changes in history
        if history_data and 'recent_changes' in history_data:
            recent_changes = history_data['recent_changes']
            if recent_changes.get('summary', {}).get('history_count', 0) > 0:
                score += 3
                details.append("Recent activity detected")
        
        return {
            'score': score,
            'max_possible': 10,
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