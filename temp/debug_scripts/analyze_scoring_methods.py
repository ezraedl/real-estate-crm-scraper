#!/usr/bin/env python
"""
Motivated Seller Scoring Analysis Script

Analyzes top 200 properties with highest motivation scores and recalculates
them using 3 different scoring methods for comparison.
"""

import sys
import os

# Add parent directory to path to ensure imports work (must be before other imports)
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))
sys.path.insert(0, project_root)

import asyncio
import csv
from datetime import datetime
from typing import Dict, List, Any, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from config import settings
from services.text_analyzer import TextAnalyzer


class ScoringMethod1_ExplicitDeclaration:
    """
    Method 1: Explicit Declaration Bonus
    If agent explicitly states "motivated seller" or equivalent, give high base score.
    Then add supporting signals.
    """
    
    def __init__(self, text_analyzer: TextAnalyzer):
        self.text_analyzer = text_analyzer
        self.explicit_declaration_keywords = [
            "motivated seller", "motivated seller!", "highly motivated seller",
            "seller is motivated", "very motivated seller", "extremely motivated seller",
            "must sell", "must sell!", "need to sell", "have to sell",
            "urgent sale", "urgent", "quick sale", "selling fast",
            "desperate seller", "seller motivated"
        ]
        
    def calculate_score(self, property_data: Dict[str, Any], analysis_data: Dict[str, Any], history_data: Optional[Dict[str, Any]]) -> tuple[float, str]:
        """Calculate score using explicit declaration method"""
        score = 0
        reasoning_parts = []
        description = self._extract_description(property_data)
        description_lower = description.lower() if description else ""
        
        # Check for explicit declarations (40-60 point base)
        has_explicit = False
        explicit_keyword_found = None
        for keyword in self.explicit_declaration_keywords:
            if keyword.lower() in description_lower:
                has_explicit = True
                explicit_keyword_found = keyword
                # Strong declarations get 50 points
                if any(strong in description_lower for strong in ["very", "highly", "extremely", "desperate", "must sell"]):
                    score += 50
                    reasoning_parts.append(f"Strong explicit declaration found: '{keyword}' (+50 points)")
                else:
                    score += 40
                    reasoning_parts.append(f"Explicit declaration found: '{keyword}' (+40 points)")
                break
        
        # Supporting signals add to base
        # Price reductions: up to 30 points
        price_score, price_reasoning = self._score_price_reductions(property_data, history_data)
        score += price_score
        if price_reasoning:
            reasoning_parts.append(price_reasoning)
        
        # Days on market: up to 20 points
        dom_score, dom_reasoning = self._score_days_on_market(property_data)
        score += dom_score
        if dom_reasoning:
            reasoning_parts.append(dom_reasoning)
        
        # Additional keywords: up to 10 points
        keyword_score, keyword_reasoning = self._score_additional_keywords(analysis_data)
        if not has_explicit:  # Only add if no explicit declaration
            score += keyword_score
            if keyword_reasoning:
                reasoning_parts.append(keyword_reasoning)
        
        # Special sale types: up to 10 points
        special_score, special_reasoning = self._score_special_sale_types(analysis_data)
        score += special_score
        if special_reasoning:
            reasoning_parts.append(special_reasoning)
        
        final_score = min(100, max(0, score))
        
        # Generate overall assessment
        if not reasoning_parts:
            reasoning_parts.append("No strong motivation indicators found")
        else:
            if final_score >= 70:
                reasoning_parts.append("→ High motivated seller: Strong indicators present")
            elif final_score >= 40:
                reasoning_parts.append("→ Moderate motivated seller: Some indicators present")
            elif final_score >= 20:
                reasoning_parts.append("→ Low motivated seller: Few indicators present")
            else:
                reasoning_parts.append("→ Very low motivated seller: Minimal indicators")
        
        reasoning = " | ".join(reasoning_parts)
        return final_score, reasoning
    
    def _extract_description(self, property_data: Dict[str, Any]) -> str:
        """Extract description from property data"""
        desc_fields = ['description.text', 'description.full_description', 'description', 'remarks', 'public_remarks']
        for field in desc_fields:
            if '.' in field:
                parts = field.split('.')
                value = property_data.get(parts[0], {}).get(parts[1], '') if len(parts) == 2 else ''
            else:
                value = property_data.get(field, '')
            if value and isinstance(value, str) and len(value) > 10:
                return value
        return ''
    
    def _score_price_reductions(self, property_data: Dict[str, Any], history_data: Optional[Dict[str, Any]]) -> tuple[float, str]:
        """Score based on price reductions (0-30 points)"""
        if not history_data or 'price_history' not in history_data:
            return 0, ""
        
        price_history = history_data['price_history']
        if not price_history or len(price_history) < 2:
            return 0, ""
        
        max_score = 0
        reasoning = ""
        for change in price_history[:3]:  # Last 3 changes
            if change.get('change_type') == 'price_change':
                percent_change = change.get('data', {}).get('percent_change', 0)
                if percent_change < 0:
                    abs_change = abs(percent_change)
                    if abs_change >= 10:
                        max_score = max(max_score, 30)
                        reasoning = f"Major price reduction: {abs_change:.1f}% (+30 points)"
                    elif abs_change >= 5:
                        max_score = max(max_score, 20)
                        reasoning = f"Significant price reduction: {abs_change:.1f}% (+20 points)"
                    elif abs_change >= 2:
                        max_score = max(max_score, 10)
                        reasoning = f"Moderate price reduction: {abs_change:.1f}% (+10 points)"
                    else:
                        max_score = max(max_score, 5)
                        reasoning = f"Minor price reduction: {abs_change:.1f}% (+5 points)"
        
        return max_score, reasoning
    
    def _score_days_on_market(self, property_data: Dict[str, Any]) -> tuple[float, str]:
        """Score based on days on market (0-20 points)"""
        days = property_data.get('days_on_mls', 0) or 0
        if days >= 270:
            return 20, f"Very long time on market: {days} days (+20 points)"
        elif days >= 180:
            return 15, f"Long time on market: {days} days (+15 points)"
        elif days >= 120:
            return 12, f"Moderate time on market: {days} days (+12 points)"
        elif days >= 90:
            return 8, f"Short time on market: {days} days (+8 points)"
        elif days >= 60:
            return 4, f"Minimal time on market: {days} days (+4 points)"
        return 0, ""
    
    def _score_additional_keywords(self, analysis_data: Dict[str, Any]) -> tuple[float, str]:
        """Score additional keywords (0-10 points)"""
        distress = analysis_data.get('distress_signals', [])
        keywords = analysis_data.get('motivated_keywords', [])
        
        total_signals = len(distress) + len(keywords)
        score = min(10, total_signals * 2)
        if score > 0:
            reasoning = f"Found {total_signals} motivation keywords/signals (+{score:.0f} points)"
            return score, reasoning
        return 0, ""
    
    def _score_special_sale_types(self, analysis_data: Dict[str, Any]) -> tuple[float, str]:
        """Score special sale types (0-10 points)"""
        sale_types = analysis_data.get('special_sale_types', [])
        if 'auction' in sale_types or 'reo' in sale_types:
            type_str = '/'.join([st for st in sale_types if st in ['auction', 'reo']])
            return 10, f"Special sale type: {type_str} (+10 points)"
        elif 'probate' in sale_types:
            return 8, f"Special sale type: probate (+8 points)"
        elif 'short_sale' in sale_types:
            return 6, f"Special sale type: short_sale (+6 points)"
        elif 'as_is' in sale_types:
            return 4, f"Special sale type: as_is (+4 points)"
        return 0, ""


class ScoringMethod2_RevisedWeights:
    """
    Method 2: Revised Weighted Scoring
    Restructured weights where explicit keywords can contribute more to final score.
    """
    
    def __init__(self, text_analyzer: TextAnalyzer):
        self.text_analyzer = text_analyzer
        self.signal_weights = {
            'explicit_declarations': 50,  # "motivated seller" etc.
            'price_reductions': 30,
            'days_on_market': 15,
            'supporting_keywords': 10,
            'special_sale_type': 5
        }
        self.explicit_keywords = [
            "motivated seller", "must sell", "urgent sale", "quick sale",
            "desperate seller", "need to sell", "have to sell"
        ]
        
    def calculate_score(self, property_data: Dict[str, Any], analysis_data: Dict[str, Any], history_data: Optional[Dict[str, Any]]) -> tuple[float, str]:
        """Calculate score using revised weighted method"""
        description = self._extract_description(property_data)
        description_lower = description.lower() if description else ""
        reasoning_parts = []
        
        # Explicit declarations: 0-50 points
        explicit_score = 0
        explicit_keyword = None
        for keyword in self.explicit_keywords:
            if keyword.lower() in description_lower:
                explicit_score = 50
                explicit_keyword = keyword
                reasoning_parts.append(f"Explicit declaration: '{keyword}' (weight: 50)")
                break
        
        # Price reductions: 0-30 points
        price_score, price_reasoning = self._score_price_reductions(property_data, history_data)
        if price_reasoning:
            reasoning_parts.append(f"Price reduction (weight: 30): {price_reasoning.split('(')[0].strip()}")
        
        # Days on market: 0-15 points
        dom_score, dom_reasoning = self._score_days_on_market(property_data)
        if dom_reasoning:
            reasoning_parts.append(f"Days on market (weight: 15): {dom_reasoning.split('(')[0].strip()}")
        
        # Supporting keywords: 0-10 points
        keyword_score, keyword_reasoning = self._score_supporting_keywords(analysis_data)
        if keyword_reasoning:
            reasoning_parts.append(f"Keywords (weight: 10): {keyword_reasoning.split('(')[0].strip()}")
        
        # Special sale types: 0-5 points
        special_score, special_reasoning = self._score_special_sale_types(analysis_data)
        if special_reasoning:
            reasoning_parts.append(f"Special sale (weight: 5): {special_reasoning.split('(')[0].strip()}")
        
        # Weighted total
        total = (
            explicit_score * (self.signal_weights['explicit_declarations'] / 50) +
            price_score * (self.signal_weights['price_reductions'] / 30) +
            dom_score * (self.signal_weights['days_on_market'] / 15) +
            keyword_score * (self.signal_weights['supporting_keywords'] / 10) +
            special_score * (self.signal_weights['special_sale_type'] / 5)
        )
        
        final_score = min(100, max(0, total))
        
        if not reasoning_parts:
            reasoning_parts.append("No strong motivation indicators found")
        else:
            if final_score >= 70:
                reasoning_parts.append("→ High motivated seller")
            elif final_score >= 40:
                reasoning_parts.append("→ Moderate motivated seller")
            elif final_score >= 20:
                reasoning_parts.append("→ Low motivated seller")
            else:
                reasoning_parts.append("→ Very low motivated seller")
        
        reasoning = " | ".join(reasoning_parts)
        return final_score, reasoning
    
    def _extract_description(self, property_data: Dict[str, Any]) -> str:
        """Extract description from property data"""
        desc_fields = ['description.text', 'description.full_description', 'description', 'remarks', 'public_remarks']
        for field in desc_fields:
            if '.' in field:
                parts = field.split('.')
                value = property_data.get(parts[0], {}).get(parts[1], '') if len(parts) == 2 else ''
            else:
                value = property_data.get(field, '')
            if value and isinstance(value, str) and len(value) > 10:
                return value
        return ''
    
    def _score_price_reductions(self, property_data: Dict[str, Any], history_data: Optional[Dict[str, Any]]) -> tuple[float, str]:
        """Score based on price reductions (0-30 points)"""
        if not history_data or 'price_history' not in history_data:
            return 0, ""
        
        price_history = history_data['price_history']
        if not price_history or len(price_history) < 2:
            return 0, ""
        
        max_score = 0
        reasoning = ""
        for change in price_history[:3]:
            if change.get('change_type') == 'price_change':
                percent_change = change.get('data', {}).get('percent_change', 0)
                if percent_change < 0:
                    abs_change = abs(percent_change)
                    if abs_change >= 10:
                        max_score = max(max_score, 30)
                        reasoning = f"Major reduction: {abs_change:.1f}%"
                    elif abs_change >= 5:
                        max_score = max(max_score, 20)
                        reasoning = f"Significant reduction: {abs_change:.1f}%"
                    elif abs_change >= 2:
                        max_score = max(max_score, 10)
                        reasoning = f"Moderate reduction: {abs_change:.1f}%"
                    else:
                        max_score = max(max_score, 5)
                        reasoning = f"Minor reduction: {abs_change:.1f}%"
        
        return max_score, reasoning
    
    def _score_days_on_market(self, property_data: Dict[str, Any]) -> tuple[float, str]:
        """Score based on days on market (0-15 points)"""
        days = property_data.get('days_on_mls', 0) or 0
        if days >= 270:
            return 15, f"Very long: {days} days"
        elif days >= 180:
            return 12, f"Long: {days} days"
        elif days >= 120:
            return 10, f"Moderate: {days} days"
        elif days >= 90:
            return 7, f"Short: {days} days"
        elif days >= 60:
            return 4, f"Minimal: {days} days"
        return 0, ""
    
    def _score_supporting_keywords(self, analysis_data: Dict[str, Any]) -> tuple[float, str]:
        """Score supporting keywords (0-10 points)"""
        distress = analysis_data.get('distress_signals', [])
        keywords = analysis_data.get('motivated_keywords', [])
        
        total_signals = len(distress) + len(keywords)
        score = min(10, total_signals * 1.5)
        if score > 0:
            return score, f"{total_signals} keywords/signals found"
        return 0, ""
    
    def _score_special_sale_types(self, analysis_data: Dict[str, Any]) -> tuple[float, str]:
        """Score special sale types (0-5 points)"""
        sale_types = analysis_data.get('special_sale_types', [])
        if 'auction' in sale_types or 'reo' in sale_types:
            type_str = '/'.join([st for st in sale_types if st in ['auction', 'reo']])
            return 5, f"Type: {type_str}"
        elif 'probate' in sale_types:
            return 4, "Type: probate"
        elif 'short_sale' in sale_types:
            return 3, "Type: short_sale"
        elif 'as_is' in sale_types:
            return 2, "Type: as_is"
        return 0, ""


class ScoringMethod3_Multiplicative:
    """
    Method 3: Multiplicative Scoring
    Base score from all signals, then apply multipliers for strong signal combinations.
    """
    
    def __init__(self, text_analyzer: TextAnalyzer):
        self.text_analyzer = text_analyzer
        self.explicit_keywords = [
            "motivated seller", "must sell", "urgent sale", "quick sale",
            "desperate seller", "need to sell", "have to sell"
        ]
        
    def calculate_score(self, property_data: Dict[str, Any], analysis_data: Dict[str, Any], history_data: Optional[Dict[str, Any]]) -> tuple[float, str]:
        """Calculate score using multiplicative method"""
        description = self._extract_description(property_data)
        description_lower = description.lower() if description else ""
        reasoning_parts = []
        
        # Base score from all components (0-60 base)
        base_score = 0
        
        # Explicit declarations: 0-25 points
        has_explicit = False
        explicit_keyword = None
        for keyword in self.explicit_keywords:
            if keyword.lower() in description_lower:
                has_explicit = True
                explicit_keyword = keyword
                base_score += 25
                reasoning_parts.append(f"Explicit declaration: '{keyword}' (+25 base)")
                break
        
        # Price reductions: 0-20 points
        price_score, price_reasoning = self._score_price_reductions(property_data, history_data)
        base_score += price_score
        if price_reasoning:
            reasoning_parts.append(f"Price reduction: {price_reasoning} (+{price_score} base)")
        
        # Days on market: 0-10 points
        dom_score, dom_reasoning = self._score_days_on_market(property_data)
        base_score += dom_score
        if dom_reasoning:
            reasoning_parts.append(f"Days on market: {dom_reasoning} (+{dom_score} base)")
        
        # Keywords: 0-5 points
        keyword_score, keyword_reasoning = self._score_keywords(analysis_data)
        base_score += keyword_score
        if keyword_reasoning:
            reasoning_parts.append(f"Keywords: {keyword_reasoning} (+{keyword_score:.0f} base)")
        
        # Apply multipliers for strong combinations
        multiplier = 1.0
        multiplier_reason = ""
        
        # Check if we have strong combinations
        has_price_reduction = price_score > 0
        has_long_dom = dom_score >= 4  # 90+ days
        has_special_type = len(analysis_data.get('special_sale_types', [])) > 0
        
        # Multiplier combinations
        if has_explicit and has_price_reduction and price_score >= 10:
            multiplier = 1.8
            multiplier_reason = "Strong combo: explicit + major price reduction (1.8x)"
        elif has_explicit and has_price_reduction:
            multiplier = 1.5
            multiplier_reason = "Combo: explicit + price reduction (1.5x)"
        elif has_explicit and has_long_dom:
            multiplier = 1.4
            multiplier_reason = "Combo: explicit + long DOM (1.4x)"
        elif has_explicit and has_special_type:
            multiplier = 1.3
            multiplier_reason = "Combo: explicit + special sale (1.3x)"
        elif has_price_reduction and has_long_dom:
            multiplier = 1.3
            multiplier_reason = "Combo: price reduction + long DOM (1.3x)"
        
        if multiplier > 1.0:
            reasoning_parts.append(f"Multiplier applied: {multiplier_reason}")
        
        final_score = base_score * multiplier
        
        if not reasoning_parts:
            reasoning_parts.append("No strong motivation indicators found")
        else:
            reasoning_parts.append(f"Base: {base_score:.0f}, Final: {final_score:.1f} (x{multiplier:.1f})")
            if final_score >= 70:
                reasoning_parts.append("→ High motivated seller")
            elif final_score >= 40:
                reasoning_parts.append("→ Moderate motivated seller")
            elif final_score >= 20:
                reasoning_parts.append("→ Low motivated seller")
            else:
                reasoning_parts.append("→ Very low motivated seller")
        
        reasoning = " | ".join(reasoning_parts)
        return min(100, max(0, final_score)), reasoning
    
    def _extract_description(self, property_data: Dict[str, Any]) -> str:
        """Extract description from property data"""
        desc_fields = ['description.text', 'description.full_description', 'description', 'remarks', 'public_remarks']
        for field in desc_fields:
            if '.' in field:
                parts = field.split('.')
                value = property_data.get(parts[0], {}).get(parts[1], '') if len(parts) == 2 else ''
            else:
                value = property_data.get(field, '')
            if value and isinstance(value, str) and len(value) > 10:
                return value
        return ''
    
    def _score_price_reductions(self, property_data: Dict[str, Any], history_data: Optional[Dict[str, Any]]) -> tuple[float, str]:
        """Score based on price reductions (0-20 points)"""
        if not history_data or 'price_history' not in history_data:
            return 0, ""
        
        price_history = history_data['price_history']
        if not price_history or len(price_history) < 2:
            return 0, ""
        
        max_score = 0
        reasoning = ""
        for change in price_history[:3]:
            if change.get('change_type') == 'price_change':
                percent_change = change.get('data', {}).get('percent_change', 0)
                if percent_change < 0:
                    abs_change = abs(percent_change)
                    if abs_change >= 10:
                        max_score = max(max_score, 20)
                        reasoning = f"{abs_change:.1f}% (major)"
                    elif abs_change >= 5:
                        max_score = max(max_score, 15)
                        reasoning = f"{abs_change:.1f}% (significant)"
                    elif abs_change >= 2:
                        max_score = max(max_score, 8)
                        reasoning = f"{abs_change:.1f}% (moderate)"
                    else:
                        max_score = max(max_score, 4)
                        reasoning = f"{abs_change:.1f}% (minor)"
        
        return max_score, reasoning
    
    def _score_days_on_market(self, property_data: Dict[str, Any]) -> tuple[float, str]:
        """Score based on days on market (0-10 points)"""
        days = property_data.get('days_on_mls', 0) or 0
        if days >= 270:
            return 10, f"{days} days (very long)"
        elif days >= 180:
            return 8, f"{days} days (long)"
        elif days >= 120:
            return 6, f"{days} days (moderate)"
        elif days >= 90:
            return 4, f"{days} days (short)"
        elif days >= 60:
            return 2, f"{days} days (minimal)"
        return 0, ""
    
    def _score_keywords(self, analysis_data: Dict[str, Any]) -> tuple[float, str]:
        """Score keywords (0-5 points)"""
        distress = analysis_data.get('distress_signals', [])
        keywords = analysis_data.get('motivated_keywords', [])
        
        total_signals = len(distress) + len(keywords)
        score = min(5, total_signals * 0.8)
        if score > 0:
            return score, f"{total_signals} signals"
        return 0, ""


async def get_history_data(db, property_id: str) -> Dict[str, Any]:
    """Get history data for a property"""
    try:
        history_collection = db.property_history
        
        # Get price history
        price_history = []
        async for entry in history_collection.find(
            {"property_id": property_id, "change_type": "price_change"}
        ).sort("timestamp", -1).limit(10):
            # Convert ObjectId to string for JSON serialization
            if '_id' in entry:
                entry['_id'] = str(entry['_id'])
            price_history.append(entry)
        
        # Get recent changes summary
        recent_changes = {"summary": {"history_count": len(price_history)}}
        
        return {
            'price_history': price_history,
            'recent_changes': recent_changes
        }
    except Exception as e:
        print(f"Error getting history for {property_id}: {e}")
        return {}


async def analyze_properties():
    """Main analysis function"""
    client = AsyncIOMotorClient(settings.MONGODB_URI)
    
    # Get database name from URI
    db_name = settings.MONGODB_URI.split('/')[-1].split('?')[0] if '/' in settings.MONGODB_URI else 'mls_scraper'
    if not db_name:
        db_name = 'mls_scraper'
    db = client[db_name]
    
    # Count total properties with non-zero scores for calculating mid point
    print("Counting properties with motivated seller scores (excluding zeros)...")
    query_non_zero = {
        "enrichment.motivated_seller.score": {
            "$exists": True,
            "$gt": 0  # Exclude zero scores
        }
    }
    total_count_non_zero = await db.properties.count_documents(query_non_zero)
    print(f"Total properties with non-zero scores: {total_count_non_zero}")
    
    # Get top 200 properties by current motivated seller score (can include zeros)
    print("Fetching top 200 properties by motivated seller score...")
    top_properties = []
    query_top = {"enrichment.motivated_seller.score": {"$exists": True}}
    async for prop in db.properties.find(query_top).sort("enrichment.motivated_seller.score", -1).limit(200):
        top_properties.append((prop, "Top 200"))
    
    # Get bottom 200 properties (excluding zero scores)
    print("Fetching bottom 200 properties (excluding zero scores)...")
    bottom_properties = []
    async for prop in db.properties.find(query_non_zero).sort("enrichment.motivated_seller.score", 1).limit(200):
        bottom_properties.append((prop, "Bottom 200"))
    
    # Get mid 200 properties (around the median, excluding zero scores)
    print("Fetching mid 200 properties (around median, excluding zeros)...")
    mid_properties = []
    if total_count_non_zero > 400:  # Only if we have more than 400 non-zero properties
        # Calculate skip amount to get to middle (minus 100 to center 200 properties around median)
        skip_amount = max(0, (total_count_non_zero // 2) - 100)
        async for prop in db.properties.find(query_non_zero).sort("enrichment.motivated_seller.score", -1).skip(skip_amount).limit(200):
            mid_properties.append((prop, "Mid 200"))
    else:
        print(f"Not enough non-zero properties to fetch mid 200 (need more than 400, found {total_count_non_zero})")
    
    # Combine all groups
    properties = top_properties + mid_properties + bottom_properties
    
    print(f"Found {len(top_properties)} top properties, {len(mid_properties)} mid properties, and {len(bottom_properties)} bottom properties to analyze")
    print(f"Total properties: {len(properties)}")
    
    if len(properties) == 0:
        print("No properties with motivated seller scores found. Exiting.")
        client.close()
        return
    
    # Initialize scoring methods
    text_analyzer = TextAnalyzer()
    method1 = ScoringMethod1_ExplicitDeclaration(text_analyzer)
    method2 = ScoringMethod2_RevisedWeights(text_analyzer)
    method3 = ScoringMethod3_Multiplicative(text_analyzer)
    
    # Prepare results
    results = []
    
    for i, (prop, category) in enumerate(properties, 1):
        if i % 10 == 0:
            print(f"Processing property {i}/{len(properties)}... ({category})")
        
        try:
            property_id = prop.get('property_id', 'N/A')
            enrichment = prop.get('enrichment', {})
            motivated_seller = enrichment.get('motivated_seller', {})
            current_score = motivated_seller.get('score', 0)
            
            # Get current method's reasoning
            current_reasoning = motivated_seller.get('reasoning', [])
            if isinstance(current_reasoning, list):
                current_reasoning_text = " | ".join(current_reasoning) if current_reasoning else "No reasoning available"
            else:
                current_reasoning_text = str(current_reasoning) if current_reasoning else "No reasoning available"
            
            # Extract data needed for scoring
            description = prop.get('description', {})
            if isinstance(description, dict):
                description_text = description.get('text') or description.get('full_description') or ''
            else:
                description_text = str(description) if description else ''
            
            # Get text analysis (re-analyze or use existing)
            if description_text:
                analysis_data = text_analyzer.analyze_property_description(description_text)
            else:
                analysis_data = {'distress_signals': [], 'motivated_keywords': [], 'special_sale_types': []}
            
            # Get history data
            history_data = await get_history_data(db, property_id)
            
            # Calculate scores with all 3 methods (now returns tuple: score, reasoning)
            score1, reasoning1 = method1.calculate_score(prop, analysis_data, history_data)
            score2, reasoning2 = method2.calculate_score(prop, analysis_data, history_data)
            score3, reasoning3 = method3.calculate_score(prop, analysis_data, history_data)
            
            # Extract additional info for comparison table
            days_on_market = prop.get('days_on_mls', 0) or 0
            has_price_reduction = len(history_data.get('price_history', [])) > 0
            
            # Check for explicit keywords in description
            desc_lower = description_text.lower()
            has_explicit = any(kw in desc_lower for kw in [
                "motivated seller", "must sell", "urgent sale", "quick sale"
            ])
            
            # Count distress signals
            distress_count = len(analysis_data.get('distress_signals', []))
            keyword_count = len(analysis_data.get('motivated_keywords', []))
            
            # Get address
            address_data = prop.get('address', {})
            if isinstance(address_data, dict):
                address = address_data.get('formatted_address') or address_data.get('street', 'N/A')
            else:
                address = str(address_data) if address_data else 'N/A'
            
            results.append({
                'category': category,
                'property_id': property_id,
                'address': address,
                'current_score': current_score,
                'current_reasoning': current_reasoning_text,
                'method1_explicit_declaration': round(score1, 1),
                'method1_reasoning': reasoning1,
                'method2_revised_weights': round(score2, 1),
                'method2_reasoning': reasoning2,
                'method3_multiplicative': round(score3, 1),
                'method3_reasoning': reasoning3,
                'days_on_market': days_on_market,
                'has_price_reduction': 'Yes' if has_price_reduction else 'No',
                'has_explicit_keyword': 'Yes' if has_explicit else 'No',
                'distress_signals_count': distress_count,
                'motivated_keywords_count': keyword_count,
                'special_sale_types': ', '.join(analysis_data.get('special_sale_types', [])) or 'None',
                'description_preview': (description_text[:100] + '...') if description_text else 'N/A'
            })
            
        except Exception as e:
            print(f"Error processing property {property_id}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Write to CSV
    output_file = f"temp/debug_scripts/motivated_seller_scoring_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    print(f"\nWriting results to {output_file}...")
    
    if results:
        fieldnames = [
            'category', 'property_id', 'address', 'current_score', 'current_reasoning',
            'method1_explicit_declaration', 'method1_reasoning',
            'method2_revised_weights', 'method2_reasoning',
            'method3_multiplicative', 'method3_reasoning',
            'days_on_market', 'has_price_reduction', 'has_explicit_keyword',
            'distress_signals_count', 'motivated_keywords_count', 'special_sale_types',
            'description_preview'
        ]
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\nAnalysis complete! Results written to {output_file}")
        print(f"Total properties analyzed: {len(results)}")
        
        # Separate results by category
        top_results = [r for r in results if r['category'] == 'Top 200']
        mid_results = [r for r in results if r['category'] == 'Mid 200']
        bottom_results = [r for r in results if r['category'] == 'Bottom 200']
        
        # Print summary statistics - Overall
        print("\n=== Overall Summary Statistics (All Properties) ===")
        avg_current = sum(r['current_score'] for r in results) / len(results)
        max_current = max(r['current_score'] for r in results)
        min_current = min(r['current_score'] for r in results)
        print(f"Current Score - Avg: {avg_current:.1f}, Max: {max_current:.1f}, Min: {min_current:.1f}")
        
        avg_method1 = sum(r['method1_explicit_declaration'] for r in results) / len(results)
        max_method1 = max(r['method1_explicit_declaration'] for r in results)
        min_method1 = min(r['method1_explicit_declaration'] for r in results)
        print(f"Method 1 (Explicit) - Avg: {avg_method1:.1f}, Max: {max_method1:.1f}, Min: {min_method1:.1f}")
        
        avg_method2 = sum(r['method2_revised_weights'] for r in results) / len(results)
        max_method2 = max(r['method2_revised_weights'] for r in results)
        min_method2 = min(r['method2_revised_weights'] for r in results)
        print(f"Method 2 (Revised Weights) - Avg: {avg_method2:.1f}, Max: {max_method2:.1f}, Min: {min_method2:.1f}")
        
        avg_method3 = sum(r['method3_multiplicative'] for r in results) / len(results)
        max_method3 = max(r['method3_multiplicative'] for r in results)
        min_method3 = min(r['method3_multiplicative'] for r in results)
        print(f"Method 3 (Multiplicative) - Avg: {avg_method3:.1f}, Max: {max_method3:.1f}, Min: {min_method3:.1f}")
        
        # Print summary statistics - Top 200
        if top_results:
            print("\n=== Top 200 Properties Statistics ===")
            avg_current_top = sum(r['current_score'] for r in top_results) / len(top_results)
            avg_method1_top = sum(r['method1_explicit_declaration'] for r in top_results) / len(top_results)
            avg_method2_top = sum(r['method2_revised_weights'] for r in top_results) / len(top_results)
            avg_method3_top = sum(r['method3_multiplicative'] for r in top_results) / len(top_results)
            print(f"Current Score - Avg: {avg_current_top:.1f}")
            print(f"Method 1 - Avg: {avg_method1_top:.1f}")
            print(f"Method 2 - Avg: {avg_method2_top:.1f}")
            print(f"Method 3 - Avg: {avg_method3_top:.1f}")
        
        # Print summary statistics - Mid 200
        if mid_results:
            print("\n=== Mid 200 Properties Statistics ===")
            avg_current_mid = sum(r['current_score'] for r in mid_results) / len(mid_results)
            avg_method1_mid = sum(r['method1_explicit_declaration'] for r in mid_results) / len(mid_results)
            avg_method2_mid = sum(r['method2_revised_weights'] for r in mid_results) / len(mid_results)
            avg_method3_mid = sum(r['method3_multiplicative'] for r in mid_results) / len(mid_results)
            print(f"Current Score - Avg: {avg_current_mid:.1f}")
            print(f"Method 1 - Avg: {avg_method1_mid:.1f}")
            print(f"Method 2 - Avg: {avg_method2_mid:.1f}")
            print(f"Method 3 - Avg: {avg_method3_mid:.1f}")
        
        # Print summary statistics - Bottom 200
        if bottom_results:
            print("\n=== Bottom 200 Properties Statistics ===")
            avg_current_bottom = sum(r['current_score'] for r in bottom_results) / len(bottom_results)
            avg_method1_bottom = sum(r['method1_explicit_declaration'] for r in bottom_results) / len(bottom_results)
            avg_method2_bottom = sum(r['method2_revised_weights'] for r in bottom_results) / len(bottom_results)
            avg_method3_bottom = sum(r['method3_multiplicative'] for r in bottom_results) / len(bottom_results)
            print(f"Current Score - Avg: {avg_current_bottom:.1f}")
            print(f"Method 1 - Avg: {avg_method1_bottom:.1f}")
            print(f"Method 2 - Avg: {avg_method2_bottom:.1f}")
            print(f"Method 3 - Avg: {avg_method3_bottom:.1f}")
        
        # Count how many reach high scores with each method (overall)
        print("\n=== Properties Scoring >= Threshold (All Properties) ===")
        for threshold in [70, 80, 90, 100]:
            count_current = sum(1 for r in results if r['current_score'] >= threshold)
            count_method1 = sum(1 for r in results if r['method1_explicit_declaration'] >= threshold)
            count_method2 = sum(1 for r in results if r['method2_revised_weights'] >= threshold)
            count_method3 = sum(1 for r in results if r['method3_multiplicative'] >= threshold)
            print(f"Score >= {threshold}: Current: {count_current}, Method 1: {count_method1}, Method 2: {count_method2}, Method 3: {count_method3}")
        
        # Count low scores (for bottom properties analysis)
        print("\n=== Properties Scoring <= Threshold (All Properties) ===")
        for threshold in [10, 20, 30]:
            count_current = sum(1 for r in results if r['current_score'] <= threshold)
            count_method1 = sum(1 for r in results if r['method1_explicit_declaration'] <= threshold)
            count_method2 = sum(1 for r in results if r['method2_revised_weights'] <= threshold)
            count_method3 = sum(1 for r in results if r['method3_multiplicative'] <= threshold)
            print(f"Score <= {threshold}: Current: {count_current}, Method 1: {count_method1}, Method 2: {count_method2}, Method 3: {count_method3}")
    else:
        print("No results to write!")
    
    client.close()


if __name__ == "__main__":
    asyncio.run(analyze_properties())

