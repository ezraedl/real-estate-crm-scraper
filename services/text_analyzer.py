"""
Text Analyzer Service

Analyzes property description text to extract keywords, big-ticket items,
distress signals, and motivated seller indicators using regex patterns.
"""

import re
from typing import Dict, List, Any, Optional, Set
import logging

logger = logging.getLogger(__name__)

class TextAnalyzer:
    """Analyzes property descriptions for structured insights"""
    
    def __init__(self):
        # Big-ticket items patterns
        self.big_ticket_patterns = {
            'roof': {
                'keywords': [
                    r'\b(?:new|updated|replaced|recent)\s+(?:roof|roofing)\b',
                    r'\b(?:roof|roofing)\s+(?:new|updated|replaced|recent)\b',
                    r'\b(?:shingle|tile|metal)\s+(?:roof|roofing)\b',
                    r'\b(?:roof|roofing)\s+(?:shingle|tile|metal)\b'
                ],
                'condition_keywords': {
                    'excellent': [r'\b(?:new|brand new|recently installed|just installed)\b'],
                    'good': [r'\b(?:updated|replaced|recent)\b'],
                    'fair': [r'\b(?:decent|okay|acceptable)\b'],
                    'poor': [r'\b(?:old|worn|needs|requires)\s+(?:replacement|repair)\b']
                },
                'age_indicators': {
                    'new': [r'\b(?:202[0-9]|2024|2023|2022|2021|2020)\b'],
                    'recent': [r'\b(?:201[5-9]|2019|2018|2017|2016|2015)\b'],
                    'older': [r'\b(?:201[0-4]|2014|2013|2012|2011|2010)\b'],
                    'old': [r'\b(?:200[0-9]|199[0-9]|older than|over)\b']
                }
            },
            'hvac': {
                'keywords': [
                    r'\b(?:hvac|heating|cooling|air conditioning|a\/c|ac)\b',
                    r'\b(?:furnace|heat pump|central air|central heat)\b',
                    r'\b(?:new|updated|replaced|recent)\s+(?:hvac|heating|cooling)\b'
                ],
                'condition_keywords': {
                    'excellent': [r'\b(?:new|brand new|recently installed|just installed)\b'],
                    'good': [r'\b(?:updated|replaced|recent|efficient)\b'],
                    'fair': [r'\b(?:decent|okay|acceptable|working)\b'],
                    'poor': [r'\b(?:old|worn|needs|requires)\s+(?:replacement|repair)\b']
                },
                'age_indicators': {
                    'new': [r'\b(?:202[0-9]|2024|2023|2022|2021|2020)\b'],
                    'recent': [r'\b(?:201[5-9]|2019|2018|2017|2016|2015)\b'],
                    'older': [r'\b(?:201[0-4]|2014|2013|2012|2011|2010)\b'],
                    'old': [r'\b(?:200[0-9]|199[0-9]|older than|over)\b']
                }
            },
            'plumbing': {
                'keywords': [
                    r'\b(?:plumbing|pipes|water lines|sewer|drain)\b',
                    r'\b(?:new|updated|replaced|recent)\s+(?:plumbing|pipes)\b',
                    r'\b(?:copper|pvc|pex)\s+(?:pipes|plumbing)\b'
                ],
                'condition_keywords': {
                    'excellent': [r'\b(?:new|brand new|recently installed|just installed)\b'],
                    'good': [r'\b(?:updated|replaced|recent)\b'],
                    'fair': [r'\b(?:decent|okay|acceptable)\b'],
                    'poor': [r'\b(?:old|worn|needs|requires)\s+(?:replacement|repair)\b']
                },
                'age_indicators': {
                    'new': [r'\b(?:202[0-9]|2024|2023|2022|2021|2020)\b'],
                    'recent': [r'\b(?:201[5-9]|2019|2018|2017|2016|2015)\b'],
                    'older': [r'\b(?:201[0-4]|2014|2013|2012|2011|2010)\b'],
                    'old': [r'\b(?:200[0-9]|199[0-9]|older than|over)\b']
                }
            },
            'electrical': {
                'keywords': [
                    r'\b(?:electrical|wiring|electrical system|panel|breaker)\b',
                    r'\b(?:new|updated|replaced|recent)\s+(?:electrical|wiring)\b',
                    r'\b(?:200 amp|100 amp|electrical panel)\b'
                ],
                'condition_keywords': {
                    'excellent': [r'\b(?:new|brand new|recently installed|just installed)\b'],
                    'good': [r'\b(?:updated|replaced|recent)\b'],
                    'fair': [r'\b(?:decent|okay|acceptable)\b'],
                    'poor': [r'\b(?:old|worn|needs|requires)\s+(?:replacement|repair)\b']
                },
                'age_indicators': {
                    'new': [r'\b(?:202[0-9]|2024|2023|2022|2021|2020)\b'],
                    'recent': [r'\b(?:201[5-9]|2019|2018|2017|2016|2015)\b'],
                    'older': [r'\b(?:201[0-4]|2014|2013|2012|2011|2010)\b'],
                    'old': [r'\b(?:200[0-9]|199[0-9]|older than|over)\b']
                }
            },
            'water_heater': {
                'keywords': [
                    r'\b(?:water heater|hot water heater|tankless|tank)\b',
                    r'\b(?:new|updated|replaced|recent)\s+(?:water heater|hot water)\b'
                ],
                'condition_keywords': {
                    'excellent': [r'\b(?:new|brand new|recently installed|just installed)\b'],
                    'good': [r'\b(?:updated|replaced|recent)\b'],
                    'fair': [r'\b(?:decent|okay|acceptable)\b'],
                    'poor': [r'\b(?:old|worn|needs|requires)\s+(?:replacement|repair)\b']
                },
                'age_indicators': {
                    'new': [r'\b(?:202[0-9]|2024|2023|2022|2021|2020)\b'],
                    'recent': [r'\b(?:201[5-9]|2019|2018|2017|2016|2015)\b'],
                    'older': [r'\b(?:201[0-4]|2014|2013|2012|2011|2010)\b'],
                    'old': [r'\b(?:200[0-9]|199[0-9]|older than|over)\b']
                }
            }
        }
        
        # Distress signals
        self.distress_signals = [
            r'\b(?:as.?is|as is|sold as is)\b',
            r'\b(?:needs|requires|needs work|fixer upper)\b',
            r'\b(?:handyman|handy man|diy|do it yourself)\b',
            r'\b(?:cash only|cash sale)\b',
            r'\b(?:auction|foreclosure|bank owned|reo)\b',
            r'\b(?:estate sale|probate|inherited)\b',
            r'\b(?:short sale|short sell)\b',
            r'\b(?:must sell|quick sale|urgent)\b',
            r'\b(?:motivated seller|motivated)\b',
            r'\b(?:price reduced|reduced price|price drop)\b',
            r'\b(?:bring offers|all offers|best offer)\b',
            r'\b(?:no inspections|no inspection)\b',
            r'\b(?:sold where is|sold where-is)\b'
        ]
        
        # Motivated seller keywords
        self.motivated_keywords = [
            r'\b(?:must sell|quick sale|urgent|immediate)\b',
            r'\b(?:motivated seller|motivated|desperate)\b',
            r'\b(?:relocating|job transfer|divorce|death)\b',
            r'\b(?:empty house|vacant|unoccupied)\b',
            r'\b(?:bring all offers|bring offers|all offers)\b',
            r'\b(?:price reduced|reduced price|price drop)\b',
            r'\b(?:quick close|fast closing)\b'
        ]
        
        # Special sale types
        self.special_sale_types = {
            'auction': [r'\b(?:auction|auction sale|going to auction)\b'],
            'reo': [r'\b(?:reo|bank owned|foreclosure|foreclosed)\b'],
            'probate': [r'\b(?:probate|estate sale|inherited|deceased)\b'],
            'short_sale': [r'\b(?:short sale|short sell|short sale approved)\b'],
            'as_is': [r'\b(?:as.?is|as is|sold as is)\b']
        }
        
        # Creative financing keywords - expanded based on database analysis
        self.creative_financing_keywords = {
            'owner_finance': [
                r'\b(?:owner\s+financing|owner\s+financing\s+available|owner\s+financing\s+options?)\b',
                r'\b(?:seller\s+will\s+carry|seller\s+carry|owner\s+carry)\b',
                r'\b(?:contract\s+for\s+deed|land\s+contract|installment\s+contract)\b',
                r'\b(?:no\s+bank\s+qualifying|no\s+qualifying|without\s+bank|no\s+bank\s+approval)\b',
                r'\b(?:seller\s+(?:can|will|may)\s+offer\s+(?:owner\s+)?financing|seller\s+(?:offers?|provides?)\s+financing)\b',
                r'\b(?:financing\s+available|owner\s+financing\s+terms?)\b'
            ],
            'subject_to': [
                r'\b(?:subject\s+to\s+existing\s+mortgage|subject\s+to\s+mortgage|subject-to|sub2)\b',
                r'\b(?:take\s+over\s+payments?|assume\s+payments?|takeover\s+payments?)\b',
                r'\b(?:assumable\s+loan|assumable\s+mortgage|assumable)\b',
                r'\b(?:assume\s+(?:loan|mortgage|payments?))\b'
            ],
            'lease_option': [
                r'\b(?:rent\s+to\s+own|rent-to-own|rental\s+purchase)\b',
                r'\b(?:lease\s+option|lease\s+with\s+option|lease\s+purchase\s+option)\b',
                r'\b(?:lease\s+purchase|lease-purchase|lease\s+with\s+purchase)\b'
            ],
            'wrap_mortgage': [
                r'\b(?:wrap\s+around|wraparound|wrap-around)\s+(?:mortgage|loan|note)\b',
                r'\b(?:wrap\s+mortgage|wrap\s+loan|wrap\s+note)\b',
                r'\b(?:wraparound\s+mortgage|wraparound\s+loan)\b'
            ],
            'distressed': [
                r'\b(?:motivated\s+seller|motivated|seller\s+motivated)\b',
                r'\b(?:must\s+sell|quick\s+sale|urgent\s+sale)\b',
                r'\b(?:as\s+is|as-is|sold\s+as\s+is)\b',
                r'\b(?:foreclosure|foreclosed|pre-foreclosure)\b'
            ],
            'partnership': [
                r'\b(?:open\s+to\s+JV|open\s+to\s+joint\s+venture|JV\s+opportunity)\b',
                r'\b(?:joint\s+venture|joint\s+venture\s+opportunity|JV)\b',
                r'\b(?:partner\s+wanted|looking\s+for\s+partner|partner\s+opportunity)\b',
                r'\b(?:investor\s+friendly|wholesale|deal\s+for\s+investor)\b'
            ]
        }
    
    def analyze_property_description(self, description: str) -> Dict[str, Any]:
        """
        Analyze property description for structured insights
        
        Args:
            description: Property description text
            
        Returns:
            Dictionary containing analysis results
        """
        if not description:
            return self._empty_analysis()
        
        # Normalize text for analysis
        text = description.lower().strip()
        
        analysis = {
            'big_ticket_items': self._analyze_big_ticket_items(text),
            'distress_signals': self._find_distress_signals(text),
            'motivated_keywords': self._find_motivated_keywords(text),
            'special_sale_types': self._identify_special_sale_types(text),
            'creative_financing': self._find_creative_financing_keywords(text),
            'keywords_found': self._extract_keywords(text),
            'summary': {
                'has_distress_signals': False,
                'has_motivated_keywords': False,
                'has_special_sale_type': False,
                'has_creative_financing': False,
                'big_ticket_items_count': 0,
                'total_keywords': 0
            }
        }
        
        # Update summary
        analysis['summary']['has_distress_signals'] = len(analysis['distress_signals']) > 0
        analysis['summary']['has_motivated_keywords'] = len(analysis['motivated_keywords']) > 0
        analysis['summary']['has_special_sale_type'] = len(analysis['special_sale_types']) > 0
        analysis['summary']['has_creative_financing'] = any(
            category['found'] for category in analysis['creative_financing'].values()
        )
        analysis['summary']['big_ticket_items_count'] = len([item for item in analysis['big_ticket_items'].values() if item['found']])
        analysis['summary']['total_keywords'] = len(analysis['keywords_found'])
        
        return analysis
    
    def _analyze_big_ticket_items(self, text: str) -> Dict[str, Any]:
        """Analyze big-ticket items in the text"""
        items = {}
        
        for item_name, patterns in self.big_ticket_patterns.items():
            item_analysis = {
                'found': False,
                'condition': None,
                'estimated_age': None,
                'keywords_found': [],
                'confidence': 0
            }
            
            # Check for keywords
            for pattern in patterns['keywords']:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    item_analysis['found'] = True
                    item_analysis['keywords_found'].extend(matches)
                    item_analysis['confidence'] += 1
            
            # Check condition
            for condition, condition_patterns in patterns['condition_keywords'].items():
                for pattern in condition_patterns:
                    if re.search(pattern, text, re.IGNORECASE):
                        item_analysis['condition'] = condition
                        item_analysis['confidence'] += 1
                        break
                if item_analysis['condition']:
                    break
            
            # Check age indicators
            for age, age_patterns in patterns['age_indicators'].items():
                for pattern in age_patterns:
                    if re.search(pattern, text, re.IGNORECASE):
                        item_analysis['estimated_age'] = age
                        item_analysis['confidence'] += 1
                        break
                if item_analysis['estimated_age']:
                    break
            
            items[item_name] = item_analysis
        
        return items
    
    def _find_distress_signals(self, text: str) -> List[str]:
        """Find distress signals in the text"""
        signals = []
        for pattern in self.distress_signals:
            matches = re.findall(pattern, text, re.IGNORECASE)
            signals.extend(matches)
        return list(set(signals))  # Remove duplicates
    
    def _find_motivated_keywords(self, text: str) -> List[str]:
        """Find motivated seller keywords in the text"""
        keywords = []
        for pattern in self.motivated_keywords:
            matches = re.findall(pattern, text, re.IGNORECASE)
            keywords.extend(matches)
        return list(set(keywords))  # Remove duplicates
    
    def _identify_special_sale_types(self, text: str) -> List[str]:
        """Identify special sale types"""
        sale_types = []
        for sale_type, patterns in self.special_sale_types.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    sale_types.append(sale_type)
                    break
        return sale_types
    
    def _find_creative_financing_keywords(self, text: str) -> Dict[str, Any]:
        """Find creative financing keywords in the text"""
        financing_found = {}
        
        for category, patterns in self.creative_financing_keywords.items():
            keywords_matched = []
            for pattern in patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    keywords_matched.extend(matches)
            
            if keywords_matched:
                financing_found[category] = {
                    'found': True,
                    'keywords_found': list(set(keywords_matched)),  # Remove duplicates
                    'count': len(keywords_matched)
                }
            else:
                financing_found[category] = {
                    'found': False,
                    'keywords_found': [],
                    'count': 0
                }
        
        return financing_found
    
    def _extract_keywords(self, text: str) -> List[str]:
        """Extract all relevant keywords found in the text"""
        all_keywords = []
        
        # Extract from big-ticket items
        for item_patterns in self.big_ticket_patterns.values():
            for pattern in item_patterns['keywords']:
                matches = re.findall(pattern, text, re.IGNORECASE)
                all_keywords.extend(matches)
        
        # Extract distress signals
        all_keywords.extend(self._find_distress_signals(text))
        
        # Extract motivated keywords
        all_keywords.extend(self._find_motivated_keywords(text))
        
        # Extract creative financing keywords
        creative_financing = self._find_creative_financing_keywords(text)
        for category, data in creative_financing.items():
            if data['found']:
                all_keywords.extend(data['keywords_found'])
        
        return list(set(all_keywords))  # Remove duplicates
    
    def _empty_analysis(self) -> Dict[str, Any]:
        """Return empty analysis structure"""
        return {
            'big_ticket_items': {
                'roof': {'found': False, 'condition': None, 'estimated_age': None, 'keywords_found': [], 'confidence': 0},
                'hvac': {'found': False, 'condition': None, 'estimated_age': None, 'keywords_found': [], 'confidence': 0},
                'plumbing': {'found': False, 'condition': None, 'estimated_age': None, 'keywords_found': [], 'confidence': 0},
                'electrical': {'found': False, 'condition': None, 'estimated_age': None, 'keywords_found': [], 'confidence': 0},
                'water_heater': {'found': False, 'condition': None, 'estimated_age': None, 'keywords_found': [], 'confidence': 0}
            },
            'distress_signals': [],
            'motivated_keywords': [],
            'special_sale_types': [],
            'creative_financing': {
                'owner_finance': {'found': False, 'keywords_found': [], 'count': 0},
                'subject_to': {'found': False, 'keywords_found': [], 'count': 0},
                'lease_option': {'found': False, 'keywords_found': [], 'count': 0},
                'wrap_mortgage': {'found': False, 'keywords_found': [], 'count': 0},
                'distressed': {'found': False, 'keywords_found': [], 'count': 0},
                'partnership': {'found': False, 'keywords_found': [], 'count': 0}
            },
            'keywords_found': [],
            'summary': {
                'has_distress_signals': False,
                'has_motivated_keywords': False,
                'has_special_sale_type': False,
                'has_creative_financing': False,
                'big_ticket_items_count': 0,
                'total_keywords': 0
            }
        }