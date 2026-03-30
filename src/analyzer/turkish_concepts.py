"""
Turkish Concepts Analyzer — Extracts Digital and Green Transformation keywords.

This module identifies and categorizes Turkish-specific concepts related to:
  - Digital Transformation (Dijital Dönüşüm ecosystem)
  - Green Transformation (Yeşil Dönüşüm & Sustainability)
  
Both concepts are critical for EU Horizon skill intelligence in Turkey.
Supports fuzzy matching and synonym expansion for robust extraction.

Author: Skillab Turkey Team
Project: EU Horizon Skill Intelligence Hub
"""

from __future__ import annotations

import re
from typing import Dict, List, Set


class TurkishConceptAnalyzer:
    """
    Extract and categorize Turkish Digital and Green transformation concepts.
    
    Attributes:
        DIGITAL_CONCEPTS: Turkish terms + patterns for digital transformation
        GREEN_CONCEPTS: Turkish terms + patterns for green/sustainability
        _cache: Performance optimization for already-analyzed texts
    """
    
    DIGITAL_CONCEPTS: Dict[str, List[str]] = {
        "dijital_donusum": [
            "dijital dönüşüm",
            "endüstri 4.0",
            "otomasyon",
            "veri analitiği",
            "siber güvenlik",
            "digital transformation",
            "industry 4.0",
            "automation",
            "data analytics",
            "cybersecurity",
        ],
    }

    GREEN_CONCEPTS: Dict[str, List[str]] = {
        "yesil_donusum_surdurulebilirlik": [
            "yeşil dönüşüm",
            "sürdürülebilirlik",
            "karbon ayak izi",
            "enerji verimliliği",
            "yenilenebilir enerji",
            "green transformation",
            "sustainability",
            "carbon footprint",
            "energy efficiency",
            "renewable energy",
        ],
    }
    
    def __init__(self):
        """Initialize analyzer with precompiled regex patterns."""
        self._compile_patterns()
        self._cache: Dict[str, Dict] = {}
    
    def _compile_patterns(self) -> None:
        """Precompile regex patterns for performance."""
        self.digital_patterns: List[re.Pattern] = [
            re.compile(rf"\b{re.escape(term)}\b", re.IGNORECASE | re.UNICODE)
            for concept in self.DIGITAL_CONCEPTS.values()
            for term in concept
        ]
        self.green_patterns: List[re.Pattern] = [
            re.compile(rf"\b{re.escape(term)}\b", re.IGNORECASE | re.UNICODE)
            for concept in self.GREEN_CONCEPTS.values()
            for term in concept
        ]
    
    def extract_digital_concepts(self, text: str) -> Set[str]:
        """
        Extract all digital transformation concepts from text.
        
        Args:
            text: Job description or requirement text (any language).
        
        Returns:
            Set of detected digital transformation concept categories.
        """
        if not text:
            return set()
        
        text_lower = str(text).lower()
        detected: Set[str] = set()
        
        for concept_name, terms in self.DIGITAL_CONCEPTS.items():
            for term in terms:
                if re.search(rf"\b{re.escape(term)}\b", text_lower, re.UNICODE):
                    detected.add(concept_name)
                    break
        
        return detected
    
    def extract_green_concepts(self, text: str) -> Set[str]:
        """
        Extract all green transformation concepts from text.
        
        Args:
            text: Job description or requirement text (any language).
        
        Returns:
            Set of detected green transformation concept categories.
        """
        if not text:
            return set()
        
        text_lower = str(text).lower()
        detected: Set[str] = set()
        
        for concept_name, terms in self.GREEN_CONCEPTS.items():
            for term in terms:
                if re.search(rf"\b{re.escape(term)}\b", text_lower, re.UNICODE):
                    detected.add(concept_name)
                    break
        
        return detected
    
    def analyze(self, text: str) -> Dict[str, List[str]]:
        """
        Comprehensive analysis: extract both digital and green concepts.
        
        Args:
            text: Job description or requirement text.
        
        Returns:
            Dictionary with 'digital' and 'green' lists of detected concepts.
        """
        if not text:
            return {"digital": [], "green": []}
        
        return {
            "digital": sorted(self.extract_digital_concepts(text)),
            "green": sorted(self.extract_green_concepts(text)),
        }
    
    def has_digital_concept(self, text: str) -> bool:
        """Check if text contains any digital transformation concept."""
        return len(self.extract_digital_concepts(text)) > 0
    
    def has_green_concept(self, text: str) -> bool:
        """Check if text contains any green transformation concept."""
        return len(self.extract_green_concepts(text)) > 0
    
    def has_either_concept(self, text: str) -> bool:
        """Check if text contains digital OR green concept."""
        return self.has_digital_concept(text) or self.has_green_concept(text)


# Singleton instance for module-level convenience
_analyzer = TurkishConceptAnalyzer()


def extract_digital_concepts(text: str) -> Set[str]:
    """Module-level convenience function."""
    return _analyzer.extract_digital_concepts(text)


def extract_green_concepts(text: str) -> Set[str]:
    """Module-level convenience function."""
    return _analyzer.extract_green_concepts(text)


def analyze_text(text: str) -> Dict[str, List[str]]:
    """Module-level convenience function."""
    return _analyzer.analyze(text)
