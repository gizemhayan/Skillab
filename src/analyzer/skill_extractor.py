"""
NLP Skill Extraction Pipeline — Stage 2 of the Skillab Turkey Architecture.

Identifies skills from raw job description text using a layered strategy:

  Layer 1 — Rule-Based Pattern Matching (spaCy PhraseMatcher)
    Exact and near-exact matches against a curated skill vocabulary.
    Works out-of-the-box without any trained model.

  Layer 2 — Heuristic Regex Patterns
    Catches formatted structures (e.g., "Python 3.x", "AWS (EC2, S3)"),
    bullet-listed skills, and common Turkish skill phrasing patterns.

  Layer 3 — Confidence Scoring
    Each match is assigned a score [0, 1] based on match type and context.

The extractor is language-agnostic at its core — it handles both Turkish and
English job descriptions, which is typical on Kariyer.net.

Output feeds directly into ESCOMapper (Stage 3).

Author: Skillab Turkey Team
Project: EU Horizon Skill Intelligence Hub
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import spacy
from spacy.matcher import PhraseMatcher
from spacy.tokens import Doc

from src.utils.logger import get_logger
from src.utils.models import ExtractedSkill, JobDetail, SkillCategory


# ---------------------------------------------------------------------------
# Category keyword seeds (extensible — values checked against skill labels)
# ---------------------------------------------------------------------------

_CATEGORY_SEEDS: Dict[SkillCategory, List[str]] = {
    SkillCategory.DIGITAL: [
        "python", "java", "javascript", "typescript", "c#", "c++", "rust", "go",
        "sql", "nosql", "mongodb", "postgresql", "mysql", "redis", "elasticsearch",
        "aws", "azure", "gcp", "google cloud", "docker", "kubernetes", "terraform",
        "git", "ci/cd", "devops", "mlops", "machine learning", "deep learning",
        "neural network", "nlp", "computer vision", "data science", "big data",
        "spark", "hadoop", "kafka", "airflow", "etl", "power bi", "tableau",
        "excel", "rest api", "graphql", "microservices", "agile", "scrum",
        "linux", "bash", "powershell", "react", "angular", "vue", "node.js",
        "django", "flask", "fastapi", "spring", ".net", "pandas", "numpy",
        "tensorflow", "pytorch", "scikit-learn", "hugging face", "llm", "rag",
        "blockchain", "cybersecurity", "penetration testing", "siem", "erp",
        "sap", "crm", "salesforce", "jira", "confluence", "figma",
        "makine öğrenmesi", "derin öğrenme", "veri bilimi", "yapay zeka",
        "bulut bilişim", "siber güvenlik", "veri tabanı", "yazılım geliştirme",
    ],
    SkillCategory.GREEN: [
        "sustainability", "sürdürülebilirlik", "renewable energy", "yenilenebilir enerji",
        "carbon footprint", "karbon ayak izi", "esg", "environmental management",
        "çevre yönetimi", "green supply chain", "yeşil tedarik zinciri",
        "circular economy", "döngüsel ekonomi", "iso 14001", "energy efficiency",
        "enerji verimliliği", "life cycle assessment", "lca", "emission reduction",
        "emisyon azaltımı", "solar", "wind energy", "rüzgar enerjisi",
        "güneş enerjisi", "electric vehicle", "elektrikli araç",
        "waste management", "atık yönetimi", "green building", "yeşil bina",
        "leed", "breeam", "climate change", "iklim değişikliği",
    ],
    SkillCategory.TRANSVERSAL: [
        "communication", "iletişim", "teamwork", "takım çalışması", "leadership",
        "liderlik", "problem solving", "problem çözme", "analytical thinking",
        "analitik düşünce", "critical thinking", "eleştirel düşünce",
        "project management", "proje yönetimi", "time management", "zaman yönetimi",
        "adaptability", "uyum yeteneği", "creativity", "yaratıcılık",
        "negotiation", "müzakere", "presentation", "sunum", "decision making",
        "karar verme", "emotional intelligence", "duygusal zeka",
        "cross-functional", "stakeholder management", "change management",
        "değişim yönetimi", "coaching", "mentoring",
    ],
    SkillCategory.LANGUAGE: [
        "english", "ingilizce", "german", "almanca", "french", "fransızca",
        "spanish", "ispanyolca", "arabic", "arapça", "chinese", "çince",
        "japanese", "japonca", "russian", "rusça", "turkish", "türkçe",
        "b2", "c1", "c2", "ielts", "toefl", "goethe",
    ],
}


class SkillExtractor:
    """
    Multi-layer NLP pipeline for extracting skills from job description text.

    Designed to process JobDetail records and return a list of ExtractedSkill
    models ready for ESCO mapping. Handles both Turkish and English text.

    The spaCy model is loaded once at construction time and reused across
    all extract() calls for efficiency in batch processing scenarios.

    Args:
        spacy_model: Name of the spaCy model to load.
                     'en_core_web_sm' works well for mixed TR/EN text.
                     Use 'tr_core_news_sm' if available for TR-first mode.
        custom_skill_path: Optional path to a plain-text file with additional
                           skill terms (one per line). Merged into Layer 1 vocab.
    """

    _MODEL_FALLBACK_CHAIN = ["en_core_web_sm", "en_core_web_md", "xx_ent_wiki_sm"]

    def __init__(
        self,
        spacy_model: str = "en_core_web_sm",
        custom_skill_path: Optional[Path] = None,
    ) -> None:
        self.logger = get_logger(__name__)
        self._nlp = self._load_spacy_model(spacy_model)
        self._matcher = PhraseMatcher(self._nlp.vocab, attr="LOWER")
        self._label_to_category: Dict[str, SkillCategory] = {}
        self._build_phrase_matcher(custom_skill_path)
        self.logger.info(
            "skill_extractor_initialized",
            spacy_model=spacy_model,
            vocab_size=len(self._label_to_category),
        )

    # -----------------------------------------------------------------------
    # Public Interface
    # -----------------------------------------------------------------------

    def extract(self, job_detail: JobDetail) -> List[ExtractedSkill]:
        """
        Run the full extraction pipeline on a single JobDetail record.

        Combines raw skill lists (if present from structured parsing) with
        free-text NLP extraction on the full description. Deduplicates results
        by normalised name, keeping the highest-confidence occurrence.

        Args:
            job_detail: Enriched job record from Stage 1.

        Returns:
            Deduplicated list of ExtractedSkill instances ordered by confidence
            descending.
        """
        self.logger.debug(
            "extraction_started",
            title=job_detail.listing.title,
            description_length=len(job_detail.full_description),
        )

        skills: Dict[str, ExtractedSkill] = {}

        # Layer 0 — Trust structured raw lists from scraper (high confidence)
        for raw in job_detail.required_skills_raw:
            skill = self._skill_from_raw(raw, confidence=0.95)
            if skill:
                self._update_if_better(skills, skill)

        for raw in job_detail.preferred_skills_raw:
            skill = self._skill_from_raw(raw, confidence=0.80)
            if skill:
                self._update_if_better(skills, skill)

        # Layer 1 — PhraseMatcher against full description
        if job_detail.full_description:
            doc = self._nlp(job_detail.full_description[:100_000])  # guard memory
            phrase_skills = self._phrase_match(doc)
            for skill in phrase_skills:
                self._update_if_better(skills, skill)

            # Layer 2 — Regex heuristics on the same text
            regex_skills = self._regex_extract(job_detail.full_description)
            for skill in regex_skills:
                self._update_if_better(skills, skill)

        result = sorted(skills.values(), key=lambda s: s.confidence_score, reverse=True)

        self.logger.debug(
            "extraction_completed",
            title=job_detail.listing.title,
            skills_found=len(result),
        )
        return result

    def extract_batch(self, job_details: List[JobDetail]) -> Dict[str, List[ExtractedSkill]]:
        """
        Extract skills from multiple JobDetail records.

        Args:
            job_details: List of enriched job records.

        Returns:
            Mapping from job URL to its extracted skill list.
        """
        self.logger.info("batch_extraction_started", total_jobs=len(job_details))
        results: Dict[str, List[ExtractedSkill]] = {}

        for detail in job_details:
            results[detail.listing.url] = self.extract(detail)

        self.logger.info(
            "batch_extraction_completed",
            processed=len(results),
            total_skills=sum(len(v) for v in results.values()),
        )
        return results

    # -----------------------------------------------------------------------
    # Vocabulary Construction
    # -----------------------------------------------------------------------

    def _build_phrase_matcher(self, custom_path: Optional[Path]) -> None:
        """Populate the PhraseMatcher with category-labelled skill phrases."""
        all_terms: Dict[str, SkillCategory] = {}

        for category, terms in _CATEGORY_SEEDS.items():
            for term in terms:
                all_terms[term.lower()] = category

        if custom_path and custom_path.is_file():
            with open(custom_path, encoding="utf-8") as fh:
                for line in fh:
                    term = line.strip().lower()
                    if term:
                        all_terms.setdefault(term, SkillCategory.TECHNICAL)
            self.logger.info(
                "custom_vocab_loaded",
                path=str(custom_path),
                total_terms=len(all_terms),
            )

        # Group patterns by category label for PhraseMatcher
        category_patterns: Dict[str, list] = {}
        for term, category in all_terms.items():
            label = category.value
            category_patterns.setdefault(label, [])
            category_patterns[label].append(self._nlp.make_doc(term))
            self._label_to_category[term] = category

        for label, patterns in category_patterns.items():
            self._matcher.add(label, patterns)

    # -----------------------------------------------------------------------
    # Extraction Layers
    # -----------------------------------------------------------------------

    def _skill_from_raw(self, raw_text: str, confidence: float) -> Optional[ExtractedSkill]:
        """Convert a raw structured skill string into an ExtractedSkill."""
        normalised = self._normalise(raw_text)
        if len(normalised) < 2:
            return None
        category = self._infer_category(normalised)
        return ExtractedSkill(
            raw_text=raw_text.strip(),
            normalized_name=normalised,
            category=category,
            confidence_score=confidence,
            source_context="structured_list",
        )

    def _phrase_match(self, doc: Doc) -> List[ExtractedSkill]:
        """Run PhraseMatcher against a spaCy Doc and return ExtractedSkill list."""
        skills: List[ExtractedSkill] = []
        matches = self._matcher(doc)

        for match_id, start, end in matches:
            span = doc[start:end]
            label = self._nlp.vocab.strings[match_id]
            category = SkillCategory(label) if label in SkillCategory._value2member_map_ else SkillCategory.TECHNICAL  # type: ignore[attr-defined]
            sentence = span.sent.text if span.has_annotation("SENT_START") else ""

            skills.append(
                ExtractedSkill(
                    raw_text=span.text,
                    normalized_name=self._normalise(span.text),
                    category=category,
                    confidence_score=0.85,
                    source_context=sentence[:200] if sentence else None,
                )
            )
        return skills

    def _regex_extract(self, text: str) -> List[ExtractedSkill]:
        """
        Heuristic regex extraction for skill formats not covered by the vocabulary.

        Catches:
          - Versioned technologies: "Python 3.10", "React 18"
          - Parenthetical expansions: "AWS (EC2, S3, Lambda)"
          - Certificate patterns: "PMP", "CISSP", "ISO 27001"
        """
        skills: List[ExtractedSkill] = []

        patterns: List[Tuple[str, float]] = [
            # Versioned tech e.g. "Python 3.x", "Node.js 20"
            (r"\b([A-Za-z][A-Za-z0-9\.\+\#\-]{1,20})\s+\d[\d\.x]*\b", 0.70),
            # ALL-CAPS certifications e.g. "AWS", "CCNA", "PMP"
            (r"\b([A-Z]{2,8}(?:[/-][A-Z]{1,6})?)\b", 0.55),
            # ISO standards e.g. "ISO 9001", "ISO/IEC 27001"
            (r"\bISO(?:/IEC)?\s*\d{4,5}(?:[:-]\d+)?\b", 0.75),
        ]

        for pattern, confidence in patterns:
            for match in re.finditer(pattern, text):
                raw = match.group(0).strip()
                normalised = self._normalise(raw)
                if len(normalised) < 2 or normalised in ("the", "and", "for"):
                    continue
                skills.append(
                    ExtractedSkill(
                        raw_text=raw,
                        normalized_name=normalised,
                        category=self._infer_category(normalised),
                        confidence_score=confidence,
                        source_context=None,
                    )
                )
        return skills

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _infer_category(self, normalised: str) -> SkillCategory:
        """Look up the category for a normalised skill string."""
        return self._label_to_category.get(normalised, SkillCategory.UNKNOWN)

    @staticmethod
    def _normalise(text: str) -> str:
        """Lowercase, collapse whitespace, strip punctuation tails."""
        return re.sub(r"\s+", " ", text.strip().lower()).rstrip(".,;:")

    @staticmethod
    def _update_if_better(
        registry: Dict[str, ExtractedSkill], candidate: ExtractedSkill
    ) -> None:
        """Keep the highest-confidence occurrence of each unique skill."""
        key = candidate.normalized_name
        existing = registry.get(key)
        if existing is None or candidate.confidence_score > existing.confidence_score:
            registry[key] = candidate

    def _load_spacy_model(self, model_name: str) -> spacy.Language:
        """
        Load a spaCy model with automatic fallback to smaller alternatives.

        On first run, prompts the user to download the model via
        `python -m spacy download <model>` if none in the fallback chain
        can be loaded.
        """
        candidates = [model_name] + [
            m for m in self._MODEL_FALLBACK_CHAIN if m != model_name
        ]
        for name in candidates:
            try:
                nlp = spacy.load(name, disable=["ner", "parser"])
                # Ensure sentencizer exists for context extraction
                if "sentencizer" not in nlp.pipe_names:
                    nlp.add_pipe("sentencizer")
                self.logger.info("spacy_model_loaded", model=name)
                return nlp
            except OSError:
                self.logger.warning("spacy_model_unavailable", model=name)

        # No model available — build a blank pipeline (rule-based still works)
        self.logger.warning(
            "spacy_model_fallback_blank",
            message=(
                "No spaCy model found. Run: python -m spacy download en_core_web_sm. "
                "Rule-based extraction will still function."
            ),
        )
        nlp = spacy.blank("en")
        nlp.add_pipe("sentencizer")
        return nlp
