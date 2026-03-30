"""
Pydantic Data Models for Skillab Turkey Pipeline.

Defines the canonical data contracts for every stage of the pipeline:
  JobListing  → raw listing scraped from a search results page
  JobDetail   → enriched record with full description and structured fields
  ExtractedSkill → NLP-identified skill token with category & confidence
  ESCOSkill   → canonical ESCO skill record loaded from taxonomy data
  SkillMatch  → alignment between an extracted skill and its ESCO counterpart
  SkillDemandMetric → aggregated market-level demand signal for one skill
  MarketSnapshot    → full market intelligence report for a keyword search

All models use strict validation via Pydantic v2. Unknown fields are ignored
to ensure forward compatibility when external data sources add new fields.

Author: Skillab Turkey Team
Project: EU Horizon Skill Intelligence Hub
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator


# ============================================================================
# Enumerations
# ============================================================================

class SkillCategory(str, Enum):
    """ESCO-aligned skill category taxonomy."""

    DIGITAL = "digital"
    GREEN = "green"
    TRANSVERSAL = "transversal"
    TECHNICAL = "technical"
    LANGUAGE = "language"
    UNKNOWN = "unknown"


class TrendDirection(str, Enum):
    """Direction of skill demand over time."""

    RISING = "rising"
    STABLE = "stable"
    DECLINING = "declining"


class MatchMethod(str, Enum):
    """Method used to align an extracted skill to ESCO."""

    EXACT = "exact"
    FUZZY = "fuzzy"
    SEMANTIC = "semantic"
    RULE_BASED = "rule_based"
    UNMAPPED = "unmapped"


# ============================================================================
# Stage 1 — Scraping
# ============================================================================

class JobListing(BaseModel):
    """
    Compact job record extracted from a search-results page.

    Represents the minimum viable data available before fetching the
    individual job detail page. Serves as the input to fetch_job_detail().
    """

    model_config = {"extra": "ignore"}

    title: str = Field(description="Job title as displayed on the listing card.")
    company: str = Field(description="Hiring company name.")
    location: str = Field(description="City or region of the position.")
    url: str = Field(description="Absolute URL to the full job detail page.")
    platform: str = Field(default="kariyer.net", description="Source platform identifier.")
    scraped_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC timestamp when the listing was captured.",
    )

    @field_validator("url", mode="before")
    @classmethod
    def ensure_absolute_url(cls, v: str) -> str:
        if v and v.startswith("/"):
            return f"https://www.kariyer.net{v}"
        return v


class JobDetail(BaseModel):
    """
    Fully enriched job record, populated after fetching the detail page.

    Contains the full description text and structured metadata extracted
    via heuristic parsing. This is the primary input for the NLP pipeline.
    """

    model_config = {"extra": "ignore"}

    listing: JobListing
    full_description: str = Field(
        default="",
        description="Raw full-text job description as extracted from the detail page.",
    )
    required_skills_raw: List[str] = Field(
        default_factory=list,
        description="Skill tokens extracted directly from structured requirement lists.",
    )
    preferred_skills_raw: List[str] = Field(
        default_factory=list,
        description="Skills listed as preferred/optional by the employer.",
    )
    experience_years: Optional[int] = Field(
        default=None,
        description="Minimum years of experience required, if parseable.",
    )
    education_level: Optional[str] = Field(
        default=None,
        description="Required education level (e.g., 'Lisans', 'Yüksek Lisans').",
    )
    salary_range: Optional[str] = Field(
        default=None,
        description="Salary information if disclosed.",
    )
    employment_type: Optional[str] = Field(
        default=None,
        description="Contract type (e.g., 'Tam Zamanlı', 'Hibrit', 'Uzaktan').",
    )
    department: Optional[str] = Field(
        default=None,
        description="Functional department or business unit.",
    )


# ============================================================================
# Stage 2 — Skill Extraction (NLP)
# ============================================================================

class ExtractedSkill(BaseModel):
    """
    A single skill token identified by the NLP extraction pipeline.

    Retains provenance information (source sentence context) to support
    downstream validation and active learning workflows.
    """

    model_config = {"extra": "ignore"}

    raw_text: str = Field(description="Original token as it appeared in the text.")
    normalized_name: str = Field(
        description="Lowercased, whitespace-normalized form used for matching."
    )
    category: SkillCategory = Field(
        default=SkillCategory.UNKNOWN,
        description="ESCO-aligned skill category inferred by the extractor.",
    )
    confidence_score: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Extraction confidence [0, 1]. Higher = more certain.",
    )
    source_context: Optional[str] = Field(
        default=None,
        description="The sentence or clause where the skill token was found.",
    )


# ============================================================================
# Stage 3 — ESCO Mapping
# ============================================================================

class ESCOSkill(BaseModel):
    """
    Canonical ESCO skill record loaded from the official taxonomy dataset.

    Fields correspond to columns in the ESCO v1.2 skills CSV export.
    Only a subset is stored here; the full definition lives in the ESCO API.
    """

    model_config = {"extra": "ignore"}

    concept_uri: str = Field(description="Canonical ESCO concept URI (primary key).")
    preferred_label: str = Field(
        description="ESCO preferred label in the configured language."
    )
    alt_labels: List[str] = Field(
        default_factory=list,
        description="Alternative labels and synonyms from ESCO alt_labels column.",
    )
    skill_type: str = Field(
        description="Either 'skill/competence' or 'knowledge' per ESCO taxonomy.",
    )
    isco_group: Optional[str] = Field(
        default=None,
        description="Primary ISCO-08 occupational group this skill is linked to.",
    )
    pillar: Optional[str] = Field(
        default=None,
        description="ESCOPlus pillar: 'digital', 'green', or None for others.",
    )
    description: Optional[str] = Field(
        default=None,
        description="ESCO skill description text.",
    )


class SkillMatch(BaseModel):
    """
    Alignment record pairing one extracted skill to its ESCO counterpart.

    Produced by the ESCOMapper for every ExtractedSkill regardless of
    whether a match was found. Unmapped skills are recorded with is_mapped=False
    to support gap tracking.
    """

    model_config = {"extra": "ignore"}

    extracted_skill: ExtractedSkill
    esco_skill: Optional[ESCOSkill] = Field(
        default=None,
        description="Matched ESCO concept; None if no match found above threshold.",
    )
    match_score: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Similarity score used for matching.",
    )
    match_method: MatchMethod = Field(
        default=MatchMethod.UNMAPPED,
        description="Algorithm that produced this match.",
    )
    is_mapped: bool = Field(
        default=False,
        description="True only when a confident ESCO match was established.",
    )


# ============================================================================
# Stage 4 — Gap Analysis & Forecasting
# ============================================================================

class SkillDemandMetric(BaseModel):
    """
    Aggregated market-level demand signal for a single ESCO skill.

    Produced by GapAnalyzer over a corpus of JobDetail records.
    """

    model_config = {"extra": "ignore"}

    skill_label: str = Field(
        description="Human-readable label (ESCO preferred label or raw text)."
    )
    esco_uri: Optional[str] = Field(
        default=None,
        description="ESCO concept URI, if the skill was successfully mapped.",
    )
    category: SkillCategory = Field(description="Skill category.")
    occurrence_count: int = Field(
        description="Number of job postings containing this skill.",
        ge=0,
    )
    demand_ratio: float = Field(
        description="Fraction of analysed jobs requiring this skill [0, 1].",
        ge=0.0,
        le=1.0,
    )
    trend_direction: TrendDirection = Field(
        default=TrendDirection.STABLE,
        description="Demand trend inferred from historical or time-series data.",
    )


class MarketSnapshot(BaseModel):
    """
    Comprehensive market intelligence report for a given keyword and date.

    Designed to be persisted to disk (JSON) and compared across time periods
    to power the H-TURF forecasting model.
    """

    model_config = {"extra": "ignore"}

    keyword: str = Field(description="Job search keyword used for this snapshot.")
    total_jobs_analyzed: int = Field(
        description="Number of JobDetail records included in this analysis.",
        ge=0,
    )
    snapshot_date: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC datetime when the snapshot was generated.",
    )
    top_skills: List[SkillDemandMetric] = Field(
        default_factory=list,
        description="Ranked list of demanded skills, highest demand first.",
    )
    unmapped_skill_count: int = Field(
        default=0,
        description="Number of extracted skills with no ESCO match (gap indicator).",
        ge=0,
    )
    skill_gap_score: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description=(
            "Composite gap score: ratio of unmapped skills to total extracted skills. "
            "Higher values indicate more local skills absent from ESCO taxonomy."
        ),
    )
    digital_skill_ratio: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Proportion of demanded skills classified as 'digital'.",
    )
    green_skill_ratio: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Proportion of demanded skills classified as 'green'.",
    )
    platform: str = Field(
        default="kariyer.net",
        description="Data source platform for this snapshot.",
    )
