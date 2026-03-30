"""
ESCO Skill Mapper — Stage 3 of the Skillab Turkey Architecture.

Aligns extracted skills (ExtractedSkill) to the official ESCO v1.2 taxonomy,
producing SkillMatch records that carry ESCO concept URIs, preferred labels,
and ESCOPlus pillar classifications (digital / green).

Matching Strategy (applied in priority order):
  1. Exact match  — normalised string equality against preferred and alt labels.
  2. Fuzzy match  — token-sort ratio via difflib; configurable threshold.
  3. Unmapped     — recorded with is_mapped=False for gap analysis reporting.

Data Source:
  The mapper loads skills from a CSV file (ESCO v1.2 export format).
  Place the file at data/esco/skills_en.csv (columns: conceptUri,
  preferredLabel, altLabels, skillType, description, pillar).

  If the CSV is absent, the mapper initialises with an empty taxonomy and
  logs a clear warning — the pipeline still runs but all skills are unmapped.

ESCO API Integration (future):
  Replace _load_taxonomy() with an async REST client against
  https://esco.ec.europa.eu/api/resource/skill to stay up-to-date without
  manual CSV exports.

Author: Skillab Turkey Team
Project: EU Horizon Skill Intelligence Hub
"""

from __future__ import annotations

import csv
import difflib
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.utils.logger import get_logger
from src.utils.models import (
    ESCOSkill,
    ExtractedSkill,
    MatchMethod,
    SkillMatch,
)


# Default path to the ESCO skills CSV export
_DEFAULT_ESCO_CSV = Path("data/esco/skills_en.csv")

# Minimum fuzzy ratio [0, 100] to accept a match
_DEFAULT_FUZZY_THRESHOLD: int = 82


class ESCOMapper:
    """
    Maps extracted skills to canonical ESCO taxonomy records.

    Maintains two in-memory indices:
      _exact_index:  normalised_label → ESCOSkill  (primary + alt labels)
      _fuzzy_corpus: sorted list of normalised labels (for difflib search)

    All indices are built once at construction time; subsequent map() calls
    are a pure lookup with no I/O overhead.

    Args:
        esco_csv_path:   Path to ESCO skills CSV. Defaults to data/esco/skills_en.csv.
        fuzzy_threshold: Minimum SequenceMatcher ratio (0–100) for fuzzy acceptance.
    """

    def __init__(
        self,
        esco_csv_path: Path = _DEFAULT_ESCO_CSV,
        fuzzy_threshold: int = _DEFAULT_FUZZY_THRESHOLD,
    ) -> None:
        self.logger = get_logger(__name__)
        self._fuzzy_threshold = fuzzy_threshold / 100.0  # normalise to [0, 1]

        self._exact_index: Dict[str, ESCOSkill] = {}
        self._fuzzy_corpus: List[str] = []
        self._corpus_to_skill: Dict[str, ESCOSkill] = {}

        self._load_taxonomy(esco_csv_path)
        self.logger.info(
            "esco_mapper_initialized",
            taxonomy_size=len(self._corpus_to_skill),
            fuzzy_threshold=fuzzy_threshold,
        )

    # -----------------------------------------------------------------------
    # Public Interface
    # -----------------------------------------------------------------------

    def map(self, extracted_skill: ExtractedSkill) -> SkillMatch:
        """
        Attempt to align a single ExtractedSkill to an ESCO concept.

        Args:
            extracted_skill: Output of SkillExtractor.extract().

        Returns:
            SkillMatch with is_mapped=True and an ESCOSkill if a confident
            match was found; is_mapped=False otherwise.
        """
        normalised = extracted_skill.normalized_name

        # --- Priority 1: Exact match ---
        exact, exact_score = self._exact_match(normalised)
        if exact:
            return SkillMatch(
                extracted_skill=extracted_skill,
                esco_skill=exact,
                match_score=exact_score,
                match_method=MatchMethod.EXACT,
                is_mapped=True,
            )

        # --- Priority 2: Fuzzy match ---
        fuzzy, fuzzy_score = self._fuzzy_match(normalised)
        if fuzzy:
            return SkillMatch(
                extracted_skill=extracted_skill,
                esco_skill=fuzzy,
                match_score=fuzzy_score,
                match_method=MatchMethod.FUZZY,
                is_mapped=True,
            )

        # --- Unmapped ---
        self.logger.debug(
            "skill_unmapped",
            skill=normalised,
            reason="no_match_above_threshold",
        )
        return SkillMatch(
            extracted_skill=extracted_skill,
            match_method=MatchMethod.UNMAPPED,
            is_mapped=False,
        )

    def map_batch(
        self, extracted_skills: List[ExtractedSkill]
    ) -> List[SkillMatch]:
        """
        Map a list of ExtractedSkill instances to ESCO concepts.

        Args:
            extracted_skills: Output of SkillExtractor.extract_batch() for one job.

        Returns:
            SkillMatch list, same order as input.
        """
        return [self.map(skill) for skill in extracted_skills]

    # -----------------------------------------------------------------------
    # Taxonomy Loading
    # -----------------------------------------------------------------------

    def _load_taxonomy(self, csv_path: Path) -> None:
        """
        Parse the ESCO skills CSV and build lookup indices.

        Expected CSV columns:
          conceptUri, preferredLabel, altLabels, skillType, description, pillar

        altLabels is a pipe-separated (|) list of synonyms.
        pillar marks "digital" or "green" ESCOPlus skills.
        """
        if not csv_path.is_file():
            self.logger.warning(
                "esco_csv_not_found",
                path=str(csv_path),
                message=(
                    "Taxonomy CSV absent — all skills will be unmapped. "
                    f"Download from https://esco.ec.europa.eu/en/use-esco/download "
                    f"and place at {csv_path}."
                ),
            )
            return

        with open(csv_path, encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            loaded = 0
            skipped = 0

            for row in reader:
                skill = self._row_to_esco_skill(row)
                if skill is None:
                    skipped += 1
                    continue

                self._index_skill(skill)
                loaded += 1

        self._fuzzy_corpus = sorted(self._corpus_to_skill.keys())
        self.logger.info(
            "taxonomy_loaded",
            loaded=loaded,
            skipped=skipped,
            total_index_entries=len(self._exact_index),
        )

    @staticmethod
    def _row_to_esco_skill(row: Dict[str, str]) -> Optional[ESCOSkill]:
        """Convert a CSV row dict to an ESCOSkill model instance."""
        try:
            uri = row.get("conceptUri", "").strip()
            label = row.get("preferredLabel", "").strip()
            if not uri or not label:
                return None

            raw_alts = row.get("altLabels", "")
            alt_labels = [
                a.strip() for a in raw_alts.split("|") if a.strip()
            ]

            return ESCOSkill(
                concept_uri=uri,
                preferred_label=label,
                alt_labels=alt_labels,
                skill_type=row.get("skillType", "skill/competence").strip(),
                pillar=row.get("pillar", "").strip() or None,
                description=row.get("description", "").strip() or None,
            )
        except Exception:
            return None

    def _index_skill(self, skill: ESCOSkill) -> None:
        """Add a skill to both the exact and fuzzy lookup indices."""
        labels = [skill.preferred_label] + skill.alt_labels
        for label in labels:
            normalised = label.strip().lower()
            if not normalised:
                continue
            self._exact_index[normalised] = skill
            self._corpus_to_skill[normalised] = skill

    # -----------------------------------------------------------------------
    # Matching Algorithms
    # -----------------------------------------------------------------------

    def _exact_match(self, normalised: str) -> Tuple[Optional[ESCOSkill], float]:
        """O(1) dictionary lookup against all indexed labels."""
        skill = self._exact_index.get(normalised)
        if skill:
            return skill, 1.0

        # Partial: check if query is a substring of any label (catches "python" → "python programming")
        for label, esco_skill in self._exact_index.items():
            if normalised in label.split() or label in normalised.split():
                return esco_skill, 0.90
        return None, 0.0

    def _fuzzy_match(self, normalised: str) -> Tuple[Optional[ESCOSkill], float]:
        """
        difflib best-match lookup against the full ESCO label corpus.

        Uses get_close_matches for efficiency (returns up to top-N candidates).
        The first match above the threshold is accepted.
        """
        if not self._fuzzy_corpus:
            return None, 0.0

        close = difflib.get_close_matches(
            normalised,
            self._fuzzy_corpus,
            n=1,
            cutoff=self._fuzzy_threshold,
        )
        if close:
            best_label = close[0]
            skill = self._corpus_to_skill[best_label]
            ratio = difflib.SequenceMatcher(
                None, normalised, best_label
            ).ratio()
            return skill, round(ratio, 4)
        return None, 0.0
