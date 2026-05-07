"""
Gap & Demand Analyzer — Stage 4 of the Skillab Turkey Architecture.

Aggregates SkillMatch records across a corpus of job postings to produce
three distinct analytical outputs aligned with the project's scientific goals:

  1. SkillDemandMetric list — per-skill demand ratios and trend signals.
  2. MarketSnapshot         — comprehensive market intelligence report.
  3. H-TURF preparation     — structured data ready for the forecasting model.

Gap Analysis Logic:
  A "gap" in the context of this project has two definitions:
    a. Local Gap:  Skills frequently demanded in Turkish job ads that have
                   no ESCO equivalent (is_mapped=False). These represent
                   emerging or locally-specific competencies absent from the
                   European taxonomy — a key research finding for ESCOPlus.
    b. Supply Gap: ESCO skills that appear in the taxonomy but rarely or
                   never surface in local job postings. Requires separate
                   workforce supply data (not yet available in v1).

Forecasting (H-TURF Preparation):
  H-TURF (Horizon-based Technology Uptake and Role Forecasting) requires:
    - Time-series snapshots of MarketSnapshot objects
    - Skill co-occurrence matrices (built from job-level skill sets)
    - NetworkX graph of skill relationships for causal path analysis
  This module generates all three artefacts and optionally persists them
  to JSON / CSV in the data/snapshots/ directory.

Author: Skillab Turkey Team
Project: EU Horizon Skill Intelligence Hub
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import networkx as nx

from src.utils.logger import get_logger
from src.utils.models import (
    JobDetail,
    MarketSnapshot,
    SkillCategory,
    SkillDemandMetric,
    SkillMatch,
    TrendDirection,
)


class GapAnalyzer:
    """
    Aggregates SkillMatch corpora into market-level intelligence reports.

    Designed to be instantiated once per analysis run and called with the
    full set of SkillMatch lists from every processed job.

    Args:
        snapshot_dir: Directory where MarketSnapshot JSON files are persisted.
                      Creates the directory if it does not exist.
        top_skills_n: Maximum number of skills to include in MarketSnapshot.top_skills.
    """

    def __init__(
        self,
        snapshot_dir: Path = Path("data/snapshots"),
        top_skills_n: int = 30,
    ) -> None:
        self.logger = get_logger(__name__)
        self._snapshot_dir = snapshot_dir
        self._top_skills_n = top_skills_n
        snapshot_dir.mkdir(parents=True, exist_ok=True)

    # -----------------------------------------------------------------------
    # Primary Analysis Entry Point
    # -----------------------------------------------------------------------

    def analyse(
        self,
        keyword: str,
        job_details: List[JobDetail],
        skill_matches_per_job: List[List[SkillMatch]],
    ) -> MarketSnapshot:
        """
        Produce a full MarketSnapshot for a keyword-driven job corpus.

        Args:
            keyword:               The search keyword used to collect the corpus.
            job_details:           JobDetail records from Stage 1.
            skill_matches_per_job: One SkillMatch list per job (output of ESCOMapper).
                                   Length must match job_details.

        Returns:
            MarketSnapshot with ranked demand metrics and gap indicators,
            also persisted to disk as a timestamped JSON file.
        """
        total_jobs = len(job_details)
        self.logger.info(
            "analysis_started",
            keyword=keyword,
            total_jobs=total_jobs,
        )

        flat_matches: List[SkillMatch] = [m for matches in skill_matches_per_job for m in matches]

        demand_counter, category_map, uri_map = self._aggregate_demand(flat_matches, total_jobs)
        unmapped_count = sum(1 for m in flat_matches if not m.is_mapped)

        top_metrics = self._build_demand_metrics(demand_counter, category_map, uri_map, total_jobs)

        total_extracted = len(flat_matches)
        gap_score = (unmapped_count / total_extracted) if total_extracted else 0.0
        digital_ratio = self._category_ratio(top_metrics, SkillCategory.DIGITAL)
        green_ratio = self._category_ratio(top_metrics, SkillCategory.GREEN)

        snapshot = MarketSnapshot(
            keyword=keyword,
            total_jobs_analyzed=total_jobs,
            top_skills=top_metrics[: self._top_skills_n],
            unmapped_skill_count=unmapped_count,
            skill_gap_score=round(gap_score, 4),
            digital_skill_ratio=round(digital_ratio, 4),
            green_skill_ratio=round(green_ratio, 4),
        )

        self._persist_snapshot(snapshot)

        self.logger.info(
            "analysis_completed",
            keyword=keyword,
            top_skills=len(snapshot.top_skills),
            gap_score=snapshot.skill_gap_score,
            digital_ratio=snapshot.digital_skill_ratio,
            green_ratio=snapshot.green_skill_ratio,
            unmapped=unmapped_count,
        )
        return snapshot

    # -----------------------------------------------------------------------
    # Co-occurrence Graph (H-TURF preparation)
    # -----------------------------------------------------------------------

    def build_skill_cooccurrence_graph(
        self,
        skill_matches_per_job: List[List[SkillMatch]],
        min_edge_weight: int = 2,
    ) -> nx.Graph:
        """
        Build a skill co-occurrence graph from the job corpus.

        Nodes  = ESCO preferred labels (or raw normalised names for unmapped).
        Edges  = skills that co-appear in at least one job posting.
        Weight = number of jobs where both skills appear together.

        This graph is the primary input to H-TURF causal discovery and
        can be exported to Gephi, Cytoscape, or any graph analysis tool.

        Args:
            skill_matches_per_job: One SkillMatch list per job.
            min_edge_weight:       Prune edges below this co-occurrence count.

        Returns:
            Weighted undirected nx.Graph.
        """
        self.logger.info(
            "building_cooccurrence_graph",
            job_count=len(skill_matches_per_job),
        )

        edge_weights: Dict[Tuple[str, str], int] = Counter()  # type: ignore[assignment]

        for matches in skill_matches_per_job:
            labels = [
                (
                    m.esco_skill.preferred_label
                    if m.esco_skill
                    else m.extracted_skill.normalized_name
                )
                for m in matches
                if m.is_mapped or m.extracted_skill.confidence_score > 0.7
            ]
            unique_labels = list(set(labels))
            for i in range(len(unique_labels)):
                for j in range(i + 1, len(unique_labels)):
                    key = tuple(sorted([unique_labels[i], unique_labels[j]]))
                    edge_weights[key] += 1  # type: ignore[index]

        graph = nx.Graph()
        for (node_a, node_b), weight in edge_weights.items():
            if weight >= min_edge_weight:
                graph.add_edge(node_a, node_b, weight=weight)

        self.logger.info(
            "cooccurrence_graph_built",
            nodes=graph.number_of_nodes(),
            edges=graph.number_of_edges(),
            min_edge_weight=min_edge_weight,
        )
        return graph

    def export_cooccurrence_matrix(
        self,
        graph: nx.Graph,
        output_path: Path,
    ) -> None:
        """
        Export the co-occurrence graph as a labelled adjacency matrix CSV.

        The matrix format is required by several causal discovery algorithms
        (PC, FCI, GES) in the causal-learn library.

        Args:
            graph:       Output of build_skill_cooccurrence_graph().
            output_path: File path for the resulting CSV.
        """
        nodes = sorted(graph.nodes)
        matrix = nx.to_numpy_array(graph, nodelist=nodes)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        import csv  # local import to avoid top-level overhead

        with open(output_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow([""] + nodes)
            for node, row in zip(nodes, matrix):
                writer.writerow([node] + [round(float(v), 4) for v in row])

        self.logger.info(
            "cooccurrence_matrix_exported",
            path=str(output_path),
            dimensions=f"{len(nodes)}x{len(nodes)}",
        )

    # -----------------------------------------------------------------------
    # Trend Analysis
    # -----------------------------------------------------------------------

    def compare_snapshots(
        self,
        older: MarketSnapshot,
        newer: MarketSnapshot,
    ) -> Dict[str, TrendDirection]:
        """
        Compare two MarketSnapshots to infer per-skill trend directions.

        Used by H-TURF to determine whether a skill's demand is rising,
        stable or declining between analysis periods.

        Args:
            older: Earlier snapshot (e.g., last month).
            newer: More recent snapshot (e.g., this month).

        Returns:
            Mapping of skill_label → TrendDirection.
        """
        old_ratios = {m.skill_label: m.demand_ratio for m in older.top_skills}
        new_ratios = {m.skill_label: m.demand_ratio for m in newer.top_skills}

        trends: Dict[str, TrendDirection] = {}
        for label in set(old_ratios) | set(new_ratios):
            old_r = old_ratios.get(label, 0.0)
            new_r = new_ratios.get(label, 0.0)
            delta = new_r - old_r

            if delta > 0.02:
                trends[label] = TrendDirection.RISING
            elif delta < -0.02:
                trends[label] = TrendDirection.DECLINING
            else:
                trends[label] = TrendDirection.STABLE

        self.logger.info(
            "trend_comparison_complete",
            skills_compared=len(trends),
            rising=sum(1 for t in trends.values() if t == TrendDirection.RISING),
            declining=sum(1 for t in trends.values() if t == TrendDirection.DECLINING),
        )
        return trends

    # -----------------------------------------------------------------------
    # Private Aggregation Helpers
    # -----------------------------------------------------------------------

    def _aggregate_demand(
        self,
        flat_matches: List[SkillMatch],
        total_jobs: int,
    ) -> Tuple[Counter, Dict[str, SkillCategory], Dict[str, Optional[str]]]:
        """Count per-skill occurrence across the corpus."""
        demand_counter: Counter = Counter()
        category_map: Dict[str, SkillCategory] = {}
        uri_map: Dict[str, Optional[str]] = {}

        for match in flat_matches:
            label = (
                match.esco_skill.preferred_label
                if match.esco_skill
                else match.extracted_skill.normalized_name
            )
            demand_counter[label] += 1
            category_map[label] = match.extracted_skill.category
            uri_map[label] = match.esco_skill.concept_uri if match.esco_skill else None

        return demand_counter, category_map, uri_map

    def _build_demand_metrics(
        self,
        demand_counter: Counter,
        category_map: Dict[str, SkillCategory],
        uri_map: Dict[str, Optional[str]],
        total_jobs: int,
    ) -> List[SkillDemandMetric]:
        """Convert raw counters into sorted SkillDemandMetric records."""
        metrics: List[SkillDemandMetric] = []
        for label, count in demand_counter.most_common():
            metrics.append(
                SkillDemandMetric(
                    skill_label=label,
                    esco_uri=uri_map.get(label),
                    category=category_map.get(label, SkillCategory.UNKNOWN),
                    occurrence_count=count,
                    demand_ratio=round(count / total_jobs, 4) if total_jobs else 0.0,
                )
            )
        return metrics

    @staticmethod
    def _category_ratio(metrics: List[SkillDemandMetric], category: SkillCategory) -> float:
        """Fraction of total demand occupied by a specific skill category."""
        total = sum(m.occurrence_count for m in metrics)
        if not total:
            return 0.0
        category_total = sum(m.occurrence_count for m in metrics if m.category == category)
        return category_total / total

    # -----------------------------------------------------------------------
    # Persistence
    # -----------------------------------------------------------------------

    def _persist_snapshot(self, snapshot: MarketSnapshot) -> None:
        """Write snapshot to a timestamped JSON file for longitudinal tracking."""
        timestamp = snapshot.snapshot_date.strftime("%Y%m%d_%H%M%S")
        filename = f"snapshot_{snapshot.keyword}_{timestamp}.json"
        output_path = self._snapshot_dir / filename

        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write(snapshot.model_dump_json(indent=2))

        self.logger.info(
            "snapshot_persisted",
            path=str(output_path),
            keyword=snapshot.keyword,
        )

    @classmethod
    def load_snapshot(cls, path: Path) -> MarketSnapshot:
        """
        Deserialise a previously persisted MarketSnapshot from JSON.

        Args:
            path: Path to a snapshot JSON file.

        Returns:
            Validated MarketSnapshot instance.
        """
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return MarketSnapshot.model_validate(data)
