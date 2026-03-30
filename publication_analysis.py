"""
Publication-ready labour market skill analysis for Skillab Turkiye.

This script:
1) Reads data/skill_inventory.xlsx
2) Builds publication-quality charts
3) Writes PNG outputs to outputs/
4) Writes tabular summaries to outputs/final_analysis_report.xlsx

Usage:
    c:/Users/Gizem/Desktop/skillab_turkiye/.venv/Scripts/python.exe publication_analysis.py
"""

from __future__ import annotations

import itertools
import re
from pathlib import Path
from typing import Dict, List, Sequence, Set, Tuple

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import seaborn as sns
from matplotlib.colors import LinearSegmentedColormap

INPUT_XLSX = Path("data/skill_inventory.xlsx")
OUTPUT_DIR = Path("outputs")
REPORT_XLSX = OUTPUT_DIR / "final_analysis_report.xlsx"

TOP_N_SKILLS = 15
NETWORK_MIN_EDGE_WEIGHT = 2

LABOUR_MARKET_SKILLS: Set[str] = {
    "python",
    "javascript",
    "html",
    "css",
    "java",
    "sql",
    "api",
    "git",
    "github",
    "bootstrap",
    "react",
    "angular",
    "node.js",
    "docker",
    "kubernetes",
    "aws",
    "azure",
    "gcp",
    "ci/cd",
    "jenkins",
}

SOCIOECONOMIC_SKILLS: Set[str] = {
    "communication",
    "teamwork",
    "leadership",
    "problem solving",
    "creativity",
    "analytical thinking",
    "time management",
    "agile",
    "scrum",
    "kanban",
}

GREEN_ENERGY_POLICY_SKILLS: Set[str] = {
    "sustainability",
    "renewable energy",
    "circular economy",
}

DIGITAL_COORDINATION_SKILLS: Set[str] = {
    "cloud computing",
    "cybersecurity",
    "digital liaison",
}

FOCUS_COLOR_MAP: Dict[str, str] = {
    "Labour Market": "#328CC1",
    "Socioeconomic Skills": "#5ABF90",
    "Green & Energy Policy": "#6BAF45",
    "Digital Coordination": "#6C63FF",
}

SMART_CURRICULUM_TARGETS: Dict[str, Dict[str, float]] = {
    "Programming Foundations": {
        "python": 1.0,
        "java": 0.8,
        "sql": 0.9,
        "git": 0.8,
        "github": 0.6,
    },
    "Web and API": {
        "javascript": 1.0,
        "html": 0.9,
        "css": 0.9,
        "api": 1.0,
        "bootstrap": 0.5,
    },
    "Cloud and DevOps": {
        "docker": 0.8,
        "kubernetes": 0.7,
        "aws": 0.8,
        "azure": 0.7,
        "ci/cd": 0.7,
    },
    "Socioeconomic Skills": {
        "agile": 0.8,
        "scrum": 0.7,
        "communication": 1.0,
        "teamwork": 1.0,
        "problem solving": 1.0,
    },
}


def get_focus_keyword_vocabulary() -> List[str]:
    """Return complete keyword vocabulary used for focus classification."""
    all_keywords = (
        LABOUR_MARKET_SKILLS
        | SOCIOECONOMIC_SKILLS
        | GREEN_ENERGY_POLICY_SKILLS
        | DIGITAL_COORDINATION_SKILLS
    )
    return sorted(all_keywords)


def configure_publication_style() -> None:
    """Configure high-DPI, clean publication style settings."""
    sns.set_theme(style="whitegrid", context="paper")
    plt.rcParams.update(
        {
            "figure.dpi": 150,
            "savefig.dpi": 400,
            "savefig.bbox": "tight",
            "font.family": "DejaVu Sans",
            "font.size": 11,
            "axes.titlesize": 15,
            "axes.titleweight": "bold",
            "axes.labelsize": 12,
            "axes.edgecolor": "#1f2a44",
            "axes.linewidth": 0.8,
            "grid.alpha": 0.2,
            "grid.linestyle": "--",
        }
    )


def ensure_output_dir(path: Path) -> None:
    """Create output folder if missing."""
    path.mkdir(parents=True, exist_ok=True)


def normalize_text(text: str) -> str:
    """Lowercase and remove punctuation for robust matching."""
    lowered = str(text).lower()
    no_punct = re.sub(r"[^a-z0-9\s./+-]", " ", lowered)
    return re.sub(r"\s+", " ", no_punct).strip()


def load_input_data(path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load Tum_Ilanlar and Yetenek_Analizi sheets from workbook."""
    if not path.exists():
        raise FileNotFoundError(f"Input workbook not found: {path}")

    xls = pd.ExcelFile(path)
    if "Tum_Ilanlar" not in xls.sheet_names or "Yetenek_Analizi" not in xls.sheet_names:
        raise ValueError("Workbook must contain 'Tum_Ilanlar' and 'Yetenek_Analizi' sheets.")

    postings_df = pd.read_excel(path, sheet_name="Tum_Ilanlar")
    frequency_df = pd.read_excel(path, sheet_name="Yetenek_Analizi")

    if not {"Yetenek", "Frekans"}.issubset(frequency_df.columns):
        raise ValueError("Yetenek_Analizi must include Yetenek and Frekans columns.")

    frequency_df = frequency_df.copy()
    frequency_df["skill"] = frequency_df["Yetenek"].astype(str).map(normalize_text)
    frequency_df["frequency"] = pd.to_numeric(frequency_df["Frekans"], errors="coerce").fillna(0).astype(int)
    frequency_df = frequency_df[["skill", "frequency"]].sort_values("frequency", ascending=False)

    return postings_df, frequency_df


def classify_skill_focus(skill: str) -> str:
    """Classify each skill under Labour Market or Socioeconomic focus."""
    if skill in GREEN_ENERGY_POLICY_SKILLS:
        return "Green & Energy Policy"
    if skill in DIGITAL_COORDINATION_SKILLS:
        return "Digital Coordination"
    if skill in SOCIOECONOMIC_SKILLS:
        return "Socioeconomic Skills"
    if skill in LABOUR_MARKET_SKILLS:
        return "Labour Market"
    return "Labour Market"


def compute_keyword_frequencies_from_postings(
    postings_df: pd.DataFrame,
    keywords: Sequence[str],
) -> pd.DataFrame:
    """Compute keyword frequencies directly from postings text columns."""
    if postings_df.empty:
        return pd.DataFrame({"skill": list(keywords), "frequency": [0] * len(keywords)})

    candidate_columns = [
        "Hard_Skills",
        "Soft_Skills",
        "Yetkinlik_Kumeleri",
        "required_skills_raw",
        "preferred_skills_raw",
        "full_description",
        "cleaned_description",
        "title",
        "department",
    ]
    existing_columns = [col for col in candidate_columns if col in postings_df.columns]
    if not existing_columns:
        return pd.DataFrame({"skill": list(keywords), "frequency": [0] * len(keywords)})

    combined_text = postings_df[existing_columns].fillna("").astype(str).agg(" ".join, axis=1)
    normalized_series = combined_text.map(normalize_text)

    frequencies = {
        keyword: int(
            normalized_series.str.count(rf"\\b{re.escape(keyword)}\\b", flags=re.IGNORECASE).sum()
        )
        for keyword in keywords
    }
    return pd.DataFrame({"skill": list(frequencies.keys()), "frequency": list(frequencies.values())})


def build_classified_frequency_table(
    postings_df: pd.DataFrame,
    frequency_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build focus-classified frequency table including extended categories."""
    table = frequency_df.copy()
    focus_keywords = get_focus_keyword_vocabulary()
    posting_freq_df = compute_keyword_frequencies_from_postings(postings_df, focus_keywords)

    merged = (
        table.groupby("skill", as_index=False)["frequency"].sum()
        .merge(
            posting_freq_df.groupby("skill", as_index=False)["frequency"].sum(),
            on="skill",
            how="outer",
            suffixes=("_sheet", "_postings"),
        )
        .fillna(0)
    )
    merged["frequency"] = (
        merged["frequency_sheet"].astype(int) + merged["frequency_postings"].astype(int)
    )
    merged = merged[["skill", "frequency"]]

    required_focus_rows = pd.DataFrame({"skill": focus_keywords})
    table = required_focus_rows.merge(merged, on="skill", how="left").fillna({"frequency": 0})
    table["frequency"] = table["frequency"].astype(int)
    table["focus"] = table["skill"].map(classify_skill_focus)
    return table.sort_values(["frequency", "skill"], ascending=[False, True]).reset_index(drop=True)


def extract_row_skill_set(row: pd.Series, canonical_skills: Sequence[str]) -> Set[str]:
    """Extract skill set from row text using canonical skill vocabulary."""
    candidate_columns = [
        "Hard_Skills",
        "Soft_Skills",
        "Yetkinlik_Kumeleri",
        "required_skills_raw",
        "preferred_skills_raw",
        "full_description",
        "cleaned_description",
    ]

    text_parts: List[str] = []
    for col in candidate_columns:
        if col in row.index and pd.notna(row[col]):
            text_parts.append(str(row[col]))

    normalized = normalize_text(" ".join(text_parts))
    found: Set[str] = set()
    for skill in canonical_skills:
        pattern = rf"\b{re.escape(skill)}\b"
        if re.search(pattern, normalized):
            found.add(skill)
    return found


def build_skill_interaction_edges(
    postings_df: pd.DataFrame,
    classified_freq_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build skill co-occurrence edge list from postings."""
    canonical_skills = classified_freq_df["skill"].dropna().unique().tolist()
    if not canonical_skills:
        return pd.DataFrame(columns=["source", "target", "weight"])

    row_skill_sets = [
        extract_row_skill_set(row, canonical_skills)
        for _, row in postings_df.iterrows()
    ]
    row_skill_sets = [s for s in row_skill_sets if len(s) >= 2]

    edge_counts: Dict[Tuple[str, str], int] = {}
    for skill_set in row_skill_sets:
        for a, b in itertools.combinations(sorted(skill_set), 2):
            edge_counts[(a, b)] = edge_counts.get((a, b), 0) + 1

    edge_df = pd.DataFrame(
        [{"source": a, "target": b, "weight": w} for (a, b), w in edge_counts.items()]
    )
    if edge_df.empty:
        return pd.DataFrame(columns=["source", "target", "weight"])

    return edge_df.sort_values("weight", ascending=False).reset_index(drop=True)


def make_top_skills_bar_chart(frequency_df: pd.DataFrame, output_png: Path) -> pd.DataFrame:
    """Create top-N horizontal bar chart, coloring bars by focus category."""
    top_df = frequency_df.nlargest(TOP_N_SKILLS, "frequency").sort_values("frequency", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 7))
    colors = [FOCUS_COLOR_MAP.get(focus, "#328CC1") for focus in top_df["focus"]]

    bars = ax.barh(top_df["skill"], top_df["frequency"], color=colors, edgecolor="#0B3C5D", linewidth=0.7)
    ax.set_title("Top 15 Skills by Labour Market Frequency")
    ax.set_xlabel("Frequency")
    ax.set_ylabel("Skill")

    for bar in bars:
        width = bar.get_width()
        ax.text(width + 0.2, bar.get_y() + bar.get_height() / 2, f"{int(width)}", va="center", fontsize=9)

    legend_handles = [
        plt.Line2D([0], [0], marker="s", color="w", markerfacecolor=color, markersize=10, label=focus)
        for focus, color in FOCUS_COLOR_MAP.items()
    ]
    ax.legend(handles=legend_handles, title="Focus", loc="lower right", frameon=True)

    plt.tight_layout()
    fig.savefig(output_png)
    plt.close(fig)
    return top_df


def make_skill_network_graph(
    classified_freq_df: pd.DataFrame,
    interaction_edges_df: pd.DataFrame,
    output_png: Path,
) -> pd.DataFrame:
    """Build and save labour-market-centered skill interaction network graph."""
    graph = nx.Graph()
    graph.add_node("Labour Market", category="center", frequency=0)

    for _, row in classified_freq_df.iterrows():
        graph.add_node(row["skill"], category=row["focus"], frequency=int(row["frequency"]))
        graph.add_edge("Labour Market", row["skill"], weight=max(int(row["frequency"]), 1))

    if not interaction_edges_df.empty:
        for _, row in interaction_edges_df.iterrows():
            if int(row["weight"]) >= NETWORK_MIN_EDGE_WEIGHT:
                graph.add_edge(row["source"], row["target"], weight=int(row["weight"]))

    frequencies = dict(classified_freq_df[["skill", "frequency"]].values.tolist())
    node_sizes = []
    node_colors = []
    for node in graph.nodes:
        if node == "Labour Market":
            node_sizes.append(2600)
            node_colors.append("#0B3C5D")
            continue
        freq = max(int(frequencies.get(node, 1)), 1)
        node_sizes.append(250 + (freq * 70))
        category = graph.nodes[node].get("category", "Labour Market")
        node_colors.append(FOCUS_COLOR_MAP.get(category, "#328CC1"))

    pos = nx.spring_layout(graph, seed=42, k=1.1)
    pos["Labour Market"] = (0.0, 0.0)

    fig, ax = plt.subplots(figsize=(12, 9))
    nx.draw_networkx_edges(
        graph,
        pos,
        ax=ax,
        width=[0.4 + 0.25 * graph[u][v].get("weight", 1) for u, v in graph.edges()],
        alpha=0.25,
        edge_color="#2C3E50",
    )
    nx.draw_networkx_nodes(graph, pos, node_size=node_sizes, node_color=node_colors, alpha=0.9, ax=ax)

    labels = {
        node: node if node == "Labour Market" or frequencies.get(node, 0) >= 2 else ""
        for node in graph.nodes()
    }
    nx.draw_networkx_labels(graph, pos, labels=labels, font_size=8, font_weight="bold", ax=ax)

    legend_handles = [
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=color, markersize=9, label=focus)
        for focus, color in FOCUS_COLOR_MAP.items()
    ]
    ax.legend(handles=legend_handles, title="Focus", loc="upper left", frameon=True)

    ax.set_title("Skill Interaction Network (Labour Market Centered)")
    ax.axis("off")
    plt.tight_layout()
    fig.savefig(output_png)
    plt.close(fig)

    edge_rows = [
        {"source": u, "target": v, "weight": int(d.get("weight", 1))}
        for u, v, d in graph.edges(data=True)
    ]
    return pd.DataFrame(edge_rows).sort_values("weight", ascending=False).reset_index(drop=True)


def build_smart_curriculum_gap_matrix(frequency_df: pd.DataFrame) -> pd.DataFrame:
    """Compute skill gap matrix against Smart Curriculum targets."""
    freq_map = dict(frequency_df[["skill", "frequency"]].values.tolist())
    max_freq = max(freq_map.values()) if freq_map else 1

    rows: List[Dict[str, float]] = []
    all_skills = sorted({skill for domain in SMART_CURRICULUM_TARGETS.values() for skill in domain.keys()})

    for domain_name, target_map in SMART_CURRICULUM_TARGETS.items():
        row: Dict[str, float] = {"domain": domain_name}
        for skill in all_skills:
            target = target_map.get(skill, 0.0)
            observed = float(freq_map.get(skill, 0)) / max_freq
            gap = max(target - observed, 0.0)
            row[skill] = round(gap, 3)
        rows.append(row)

    return pd.DataFrame(rows).set_index("domain")


def make_skill_gap_heatmap(gap_matrix: pd.DataFrame, output_png: Path) -> None:
    """Render publication-ready heatmap for Smart Curriculum skill gaps."""
    fig, ax = plt.subplots(figsize=(13, 5.5))
    sns.heatmap(
        gap_matrix,
        cmap=sns.color_palette("Blues", as_cmap=True),
        annot=True,
        fmt=".2f",
        linewidths=0.4,
        linecolor="white",
        cbar_kws={"label": "Gap Score (Target - Observed)"},
        ax=ax,
    )
    ax.set_title("Skill Gap Heatmap vs Smart Curriculum Targets")
    ax.set_xlabel("Skills")
    ax.set_ylabel("Curriculum Domains")
    plt.xticks(rotation=40, ha="right")
    plt.tight_layout()
    fig.savefig(output_png)
    plt.close(fig)


def write_final_excel_report(
    classified_freq_df: pd.DataFrame,
    network_edges_df: pd.DataFrame,
    gap_matrix: pd.DataFrame,
    report_path: Path,
) -> None:
    """Write final multi-sheet analysis workbook."""
    ordered_focus = [
        "Labour Market",
        "Socioeconomic Skills",
        "Green & Energy Policy",
        "Digital Coordination",
    ]
    focus_order_map = {focus: idx for idx, focus in enumerate(ordered_focus)}
    top_skills = classified_freq_df.copy()
    top_skills["focus_order"] = top_skills["focus"].map(lambda x: focus_order_map.get(x, 99))
    top_skills = top_skills.sort_values(["focus_order", "frequency", "skill"], ascending=[True, False, True])
    top_skills = top_skills.drop(columns=["focus_order"])

    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        top_skills.rename(columns={"skill": "Skill", "frequency": "Frequency", "focus": "Focus"}).to_excel(
            writer, sheet_name="Top_Skills", index=False
        )
        network_edges_df.rename(columns={"source": "Source", "target": "Target", "weight": "Weight"}).to_excel(
            writer, sheet_name="Skill_Network", index=False
        )
        gap_matrix.to_excel(writer, sheet_name="Skill_Gap_Heatmap")


def run_publication_analysis(input_xlsx: Path = INPUT_XLSX) -> None:
    """Execute complete publication-quality analysis pipeline."""
    configure_publication_style()
    ensure_output_dir(OUTPUT_DIR)

    postings_df, frequency_df = load_input_data(input_xlsx)
    classified_freq_df = build_classified_frequency_table(postings_df, frequency_df)
    interaction_edges_df = build_skill_interaction_edges(postings_df, classified_freq_df)

    make_top_skills_bar_chart(
        classified_freq_df,
        OUTPUT_DIR / "top_15_skills.png",
    )
    network_edges_df = make_skill_network_graph(
        classified_freq_df,
        interaction_edges_df,
        OUTPUT_DIR / "skill_interaction_network.png",
    )
    gap_matrix = build_smart_curriculum_gap_matrix(classified_freq_df)
    make_skill_gap_heatmap(gap_matrix, OUTPUT_DIR / "skill_gap_heatmap.png")

    write_final_excel_report(
        classified_freq_df,
        network_edges_df,
        gap_matrix,
        REPORT_XLSX,
    )

    print("Analysis completed.")
    print(f"Input workbook: {input_xlsx}")
    print(f"Output charts: {OUTPUT_DIR.resolve()}")
    print(f"Final report : {REPORT_XLSX.resolve()}")


if __name__ == "__main__":
    run_publication_analysis()
