#!/usr/bin/env python3
"""
ESCO Skill Demand Analysis Pipeline
Turkey Digital & Green Transformation — Tech Sector LinkedIn Jobs
Skillab-aligned methodology (ESCO v1.2, coverage-based ranking, gap analysis)
"""

import json
import sys
from collections import Counter
from itertools import combinations
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

INPUT  = Path("outputs/final_esco_analysis.xlsx")
OUTDIR = Path("outputs/analysis")
OUTDIR.mkdir(parents=True, exist_ok=True)

sns.set_theme(style="whitegrid", context="paper")
plt.rcParams.update({
    "figure.dpi": 160,
    "savefig.dpi": 320,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.28,
    "font.size": 10,
    "axes.titlesize": 13,
    "axes.titleweight": "bold",
    "axes.titlepad": 10,
    "axes.labelsize": 11,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "grid.alpha": 0.18,
    "grid.linestyle": "--",
})
DIGITAL_COLOR  = "#0A66C2"
GREEN_COLOR    = "#2E7D32"
GENERAL_COLOR  = "#6A1B9A"
HTURF_COLOR    = "#E65100"


# ── Helpers ──────────────────────────────────────────────────────────────────

def parse_skills(cell) -> list[str]:
    """Parse JSON-encoded skill array from cell.
    
    Args:
        cell: Cell content (str or list)
    
    Returns:
        List of normalized skill strings (lowercase, stripped)
    """
    try:
        skills = json.loads(cell) if isinstance(cell, str) else []
        return [s.strip().lower() for s in skills if isinstance(s, str) and s.strip()]
    except Exception:
        return []


def load_data() -> pd.DataFrame:
    """Load and parse ESCO-tagged job data from Excel.
    
    Reads from outputs/final_esco_analysis.xlsx, parses JSON-encoded skill columns,
    and extracts normalized job titles.
    
    Returns:
        DataFrame with columns: all_skills, digital_skills, green_skills, general_skills,
                                _all, _digital, _green, _general (parsed), _title (normalized)
    """
    df = pd.read_excel(INPUT, sheet_name="Jobs")
    df["_all"]     = df["all_skills"].apply(parse_skills)
    df["_digital"] = df["digital_skills"].apply(parse_skills)
    df["_green"]   = df["green_skills"].apply(parse_skills)
    df["_general"] = df["general_skills"].apply(parse_skills) if "general_skills" in df.columns else [[] for _ in range(len(df))]
    title_col = "Title_EN" if "Title_EN" in df.columns else "Title"
    df["_title"] = df[title_col].fillna("").str.strip()
    return df


def freq_table(series_of_lists, top_n=30) -> pd.DataFrame:
    """Generate frequency table from nested skill lists.
    
    Args:
        series_of_lists: Pandas Series of lists (each row is a list of skills)
        top_n: Number of top skills to return
    
    Returns:
        DataFrame with columns: [skill, count, percent] sorted by frequency
    """
    counter = Counter(skill for lst in series_of_lists for skill in lst)
    rows = pd.DataFrame(counter.most_common(top_n), columns=["skill", "count"])
    rows["percent"] = (rows["count"] / len(series_of_lists) * 100).round(1)
    return rows


def save_figure(fig, path: Path) -> None:
    """Save figure with publication-quality settings.
    
    Args:
        fig: Matplotlib figure object
        path: Output file path (PNG)
    
    Applies: 320 DPI, 0.28" padding, tight bounding box, background color
    """
    fig.savefig(path, dpi=320, bbox_inches="tight", pad_inches=0.28, facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"  Saved: {path}")


def bar_chart(df_freq, title, color, filename, top_n=20, xlabel="Number of Job Postings"):
    """Render and save a horizontal top-skills bar chart.

    Args:
        df_freq: Frequency table with columns [skill, count, percent]
        title: Figure title
        color: Main bar color
        filename: Output PNG name under outputs/analysis
        top_n: Number of rows to display
        xlabel: X-axis label
    """
    data = df_freq.head(top_n)
    fig, ax = plt.subplots(figsize=(11.5, 6.8))
    fig.patch.set_facecolor("#F8FAFC")
    ax.set_facecolor("#F8FAFC")
    bars = ax.barh(
        data["skill"][::-1],
        data["count"][::-1],
        color=color,
        alpha=0.88,
        edgecolor="white",
        linewidth=0.6,
    )
    ax.bar_label(bars, fmt="%d", padding=4, fontsize=8, color="#334155")
    ax.set_title(title, pad=14)
    ax.set_xlabel(xlabel)
    ax.grid(axis="x", alpha=0.18)
    ax.set_axisbelow(True)
    ax.margins(x=0.08)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    path = OUTDIR / filename
    save_figure(fig, path)


def group_title(t: str) -> str:
    """Map raw title text to a normalized job-title group label."""
    t = t.lower()
    if any(k in t for k in ["data scientist", "data science"]): return "Data Scientist"
    if any(k in t for k in ["machine learning", "ml engineer", "ai engineer", "artificial intelligence"]): return "ML/AI Engineer"
    if any(k in t for k in ["backend", "back-end", "back end"]): return "Backend Developer"
    if any(k in t for k in ["frontend", "front-end", "front end"]): return "Frontend Developer"
    if any(k in t for k in ["full stack", "fullstack"]): return "Full Stack Developer"
    if any(k in t for k in ["devops", "site reliability", "platform engineer"]): return "DevOps/Platform"
    if any(k in t for k in ["cloud", "aws", "azure", "gcp"]): return "Cloud Engineer"
    if any(k in t for k in ["cyber", "security", "penetration", "information security"]): return "Cybersecurity"
    if any(k in t for k in ["data engineer"]): return "Data Engineer"
    if any(k in t for k in ["mobile", "android", "ios", "flutter"]): return "Mobile Developer"
    if any(k in t for k in ["software engineer", "software developer", "yazilim"]): return "Software Engineer"
    if any(k in t for k in ["analyst", "business intelligence"]): return "Analyst"
    if any(k in t for k in ["architect", "solution architect"]): return "Solution Architect"
    if any(k in t for k in ["game", "unity", "unreal"]): return "Game Developer"
    if any(k in t for k in ["embedded", "firmware", "rtos"]): return "Embedded Engineer"
    return "Other"


# ── Analysis 1: Top Skills Frequency ─────────────────────────────────────────

def analyze_top_skills(df):
    """Compute and visualize top demanded skills by category.

    Outputs:
        CSV files: top_digital_skills.csv, top_green_skills.csv,
                   top_general_skills.csv, top_all_skills.csv
        PNG files: top_digital_skills.png, top_green_skills.png,
                   top_general_skills.png

    Returns:
        Tuple of frequency DataFrames: (digital, green, general)
    """
    print("\n[1] Top demanded skills by category")

    dig_freq = freq_table(df["_digital"], top_n=30)
    grn_freq = freq_table(df["_green"],   top_n=30)
    gen_freq = freq_table(df["_general"], top_n=30)
    all_freq = freq_table(df["_all"],     top_n=30)

    dig_freq.to_csv(OUTDIR / "top_digital_skills.csv",  index=False)
    grn_freq.to_csv(OUTDIR / "top_green_skills.csv",    index=False)
    gen_freq.to_csv(OUTDIR / "top_general_skills.csv",  index=False)
    all_freq.to_csv(OUTDIR / "top_all_skills.csv",      index=False)

    bar_chart(dig_freq, "Top 20 Digital Skills (ESCO) — Turkey Tech Sector",
              DIGITAL_COLOR, "top_digital_skills.png")
    bar_chart(grn_freq, "Top 20 Green Skills (ESCO) — Turkey Tech Sector",
              GREEN_COLOR,   "top_green_skills.png")
    bar_chart(gen_freq, "Top 20 General / Transversal Skills (ESCO) — Turkey Tech Sector",
              GENERAL_COLOR, "top_general_skills.png")

    print(f"  Unique digital skills  : {len(Counter(s for l in df['_digital'] for s in l))}")
    print(f"  Unique green skills    : {len(Counter(s for l in df['_green']   for s in l))}")
    print(f"  Unique general skills  : {len(Counter(s for l in df['_general'] for s in l))}")
    return dig_freq, grn_freq, gen_freq


# ── Analysis 2: Skills by Job Title Group (Heatmap) ──────────────────────────

def analyze_by_title(df):
    """Analyze skill density by normalized title groups and export heatmaps.

    Creates summary table per role group and two heatmaps:
    digital-skill density and green-skill density (% of postings).
    """
    print("\n[2] Skill distribution by job title group")

    df["_group"] = df["_title"].apply(group_title)

    rows = []
    for grp, gdf in df.groupby("_group"):
        if len(gdf) < 5:
            continue
        dig_cnt = Counter(s for l in gdf["_digital"] for s in l)
        grn_cnt = Counter(s for l in gdf["_green"]   for s in l)
        gen_cnt = Counter(s for l in gdf["_general"] for s in l)
        rows.append({
            "Job Title Group":        grp,
            "Job Postings":           len(gdf),
            "Avg Digital Skills":     round(gdf["_digital"].apply(len).mean(), 1),
            "Avg Green Skills":       round(gdf["_green"].apply(len).mean(), 1),
            "Avg General Skills":     round(gdf["_general"].apply(len).mean(), 1),
            "Top Digital (5)":        ", ".join([s for s, _ in dig_cnt.most_common(5)]),
            "Top Green (5)":          ", ".join([s for s, _ in grn_cnt.most_common(5)]),
            "Top General (5)":        ", ".join([s for s, _ in gen_cnt.most_common(5)]),
        })

    title_df = pd.DataFrame(rows).sort_values("Job Postings", ascending=False)
    title_df.to_csv(OUTDIR / "skills_by_title_group.csv", index=False)
    print(f"  {len(title_df)} title groups analyzed")

    # Heatmap: title group x top 15 digital skills
    top_dig = [s for s, _ in Counter(sk for l in df["_digital"] for sk in l).most_common(15)]
    groups_sorted = title_df["Job Title Group"].tolist()

    heat_data = []
    for grp in groups_sorted:
        gdf = df[df["_group"] == grp]
        cnt = Counter(s for l in gdf["_digital"] for s in l)
        total = len(gdf)
        heat_data.append({s: round(cnt.get(s, 0) / total * 100, 1) for s in top_dig})

    heat_df = pd.DataFrame(heat_data, index=groups_sorted)
    fig, ax = plt.subplots(figsize=(15, 7))
    sns.heatmap(heat_df, ax=ax, cmap="YlOrRd", annot=True, fmt=".0f",
                linewidths=0.4, cbar_kws={"label": "% of Job Postings"})
    ax.set_title("Digital Skill Density by Job Title Group (%) — Turkey Tech Sector",
                 fontsize=12, fontweight="bold")
    ax.set_xlabel("ESCO Digital Skill")
    ax.set_ylabel("Job Title Group")
    plt.xticks(rotation=35, ha="right", fontsize=8)
    plt.tight_layout()
    fig.savefig(OUTDIR / "heatmap_title_digital.png")
    plt.close(fig)
    print(f"  Saved: {OUTDIR / 'heatmap_title_digital.png'}")

    # Heatmap: title group x top 10 green skills
    top_grn = [s for s, _ in Counter(sk for l in df["_green"] for sk in l).most_common(10)]
    if top_grn:
        heat_grn = []
        for grp in groups_sorted:
            gdf = df[df["_group"] == grp]
            cnt = Counter(s for l in gdf["_green"] for s in l)
            total = len(gdf)
            heat_grn.append({s: round(cnt.get(s, 0) / total * 100, 1) for s in top_grn})
        heat_grn_df = pd.DataFrame(heat_grn, index=groups_sorted)
        fig2, ax2 = plt.subplots(figsize=(13, 7))
        sns.heatmap(heat_grn_df, ax=ax2, cmap="Greens", annot=True, fmt=".0f",
                    linewidths=0.4, cbar_kws={"label": "% of Job Postings"})
        ax2.set_title("Green Skill Density by Job Title Group (%) — Turkey Tech Sector",
                      fontsize=12, fontweight="bold")
        ax2.set_xlabel("ESCO Green Skill")
        ax2.set_ylabel("Job Title Group")
        plt.xticks(rotation=35, ha="right", fontsize=8)
        plt.tight_layout()
        fig2.savefig(OUTDIR / "heatmap_title_green.png")
        plt.close(fig2)
        print(f"  Saved: {OUTDIR / 'heatmap_title_green.png'}")

    return title_df


# ── Analysis 3: Digital & Green Co-occurrence ─────────────────────────────────

def analyze_cooccurrence(df):
    """Measure digital/green co-occurrence and role-level demand relationship.

    Outputs:
        CSV: cooccurrence_summary.csv
        PNG: cooccurrence_bar.png, scatter_digital_vs_green.png
    """
    print("\n[3] Digital & Green skill co-occurrence analysis")

    has_dig = df["_digital"].apply(len) > 0
    has_grn = df["_green"].apply(len) > 0
    has_gen = df["_general"].apply(len) > 0

    both      = (has_dig & has_grn).sum()
    dig_only  = (has_dig & ~has_grn).sum()
    grn_only  = (~has_dig & has_grn).sum()
    neither   = (~has_dig & ~has_grn).sum()

    cooc = pd.DataFrame({
        "Category":      ["Digital + Green", "Digital Only", "Green Only", "Neither"],
        "Job Postings":  [both, dig_only, grn_only, neither],
    })
    cooc["Percent"] = (cooc["Job Postings"] / len(df) * 100).round(1)
    cooc.to_csv(OUTDIR / "cooccurrence_summary.csv", index=False)

    fig, ax = plt.subplots(figsize=(8, 5))
    colors = [DIGITAL_COLOR, "#42A5F5", GREEN_COLOR, "#BDBDBD"]
    bars = ax.bar(cooc["Category"], cooc["Job Postings"], color=colors, alpha=0.85)
    ax.bar_label(bars,
                 labels=[f"{v}\n({p}%)" for v, p in zip(cooc["Job Postings"], cooc["Percent"])],
                 padding=4, fontsize=9)
    ax.set_title("Digital & Green Skill Co-occurrence — Turkey Tech Sector",
                 fontsize=12, fontweight="bold")
    ax.set_ylabel("Number of Job Postings")
    ax.set_xlabel("")
    plt.xticks(rotation=10)
    plt.tight_layout()
    fig.savefig(OUTDIR / "cooccurrence_bar.png")
    plt.close(fig)
    print(f"  Digital+Green together : {both} postings ({both/len(df)*100:.1f}%)")
    print(f"  Digital only           : {dig_only} postings ({dig_only/len(df)*100:.1f}%)")
    print(f"  Green only             : {grn_only} postings ({grn_only/len(df)*100:.1f}%)")

    # Scatter: avg digital skill count vs avg green skill count per title group
    df["_group"] = df["_title"].apply(group_title)
    scatter_rows = []
    for grp, gdf in df.groupby("_group"):
        if len(gdf) < 5:
            continue
        scatter_rows.append({
            "group": grp,
            "avg_digital": gdf["_digital"].apply(len).mean(),
            "avg_green":   gdf["_green"].apply(len).mean(),
            "count":       len(gdf),
        })
    sdf = pd.DataFrame(scatter_rows)
    if not sdf.empty:
        fig2, ax2 = plt.subplots(figsize=(9, 6))
        scatter = ax2.scatter(sdf["avg_digital"], sdf["avg_green"],
                              s=sdf["count"] * 8, alpha=0.7, color=DIGITAL_COLOR)
        for _, r in sdf.iterrows():
            ax2.annotate(r["group"], (r["avg_digital"], r["avg_green"]),
                         fontsize=7, ha="center", va="bottom")
        ax2.set_xlabel("Avg Digital Skills per Job Posting")
        ax2.set_ylabel("Avg Green Skills per Job Posting")
        ax2.set_title("Digital vs Green Skill Demand by Role — Turkey Tech Sector",
                      fontsize=12, fontweight="bold")
        plt.tight_layout()
        fig2.savefig(OUTDIR / "scatter_digital_vs_green.png")
        plt.close(fig2)
        print(f"  Saved: {OUTDIR / 'scatter_digital_vs_green.png'}")


# ── Analysis 4: Coverage-based skill ranking ────────────────────────────────

def analyze_hturf(df, skill_col="_digital", top_k=15, label="Digital"):
    """Greedy coverage-based skill ranking: select skills maximizing cumulative reach.
    
    Args:
        df: DataFrame with parsed skill columns (_digital, _green, etc.)
        skill_col: Column name to analyze (default: "_digital")
        top_k: Number of top skills to rank
        label: Category label for output (default: "Digital")
    
    Outputs:
        CSV: outputs/analysis/hturf_{label.lower()}_skills.csv
        PNG: outputs/analysis/hturf_{label.lower()}_skills.png
    """
    print(f"\n[4] Coverage-based skill ranking — {label} skills")

    sets = [set(row) for row in df[skill_col]]
    total = len(sets)
    if total == 0:
        return

    all_skills = Counter(s for row in sets for s in row)
    candidates = [s for s, _ in all_skills.most_common(50)]

    selected = []
    covered  = set()
    rows     = []

    for _ in range(min(top_k, len(candidates))):
        best_skill, best_new = None, -1
        for skill in candidates:
            if skill in selected:
                continue
            new_coverage = sum(1 for s in sets if skill in s and not (s & set(covered)))
            if new_coverage > best_new:
                best_new, best_skill = new_coverage, skill
        if best_skill is None:
            break
        selected.append(best_skill)
        covered.add(best_skill)
        newly_reached = sum(1 for s in sets if best_skill in s)
        cumulative    = sum(1 for s in sets if any(sk in s for sk in selected))
        rows.append({
            "Rank":              len(selected),
            "Skill":             best_skill,
            "Individual Reach":  newly_reached,
            "Reach %":           round(newly_reached / total * 100, 1),
            "Cumulative Reach":  cumulative,
            "Cumulative %":      round(cumulative / total * 100, 1),
        })

    hturf_df = pd.DataFrame(rows)
    fname = f"hturf_{label.lower()}_skills"
    hturf_df.to_csv(OUTDIR / f"{fname}.csv", index=False)

    fig, ax = plt.subplots(figsize=(14.5, 6.8))
    fig.patch.set_facecolor("#F8FAFC")
    ax.set_facecolor("#F8FAFC")
    cum_bars = ax.barh(
        hturf_df["Skill"][::-1],
        hturf_df["Cumulative %"][::-1],
        color=HTURF_COLOR,
        alpha=0.72,
        label="Cumulative Reach %",
        edgecolor="white",
        linewidth=0.5,
    )
    ax.barh(
        hturf_df["Skill"][::-1],
        hturf_df["Reach %"][::-1],
        color=DIGITAL_COLOR if label == "Digital" else GREEN_COLOR,
        alpha=0.9,
        label="Individual Reach %",
        edgecolor="white",
        linewidth=0.5,
    )
    ax.bar_label(cum_bars, labels=[f"{v:.1f}%" for v in hturf_df["Cumulative %"][::-1]],
                 padding=6, fontsize=8, color="#334155")
    ax.set_xlabel("% of Job Postings Reached")
    ax.set_title(f"Coverage-Based {label} Skill Combinations — Turkey Tech Sector",
                 fontsize=12, fontweight="bold")
    ax.legend(fontsize=9, frameon=False, loc="center left", bbox_to_anchor=(1.02, 0.5))
    ax.grid(axis="x", alpha=0.18)
    ax.set_axisbelow(True)
    ax.margins(x=0.01)
    max_x = max(float(hturf_df["Cumulative %"].max()), float(hturf_df["Reach %"].max())) if not hturf_df.empty else 0
    ax.set_xlim(0, max(10.0, max_x * 1.12))
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    fig.subplots_adjust(right=0.82)
    save_figure(fig, OUTDIR / f"{fname}.png")
    print(f"  Top skill: '{hturf_df.iloc[0]['Skill']}' reaches {hturf_df.iloc[0]['Reach %']}% individually")
    print(f"  Top {len(hturf_df)} skills together reach {hturf_df.iloc[-1]['Cumulative %']}% of all postings")
    return hturf_df


# ── Analysis 5: Skill Gap Analysis ───────────────────────────────────────────

def analyze_skill_gap(df):
    """Identify emerging skills in the 2-10% prevalence band.

    The 2-10% range captures skills that are neither dominant nor negligible,
    useful for curriculum and workforce upskilling planning.
    """
    print("\n[5] Skill gap analysis (rare but emerging skills)")

    total = len(df)
    all_counter  = Counter(s for l in df["_all"]     for s in l)
    dig_counter  = Counter(s for l in df["_digital"] for s in l)
    grn_counter  = Counter(s for l in df["_green"]   for s in l)

    # Emerging digital: appears in 2-10% of postings (rare but not negligible)
    low, high = int(total * 0.02), int(total * 0.10)
    emerging_dig = [(s, c) for s, c in dig_counter.items() if low <= c <= high]
    emerging_dig.sort(key=lambda x: -x[1])

    emerging_grn = [(s, c) for s, c in grn_counter.items() if low <= c <= high]
    emerging_grn.sort(key=lambda x: -x[1])

    gap_df = pd.DataFrame({
        "Digital Emerging Skill": [s for s, _ in emerging_dig[:15]] + [""] * max(0, 15 - len(emerging_dig)),
        "Digital Count":          [c for _, c in emerging_dig[:15]] + [0] * max(0, 15 - len(emerging_dig)),
        "Green Emerging Skill":   [s for s, _ in emerging_grn[:15]] + [""] * max(0, 15 - len(emerging_grn)),
        "Green Count":            [c for _, c in emerging_grn[:15]] + [0] * max(0, 15 - len(emerging_grn)),
    })
    gap_df.to_csv(OUTDIR / "skill_gap_emerging.csv", index=False)

    # Visualize emerging digital skills
    if emerging_dig:
        names = [s for s, _ in emerging_dig[:15]]
        counts = [c for _, c in emerging_dig[:15]]
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.barh(names[::-1], counts[::-1], color="#FF7043", alpha=0.85)
        ax.bar_label(ax.containers[0], fmt="%d", padding=3, fontsize=8)
        ax.set_title("Emerging Digital Skills (2–10% of Job Postings) — Turkey Tech Sector",
                     fontsize=12, fontweight="bold")
        ax.set_xlabel("Number of Job Postings")
        plt.tight_layout()
        fig.savefig(OUTDIR / "skill_gap_emerging_digital.png")
        plt.close(fig)
        print(f"  {len(emerging_dig)} emerging digital skills identified")

    if emerging_grn:
        names = [s for s, _ in emerging_grn[:15]]
        counts = [c for _, c in emerging_grn[:15]]
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.barh(names[::-1], counts[::-1], color="#66BB6A", alpha=0.85)
        ax.bar_label(ax.containers[0], fmt="%d", padding=3, fontsize=8)
        ax.set_title("Emerging Green Skills (2–10% of Job Postings) — Turkey Tech Sector",
                     fontsize=12, fontweight="bold")
        ax.set_xlabel("Number of Job Postings")
        plt.tight_layout()
        fig.savefig(OUTDIR / "skill_gap_emerging_green.png")
        plt.close(fig)
        print(f"  {len(emerging_grn)} emerging green skills identified")

    print(f"  Saved: skill_gap_emerging_digital.png / skill_gap_emerging_green.png")


# ── Analysis 6: Transversal Skill Profile ────────────────────────────────────

def analyze_transversal(df):
    """Compare digital, green, and general skill profiles across the dataset.

    Outputs:
        CSV: transversal_profile.csv
        PNG: transversal_profile.png, transversal_by_group.png
    """
    print("\n[6] Transversal skill profile (digital vs green vs general)")

    total = len(df)

    # Skill category count distribution per job
    df["_n_digital"] = df["_digital"].apply(len)
    df["_n_green"]   = df["_green"].apply(len)
    df["_n_general"] = df["_general"].apply(len)
    df["_n_all"]     = df["_all"].apply(len)

    summary = pd.DataFrame({
        "Category": ["Digital", "Green", "General / Transversal"],
        "Total Occurrences": [
            df["_n_digital"].sum(),
            df["_n_green"].sum(),
            df["_n_general"].sum(),
        ],
        "Avg per Job": [
            round(df["_n_digital"].mean(), 2),
            round(df["_n_green"].mean(), 2),
            round(df["_n_general"].mean(), 2),
        ],
        "Jobs with ≥1 skill": [
            (df["_n_digital"] > 0).sum(),
            (df["_n_green"] > 0).sum(),
            (df["_n_general"] > 0).sum(),
        ],
        "Coverage %": [
            round((df["_n_digital"] > 0).mean() * 100, 1),
            round((df["_n_green"] > 0).mean() * 100, 1),
            round((df["_n_general"] > 0).mean() * 100, 1),
        ],
    })
    summary.to_csv(OUTDIR / "transversal_profile.csv", index=False)

    # Bar chart: avg skill count per category
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), gridspec_kw={"wspace": 0.24})
    fig.patch.set_facecolor("#F8FAFC")
    for ax in axes:
        ax.set_facecolor("#F8FAFC")
        ax.grid(axis="y", alpha=0.18)
        ax.set_axisbelow(True)

    bars0 = axes[0].bar(summary["Category"], summary["Avg per Job"],
                        color=[DIGITAL_COLOR, GREEN_COLOR, GENERAL_COLOR], alpha=0.88,
                        edgecolor="white", linewidth=0.7)
    axes[0].bar_label(bars0, labels=[f"{v:.2f}" for v in summary["Avg per Job"]],
                      padding=4, fontsize=9, color="#334155")
    axes[0].set_title("Avg ESCO Skills per Job Posting by Category")
    axes[0].set_ylabel("Average Count")
    axes[0].set_ylim(0, max(summary["Avg per Job"]) * 1.25)

    bars1 = axes[1].bar(summary["Category"], summary["Coverage %"],
                        color=[DIGITAL_COLOR, GREEN_COLOR, GENERAL_COLOR], alpha=0.88,
                        edgecolor="white", linewidth=0.7)
    axes[1].bar_label(bars1, labels=[f"{v:.1f}%" for v in summary["Coverage %"]],
                      padding=4, fontsize=9, color="#334155")
    axes[1].set_title("Job Postings Containing ≥1 ESCO Skill (%)")
    axes[1].set_ylabel("Coverage (%)")
    axes[1].set_ylim(0, 110)

    plt.suptitle("Transversal Skill Profile — Turkey Tech Sector LinkedIn Jobs",
                 fontsize=13, fontweight="bold", y=1.04)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    save_figure(fig, OUTDIR / "transversal_profile.png")

    # Stacked bar per title group
    df["_group"] = df["_title"].apply(group_title)
    grp_avg = df.groupby("_group")[["_n_digital", "_n_green", "_n_general"]].mean().round(2)
    grp_avg = grp_avg[grp_avg.index.map(lambda g: (df["_group"] == g).sum() >= 5)]
    grp_avg.columns = ["Digital", "Green", "General"]
    grp_avg = grp_avg.sort_values("Digital", ascending=False)

    fig2, ax2 = plt.subplots(figsize=(13.8, 6.8))
    fig2.patch.set_facecolor("#F8FAFC")
    ax2.set_facecolor("#F8FAFC")
    grp_avg.plot(kind="bar", ax=ax2,
                 color=[DIGITAL_COLOR, GREEN_COLOR, GENERAL_COLOR], alpha=0.85)
    ax2.set_title("Avg ESCO Skill Count by Category and Job Title Group — Turkey Tech Sector",
                  fontsize=12, fontweight="bold", pad=16)
    ax2.set_ylabel("Average Number of Skills")
    ax2.set_xlabel("")
    ax2.grid(axis="y", alpha=0.18)
    ax2.set_axisbelow(True)
    ax2.tick_params(axis="x", rotation=25, labelsize=9)
    ax2.legend(title="Skill Category", frameon=False, loc="upper left",
               bbox_to_anchor=(1.01, 1.0), borderaxespad=0.0)
    fig2.subplots_adjust(right=0.82, top=0.88, bottom=0.22)
    save_figure(fig2, OUTDIR / "transversal_by_group.png")
    print(f"  Saved: transversal_profile.png, transversal_by_group.png")
    print(summary.to_string(index=False))


# ── Analysis 7: Summary Report ───────────────────────────────────────────────

def generate_summary(df, dig_freq, grn_freq, gen_freq):
    """Write a plain-text executive summary with top skill rankings."""
    print("\n[7] Summary report")

    lines = [
        "=" * 65,
        "ESCO Skill Demand Analysis — Turkey Tech Sector LinkedIn Jobs",
        "Skillab-aligned | ESCO v1.2 | Semantic extraction (threshold=0.50)",
        "=" * 65,
        f"Total job postings      : {len(df)}",
        f"With digital skills     : {(df['_digital'].apply(len)>0).sum()} "
        f"({(df['_digital'].apply(len)>0).mean()*100:.1f}%)",
        f"With green skills       : {(df['_green'].apply(len)>0).sum()} "
        f"({(df['_green'].apply(len)>0).mean()*100:.1f}%)",
        f"With general skills     : {(df['_general'].apply(len)>0).sum()} "
        f"({(df['_general'].apply(len)>0).mean()*100:.1f}%)",
        f"Unique digital skills   : {len(Counter(s for l in df['_digital'] for s in l))}",
        f"Unique green skills     : {len(Counter(s for l in df['_green']   for s in l))}",
        f"Unique general skills   : {len(Counter(s for l in df['_general'] for s in l))}",
        "",
        "TOP 10 DIGITAL SKILLS (ESCO):",
    ]
    for i, row in dig_freq.head(10).iterrows():
        lines.append(f"  {i+1:2}. {row['skill']:<50} {row['count']:>4} jobs ({row['percent']}%)")

    lines += ["", "TOP 10 GREEN SKILLS (ESCO):"]
    for i, row in grn_freq.head(10).iterrows():
        lines.append(f"  {i+1:2}. {row['skill']:<50} {row['count']:>4} jobs ({row['percent']}%)")

    lines += ["", "TOP 10 GENERAL / TRANSVERSAL SKILLS (ESCO):"]
    for i, row in gen_freq.head(10).iterrows():
        lines.append(f"  {i+1:2}. {row['skill']:<50} {row['count']:>4} jobs ({row['percent']}%)")

    lines += ["", "=" * 65]
    report = "\n".join(lines)
    (OUTDIR / "summary_report.txt").write_text(report, encoding="utf-8")
    print(report)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    """Run the end-to-end ESCO skill analysis pipeline."""
    print("ESCO Skill Analysis Pipeline — Turkey Tech Sector")
    print(f"Input : {INPUT}")
    print(f"Output: {OUTDIR}/")

    df = load_data()
    print(f"Loaded: {len(df)} job postings")

    dig_freq, grn_freq, gen_freq = analyze_top_skills(df)
    analyze_by_title(df)
    analyze_cooccurrence(df)
    analyze_hturf(df, skill_col="_digital", top_k=15, label="Digital")
    analyze_hturf(df, skill_col="_green",   top_k=10, label="Green")
    analyze_skill_gap(df)
    analyze_transversal(df)
    generate_summary(df, dig_freq, grn_freq, gen_freq)

    print(f"\nAll outputs saved to {OUTDIR}/")


if __name__ == "__main__":
    main()
