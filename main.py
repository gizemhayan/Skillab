"""
Skillab Turkey - Deep Scraping Entry Point.

This module runs deep scraping on Kariyer.net by visiting each job detail URL,
collecting full job descriptions, and saving the raw dataset to data/raw_jobs.csv.

Author: Skillab Turkey Team
Project: EU Horizon Skill Intelligence Hub
"""

from __future__ import annotations

import argparse
import os
import random
import re
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

import pandas as pd
from dotenv import load_dotenv

try:
    import matplotlib.pyplot as plt
except ImportError:
    plt = None

from publication_analysis import run_publication_analysis
from src.scraper.kariyer_scraper import KariyerScraper
from src.processing.skill_extractor import run_extraction
from src.utils.logger import get_logger
from src.analyzer.turkish_concepts import TurkishConceptAnalyzer
from src.utils.checkpoint_utils import FULL_RECOVERY_CSV_PATH, reset_recovery_csv, clear_checkpoint

logger = get_logger(__name__)

load_dotenv()

DEFAULT_SEARCH_KEYWORDS: List[str] = [
    "Yazılım",  # Only Turkish keyword: "Software" - MAXIMUM 985 job listings operation
]

# Force single keyword - NO ALTERNATIVES
SEARCH_KEYWORDS: List[str] = DEFAULT_SEARCH_KEYWORDS

# Full lossless operation limits
PAGE_COUNT: Optional[int] = 50
MAX_JOBS: Optional[int] = 1000

# Date-stamped output paths for tracking scrape sessions
TODAY_STAMP: str = datetime.now().strftime("%d_%m_%Y")
RAW_OUTPUT_PATH: Path = Path(f"data/raw_jobs_{TODAY_STAMP}.csv")
LEGACY_RAW_PATH: Path = Path("data/raw_jobs.csv")
CHECKPOINT_DIR: Path = Path("data/checkpoints")
INVENTORY_OUTPUT_PATH: Path = Path(f"data/skill_inventory_{TODAY_STAMP}.xlsx")
FINAL_REPORT_PATH: Path = Path(f"data/analysis_report_{TODAY_STAMP}.txt")
FINAL_CHART_PATH: Path = Path(f"outputs/summary_chart_{TODAY_STAMP}.png")

CRITICAL_DESCRIPTORS: List[str] = [
    "green economy",
    "circular economy",
    "digital coordination",
    "resource planning",
    "sustainability",
    "renewable energy",
    "cloud computing",
    "cybersecurity",
    "digital liaison",
]


def _slugify_keyword(keyword: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", keyword.lower()).strip("_")


def _normalize_text(text: str) -> str:
    lowered = str(text).lower()
    stripped = re.sub(r"[^a-z0-9\s./+-]", " ", lowered)
    return re.sub(r"\s+", " ", stripped).strip()


def _annotate_descriptors(df: pd.DataFrame) -> pd.DataFrame:
    """Add descriptor hit columns for Skillab descriptors without dropping rows."""
    if df.empty:
        df["descriptor_hits"] = ""
        df["descriptor_hit_count"] = 0
        return df

    normalized = df["full_description"].fillna("").astype(str).map(_normalize_text)

    def _find_hits(text: str) -> List[str]:
        return [descriptor for descriptor in CRITICAL_DESCRIPTORS if re.search(rf"\b{re.escape(descriptor)}\b", text)]

    hit_lists = normalized.map(_find_hits)
    enriched = df.copy()
    enriched["descriptor_hits"] = hit_lists.map(lambda items: " | ".join(items))
    enriched["descriptor_hit_count"] = hit_lists.map(len)
    enriched["is_descriptor_match"] = enriched["descriptor_hit_count"] > 0

    logger.info(
        "descriptor_filter_applied",
        descriptors=len(CRITICAL_DESCRIPTORS),
        matched=int(enriched["is_descriptor_match"].sum()),
        total=len(enriched),
    )
    return enriched.reset_index(drop=True)


def _save_listing_checkpoint(keyword: str, page: int, listings_count: int) -> None:
    """Write lightweight checkpoint metadata every 10 pages."""
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint_file = CHECKPOINT_DIR / "scrape_progress.csv"
    row = pd.DataFrame(
        [
            {
                "keyword": keyword,
                "page": page,
                "cumulative_listings": listings_count,
            }
        ]
    )
    if checkpoint_file.exists():
        row.to_csv(checkpoint_file, mode="a", header=False, index=False, encoding="utf-8-sig")
    else:
        row.to_csv(checkpoint_file, index=False, encoding="utf-8-sig")


def run_deep_scrape() -> pd.DataFrame:
    """
    Run deep scrape workflow and persist output to CSV.

    Returns:
        DataFrame containing deep-scraped job records.
    """
    logger.info(
        "deep_scrape_started",
        keywords=SEARCH_KEYWORDS,
        page_count=PAGE_COUNT,
        max_jobs=MAX_JOBS,
    )

    RAW_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    # Fresh recovery run: clear old progress artifacts.
    reset_recovery_csv()
    clear_checkpoint()

    collected_frames: List[pd.DataFrame] = []

    for keyword in SEARCH_KEYWORDS:
        logger.info("keyword_scrape_started", keyword=keyword)

        def _page_callback(page: int, listings: List[object]) -> None:
            if page % 10 == 0:
                _save_listing_checkpoint(keyword, page, len(listings))
                logger.info(
                    "checkpoint_saved",
                    keyword=keyword,
                    page=page,
                    listings=len(listings),
                )

        with KariyerScraper() as scraper:
            keyword_df = scraper.deep_scrape_jobs(
                keyword=keyword,
                page_count=PAGE_COUNT,
                max_jobs=MAX_JOBS,
                on_page_complete=_page_callback,
            )

        if keyword_df.empty:
            logger.warning("keyword_scrape_empty", keyword=keyword)
            cooldown = random.uniform(2.0, 5.0)
            logger.info("keyword_cooldown", keyword=keyword, seconds=round(cooldown, 2))
            time.sleep(cooldown)
            continue

        keyword_df["search_keyword"] = keyword
        collected_frames.append(keyword_df)

        checkpoint_raw = pd.concat(collected_frames, ignore_index=True)
        logger.info(
            "keyword_scrape_completed",
            keyword=keyword,
            rows=len(keyword_df),
            cumulative_rows=len(checkpoint_raw),
        )

        cooldown = random.uniform(2.0, 5.0)
        logger.info("keyword_cooldown", keyword=keyword, seconds=round(cooldown, 2))
        time.sleep(cooldown)

    # Authoritative dataset is the immediate recovery CSV written row-by-row.
    if FULL_RECOVERY_CSV_PATH.exists():
        jobs_df = pd.read_csv(FULL_RECOVERY_CSV_PATH, encoding="utf-8-sig")
    else:
        jobs_df = pd.concat(collected_frames, ignore_index=True) if collected_frames else pd.DataFrame()

    if jobs_df.empty:
        logger.warning(
            "deep_scrape_empty",
            keywords=SEARCH_KEYWORDS,
            message="No jobs with full descriptions were collected.",
        )
        return jobs_df

    # Pandas processing layer for consistent raw schema.
    jobs_df = jobs_df.sort_values(
        by=["company", "title", "scraped_at", "search_keyword"],
        na_position="last",
    ).drop_duplicates(subset=["url"], keep="first").reset_index(drop=True)

    logger.info(
        "deep_scrape_recovery_ready",
        output_path=str(FULL_RECOVERY_CSV_PATH),
        row_count=len(jobs_df),
    )

    return jobs_df


def run_pipeline() -> None:
    """
    Orchestrate multi-keyword scraping with anti-503 protection and analytics.

    Stage 1 — Scraping:   Iterate over SEARCH_KEYWORDS, fetching from Kariyer.net  
    Stage 2 — Extraction: skill_extractor processes all results
    Stage 3 — Analytics:  Generate dated reports and visualization
    """
    logger.info("pipeline_started", keywords=SEARCH_KEYWORDS)

    jobs_df = run_deep_scrape()
    combined_df = jobs_df.copy() if not jobs_df.empty else pd.DataFrame()

    if combined_df.empty:
        if LEGACY_RAW_PATH.exists():
            logger.warning(
                "pipeline_fallback_to_legacy",
                fallback_input=str(LEGACY_RAW_PATH),
            )
            run_extraction(input_path=LEGACY_RAW_PATH, output_path=INVENTORY_OUTPUT_PATH)
            run_publication_analysis(input_xlsx=INVENTORY_OUTPUT_PATH)
            logger.info("pipeline_completed")
            return
        logger.warning("pipeline_stopped_no_data", reason="no_jobs_collected")
        logger.info("pipeline_completed")
        return

    logger.info("combined_recovery_ready", output_path=str(FULL_RECOVERY_CSV_PATH), total_rows=len(combined_df))

    # Process through extraction pipeline
    run_extraction(input_path=FULL_RECOVERY_CSV_PATH, output_path=INVENTORY_OUTPUT_PATH)
    run_publication_analysis(input_xlsx=INVENTORY_OUTPUT_PATH)

    # Generate final analytics report
    _generate_final_report(combined_df)

    logger.info("pipeline_completed")


def _generate_final_report(combined_df: pd.DataFrame) -> None:
    """Generate summary statistics, report, and visualization after all scraping."""
    if combined_df.empty:
        logger.warning("final_report_skipped", reason="empty_dataset")
        return

    # Analyze Turkish concepts
    analysis = _analyze_turkish_concepts(combined_df)
    
    stats = {
        "Total Jobs": len(combined_df),
        "Unique Companies": combined_df["company"].nunique() if "company" in combined_df.columns else 0,
        "Keywords Scraped": len(SEARCH_KEYWORDS),
        "Digital Transformation": analysis["digital_count"],
        "Digital Percentage": analysis["digital_percentage"],
        "Green Transformation": analysis["green_count"],
        "Green Percentage": analysis["green_percentage"],
    }

    report_lines: List[str] = [
        "=" * 80,
        "Skillab Türkiye — Tarama Analiz Raporu",
        "Skillab Turkey — Scraping Analysis Report",
        f"Oluşturma Tarihi / Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 80,
        "",
        "ÖZET İSTATİSTİKLER / SUMMARY STATISTICS:",
        f"  Toplam İlan Sayısı / Total Jobs: {stats['Total Jobs']}",
        f"  Benzersiz Şirketler / Unique Companies: {stats['Unique Companies']}",
        f"  Aranmış Anahtar Kelimeler / Keywords Searched: {', '.join(SEARCH_KEYWORDS)}",
        "",
        "DÖNÜŞÜM ANALİZİ / TRANSFORMATION ANALYSIS:",
        f"  Dijital Dönüşüm İlanları / Digital Transformation: {stats['Digital Transformation']} ({stats['Digital Percentage']}%)",
        f"  Yeşil Dönüşüm İlanları / Green Transformation: {stats['Green Transformation']} ({stats['Green Percentage']}%)",
        "",
    ]

    # Top Digital transformation concepts
    if analysis["digital_concepts_freq"]:
        report_lines.append("BAŞLICA DİJİTAL DÖNÜŞÜM KAVRAMLARI / TOP DIGITAL TRANSFORMATION CONCEPTS:")
        digital_sorted = sorted(analysis["digital_concepts_freq"].items(), key=lambda x: x[1], reverse=True)[:10]
        for concept, count in digital_sorted:
            report_lines.append(f"  • {concept}: {count} ilanlar / postings")
        report_lines.append("")
    
    # Top Green transformation concepts
    if analysis["green_concepts_freq"]:
        report_lines.append("BAŞLICA YEŞIL DÖNÜŞÜM KAVRAMLARI / TOP GREEN TRANSFORMATION CONCEPTS:")
        green_sorted = sorted(analysis["green_concepts_freq"].items(), key=lambda x: x[1], reverse=True)[:10]
        for concept, count in green_sorted:
            report_lines.append(f"  • {concept}: {count} ilanlar / postings")
        report_lines.append("")

    if "company" in combined_df.columns:
        top_companies = combined_df["company"].value_counts().head(10)
        report_lines.append("EN ÇALIŞAN 10 ŞİRKET / TOP 10 RECRUITING COMPANIES:")
        for company, count in top_companies.items():
            report_lines.append(f"  {company}: {count} pozisyon / positions")
        report_lines.append("")

    if "location" in combined_df.columns:
        top_locations = combined_df["location"].value_counts().head(5)
        report_lines.append("EN ÇALIŞAN 5 LOKASYOn / TOP 5 LOCATIONS:")
        for location, count in top_locations.items():
            report_lines.append(f"  {location}: {count} pozisyon / positions")
        report_lines.append("")

    report_lines.extend([
        "=" * 80,
        "OLUŞTURULAN ÇIKTI DOSYLARI / OUTPUT FILES GENERATED:",
        f"  Ham Veri / Raw Data: {RAW_OUTPUT_PATH}",
        f"  Yetenek Envanteri / Skill Inventory: {INVENTORY_OUTPUT_PATH}",
        f"  Analiz Raporu / Analysis Report: {FINAL_REPORT_PATH}",
        f"  Dönüşüm Karnesi / Transformation Scorecard: outputs/TURKIYE_DONUSUM_KARNESI_{TODAY_STAMP}.png",
    ])

    # Save text report
    FINAL_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(FINAL_REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

    logger.info("final_report_saved", output_path=str(FINAL_REPORT_PATH))

    # Generate Turkish transformation scorecard
    try:
        _generate_turkish_scorecard(combined_df, analysis)
    except Exception as exc:
        logger.warning("scorecard_generation_failed", detail=str(exc))

    # Generate visualization if matplotlib available
    if plt:
        try:
            _generate_summary_chart(combined_df)
        except Exception as exc:
            logger.warning("chart_generation_failed", detail=str(exc))


def _generate_summary_chart(df: pd.DataFrame) -> None:
    """Create a summary visualization of scraping results."""
    if "company" not in df.columns:
        logger.warning("chart_skipped", reason="no_company_column")
        return

    fig, ax = plt.subplots(figsize=(12, 6))
    top_companies = df["company"].value_counts().head(10)
    top_companies.plot(kind="barh", ax=ax, color="steelblue")
    ax.set_xlabel("Number of Job Postings")
    ax.set_title(f"Top 10 Recruiting Companies (As of {TODAY_STAMP})")
    ax.invert_yaxis()

    FINAL_CHART_PATH.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(FINAL_CHART_PATH, dpi=100, bbox_inches="tight")
    logger.info("summary_chart_saved", output_path=str(FINAL_CHART_PATH))
    plt.close()


def _analyze_turkish_concepts(df: pd.DataFrame) -> Dict[str, any]:
    """
    Analyze Digital and Green transformation concepts in job descriptions.
    
    Returns comprehensive statistics on Turkish transformation keywords.
    """
    analyzer = TurkishConceptAnalyzer()
    
    digital_count = 0
    green_count = 0
    digital_jobs = []
    green_jobs = []
    digital_concepts_freq: Dict[str, int] = {}
    green_concepts_freq: Dict[str, int] = {}
    
    descriptions = df["full_description"].fillna("").astype(str)
    
    for idx, desc in enumerate(descriptions):
        if not desc.strip():
            continue
        
        digital_concepts = analyzer.extract_digital_concepts(desc)
        green_concepts = analyzer.extract_green_concepts(desc)
        
        if digital_concepts:
            digital_count += 1
            digital_jobs.append((df.iloc[idx]["company"], df.iloc[idx]["title"]))
            for concept in digital_concepts:
                digital_concepts_freq[concept] = digital_concepts_freq.get(concept, 0) + 1
        
        if green_concepts:
            green_count += 1
            green_jobs.append((df.iloc[idx]["company"], df.iloc[idx]["title"]))
            for concept in green_concepts:
                green_concepts_freq[concept] = green_concepts_freq.get(concept, 0) + 1
    
    return {
        "digital_count": digital_count,
        "green_count": green_count,
        "digital_percentage": round((digital_count / len(df) * 100) if len(df) > 0 else 0, 2),
        "green_percentage": round((green_count / len(df) * 100) if len(df) > 0 else 0, 2),
        "digital_concepts_freq": digital_concepts_freq,
        "green_concepts_freq": green_concepts_freq,
        "digital_jobs": digital_jobs,
        "green_jobs": green_jobs,
    }


def _generate_turkish_scorecard(df: pd.DataFrame, analysis: Dict) -> None:
    """
    Generate a comprehensive scorecard visualization showing Turkey's Digital and Green transformation.
    
    Creates a visual summary with statistics and concept frequency analysis.
    """
    if not plt:
        logger.warning("scorecard_generation_skipped", reason="matplotlib_not_available")
        return
    
    try:
        scorecard_path = Path(f"outputs/TURKIYE_DONUSUM_KARNESI_{TODAY_STAMP}.png")
        scorecard_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create figure with subplots for comprehensive analysis
        fig = plt.figure(figsize=(16, 10))
        gs = fig.add_gridspec(3, 3, hspace=0.4, wspace=0.3)
        
        # Title
        fig.suptitle("Türkiye'nin Dijital ve Yeşil Dönüşüm Karnesi\n(Turkish Skill Market - Software Jobs)", 
                     fontsize=18, fontweight="bold", y=0.98)
        
        # 1. Main transformation overview (big numbers)
        ax_main = fig.add_subplot(gs[0, :])
        ax_main.axis("off")
        
        total_jobs = len(df)
        digital_count = analysis["digital_count"]
        green_count = analysis["green_count"]
        digital_pct = analysis["digital_percentage"]
        green_pct = analysis["green_percentage"]
        
        summary_text = f"""
        TARAMA ÖZETİ (Scraping Summary):
        • Toplam İlan Sayısı: {total_jobs}
        • Dijital Dönüşüm İlanları: {digital_count} (%{digital_pct})
        • Yeşil Dönüşüm İlanları: {green_count} (%{green_pct})
        • İşveren Çeşitliliği: {df["company"].nunique()} şirket
        """
        
        ax_main.text(0.05, 0.5, summary_text, fontsize=12, family="monospace",
                    bbox=dict(boxstyle="round", facecolor="#E8F4F8", alpha=0.8),
                    verticalalignment="center")
        
        # 2. Digital transformation concepts (top left)
        ax_digital = fig.add_subplot(gs[1, 0:2])
        digital_freq = dict(sorted(analysis["digital_concepts_freq"].items(), 
                                  key=lambda x: x[1], reverse=True)[:8])
        if digital_freq:
            concepts = list(digital_freq.keys())
            counts = list(digital_freq.values())
            colors_digital = plt.cm.Blues([(i + 1) / len(concepts) for i in range(len(concepts))])
            bars = ax_digital.barh(concepts, counts, color=colors_digital)
            ax_digital.set_xlabel("Frekans / Frequency", fontsize=10)
            ax_digital.set_title("Dijital Dönüşüm Kavramları / Digital Transformation Concepts", 
                                fontsize=12, fontweight="bold")
            ax_digital.invert_yaxis()
            for i, bar in enumerate(bars):
                ax_digital.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2, 
                              f"{int(counts[i])}", va="center", fontsize=9)
        
        # 3. Green transformation concepts (top right)
        ax_green = fig.add_subplot(gs[1, 2])
        green_freq = dict(sorted(analysis["green_concepts_freq"].items(), 
                                key=lambda x: x[1], reverse=True)[:6])
        if green_freq:
            concepts = list(green_freq.keys())
            counts = list(green_freq.values())
            colors_green = plt.cm.Greens([(i + 1) / len(concepts) for i in range(len(concepts))])
            bars = ax_green.barh(concepts, counts, color=colors_green)
            ax_green.set_xlabel("Frekans", fontsize=9)
            ax_green.set_title("Yeşil Dönüşüm\nGreen Transformation", 
                             fontsize=11, fontweight="bold")
            ax_green.invert_yaxis()
            for i, bar in enumerate(bars):
                ax_green.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height()/2, 
                            f"{int(counts[i])}", va="center", fontsize=8)
        
        # 4. Pie chart: Digital vs Green vs Neither
        ax_pie = fig.add_subplot(gs[2, 0])
        neither_count = total_jobs - digital_count - green_count
        
        pie_data = [digital_count, green_count, neither_count]
        pie_labels = [f"Dijital\n{digital_pct}%", f"Yeşil\n{green_pct}%", 
                     f"Diğer\n{round(100 - digital_pct - green_pct, 1)}%"]
        colors_pie = ["#1f77b4", "#2ca02c", "#d3d3d3"]
        
        wedges, texts, autotexts = ax_pie.pie(pie_data, labels=pie_labels, colors=colors_pie,
                                               autopct="%d", startangle=90, textprops={"fontsize": 9})
        ax_pie.set_title("Dönüşüm Dağılımı\nTransformation Distribution", 
                        fontsize=11, fontweight="bold")
        
        # 5. Top companies with transformations
        ax_companies = fig.add_subplot(gs[2, 1:])
        top_companies = df["company"].value_counts().head(8).index.tolist()
        company_digital = []
        company_green = []
        
        analyst = TurkishConceptAnalyzer()
        for company in top_companies:
            company_df = df[df["company"] == company]
            digital_in_company = sum(1 for desc in company_df["full_description"] 
                                   if analyst.has_digital_concept(str(desc)))
            green_in_company = sum(1 for desc in company_df["full_description"] 
                                 if analyst.has_green_concept(str(desc)))
            company_digital.append(digital_in_company)
            company_green.append(green_in_company)
        
        x_pos = range(len(top_companies))
        width = 0.35
        
        bars1 = ax_companies.bar([x - width/2 for x in x_pos], company_digital, width, 
                                 label="Dijital / Digital", color="#1f77b4")
        bars2 = ax_companies.bar([x + width/2 for x in x_pos], company_green, width, 
                                 label="Yeşil / Green", color="#2ca02c")
        
        ax_companies.set_ylabel("İlan Sayısı / Job Count", fontsize=10)
        ax_companies.set_title("Top 8 İşverenler - Dönüşüm Algılaması / Top 8 Employers - Transformation Match", 
                             fontsize=11, fontweight="bold")
        ax_companies.set_xticks(x_pos)
        ax_companies.set_xticklabels([c[:15] for c in top_companies], rotation=45, ha="right", fontsize=9)
        ax_companies.legend(fontsize=10)
        ax_companies.grid(axis="y", alpha=0.3)
        
        plt.savefig(scorecard_path, dpi=150, bbox_inches="tight")
        logger.info("turkish_scorecard_saved", output_path=str(scorecard_path))
        plt.close()
        
    except Exception as e:
        logger.error("scorecard_generation_failed", detail=str(e), exc_info=True)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="Skillab Turkiye pipeline runner",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Run scraping + extraction + analysis pipeline.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    if not args.run:
        parser.print_help()
        return 0

    run_pipeline()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
