#!/usr/bin/env python3
"""
Semantic ESCO skill extractor for LinkedIn job descriptions.

Pipeline:
  1. Translate Turkish descriptions → English (deep-translator / Google)
  2. Extract ESCO URIs semantically (esco-skill-extractor / all-MiniLM-L6-v2)
  3. Classify URIs as Digital / Green using the ESCO sub-collection CSVs
  4. Output cleaned Excel: Title | Full Description | all_skills | digital_skills | green_skills
"""

import argparse
import json
import time
from pathlib import Path

import pandas as pd
from deep_translator import GoogleTranslator
from esco_skill_extractor import SkillExtractor
from src.utils.esco_loader import load_esco_taxonomy

DIGITAL_CSV = Path("data/esco/digitalSkillsCollection_en.csv")
GREEN_CSV = Path("data/esco/greenSkillsCollection_en.csv")
INPUT_XLSX = Path("outputs/linkedin_esco_tagged_v4.xlsx")
OUTPUT_XLSX = Path("outputs/linkedin_esco_skills.xlsx")

TRANSLATE_BATCH = 20   # descriptions per translation batch
EXTRACT_BATCH = 50     # descriptions per SkillExtractor call
SKILLS_THRESHOLD = 0.50
# Google Translate character limit per request
MAX_CHARS = 4800


def _translate_batch(texts: list[str], delay: float = 0.5) -> list[str]:
    """Translate a batch of texts from Turkish to English."""
    translator = GoogleTranslator(source="tr", target="en")
    translated = []
    for text in texts:
        if not text.strip():
            translated.append("")
            continue
        # Truncate to stay within Google's per-request character limit
        chunk = text[:MAX_CHARS]
        try:
            result = translator.translate(chunk)
            translated.append(result or "")
        except Exception:
            translated.append("")  # keep empty on failure, don't crash
        time.sleep(delay)
    return translated


SKILLS_FULL_CSV = Path("data/esco/skills_en.csv")


def _load_uri_maps(digital_csv: Path, green_csv: Path):
    # Delegate loading to shared loader which also returns occupations and relations
    digital_uris, green_uris, uri_to_label, occupation_uri_to_label, occupation_to_skill_uris = load_esco_taxonomy(
        data_dir=Path("data/esco"),
        digital_csv=digital_csv,
        green_csv=green_csv,
        skills_csv=SKILLS_FULL_CSV,
    )
    # Keep backward-compatible return (digital, green, uri_to_label)
    return digital_uris, green_uris, uri_to_label


def _classify_uris(
    uris: list[str],
    digital_uris: set[str],
    green_uris: set[str],
    uri_to_label: dict[str, str],
) -> tuple[list[str], list[str], list[str], list[str]]:
    """Partition ESCO URIs into all / digital / green / general label lists."""
    all_labels: list[str] = []
    digital_labels: list[str] = []
    green_labels: list[str] = []
    general_labels: list[str] = []
    seen: set[str] = set()

    for uri in uris:
        uri = uri.strip()
        if uri in seen:
            continue
        seen.add(uri)
        label = uri_to_label.get(uri, uri)
        all_labels.append(label)
        if uri in digital_uris:
            digital_labels.append(label)
        elif uri in green_uris:
            green_labels.append(label)
        else:
            general_labels.append(label)

    return all_labels, digital_labels, green_labels, general_labels


def main():
    parser = argparse.ArgumentParser(description="Semantic ESCO skill extraction with translation")
    parser.add_argument("--input", type=Path, default=INPUT_XLSX)
    parser.add_argument("--output", type=Path, default=OUTPUT_XLSX)
    parser.add_argument("--digital", type=Path, default=DIGITAL_CSV)
    parser.add_argument("--green", type=Path, default=GREEN_CSV)
    parser.add_argument("--translate-batch", type=int, default=TRANSLATE_BATCH)
    parser.add_argument("--extract-batch", type=int, default=EXTRACT_BATCH)
    parser.add_argument("--threshold", type=float, default=SKILLS_THRESHOLD)
    parser.add_argument(
        "--delay",
        type=float,
        default=0.3,
        help="Seconds between Google Translate requests (avoid rate-limiting)",
    )
    args = parser.parse_args()

    print("[1] Loading ESCO sub-collection CSVs...")
    digital_uris, green_uris, uri_to_label = _load_uri_maps(args.digital, args.green)
    print(f"    Digital URIs : {len(digital_uris)}")
    print(f"    Green URIs   : {len(green_uris)}")

    print("[2] Loading job data...")
    src = pd.ExcelFile(args.input)
    df = pd.read_excel(args.input, sheet_name="Jobs")
    descriptions = df["Full Description"].fillna("").astype(str).tolist()
    total = len(descriptions)
    print(f"    {total} rows")

    print("[3] Translating titles (TR -> EN)...")
    titles = df["Title"].fillna("").astype(str).tolist()
    translated_titles: list[str] = []
    for start in range(0, total, args.translate_batch):
        batch = titles[start : start + args.translate_batch]
        end = min(start + args.translate_batch, total)
        print(f"    [{start + 1}-{end}/{total}]", end="", flush=True)
        translated_titles.extend(_translate_batch(batch, delay=args.delay))
        print(" done", flush=True)

    print("[3b] Translating descriptions (TR -> EN)...")
    translated: list[str] = []
    for start in range(0, total, args.translate_batch):
        batch = descriptions[start : start + args.translate_batch]
        end = min(start + args.translate_batch, total)
        print(f"    [{start + 1}-{end}/{total}]", end="", flush=True)
        translated.extend(_translate_batch(batch, delay=args.delay))
        print(" done", flush=True)

    print("[4] Initializing SkillExtractor...")
    extractor = SkillExtractor(skills_threshold=args.threshold)
    print(f"    threshold={args.threshold}")

    all_skills_col: list[str] = []
    digital_skills_col: list[str] = []
    green_skills_col: list[str] = []
    general_skills_col: list[str] = []

    print(f"[5] Extracting skills in batches of {args.extract_batch}...")
    for start in range(0, total, args.extract_batch):
        batch = translated[start : start + args.extract_batch]
        end = min(start + args.extract_batch, total)
        print(f"    [{start + 1}-{end}/{total}]", end="", flush=True)

        batch_uris: list[list[str]] = extractor.get_skills(batch)

        for uris in batch_uris:
            all_l, dig_l, grn_l, gen_l = _classify_uris(uris, digital_uris, green_uris, uri_to_label)
            all_skills_col.append(json.dumps(all_l, ensure_ascii=False))
            digital_skills_col.append(json.dumps(dig_l, ensure_ascii=False))
            green_skills_col.append(json.dumps(grn_l, ensure_ascii=False))
            general_skills_col.append(json.dumps(gen_l, ensure_ascii=False))

        print(" done", flush=True)

    print("[6] Building output...")
    out_df = pd.DataFrame(
        {
            "Title": df["Title"],
            "Title_EN": translated_titles,
            "Full Description": df["Full Description"],
            "Description_EN": translated,
            "all_skills": all_skills_col,
            "digital_skills": digital_skills_col,
            "green_skills": green_skills_col,
            "general_skills": general_skills_col,
        }
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(args.output, engine="openpyxl") as writer:
        out_df.to_excel(writer, sheet_name="Jobs", index=False)
        for sheet in src.sheet_names:
            if sheet == "Jobs":
                continue
            extra = pd.read_excel(args.input, sheet_name=sheet)
            extra.to_excel(writer, sheet_name=sheet, index=False)

    d_count = sum(1 for x in digital_skills_col if json.loads(x))
    g_count = sum(1 for x in green_skills_col if json.loads(x))
    a_count = sum(1 for x in all_skills_col if json.loads(x))
    n_count = sum(1 for x in general_skills_col if json.loads(x))
    print(f"[7] Done -> {args.output.resolve()}")
    print(f"    Jobs with any skill     : {a_count}/{total}")
    print(f"    Jobs with digital skill : {d_count}/{total}")
    print(f"    Jobs with green skill   : {g_count}/{total}")
    print(f"    Jobs with general skill : {n_count}/{total}")


if __name__ == "__main__":
    main()
