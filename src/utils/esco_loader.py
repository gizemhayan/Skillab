from pathlib import Path
import csv
from typing import Dict, Set, Tuple


def load_esco_taxonomy(
    data_dir: Path = Path("data/esco"),
    digital_csv: Path | None = None,
    green_csv: Path | None = None,
    skills_csv: Path | None = None,
    occupations_csv: Path | None = None,
    occupation_skill_rel_csv: Path | None = None,
) -> Tuple[Set[str], Set[str], Dict[str, str], Dict[str, str], Dict[str, list]]:
    """Load ESCO taxonomy pieces used across the pipeline.

    Returns:
      - digital_uris: set of conceptUri from digital sub-collection
      - green_uris: set of conceptUri from green sub-collection
      - uri_to_label: map skillUri -> preferredLabel (from full skills CSV)
      - occupation_uri_to_label: map occupationUri -> preferredLabel (if occupations CSV present)
      - occupation_to_skill_uris: map occupationUri -> list of skillUris (from occupationSkillRelations)

    All paths default to files under `data/esco` when not provided.
    """
    data_dir = Path(data_dir)
    digital_csv = Path(digital_csv) if digital_csv else data_dir / "digitalSkillsCollection_en.csv"
    green_csv = Path(green_csv) if green_csv else data_dir / "greenSkillsCollection_en.csv"
    skills_csv = Path(skills_csv) if skills_csv else data_dir / "skills_en.csv"
    occupations_csv = Path(occupations_csv) if occupations_csv else data_dir / "occupations_en.csv"
    occ_rel_csv = Path(occupation_skill_rel_csv) if occupation_skill_rel_csv else data_dir / "occupationSkillRelations_en.csv"

    digital_uris: Set[str] = set()
    green_uris: Set[str] = set()
    uri_to_label: Dict[str, str] = {}
    occupation_uri_to_label: Dict[str, str] = {}
    occupation_to_skill_uris: Dict[str, list] = {}

    # Load full skills -> labels
    if skills_csv.exists():
        with open(skills_csv, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                uri = (row.get("conceptUri") or "").strip()
                label = (row.get("preferredLabel") or "").strip()
                if uri and label and label.lower() not in ("nan", "none", ""):
                    uri_to_label[uri] = label

    # Load digital/green sub-collections
    if digital_csv.exists():
        with open(digital_csv, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                uri = (row.get("conceptUri") or "").strip()
                if uri:
                    digital_uris.add(uri)

    if green_csv.exists():
        with open(green_csv, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                uri = (row.get("conceptUri") or "").strip()
                if uri:
                    green_uris.add(uri)

    # Load occupations labels
    if occupations_csv.exists():
        with open(occupations_csv, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                uri = (row.get("conceptUri") or "").strip()
                label = (row.get("preferredLabel") or "").strip()
                if uri and label:
                    occupation_uri_to_label[uri] = label

    # Load occupation -> skill relations
    if occ_rel_csv.exists():
        with open(occ_rel_csv, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                occ_uri = (row.get("occupationUri") or "").strip()
                skill_uri = (row.get("skillUri") or "").strip()
                if occ_uri and skill_uri:
                    occupation_to_skill_uris.setdefault(occ_uri, []).append(skill_uri)

    return digital_uris, green_uris, uri_to_label, occupation_uri_to_label, occupation_to_skill_uris
