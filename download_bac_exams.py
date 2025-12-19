"""Download Moroccan Baccalauréat SPC BIOF exams and corrections.

This script automates the collection of Mathématiques, Sciences Physiques (PC),
and Sciences de la Vie et de la Terre (SVT) national exam PDFs together with
their official corrections. It targets reliable Ministry-backed sources such as
TelmidTICE and organises the material into subject-specific folders.

Deliverables:
1. Downloaded PDF files stored under ./Math, ./PC, and ./SVT directories.
2. A JSON and CSV manifest (`exams_metadata.json` / `exams_metadata.csv`) that
   list metadata for every successfully captured exam or correction.

Usage:
    python download_bac_exams.py

The script is idempotent: already downloaded files are skipped safely, making
future extensions straightforward (just add new source URLs below).
"""

from __future__ import annotations

import csv
import json
import logging
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parent


SUBJECT_SOURCES: Dict[str, Dict[str, Iterable[str]]] = {
    "Math": {
        "label": "Mathématiques",
        "folder": ROOT_DIR / "Math",
        "pages": [
            "https://telmidtice.com/2bac-pc-mathematiques-examens-nationaux/",
            "https://www.taalime.ma/examen-national-math-bac-sciences-mathematique-avec-correction-biof-pdf-maroc/",
        ],
    },
    "PC": {
        "label": "Sciences Physiques (PC)",
        "folder": ROOT_DIR / "PC",
        "pages": [
            "https://telmidtice.com/2bac-pc-pc-examens-nationaux/",
            "https://www.taalime.ma/examen-national-physique-chimie-bac-sciences-physique-svt-avec-correction-biof-pdf-maroc/",
        ],
    },
    "SVT": {
        "label": "Sciences de la Vie et de la Terre (SVT)",
        "folder": ROOT_DIR / "SVT",
        "pages": [
            "https://telmidtice.com/2bac-pc-svt-examens-nationaux/",
            "https://www.taalime.ma/examen-national-svt-bac-sciences-de-la-vie-et-de-la-terre-avec-correction-biof-pdf-maroc/",
        ],
    },
}


YEARS = list(range(2008, 2025))
TARGET_SESSIONS = ("Normale", "Rattrapage")
TARGET_ASSET_TYPES = ("MainExam", "Correction")
SESSION_LABELS = {"Normale": "Normale", "Rattrapage": "Rattrapage"}
TYPE_LABELS = {"MainExam": "Sujet", "Correction": "Corrigé"}
SESSION_ORDER = {"Normale": 0, "Rattrapage": 1}
TYPE_ORDER = {"MainExam": 0, "Correction": 1}
PREFERRED_DOMAINS = (
    "telmidtice.com",
    "men.gov.ma",
    "drive.google.com",
    "docs.google.com",
)

TELMID_PATTERNS = {
    "Math": {
        "base_url": "https://telmidtice.com/assets/2bac-pc/maths-fr/Examens Nationaux/",
        "file_template": (
            "TelmidTice - Examen National Maths Sciences et Technologies {year} "
            "{session_label} - {type_label}.pdf"
        ),
        "title_template": (
            "TelmidTICE Mathématiques {year} {session_label} – {type_label}"
        ),
    },
    "PC": {
        "base_url": "https://telmidtice.com/assets/2bac-pc/pc-fr/Examens Nationaux/",
        "file_template": (
            "TelmidTice - Examen National Physique-Chimie SPC {year} "
            "{session_label} - {type_label}.pdf"
        ),
        "title_template": (
            "TelmidTICE Physique-Chimie {year} {session_label} – {type_label}"
        ),
    },
    "SVT": {
        "base_url": "https://telmidtice.com/assets/2bac-pc/svt-fr/Examens Nationaux/",
        "file_template": (
            "TelmidTice - Examen National SVT Sciences Physiques {year} "
            "{session_label} - {type_label}.pdf"
        ),
        "title_template": (
            "TelmidTICE SVT {year} {session_label} – {type_label}"
        ),
    },
}

SESSION_KEYWORDS = (
    (("normale", "normal", "principale", "main", "regular"), "Normale"),
    (("rattrap", "retake", "extraordinaire"), "Rattrapage"),
)


# ---------------------------------------------------------------------------
# Data structures & helpers
# ---------------------------------------------------------------------------

EXAM_YEAR_RE = re.compile(r"(20\d{2}|19\d{2})")


@dataclass
class ExamAsset:
    subject_code: str
    subject_label: str
    year: Optional[str]
    session: Optional[str]
    asset_type: str  # "MainExam" or "Correction"
    source_title: str
    source_page: str
    pdf_url: str
    local_path: Path

    def to_dict(self) -> Dict[str, str]:
        data = asdict(self)
        data["local_path"] = str(self.local_path)
        return data


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )


def ensure_directories() -> None:
    for config in SUBJECT_SOURCES.values():
        folder: Path = config["folder"]  # type: ignore[assignment]
        folder.mkdir(parents=True, exist_ok=True)


def create_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0 Safari/537.36"
            )
        }
    )
    return session


def normalize_pdf_url(href: str, base_url: str) -> Optional[str]:
    """Resolve download helper links to the actual PDF URL."""

    absolute = urljoin(base_url, href)
    parsed = urlparse(absolute)

    if "telecharger" in parsed.path:
        target = parse_qs(parsed.query).get("url")
        if target:
            return unquote(target[0])

    if parsed.netloc in {"drive.google.com", "docs.google.com"}:
        # Convert drive preview URLs into a direct download endpoint.
        parts = parsed.path.strip("/").split("/")
        if "file" in parts and "d" in parts:
            try:
                file_id = parts[parts.index("d") + 1]
            except (ValueError, IndexError):
                return absolute
            return f"https://drive.google.com/uc?export=download&id={file_id}"

    return absolute


def identify_year(text: str) -> Optional[str]:
    match = EXAM_YEAR_RE.search(text)
    return match.group(1) if match else None


def identify_session(text: str) -> Optional[str]:
    lowered = text.lower()
    for keywords, label in SESSION_KEYWORDS:
        if any(keyword in lowered for keyword in keywords):
            return label
    return None


def identify_asset_type(text: str) -> Optional[str]:
    lowered = text.lower()
    if "corrig" in lowered:
        return "Correction"
    if "sujet" in lowered or "examen" in lowered:
        return "MainExam"
    return None


def sanitize_filename(*parts: str, suffix: str = ".pdf") -> str:
    safe_parts: List[str] = []
    for part in parts:
        clean = re.sub(r"[^A-Za-z0-9]+", "_", part).strip("_")
        if clean:
            safe_parts.append(clean)
    combined = "_".join(safe_parts) if safe_parts else "document"
    return f"{combined}{suffix}"


def asset_key(asset: ExamAsset) -> Optional[tuple[str, int, str, str]]:
    if asset.year is None or asset.session is None:
        return None
    try:
        year_int = int(asset.year)
    except (TypeError, ValueError):
        return None
    session_name = SESSION_LABELS.get(asset.session, asset.session)
    if session_name not in TARGET_SESSIONS:
        return None
    if asset.asset_type not in TARGET_ASSET_TYPES:
        return None
    return (asset.subject_code, year_int, session_name, asset.asset_type)


def prefer_asset(candidate: ExamAsset, current: ExamAsset) -> bool:
    candidate_host = urlparse(candidate.pdf_url).netloc.lower()
    current_host = urlparse(current.pdf_url).netloc.lower()

    def host_rank(host: str) -> int:
        for idx, domain in enumerate(PREFERRED_DOMAINS):
            if host.endswith(domain):
                return idx
        return len(PREFERRED_DOMAINS)

    return host_rank(candidate_host) < host_rank(current_host)


def build_telmid_asset(
    http_session: requests.Session,
    subject_code: str,
    subject_label: str,
    folder: Path,
    year: int,
    session_name: str,
    asset_type: str,
) -> Optional[ExamAsset]:
    pattern = TELMID_PATTERNS.get(subject_code)
    if not pattern:
        return None

    session_label = SESSION_LABELS.get(session_name, session_name)
    type_label = TYPE_LABELS.get(asset_type)
    if type_label is None:
        return None

    filename = pattern["file_template"].format(
        year=year, session_label=session_label, type_label=type_label
    )
    pdf_url = pattern["base_url"] + quote(filename)

    try:
        response = http_session.head(pdf_url, allow_redirects=True, timeout=20)
        if response.status_code != requests.codes.ok:
            return None
    except requests.RequestException:
        return None

    local_filename = sanitize_filename(
        subject_code,
        str(year),
        session_name,
        asset_type,
    )

    return ExamAsset(
        subject_code=subject_code,
        subject_label=subject_label,
        year=str(year),
        session=session_name,
        asset_type=asset_type,
        source_title=pattern["title_template"].format(
            year=year,
            session_label=session_label,
            type_label=type_label,
        ),
        source_page=pattern["base_url"],
        pdf_url=pdf_url,
        local_path=folder / local_filename,
    )


def parse_exam_links(
    html: str,
    page_url: str,
    subject_code: str,
    subject_label: str,
    target_folder: Path,
) -> List[ExamAsset]:
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.select("article a[href], main a[href], .entry-content a[href]")

    assets: List[ExamAsset] = []
    seen_pdf_urls: set[str] = set()

    for anchor in anchors:
        href = anchor.get("href")
        if not href:
            continue

        pdf_url = normalize_pdf_url(href, page_url)
        if not pdf_url:
            continue

        parsed_pdf = urlparse(pdf_url)
        if not pdf_url.lower().endswith(".pdf") and parsed_pdf.netloc not in {
            "drive.google.com",
            "docs.google.com",
        }:
            continue

        title = anchor.get_text(strip=True)
        if not title:
            continue

        if "préparation" in title.lower() or "preparation" in title.lower():
            # Skip drill sheets – keep the focus on official exams.
            continue

        asset_type = identify_asset_type(title)
        if asset_type is None:
            continue

        pdf_url_key = pdf_url.strip()
        if pdf_url_key in seen_pdf_urls:
            continue

        seen_pdf_urls.add(pdf_url_key)
        year = identify_year(title)
        session = identify_session(title)

        filename = sanitize_filename(
            subject_code,
            year or "unknown_year",
            session or "session",
            asset_type,
        )

        asset = ExamAsset(
            subject_code=subject_code,
            subject_label=subject_label,
            year=year,
            session=session,
            asset_type=asset_type,
            source_title=title,
            source_page=page_url,
            pdf_url=pdf_url,
            local_path=target_folder / filename,
        )
        assets.append(asset)

    return assets


def download_pdf(session: requests.Session, asset: ExamAsset) -> bool:
    if asset.local_path.exists():
        logging.info("Skipping existing file %s", asset.local_path.name)
        return True

    asset.local_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with session.get(asset.pdf_url, stream=True, timeout=45) as response:
            response.raise_for_status()
            content_type = (response.headers.get("Content-Type", "") or "").lower()
            if "pdf" not in content_type:
                logging.warning(
                    "Unexpected content-type for %s (%s)",
                    asset.pdf_url,
                    response.headers.get("Content-Type"),
                )
            first_bytes: Optional[bytes] = None
            with asset.local_path.open("wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if not chunk:
                        continue
                    if first_bytes is None:
                        first_bytes = chunk
                    f.write(chunk)
            if first_bytes is None or not first_bytes.lstrip().startswith(b"%PDF"):
                logging.error(
                    "Downloaded content for %s does not look like a PDF; skipping",
                    asset.local_path.name,
                )
                asset.local_path.unlink(missing_ok=True)
                return False
        logging.info("Downloaded %s", asset.local_path.name)
        return True
    except requests.RequestException as exc:
        logging.error("Failed to download %s: %s", asset.pdf_url, exc)
        if asset.local_path.exists():
            asset.local_path.unlink(missing_ok=True)
        return False


def write_metadata(manifest: List[ExamAsset]) -> None:
    if not manifest:
        logging.warning("No metadata to write.")
        return

    json_path = ROOT_DIR / "exams_metadata.json"
    csv_path = ROOT_DIR / "exams_metadata.csv"

    data = [entry.to_dict() for entry in manifest]

    with json_path.open("w", encoding="utf-8") as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=2)
    logging.info("Wrote JSON manifest with %d entries", len(data))

    fieldnames = list(data[0].keys())
    with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    logging.info("Wrote CSV manifest with %d entries", len(data))


def harvest_subject(
    session: requests.Session,
    subject_code: str,
    label: str,
    folder: Path,
    pages: Iterable[str],
) -> List[ExamAsset]:
    assets: List[ExamAsset] = []

    for page_url in pages:
        logging.info("Processing %s", page_url)
        try:
            response = session.get(page_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            logging.error("Failed to fetch %s: %s", page_url, exc)
            continue

        new_assets = parse_exam_links(
            response.text,
            page_url=page_url,
            subject_code=subject_code,
            subject_label=label,
            target_folder=folder,
        )

        if not new_assets:
            logging.warning("No PDF links detected at %s", page_url)
        assets.extend(new_assets)

    return assets


def main() -> int:
    setup_logging()
    ensure_directories()

    session = create_http_session()
    assets_by_key: Dict[tuple[str, int, str, str], ExamAsset] = {}

    for subject_code, config in SUBJECT_SOURCES.items():
        label = config["label"]
        folder: Path = config["folder"]
        pages = config["pages"]

        subject_assets = harvest_subject(session, subject_code, label, folder, pages)
        for asset in subject_assets:
            key = asset_key(asset)
            if key is None:
                continue
            existing = assets_by_key.get(key)
            if existing is None or prefer_asset(asset, existing):
                assets_by_key[key] = asset

    for subject_code, config in SUBJECT_SOURCES.items():
        label = config["label"]
        folder: Path = config["folder"]

        for year in YEARS:
            for session_name in TARGET_SESSIONS:
                for asset_type in TARGET_ASSET_TYPES:
                    key = (subject_code, year, session_name, asset_type)
                    if key in assets_by_key:
                        continue
                    fallback_asset = build_telmid_asset(
                        session,
                        subject_code,
                        label,
                        folder,
                        year,
                        session_name,
                        asset_type,
                    )
                    if fallback_asset:
                        assets_by_key[key] = fallback_asset

    missing_keys: List[tuple[str, int, str, str]] = []
    for subject_code in SUBJECT_SOURCES.keys():
        for year in YEARS:
            for session_name in TARGET_SESSIONS:
                for asset_type in TARGET_ASSET_TYPES:
                    key = (subject_code, year, session_name, asset_type)
                    if key not in assets_by_key:
                        missing_keys.append(key)

    if missing_keys:
        for subject_code, year, session_name, asset_type in missing_keys:
            logging.warning(
                "Missing asset after fallback search: %s %s %s %s",
                subject_code,
                year,
                session_name,
                asset_type,
            )
    else:
        logging.info("All target assets located for 2008-2024 Normale and Rattrapage sessions.")

    sorted_assets = sorted(
        assets_by_key.values(),
        key=lambda asset: (
            asset.subject_code,
            int(asset.year) if asset.year and asset.year.isdigit() else 0,
            SESSION_ORDER.get(asset.session or "", 99),
            TYPE_ORDER.get(asset.asset_type, 99),
            asset.local_path.name,
        ),
    )

    manifest: List[ExamAsset] = []
    for asset in sorted_assets:
        if download_pdf(session, asset):
            manifest.append(asset)

    write_metadata(manifest)
    logging.info("Completed with %d downloadable assets", len(manifest))
    return 0


if __name__ == "__main__":
    sys.exit(main())


