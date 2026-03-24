"""Scrape Article 6.4 Designated National Authorities (DNA) table data.

The script fetches:
https://unfccc.int/process-and-meetings/the-paris-agreement/article-64-mechanism/national-authorities#country_AtoZ
and extracts rows from the "List of DNAs" tab, including these columns:
- Country
- Organization & Address
- Contact
- Submitted Host Party participation requirements

Some source tables use a slightly shorter final header label
("Submitted Host Party requirements"). This scraper normalizes both variants into
one output column.
"""

from __future__ import annotations

import argparse
from typing import Dict, List, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://unfccc.int"
TARGET_URL = (
    "https://unfccc.int/process-and-meetings/the-paris-agreement/article-64-"
    "mechanism/national-authorities#country_AtoZ"
)


def fetch_html(url: str = TARGET_URL) -> str:
    """Fetch and return HTML from the DNA page."""

    headers = {"User-Agent": "Mozilla/5.0 (compatible; webscrape-dna/1.0)"}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.text


def _normalize_header(header: str) -> str:
    """Normalize source header labels to stable output field names."""

    cleaned = " ".join(header.lower().split())
    if cleaned == "submitted host party requirements":
        cleaned = "submitted host party participation requirements"

    mapping = {
        "country": "country",
        "organization & address": "organization_address",
        "contact": "contact",
        "submitted host party participation requirements": (
            "submitted_host_party_participation_requirements"
        ),
    }
    return mapping.get(cleaned, cleaned.replace(" ", "_"))


def _cell_text(cell) -> Optional[str]:
    value = cell.get_text(" ", strip=True)
    return value or None


def _cell_first_link(cell) -> Optional[str]:
    anchor = cell.find("a")
    if not anchor:
        return None
    href = anchor.get("href")
    if not href:
        return None
    return urljoin(BASE_URL, href)


def parse_dna_rows(html: str) -> List[Dict[str, Optional[str]]]:
    """Parse all List of DNAs tables into normalized row dictionaries."""

    soup = BeautifulSoup(html, "html.parser")
    container = soup.find(id="country_AtoZ")
    if container is None:
        raise ValueError("Could not find the 'List of DNAs' tab container")

    rows: List[Dict[str, Optional[str]]] = []
    for table in container.find_all("table"):
        header_cells = table.find_all("th")
        if not header_cells:
            first_row = table.find("tr")
            if not first_row:
                continue
            header_cells = first_row.find_all("td")

        headers = [_normalize_header(cell.get_text(" ", strip=True)) for cell in header_cells]
        if "country" not in headers or "organization_address" not in headers:
            continue

        body = table.find("tbody") or table
        for tr in body.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) < len(headers):
                continue

            row: Dict[str, Optional[str]] = {}
            for idx, header in enumerate(headers):
                cell = cells[idx]
                row[header] = _cell_text(cell)
                if header == "submitted_host_party_participation_requirements":
                    row[f"{header}_url"] = _cell_first_link(cell)

            # Skip header-like rows accidentally included in tbody.
            if row.get("country", "").strip().lower() == "country":
                continue
            rows.append(row)

    if not rows:
        raise ValueError("No DNA rows found in the 'List of DNAs' tab")

    return rows


def scrape_to_dataframe() -> pd.DataFrame:
    """Public helper that returns DNA rows as a pandas DataFrame."""

    html = fetch_html()
    rows = parse_dna_rows(html)
    return pd.DataFrame(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        metavar="PATH",
        help="Write the full table to PATH as CSV (instead of printing a preview).",
    )
    args = parser.parse_args()

    df = scrape_to_dataframe()
    print(f"Scraped {len(df)} rows with columns: {', '.join(df.columns)}")
    if args.csv:
        df.to_csv(args.csv, index=False)
        print(f"Wrote CSV to {args.csv}")
    else:
        print(df.head())
