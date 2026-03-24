"""Scrape the UNFCCC NDC Registry table into a pandas DataFrame.

The script fetches https://unfccc.int/NDCREG and extracts the main table
with columns like Party, Title, Language, Translation, Version, Status,
Submission Date, and Additional documents URL(s).
"""

from __future__ import annotations

import argparse
from typing import Dict, List, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://unfccc.int"
TARGET_URL = "https://unfccc.int/NDCREG"


def fetch_html(url: str = TARGET_URL) -> str:
    """Return raw HTML for the NDC Registry page."""

    headers = {"User-Agent": "Mozilla/5.0 (compatible; webscrape-ndcreg/1.0)"}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.text


def _target_table(soup: BeautifulSoup):
    """Locate the table whose header includes Party/Submission Date."""

    for table in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        if "party" in headers and "submission date" in headers:
            return table
    return None


def _cell_text(cell) -> Optional[str]:
    """Return normalized text from a cell, or None when empty."""

    value = cell.get_text(" ", strip=True)
    return value or None


def _cell_links(cell) -> List[str]:
    """Return absolute URLs from links found in a table cell."""

    links: List[str] = []
    for anchor in cell.find_all("a"):
        href = anchor.get("href")
        if href:
            links.append(urljoin(BASE_URL, href))
    return links


def parse_rows(html: str) -> List[Dict[str, Optional[str]]]:
    """Parse the NDC table rows into dictionaries."""

    soup = BeautifulSoup(html, "html.parser")
    table = _target_table(soup)
    if table is None:
        raise ValueError("Could not find the NDC Registry table")

    rows: List[Dict[str, Optional[str]]] = []
    body = table.find("tbody") or table

    for tr in body.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 8:
            continue

        title_links = _cell_links(cells[1])
        additional_links = _cell_links(cells[7])
        row = {
            "party": _cell_text(cells[0]),
            "title": _cell_text(cells[1]),
            "language": _cell_text(cells[2]),
            "translation": _cell_text(cells[3]),
            "version": _cell_text(cells[4]),
            "status": _cell_text(cells[5]),
            "submission_date": _cell_text(cells[6]),
            "additional_documents_url": " | ".join(additional_links) or None,
            "document_url": title_links[0] if title_links else None,
        }
        rows.append(row)

    return rows


def scrape_to_dataframe() -> pd.DataFrame:
    """Public helper that fetches and parses the NDC Registry table."""

    html = fetch_html()
    rows = parse_rows(html)
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
