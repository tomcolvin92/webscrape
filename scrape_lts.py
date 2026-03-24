"""Scrape the UNFCCC Long-term Strategies table into a pandas DataFrame.

The script fetches
https://unfccc.int/process/the-paris-agreement/long-term-strategies
and extracts the main table with columns for party, current submission,
current submission URL(s), previous submission, and previous submission URL(s).
"""

from __future__ import annotations

import argparse
from typing import Dict, List, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://unfccc.int"
TARGET_URL = "https://unfccc.int/process/the-paris-agreement/long-term-strategies"


def fetch_html(url: str = TARGET_URL) -> str:
    """Return raw HTML for the Long-term Strategies page."""

    headers = {"User-Agent": "Mozilla/5.0 (compatible; webscrape-lts/1.0)"}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.text


def _target_table(soup: BeautifulSoup):
    """Locate the LTS table whose header includes Party/Current Submission."""

    for table in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        if "party" in headers and "current submission" in headers and "previous submission" in headers:
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
    """Parse the LTS table rows into dictionaries."""

    soup = BeautifulSoup(html, "html.parser")
    table = _target_table(soup)
    if table is None:
        raise ValueError("Could not find the Long-term Strategies table")

    rows: List[Dict[str, Optional[str]]] = []
    body = table.find("tbody") or table

    for tr in body.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue

        current_links = _cell_links(cells[1])
        previous_links = _cell_links(cells[2])

        row = {
            "party": _cell_text(cells[0]),
            "current_submission": _cell_text(cells[1]),
            "current_submission_url": " | ".join(current_links) or None,
            "previous_submission": _cell_text(cells[2]),
            "previous_submission_url": " | ".join(previous_links) or None,
        }
        rows.append(row)

    return rows


def scrape_to_dataframe() -> pd.DataFrame:
    """Public helper that fetches and parses the LTS table."""

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
