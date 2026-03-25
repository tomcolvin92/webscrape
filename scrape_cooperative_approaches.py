"""Scrape UNFCCC Article 6.2 cooperative approaches and latest report URL.

The scraper fetches the cooperative approaches table from the UNFCCC CARP page and
returns one row per cooperative approach with a single ``most_updated_url`` column.
If multiple report links are present for a row, URLs explicitly marked as "updated"
are prioritized; otherwise the newest dated URL (when detected) is used.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

TARGET_URL = (
    "https://unfccc.int/process-and-meetings/the-paris-agreement/article-6/"
    "article-62/carp/cooperative-approaches"
)


@dataclass
class CooperativeApproachRow:
    cooperative_approach_id: Optional[str]
    cooperative_approach_name: Optional[str]
    participating_parties: Optional[str]
    most_updated_url: Optional[str]


def fetch_html() -> str:
    """Fetch the cooperative approaches page and return raw HTML."""

    headers = {"User-Agent": "Mozilla/5.0 (compatible; webscrape-coop/1.0)"}
    response = requests.get(TARGET_URL, headers=headers, timeout=30)
    response.raise_for_status()
    return response.text


def _cell_text(cell) -> Optional[str]:
    text = cell.get_text(" ", strip=True)
    return text or None


def _extract_date_score(text: str) -> tuple[int, str]:
    """Return sortable score for dates found in text, newest first."""

    yyyymmdd_matches = re.findall(r"\b(20\d{2})(\d{2})(\d{2})\b", text)
    scores: List[int] = []
    for year, month, day in yyyymmdd_matches:
        try:
            value = int(datetime(int(year), int(month), int(day)).strftime("%Y%m%d"))
            scores.append(value)
        except ValueError:
            continue

    return (max(scores) if scores else 0, text)


def _best_link(cell) -> Optional[str]:
    """Return the most updated link from the report cell."""

    links = []
    for index, anchor in enumerate(cell.find_all("a")):
        href = anchor.get("href")
        if not href:
            continue

        label = anchor.get_text(" ", strip=True).lower()
        absolute_url = urljoin(TARGET_URL, href.strip())
        haystack = f"{label} {absolute_url.lower()}"
        updated_score = 1 if "updated" in haystack else 0
        date_score, _ = _extract_date_score(absolute_url)

        # Later links receive a small tie-breaker as a fallback ordering.
        links.append((updated_score, date_score, index, absolute_url))

    if not links:
        return None

    # Prefer explicit updated links, then newest detectable date, then later anchor order.
    links.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    return links[0][3]


def _find_target_table(soup: BeautifulSoup):
    for table in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        if "cooperative approach id" in headers and "cooperative approach name" in headers:
            return table
    return None


def parse_rows(html: str) -> List[CooperativeApproachRow]:
    """Parse the cooperative approaches table into typed rows."""

    soup = BeautifulSoup(html, "html.parser")
    table = _find_target_table(soup)
    if table is None:
        raise ValueError("Could not find cooperative approaches table on the target page")

    rows: List[CooperativeApproachRow] = []
    tbody = table.find("tbody") or table

    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 4:
            continue

        rows.append(
            CooperativeApproachRow(
                cooperative_approach_id=_cell_text(cells[0]),
                cooperative_approach_name=_cell_text(cells[1]),
                participating_parties=_cell_text(cells[2]),
                most_updated_url=_best_link(cells[3]),
            )
        )

    if not rows:
        raise ValueError("No cooperative approach rows were parsed")

    return rows


def scrape_cooperative_approaches_dataframe() -> pd.DataFrame:
    """Fetch and return cooperative approaches as a pandas DataFrame."""

    html = fetch_html()
    rows = parse_rows(html)
    return pd.DataFrame([row.__dict__ for row in rows])


def _print_one_row_per_approach(df: pd.DataFrame) -> None:
    """Print each cooperative approach on its own line with URL."""

    for _, row in df.iterrows():
        approach = row.get("cooperative_approach_name") or row.get("cooperative_approach_id")
        url = row.get("most_updated_url") or ""
        print(f"{approach}\t{url}")


def _to_csv(df: pd.DataFrame, csv_path: str) -> None:
    columns = [
        "cooperative_approach_id",
        "cooperative_approach_name",
        "participating_parties",
        "most_updated_url",
    ]
    df.to_csv(csv_path, columns=columns, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", metavar="PATH", help="Optional CSV output path")
    args = parser.parse_args()

    dataframe = scrape_cooperative_approaches_dataframe()
    _print_one_row_per_approach(dataframe)

    if args.csv:
        _to_csv(dataframe, args.csv)
        print(f"Saved {len(dataframe)} rows to {args.csv}")
