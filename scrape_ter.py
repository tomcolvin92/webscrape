"""Scrape Article 6 TER report tracking tables into a pandas DataFrame.

The script fetches the CARP reports page and extracts rows from report
tracking tables that include the "Article 6 Technical Expert Review
Reports" column. It returns the visible table fields and companion URL
columns for the report documents.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://unfccc.int"
TARGET_URL = (
    "https://unfccc.int/process-and-meetings/the-paris-agreement/article-6/"
    "article-62/carp/reports#Initial-reports-and-updated-initial-reports"
)


@dataclass
class TerRow:
    party: Optional[str]
    ndc_period: Optional[str]
    original_submission_date: Optional[str]
    reports: Optional[str]
    reports_url: Optional[str]
    article_6_technical_expert_review_reports: Optional[str]
    article_6_technical_expert_review_reports_url: Optional[str]
    status_of_review: Optional[str]


def fetch_html() -> str:
    """Fetch the TER table page and return its HTML."""

    headers = {"User-Agent": "Mozilla/5.0 (compatible; webscrape-ter/1.0)"}
    response = requests.get(TARGET_URL, headers=headers, timeout=30)
    response.raise_for_status()
    return response.text


def _cell_text(cell) -> Optional[str]:
    text = cell.get_text(" ", strip=True)
    return text or None


def _cell_first_link(cell) -> Optional[str]:
    anchor = cell.find("a")
    if not anchor:
        return None
    href = anchor.get("href")
    if not href:
        return None
    return urljoin(BASE_URL, href)


def _rows_with_rowspan(table) -> List[List[object]]:
    """Return row cells with rowspans expanded for table parsing."""

    body = table.find("tbody") or table
    rows: List[List[object]] = []
    active: List[dict | None] = [None] * 6

    for tr in body.find_all("tr"):
        cells: List[object] = [None] * 6

        for idx, span in enumerate(active):
            if span is None:
                continue
            cells[idx] = span["cell"]
            span["remaining"] -= 1
            if span["remaining"] <= 0:
                active[idx] = None

        col = 0
        for td in tr.find_all("td"):
            while col < 6 and cells[col] is not None:
                col += 1
            if col >= 6:
                break

            rowspan = int(td.get("rowspan", 1) or 1)
            colspan = int(td.get("colspan", 1) or 1)

            for offset in range(colspan):
                tgt = col + offset
                if tgt >= 6:
                    break
                cells[tgt] = td
                if rowspan > 1:
                    active[tgt] = {"cell": td, "remaining": rowspan - 1}

            col += colspan

        if any(cell is not None for cell in cells):
            rows.append(cells)

    return rows


def parse_ter_rows(html: str) -> List[TerRow]:
    """Parse TER rows from all matching CARP report tables."""

    soup = BeautifulSoup(html, "html.parser")
    rows: List[TerRow] = []

    for table in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        if "article 6 technical expert review reports" not in headers:
            continue
        if "party" not in headers:
            continue

        for cells in _rows_with_rowspan(table):
            party = _cell_text(cells[0]) if cells[0] else None
            ndc_period = _cell_text(cells[1]) if cells[1] else None
            original_submission_date = _cell_text(cells[2]) if cells[2] else None
            reports = _cell_text(cells[3]) if cells[3] else None
            reports_url = _cell_first_link(cells[3]) if cells[3] else None
            ter_report = _cell_text(cells[4]) if cells[4] else None
            ter_report_url = _cell_first_link(cells[4]) if cells[4] else None
            status = _cell_text(cells[5]) if cells[5] else None

            rows.append(
                TerRow(
                    party=party,
                    ndc_period=ndc_period,
                    original_submission_date=original_submission_date,
                    reports=reports,
                    reports_url=reports_url,
                    article_6_technical_expert_review_reports=ter_report,
                    article_6_technical_expert_review_reports_url=ter_report_url,
                    status_of_review=status,
                )
            )

    if not rows:
        raise ValueError("No TER report tables found on the CARP reports page")

    return rows


def scrape_ter_dataframe() -> pd.DataFrame:
    """Public helper that returns TER rows as a pandas DataFrame."""

    html = fetch_html()
    rows = parse_ter_rows(html)
    return pd.DataFrame([row.__dict__ for row in rows])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", metavar="PATH", help="Optional CSV output path")
    args = parser.parse_args()

    dataframe = scrape_ter_dataframe()
    print(dataframe.head())

    if args.csv:
        dataframe.to_csv(args.csv, index=False)
        print(f"Saved {len(dataframe)} rows to {args.csv}")
