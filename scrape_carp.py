"""Scrape CARP Article 6 authorization table into a pandas DataFrame.

The script fetches the Article 6 CARP authorizations table from
https://unfccc.int/process-and-meetings/the-paris-agreement/article-6/article-62/carp/authorizations
and extracts both the visible cell text and the hyperlinks contained in
those cells. Link URLs are stored in companion columns with a ``_links``
suffix for each column, plus a flattened ``urls`` column for convenience.
"""

from __future__ import annotations

import argparse
from typing import Dict, List, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://unfccc.int"
TARGET_URL = (
    "https://unfccc.int/process-and-meetings/the-paris-agreement/article-6/"
    "article-62/carp/authorizations"
)


def fetch_html() -> str:
    """Fetch the CARP authorizations page and return its HTML."""

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; webscrape-carp/1.0)",
    }
    response = requests.get(TARGET_URL, headers=headers, timeout=30)
    response.raise_for_status()
    return response.text


def _heading_for_table(table) -> str:
    """Find the nearest heading text that precedes the given table."""

    current = table
    steps = 0
    while current and steps < 50:
        current = current.find_previous()  # type: ignore[attr-defined]
        if current is None:
            break
        steps += 1
        if current.name in {"h2", "h3", "h4"}:
            text = current.get_text(" ", strip=True)
            if text:
                return text
        if current.name == "a":
            text = current.get_text(" ", strip=True)
            if text and "report" in text.lower():
                return text
        if current.name in {"div", "p", "span"}:
            if getattr(current, "find", lambda *_, **__: None)("table"):
                continue
            text = current.get_text(" ", strip=True)
            if text and "report" in text.lower() and len(text) < 120:
                return text
    return "Unnamed table"


def _cell_value(cell) -> Tuple[str | None, List[str]]:
    """Return the normalized text and absolute links for a table cell."""

    text = cell.get_text(" ", strip=True) or None
    links: List[str] = []
    for anchor in cell.find_all("a"):
        href = anchor.get("href")
        if href:
            links.append(urljoin(BASE_URL, href))
    return text, links


def _expanded_rows(table, headers: List[str]) -> List[Dict[str, object]]:
    """Yield rows with HTML rowspans/colspans expanded across columns."""

    rows: List[Dict[str, object]] = []
    tbody = table.find("tbody") or table

    # Track active rowspans for each column index.
    active_spans: List[Dict[str, object] | None] = [None] * len(headers)

    for tr in tbody.find_all("tr"):
        row_values: List[str | None] = [None] * len(headers)
        row_links: List[List[str]] = [[] for _ in headers]

        # Pre-fill values from active rowspans.
        for idx, span in enumerate(active_spans):
            if span:
                row_values[idx] = span["text"]
                row_links[idx] = span["links"]
                span["remaining"] -= 1
                if span["remaining"] <= 0:
                    active_spans[idx] = None

        col = 0
        for cell in tr.find_all("td"):
            # Advance to the next empty column.
            while col < len(headers) and row_values[col] is not None:
                col += 1
            text, links = _cell_value(cell)
            rowspan = int(cell.get("rowspan", 1) or 1)
            colspan = int(cell.get("colspan", 1) or 1)

            for offset in range(colspan):
                target = col + offset
                if target >= len(headers):
                    break
                row_values[target] = text
                row_links[target] = links
                if rowspan > 1:
                    active_spans[target] = {
                        "text": text,
                        "links": links,
                        "remaining": rowspan - 1,
                    }
            col += colspan

        row: Dict[str, object] = {}
        for header, value, links in zip(headers, row_values, row_links):
            row[header] = value
            row[f"{header}_links"] = links
        rows.append(row)

    return rows


def parse_tables(html: str) -> Dict[str, pd.DataFrame]:
    """Parse CARP authorization tables into DataFrames keyed by heading title."""

    soup = BeautifulSoup(html, "html.parser")

    candidate_tables: List[Tuple[object, List[str]]] = []
    for table in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True) for th in table.find_all("th")]
        header_lower = [h.lower() for h in headers]
        if "party" in header_lower and "documents" in header_lower:
            candidate_tables.append((table, headers))

    if not candidate_tables:
        raise ValueError("No CARP authorization tables found on the page")

    dataframes: Dict[str, pd.DataFrame] = {}
    for idx, (table, headers) in enumerate(candidate_tables, start=1):
        rows = _expanded_rows(table, headers)
        for row in rows:
            urls: List[str] = []
            for key, value in row.items():
                if key.endswith("_links") and value:
                    urls.extend(value)
            row["urls"] = "; ".join(dict.fromkeys(urls)) or None

        title = _heading_for_table(table) or f"Table {idx}"
        if title in dataframes:
            title = f"{title} ({idx})"
        dataframes[title] = pd.DataFrame(rows)

    return dataframes


def scrape_carp_tables() -> Dict[str, pd.DataFrame]:
    """Public helper to fetch and parse CARP report tables into DataFrames."""

    html = fetch_html()
    return parse_tables(html)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv-dir",
        metavar="PATH",
        help=(
            "Optional directory to write each table as CSV. Filenames are derived from "
            "their headings."
        ),
    )
    args = parser.parse_args()

    tables = scrape_carp_tables()
    for name, df in tables.items():
        print(f"\n=== {name} ===")
        print(df.head())
        if args.csv_dir:
            safe_name = "_".join(name.lower().split())
            path = f"{args.csv_dir.rstrip('/')}/{safe_name}.csv"
            df.to_csv(path, index=False)
            print(f"Saved to {path}")
