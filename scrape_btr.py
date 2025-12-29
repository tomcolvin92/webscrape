"""Scrape Biennial Transparency Report table into a pandas DataFrame.

The script fetches the table shown on
https://unfccc.int/first-biennial-transparency-reports and normalizes
submission dates and links for BTR, NID, CRT, annexes, TERR, and FMCP
summary reports.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import urljoin

BASE_URL = "https://unfccc.int"
TARGET_URL = "https://unfccc.int/first-biennial-transparency-reports"


def fetch_html(url: str = TARGET_URL) -> str:
    """Return the raw HTML for the target page.

    Requests is imported lazily so that the module can still be
    inspected or type-checked without third-party dependencies being
    installed.

    The helper attempts multiple strategies, starting with the most
    restrictive proxy-disabling approach and slowly allowing environment
    settings to be used:
    1) Explicitly clear proxies using empty strings while bypassing env
    2) Disable env proxies with explicit ``None`` overrides
    3) Disable env proxies without overrides
    4) Fall back to the default environment configuration (e.g., corporate proxy)
    """

    import requests

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; webscrape-btr/1.0)",
    }

    errors: List[str] = []

    def attempt(
        trust_env: bool, proxies: Optional[Dict[str, Optional[str]]], label: str
    ):
        session = requests.Session()
        session.trust_env = trust_env
        try:
            response = session.get(
                url,
                headers=headers,
                timeout=30,
                proxies=proxies,
            )
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:  # type: ignore[name-defined]
            errors.append(f"{label}: {exc}")
            return None

    # Prefer a proxy-less request to avoid MITM proxies returning 403 for
    # CONNECT tunnels. The empty-string proxies variant prevents Requests
    # from inheriting proxy settings even when an intermediate network
    # layer injects them.
    html = attempt(
        trust_env=False,
        proxies={"http": "", "https": ""},
        label="proxy-cleared (empty strings)",
    )
    if html is None:
        html = attempt(
            trust_env=False,
            proxies={"http": None, "https": None},
            label="proxy-cleared (None)",
        )
    if html is None:
        html = attempt(trust_env=False, proxies=None, label="trust_env=False")
    if html is None:
        # Fall back to the environment configuration (e.g., corporate proxy)
        html = attempt(trust_env=True, proxies=None, label="trust_env=True")

    if html is None:
        raise ConnectionError(
            "Unable to fetch the Biennial Transparency Reports page. "
            f"Tried strategies: {'; '.join(errors)}."
        )

    return html


@dataclass
class SubmissionEntry:
    label: str
    date: Optional[str]
    url: Optional[str]


@dataclass
class CountryRow:
    party: str
    submission_date: Optional[str]
    btr_date: Optional[str]
    btr_link: Optional[str]
    nid_date: Optional[str]
    nid_link: Optional[str]
    crt_date: Optional[str]
    crt_link: Optional[str]
    ctf_date: Optional[str]
    ctf_link: Optional[str]
    annex_date: Optional[str]
    annex_link: Optional[str]
    terr_link: Optional[str]
    fmcp_summary_link: Optional[str]


def _target_table(soup) -> Optional["bs4.element.Tag"]:
    """Find the table with a header containing *Party*.

    The UNFCCC page only exposes one large table, but the header check
    provides a safeguard in case the page layout changes slightly.
    """

    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        if any(h.lower() == "party" for h in headers):
            return table
    return None


def _collect_links(cell) -> List[str]:
    """Return absolute URLs from all anchors in a table cell."""

    links: List[str] = []
    for anchor in cell.find_all("a"):
        href = anchor.get("href")
        if href:
            links.append(urljoin(BASE_URL, href))
    return links


def _extract_entries(cell, labels: List[str]) -> Dict[str, SubmissionEntry]:
    """Extract submission dates and URLs keyed by label.

    The cells for BTR/NID/CRT and annexes are structured as a label
    followed by a date on the next line. Anchors are used for the actual
    download links. This parser attempts to stay resilient by looking for
    label keywords in both the text and link text.
    """

    from bs4 import BeautifulSoup  # lazy import for optional dependency

    soup = BeautifulSoup(str(cell), "html.parser")

    entries: Dict[str, SubmissionEntry] = {
        label: SubmissionEntry(label=label, date=None, url=None)
        for label in labels
    }

    # Associate links with their labels.
    for anchor in soup.find_all("a"):
        label = _match_label(anchor.get_text(" ", strip=True), labels)
        if label:
            entries[label].url = urljoin(BASE_URL, anchor.get("href"))

    # Capture dates based on the textual layout: label line followed by
    # date line.
    current: Optional[str] = None
    for line in _cell_lines(soup):
        label = _match_label(line, labels)
        if label:
            current = label
            continue
        if current and line:
            entries[current].date = line
            current = None

    return entries


def _merge_entries(
    primary: Dict[str, SubmissionEntry],
    fallback: Dict[str, SubmissionEntry],
) -> Dict[str, SubmissionEntry]:
    """Merge two SubmissionEntry mappings, preferring primary values."""

    merged: Dict[str, SubmissionEntry] = {}
    for label, primary_entry in primary.items():
        fallback_entry = fallback.get(label, SubmissionEntry(label, None, None))
        merged[label] = SubmissionEntry(
            label=label,
            date=primary_entry.date or fallback_entry.date,
            url=primary_entry.url or fallback_entry.url,
        )
    return merged


def _match_label(text: str, labels: List[str]) -> Optional[str]:
    """Match text to one of the expected labels, case-insensitively."""

    normalized = text.lower()
    for label in labels:
        if label.lower() in normalized:
            return label
    return None


def _cell_lines(cell) -> List[str]:
    """Return significant text lines from a table cell."""

    text = cell.get_text("\n", strip=True)
    return [line.strip() for line in text.split("\n") if line.strip()]


def parse_rows(html: str) -> List[CountryRow]:
    """Convert the HTML page into structured CountryRow entries."""

    from bs4 import BeautifulSoup  # lazy import for optional dependency

    soup = BeautifulSoup(html, "html.parser")
    table = _target_table(soup)
    if not table:
        raise ValueError("Could not find the Biennial Transparency Report table")

    rows: List[CountryRow] = []
    body = table.find("tbody") or table
    for tr in body.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 7:
            continue

        party = cells[0].get_text(strip=True)
        submission_date = cells[1].get_text(" ", strip=True) or None

        btr_entries = _extract_entries(cells[2], ["BTR", "NID", "CRT"])
        ctf_dates = _extract_entries(cells[3], ["CTF"])
        ctf_links = _extract_entries(cells[4], ["CTF"])
        annex_entries = _merge_entries(ctf_links, ctf_dates)

        terr_links = _collect_links(cells[5])
        fmcp_links = _collect_links(cells[6])

        rows.append(
            CountryRow(
                party=party,
                submission_date=submission_date or None,
                btr_date=btr_entries["BTR"].date,
                btr_link=btr_entries["BTR"].url,
                nid_date=btr_entries["NID"].date,
                nid_link=btr_entries["NID"].url,
                crt_date=btr_entries["CRT"].date,
                crt_link=btr_entries["CRT"].url,
                ctf_date=annex_entries["CTF"].date,
                ctf_link=annex_entries["CTF"].url,
                annex_date=annex_entries["CTF"].date,
                annex_link=annex_entries["CTF"].url,
                terr_link=terr_links[0] if terr_links else None,
                fmcp_summary_link=fmcp_links[0] if fmcp_links else None,
            )
        )

    return rows


def to_dataframe(rows: List[CountryRow]):
    """Convert the parsed rows into a pandas DataFrame."""

    import pandas as pd

    return pd.DataFrame([row.__dict__ for row in rows])


def scrape_to_dataframe() -> "pd.DataFrame":
    """Public helper that fetches the page and returns a pandas DataFrame."""

    html = fetch_html()
    rows = parse_rows(html)
    return to_dataframe(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        metavar="PATH",
        help="Write the full table to PATH as CSV (instead of printing a preview).",
    )
    args = parser.parse_args()

    try:
        df = scrape_to_dataframe()
    except ImportError as exc:
        missing = getattr(exc, "name", str(exc))
        raise SystemExit(
            f"Missing dependency '{missing}'. Install with: pip install -r requirements.txt"
        ) from exc

    print(f"Scraped {len(df)} rows with columns: {', '.join(df.columns)}")
    if args.csv:
        df.to_csv(args.csv, index=False)
        print(f"Wrote CSV to {args.csv}")
    else:
        print(df.head())
