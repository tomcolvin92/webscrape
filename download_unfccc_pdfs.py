"""Helpers for downloading UNFCCC PDFs from scraped URLs.

Supported URL patterns include:
- /documents/<id> pages where the PDF is linked behind a download button.
- /node/<id> pages that redirect to /documents/<id>.
- Direct .pdf links (including links that include a page fragment like #page=2).
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Iterable
from urllib.parse import unquote, urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

USER_AGENT = "Mozilla/5.0 (compatible; unfccc-pdf-downloader/1.0)"
TIMEOUT = 30


def _safe_name(value: str, fallback: str) -> str:
    """Return a filesystem-safe name."""
    cleaned = re.sub(r"[\\/:*?\"<>|]+", "_", str(value or "")).strip(" .")
    return cleaned or fallback


def _filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = Path(unquote(parsed.path)).name
    if not name:
        return "document.pdf"
    if not name.lower().endswith(".pdf"):
        return f"{name}.pdf"
    return name


def _filename_from_headers(response: requests.Response) -> str | None:
    disposition = response.headers.get("content-disposition", "")
    if not disposition:
        return None

    match = re.search(r"filename\*=UTF-8''([^;]+)", disposition, flags=re.IGNORECASE)
    if match:
        return _safe_name(unquote(match.group(1)), "document.pdf")

    match = re.search(r'filename="?([^";]+)"?', disposition, flags=re.IGNORECASE)
    if match:
        return _safe_name(match.group(1), "document.pdf")

    return None


def _looks_like_pdf_response(response: requests.Response) -> bool:
    content_type = (response.headers.get("content-type") or "").lower()
    return "application/pdf" in content_type or response.url.lower().endswith(".pdf")


def _extract_candidate_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []

    for tag in soup.select("a[href]"):
        href = tag["href"].strip()
        if not href:
            continue
        full_url = urljoin(base_url, href)

        lowered_href = href.lower()
        lowered_text = tag.get_text(" ", strip=True).lower()
        if (
            ".pdf" in lowered_href
            or "download" in lowered_href
            or lowered_text in {"download", "download pdf", "pdf"}
            or "/documents/" in lowered_href
        ):
            links.append(full_url)

    # Keep order but drop duplicates.
    deduped = list(dict.fromkeys(links))

    # Prefer obvious PDF or download URLs first.
    def rank(u: str) -> tuple[int, str]:
        lower = u.lower()
        if lower.endswith(".pdf"):
            return (0, lower)
        if "download" in lower:
            return (1, lower)
        if "/documents/" in lower:
            return (2, lower)
        return (3, lower)

    return sorted(deduped, key=rank)


def resolve_pdf_url(url: str, session: requests.Session | None = None) -> tuple[str, str | None]:
    """Resolve a URL to a final downloadable PDF URL.

    Returns:
        (resolved_pdf_url, filename_hint)
    """
    session = session or requests.Session()
    session.headers.setdefault("User-Agent", USER_AGENT)

    clean_url = url.split("#", 1)[0]
    response = session.get(clean_url, allow_redirects=True, timeout=TIMEOUT)

    if _looks_like_pdf_response(response):
        return response.url, _filename_from_headers(response) or _filename_from_url(response.url)

    candidates = _extract_candidate_links(response.text, response.url)
    for candidate in candidates:
        probe_url = candidate.split("#", 1)[0]
        try:
            candidate_response = session.get(probe_url, allow_redirects=True, timeout=TIMEOUT)
        except requests.RequestException:
            continue
        if _looks_like_pdf_response(candidate_response):
            return (
                candidate_response.url,
                _filename_from_headers(candidate_response)
                or _filename_from_url(candidate_response.url),
            )

    raise ValueError(f"Could not resolve a downloadable PDF from URL: {url}")


def download_pdf(
    url: str,
    party: str,
    output_root: str | Path,
    recommended_name: str | None = None,
    overwrite: bool = False,
    session: requests.Session | None = None,
) -> Path:
    """Download one PDF under <output_root>/<party>/<filename>."""
    session = session or requests.Session()
    session.headers.setdefault("User-Agent", USER_AGENT)

    pdf_url, filename_hint = resolve_pdf_url(url, session=session)
    file_name = _safe_name(recommended_name or filename_hint or _filename_from_url(pdf_url), "document.pdf")
    if not file_name.lower().endswith(".pdf"):
        file_name = f"{file_name}.pdf"

    party_folder = Path(output_root) / _safe_name(party, "Unknown")
    party_folder.mkdir(parents=True, exist_ok=True)

    destination = party_folder / file_name
    if destination.exists() and not overwrite:
        base = destination.stem
        suffix = destination.suffix
        idx = 2
        while True:
            candidate = party_folder / f"{base}_{idx}{suffix}"
            if not candidate.exists():
                destination = candidate
                break
            idx += 1

    with session.get(pdf_url, stream=True, timeout=TIMEOUT) as response:
        response.raise_for_status()
        with destination.open("wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 64):
                if chunk:
                    f.write(chunk)

    return destination


def _coerce_urls(value: object) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []

    if isinstance(value, (list, tuple, set)):
        return [str(v).strip() for v in value if str(v).strip()]

    text = str(value).strip()
    if not text:
        return []

    if text.startswith("[") and text.endswith("]"):
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(text)
                if isinstance(parsed, list):
                    return [str(v).strip() for v in parsed if str(v).strip()]
            except Exception:
                pass

    found = re.findall(r"https?://[^\s,;|]+", text)
    if found:
        return found

    return [text]


def download_pdfs_from_dataframe(
    df: pd.DataFrame,
    output_root: str | Path,
    party_col: str = "party",
    urls_col: str = "urls",
    filename_col: str | None = "recommended_name",
    overwrite: bool = False,
) -> list[dict[str, str]]:
    """Download PDFs for each row and return a log of outcomes.

    Returns entries with keys: status, party, source_url, file_path, error.
    """
    results: list[dict[str, str]] = []
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    for _, row in df.iterrows():
        party = str(row.get(party_col, "Unknown") or "Unknown")
        recommended = str(row.get(filename_col)).strip() if filename_col and pd.notna(row.get(filename_col)) else None

        for source_url in _coerce_urls(row.get(urls_col)):
            try:
                path = download_pdf(
                    url=source_url,
                    party=party,
                    output_root=output_root,
                    recommended_name=recommended,
                    overwrite=overwrite,
                    session=session,
                )
                results.append(
                    {
                        "status": "ok",
                        "party": party,
                        "source_url": source_url,
                        "file_path": str(path),
                        "error": "",
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        "status": "error",
                        "party": party,
                        "source_url": source_url,
                        "file_path": "",
                        "error": str(exc),
                    }
                )

    return results


if __name__ == "__main__":
    # Example usage:
    # df = pd.DataFrame(
    #     [
    #         {
    #             "party": "Chile",
    #             "urls": "https://unfccc.int/node/645238",
    #             "recommended_name": "chile_submission",
    #         }
    #     ]
    # )
    # print(download_pdfs_from_dataframe(df, output_root="./downloads"))
    pass
