# UNFCCC BTR table scraper

This repository contains helpers for turning UNFCCC tables into structured pandas DataFrames with normalized dates and document links:

- Biennial Transparency Reports table from the [UNFCCC website](https://unfccc.int/first-biennial-transparency-reports), including BTR, NID, CRT, CTF tables, TERR and FMCP summary.
- Article 6 CARP tables from both the [CARP reports page](https://unfccc.int/process-and-meetings/the-paris-agreement/article-6/article-62/carp/reports) and [CARP authorizations page](https://unfccc.int/process-and-meetings/the-paris-agreement/article-6/article-62/carp/authorizations), including extracted document URL columns and a flattened `urls` field.
- NDC Registry table from the [UNFCCC NDCREG page](https://unfccc.int/NDCREG), including party, title, language, translation marker, version, status, submission date, a single NDR URL, and translation English URL (when available).
- Long-term Strategies table from the [UNFCCC long-term strategies page](https://unfccc.int/process/the-paris-agreement/long-term-strategies), including party, current submission, current submission URL(s), previous submission, and previous submission URL(s).
- Article 6 TER report-tracking tables from the [CARP reports page](https://unfccc.int/process-and-meetings/the-paris-agreement/article-6/article-62/carp/reports#Initial-reports-and-updated-initial-reports), including report and TER URL columns.
- Article 6.4 Designated National Authorities (DNA) tables from the [National Authorities page](https://unfccc.int/process-and-meetings/the-paris-agreement/article-64-mechanism/national-authorities#country_AtoZ), including country, organization & address, contact, and submitted host party participation requirements.

## Usage

1. Install dependencies:
   ```bash
   python -m pip install -r requirements.txt
   ```
2. Run the BTR scraper to print the first few rows:
   ```bash
   python scrape_btr.py
   ```

   To save the full table to CSV instead of printing a preview, provide an output path:
   ```bash
   python scrape_btr.py --csv btr_table.csv
   ```

You can also import `scrape_to_dataframe` in your own projects:

```python
from scrape_btr import scrape_to_dataframe

df = scrape_to_dataframe()
print(df.head())
```

## Article 6 CARP authorizations

Run the CARP scraper to print the first few rows of each table found on both CARP pages (reports + authorizations):

```bash
python scrape_carp.py
```

To save each table as a CSV (filename derived from the table heading), supply a directory path:

```bash
python scrape_carp.py --csv-dir ./carp_tables
```

## NDC Registry table

Run the NDC Registry scraper to print the first few rows:

```bash
python scrape_ndcreg.py
```

To save the full table to CSV:

```bash
python scrape_ndcreg.py --csv ndc_registry.csv
```

## Long-term Strategies table

Run the Long-term Strategies scraper to print the first few rows:

```bash
python scrape_lts.py
```

To save the full table to CSV:

```bash
python scrape_lts.py --csv long_term_strategies.csv
```

## Article 6 TER report tracking table

Run the TER scraper to print the first few rows:

```bash
python scrape_ter.py
```

To save the full table to CSV:

```bash
python scrape_ter.py --csv ter_reports.csv
```

## Article 6.4 DNA list table

Run the DNA scraper to print the first few rows:

```bash
python scrape_dna.py
```

To save the full table to CSV:

```bash
python scrape_dna.py --csv dna_list.csv
```
