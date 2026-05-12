"""
RMP_PGIM mapper — PGIM EM Debt Local Currency Fund

Responsibility: EXTRACT + TRANSFORM.
  Opens the downloaded PDF (using pdfplumber), extracts Portfolio Statistics,
  Country Breakdown, and Currency Breakdown tables, and transforms the result
  into the standard 2-header-row output DataFrame.

All PDF parsing logic lives in map1.py (unchanged). This file provides the
standard map_to_output() / build_metadata_rows() interface required by CLAUDE.md.

Raw file layout (PDF, ~2 pages):
  Page 1: Portfolio Statistics table (Fund vs Bench, 15 rows each)
  Page 1: Side-by-side Country Breakdown / Currency Breakdown table
  176 series total → loaded from headers.json

Output: 2 header rows (codes, descriptions) + data rows, one row per period.
"""

import json
import os
import sys

import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 176 series → load from headers.json
with open(os.path.join(os.path.dirname(__file__), "headers.json"), encoding="utf-8") as _f:
    COLUMN_HEADERS = json.load(_f)


# 18 series added in the updated reference (not present in map1.PGIM_DATA_MAPPING)
_EXTRA_MAPPING = [
    # Country Breakdown — Fund
    {"CODE": "RMP.PGIM.COUNTRY.FUND.ECU.M",  "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Ecuador", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.SLV.M",  "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("El Salvador", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.OMN.M",  "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Oman", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.SAU.M",  "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Saudi Arabia", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.UGA.M",  "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Uganda", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.PRY.M",  "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Paraguay", ["", ""])[0]},
    # Country Breakdown — Bench
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.ECU.M", "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Ecuador", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.SLV.M", "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("El Salvador", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.OMN.M", "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Oman", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.SAU.M", "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Saudi Arabia", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.UGA.M", "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Uganda", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.PRY.M", "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Paraguay", ["", ""])[1]},
    # Currency Breakdown — Fund
    {"CODE": "RMP.PGIM.CURRENCY.FUND.NGN.M", "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Nigerian Naira", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.UGX.M", "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Uganda Shilling", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.PYG.M", "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Paraguay Guarani", ["", ""])[0]},
    # Currency Breakdown — Bench
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.NGN.M", "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Nigerian Naira", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.UGX.M", "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Uganda Shilling", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.PYG.M", "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Paraguay Guarani", ["", ""])[1]},
]


def _extract(pdf_path: str) -> dict:
    """
    Extract all table data from the PGIM PDF.
    Replicates the extraction sequence from map1.process_pgim_pdf()
    without the CSV-save step — all parsing functions are imported unchanged.
    """
    from map1 import (
        extract_tables_from_pdf,
        get_pgim_time_period,
        identify_table_type,
        parse_table_to_dict,
        parse_side_by_side_table,
        split_country_currency_data,
        normalize_extracted_data,
        PGIM_DATA_MAPPING,
    )

    pages_data = extract_tables_from_pdf(pdf_path)
    if not pages_data:
        raise ValueError(f"No pages extracted from {pdf_path}")

    all_texts = [p["text"] for p in pages_data if p["text"]]
    time_period = get_pgim_time_period(all_texts)
    if not time_period:
        raise ValueError(f"Could not determine time period from {pdf_path}")
    print(f"[mapper] Period: {time_period}")

    extracted_data: dict = {}

    for page_data in pages_data:
        page_num = page_data["page_num"]
        page_text = page_data["text"]
        tables = page_data["tables"]
        print(f"[mapper] Page {page_num}: {len(tables)} table(s)")

        for table_idx, table in enumerate(tables):
            if not table or len(table) < 2:
                continue

            table_type = identify_table_type(table, page_text)
            print(f"[mapper]   Table {table_idx + 1}: {table_type!r}")

            if table_type == "portfolio_statistics":
                data = parse_table_to_dict(table)
                if data and len(data) >= 10:
                    if "Portfolio Statistics" not in extracted_data or len(data) > len(
                        extracted_data["Portfolio Statistics"]
                    ):
                        extracted_data["Portfolio Statistics"] = data
                        print(f"[mapper]   Portfolio Statistics: {len(data)} items")

            elif table_type == "side_by_side":
                parsed = parse_side_by_side_table(table)
                if parsed:
                    country_data, currency_data = split_country_currency_data(parsed)
                    if country_data and "Country Breakdown (%)" not in extracted_data:
                        extracted_data["Country Breakdown (%)"] = country_data
                        print(f"[mapper]   Country Breakdown: {len(country_data)} items")
                    if currency_data and "Currency Breakdown (%)" not in extracted_data:
                        extracted_data["Currency Breakdown (%)"] = currency_data
                        print(f"[mapper]   Currency Breakdown: {len(currency_data)} items")

            elif table_type == "country_single":
                data = parse_table_to_dict(table)
                if data:
                    if "Country Breakdown (%)" not in extracted_data or len(data) > len(
                        extracted_data["Country Breakdown (%)"]
                    ):
                        extracted_data["Country Breakdown (%)"] = data
                        print(f"[mapper]   Country Breakdown (single): {len(data)} items")

            elif table_type == "currency_single":
                data = parse_table_to_dict(table)
                if data:
                    if "Currency Breakdown (%)" not in extracted_data or len(data) > len(
                        extracted_data["Currency Breakdown (%)"]
                    ):
                        extracted_data["Currency Breakdown (%)"] = data
                        print(f"[mapper]   Currency Breakdown (single): {len(data)} items")

    for section in ["Portfolio Statistics", "Country Breakdown (%)", "Currency Breakdown (%)"]:
        if section not in extracted_data:
            extracted_data[section] = {}
            print(f"[mapper] WARNING: '{section}' not found")

    extracted_data = normalize_extracted_data(extracted_data)

    # Build {code: value} dict using the same lambdas as PGIM_DATA_MAPPING + _EXTRA_MAPPING
    values = {}
    for item in list(PGIM_DATA_MAPPING) + _EXTRA_MAPPING:
        raw = item["VALUE"](extracted_data)
        if raw and raw != "":
            try:
                values[item["CODE"]] = float(str(raw).replace(",", "").replace("%", "").strip())
            except (ValueError, TypeError):
                values[item["CODE"]] = raw

    print(f"[mapper] Total series extracted: {len(values)}")
    return {"period": time_period, "data": values}


def _record_to_row(period: str, data: dict) -> list:
    """Convert period + data dict to a 177-element output row [date, v1..v176]."""
    codes = COLUMN_HEADERS["codes"]
    row = [period] + [None] * (len(codes) - 1)
    for i, code in enumerate(codes[1:], start=1):
        row[i] = data.get(code)
    return row


def map_to_output(pdf_path: str, existing_path: str = None) -> pd.DataFrame:
    """
    Extract data from the PDF and merge into existing history.

    pdf_path:      path to the downloaded PGIM PDF
    existing_path: path to existing DATA xlsx (rows 0/1 = headers, row 2+ = data)

    Returns DataFrame with 2 header rows + date-sorted data rows.
    """
    existing_rows: dict[str, list] = {}

    if existing_path and os.path.exists(existing_path):
        ex = pd.read_excel(existing_path, header=None)
        for _, r in ex.iloc[2:].iterrows():
            date_val = str(r.iloc[0]) if pd.notna(r.iloc[0]) else None
            if date_val:
                existing_rows[date_val] = list(r)
        print(f"[mapper] Loaded {len(existing_rows)} existing rows from {existing_path}")

    extracted = _extract(pdf_path)
    period = extracted["period"]
    row = _record_to_row(period, extracted["data"])
    existing_rows[period] = row
    print(f"[mapper] Mapped period: {period}")

    sorted_rows = [existing_rows[d] for d in sorted(existing_rows)]
    all_rows = [COLUMN_HEADERS["codes"], COLUMN_HEADERS["descriptions"]] + sorted_rows
    return pd.DataFrame(all_rows)


def build_metadata_rows() -> list:
    """Return list of metadata dicts for each series — used by main.py for META xlsx."""
    codes = COLUMN_HEADERS["codes"][1:]
    descs = COLUMN_HEADERS["descriptions"][1:]
    rows = []
    for code, desc in zip(codes, descs):
        rows.append({
            "CODE":              code,
            "DESCRIPTION":       desc,
            "FREQUENCY":         "Monthly",
            "UNIT":              "%",
            "SOURCE_NAME":       "PGIM",
            "SOURCE_URL":        "https://www.pgim.com/us/en/intermediary/investment-capabilities/"
                                 "products/mutual-funds/pgim-emerging-markets-debt-local-currency-fund",
            "DATASET":           "RMP_PGIM",
            "NEXT_RELEASE_DATE": "",
        })
    return rows
