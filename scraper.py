"""
RMP_PGIM scraper — PGIM EM Debt Local Currency Fund

Responsibility: DOWNLOAD ONLY.
  Downloads the EMLocal datasheet PDF from the PGIM website using
  undetected_chromedriver and saves it to downloads/.

Source: https://www.pgim.com/us/en/intermediary/investment-capabilities/
        products/mutual-funds/pgim-emerging-markets-debt-local-currency-fund#literature

All download logic lives in main1.py (unchanged). This file provides the
standard fetch_data(downloads_dir) interface required by CLAUDE.md.

Usage:
  cd RMP_PGIM
  python -c "import scraper; print(scraper.fetch_data())"
"""

import os
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

PAGE_URL = (
    "https://www.pgim.com/us/en/intermediary/investment-capabilities/products/"
    "mutual-funds/pgim-emerging-markets-debt-local-currency-fund#literature"
)


def fetch_data(downloads_dir: str = "downloads") -> str:
    """
    Download the PGIM EMLocal datasheet PDF.

    Saves the PDF to downloads_dir and returns its absolute path.
    All browser/download logic is unchanged from main1.download_emlocal_datasheet().
    """
    import main1

    os.makedirs(downloads_dir, exist_ok=True)
    abs_dir = os.path.abspath(downloads_dir)

    # Temporarily redirect main1's DOWNLOAD_DIR to our downloads folder
    original_dir = main1.DOWNLOAD_DIR
    main1.DOWNLOAD_DIR = abs_dir
    try:
        main1.download_emlocal_datasheet()
    finally:
        main1.DOWNLOAD_DIR = original_dir

    # Return the most recently downloaded PDF
    pdfs = sorted(
        Path(abs_dir).glob("*.pdf"),
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )
    if pdfs:
        print(f"[scraper] Downloaded: {pdfs[0].name}")
        return str(pdfs[0])
    raise FileNotFoundError(f"No PDF found in {downloads_dir} after download")
