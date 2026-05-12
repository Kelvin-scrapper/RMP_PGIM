"""
RMP_PGIM mapper — PGIM EM Debt Local Currency Fund

Responsibility: EXTRACT + TRANSFORM.
  Opens the downloaded PDF (using pdfplumber), extracts Portfolio Statistics,
  Country Breakdown, and Currency Breakdown tables, and transforms the result
  into the standard 2-header-row output DataFrame.

Raw file layout (PDF, ~2 pages):
  Page 1: Portfolio Statistics table (Fund vs Bench, 15 rows each)
  Page 1: Side-by-side Country Breakdown / Currency Breakdown table
  194 series total → loaded from headers.json

Output: 2 header rows (codes, descriptions) + data rows, one row per period.
"""

import json
import os
import re
import sys
from difflib import SequenceMatcher

import pandas as pd
import pdfplumber

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 194 series → load from headers.json
with open(os.path.join(os.path.dirname(__file__), "headers.json"), encoding="utf-8") as _f:
    COLUMN_HEADERS = json.load(_f)

_MONTH_MAP = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}

# ── Full data mapping (194 series) ────────────────────────────────────────────
PGIM_DATA_MAPPING = [
    # Portfolio Statistics — Fund
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.TOTALNETASSET.M",  "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Total Net Assets ($ millions)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.AVERPRICE.M",      "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Average Price", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.DURATION.M",       "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Duration (yrs)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.CONVEXITY.M",      "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Convexity", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.YIELDWORST.M",     "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Yield to Worst (YTW) (gross)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.YIELDMATURITY.M",  "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Yield to Maturity (YTM) (gross)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.WEIGHAVERLIFE.M",  "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Weighted Average Life (yrs)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.AVERMATURITY.M",   "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Average Maturity (yrs)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.OPTADJUSTSPREAD.M","VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Option Adjusted Spread (bps)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.SPREADDURATION.M", "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Spread Duration (yrs)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.AVERCOUPON.M",     "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Average Coupon (gross)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.NUMISSUES.M",      "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Number of Issues", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.NUMISSUERS.M",     "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Number of Issuers", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.SEC30SUBSIDYIELD.M","VALUE": lambda d: d.get("Portfolio Statistics", {}).get("SEC 30-Day Subsidized Yield", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.SEC30UNSUBYIELD.M","VALUE": lambda d: d.get("Portfolio Statistics", {}).get("SEC 30-Day Unsubsidized Yield", ["", ""])[0]},
    # Portfolio Statistics — Bench
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.TOTALNETASSET.M", "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Total Net Assets ($ millions)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.AVERPRICE.M",     "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Average Price", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.DURATION.M",      "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Duration (yrs)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.CONVEXITY.M",     "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Convexity", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.YIELDWORST.M",    "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Yield to Worst (YTW) (gross)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.YIELDMATURITY.M", "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Yield to Maturity (YTM) (gross)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.WEIGHAVERLIFE.M", "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Weighted Average Life (yrs)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.AVERMATURITY.M",  "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Average Maturity (yrs)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.OPTADJUSTSPREAD.M","VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Option Adjusted Spread (bps)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.SPREADDURATION.M","VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Spread Duration (yrs)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.AVERCOUPON.M",    "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Average Coupon (gross)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.NUMISSUES.M",     "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Number of Issues", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.NUMISSUERS.M",    "VALUE": lambda d: d.get("Portfolio Statistics", {}).get("Number of Issuers", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.SEC30SUBSIDYIELD.M","VALUE": lambda d: d.get("Portfolio Statistics", {}).get("SEC 30-Day Subsidized Yield", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.SEC30UNSUBYIELD.M","VALUE": lambda d: d.get("Portfolio Statistics", {}).get("SEC 30-Day Unsubsidized Yield", ["", ""])[1]},
    # Country Breakdown — Fund
    {"CODE": "RMP.PGIM.COUNTRY.FUND.USA.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("United States", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.NONUSA.M",      "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Non-U.S.", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.ARG.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Argentina", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.BRA.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Brazil", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.CHL.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Chile", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.CHN.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("China", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.COL.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Colombia", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.CIV.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Cote D'Ivoire", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.CZE.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Czech Republic", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.DOM.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Dominican Republic", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.DEU.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Germany", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.GTM.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Guatemala", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.HUN.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Hungary", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.IND.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("India", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.IDN.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Indonesia", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.JAM.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Jamaica", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.KOR.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Korea (South), Republic of", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.MYS.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Malaysia", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.MEX.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Mexico", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.MULTNATIONAL.M","VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Multinational", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.PAN.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Panama", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.PER.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Peru", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.PHL.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Philippines", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.POL.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Poland", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.ROU.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Romania", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.ZAF.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("South Africa", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.SUPNATIONAL.M", "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Supranational", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.THA.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Thailand", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.SRB.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("The Republic of Serbia", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.TUR.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Turkey", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.URY.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Uruguay", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.CASH.M",        "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Cash/Cash Equivalents", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.TOTAL.M",       "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Total", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.MWI.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Malawi", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.ECU.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Ecuador", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.SLV.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("El Salvador", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.OMN.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Oman", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.SAU.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Saudi Arabia", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.UGA.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Uganda", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.PRY.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Paraguay", ["", ""])[0]},
    # Country Breakdown — Bench
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.USA.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("United States", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.NONUSA.M",      "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Non-U.S.", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.ARG.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Argentina", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.BRA.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Brazil", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.CHL.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Chile", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.CHN.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("China", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.COL.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Colombia", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.CIV.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Cote D'Ivoire", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.CZE.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Czech Republic", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.DOM.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Dominican Republic", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.DEU.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Germany", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.GTM.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Guatemala", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.HUN.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Hungary", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.IND.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("India", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.IDN.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Indonesia", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.JAM.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Jamaica", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.KOR.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Korea (South), Republic of", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.MYS.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Malaysia", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.MEX.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Mexico", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.MULTNATIONAL.M","VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Multinational", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.PAN.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Panama", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.PER.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Peru", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.PHL.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Philippines", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.POL.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Poland", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.ROU.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Romania", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.ZAF.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("South Africa", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.SUPNATIONAL.M", "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Supranational", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.THA.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Thailand", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.SRB.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("The Republic of Serbia", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.TUR.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Turkey", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.URY.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Uruguay", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.CASH.M",        "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Cash/Cash Equivalents", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.TOTAL.M",       "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Total", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.MWI.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Malawi", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.ECU.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Ecuador", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.SLV.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("El Salvador", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.OMN.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Oman", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.SAU.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Saudi Arabia", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.UGA.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Uganda", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.PRY.M",         "VALUE": lambda d: d.get("Country Breakdown (%)", {}).get("Paraguay", ["", ""])[1]},
    # Currency Breakdown — Fund
    {"CODE": "RMP.PGIM.CURRENCY.FUND.USD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("U.S. Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.NONUSD.M", "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Non-U.S. Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.ARS.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Argentinian Peso", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.AUD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Australian Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.BRL.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Brazilian Real", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.GBP.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("British Pound", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.CAD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Canadian Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.CLP.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Chilean Peso", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.CNY.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Chinese Yuan", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.CNH.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Chinese Offshore Yuan", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.COP.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Colombian Peso", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.CZK.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Czech Koruna", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.DOP.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Dominican Peso", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.EGP.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Egyptian Pound", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.EUR.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Euro", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.HKD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Hong Kong Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.HUF.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Hungarian Forint", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.INR.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Indian Rupee", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.IDR.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Indonesian Rupiah", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.ILS.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Israeli Shekel", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.JPY.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Japanese Yen", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.MYR.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Malaysian Ringgit", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.MXN.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Mexican Peso", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.NZD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("New Zealand Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.NOK.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Norwegian Kroner", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.PEN.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Peruvian Nuevo Sol", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.PHP.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Philippine Peso", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.PLN.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Polish Zloty", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.RON.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Romanian Lei", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.RSD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Serbian Dinar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.SGD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Singapore Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.ZAR.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("South African Rand", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.KRW.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("South Korean Won", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.CHF.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Swiss Franc", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.TWD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Taiwan Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.THB.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Thai Baht", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.TRY.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Turkish Lira", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.UYU.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Uruguayan Peso", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.NGN.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Nigerian Naira", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.UGX.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Uganda Shilling", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.PYG.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Paraguay Guarani", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.TOTAL.M",  "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Total", ["", ""])[0]},
    # Currency Breakdown — Bench
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.USD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("U.S. Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.NONUSD.M", "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Non-U.S. Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.ARS.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Argentinian Peso", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.AUD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Australian Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.BRL.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Brazilian Real", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.GBP.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("British Pound", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.CAD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Canadian Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.CLP.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Chilean Peso", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.CNY.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Chinese Yuan", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.CNH.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Chinese Offshore Yuan", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.COP.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Colombian Peso", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.CZK.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Czech Koruna", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.DOP.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Dominican Peso", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.EGP.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Egyptian Pound", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.EUR.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Euro", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.HKD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Hong Kong Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.HUF.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Hungarian Forint", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.INR.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Indian Rupee", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.IDR.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Indonesian Rupiah", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.ILS.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Israeli Shekel", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.JPY.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Japanese Yen", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.MYR.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Malaysian Ringgit", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.MXN.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Mexican Peso", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.NZD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("New Zealand Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.NOK.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Norwegian Kroner", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.PEN.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Peruvian Nuevo Sol", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.PHP.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Philippine Peso", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.PLN.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Polish Zloty", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.RON.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Romanian Lei", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.RSD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Serbian Dinar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.SGD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Singapore Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.ZAR.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("South African Rand", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.KRW.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("South Korean Won", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.CHF.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Swiss Franc", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.TWD.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Taiwan Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.THB.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Thai Baht", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.TRY.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Turkish Lira", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.UYU.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Uruguayan Peso", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.NGN.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Nigerian Naira", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.UGX.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Uganda Shilling", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.PYG.M",    "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Paraguay Guarani", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.TOTAL.M",  "VALUE": lambda d: d.get("Currency Breakdown (%)", {}).get("Total", ["", ""])[1]},
]

# ── PDF extraction helpers ────────────────────────────────────────────────────

def _extract_tables_from_pdf(pdf_path):
    pages = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                pages.append({
                    "page_num": i + 1,
                    "text":     page.extract_text(),
                    "tables":   page.extract_tables(),
                })
    except Exception as e:
        print(f"[mapper] Error reading PDF: {e}")
    return pages


def _get_time_period(texts):
    for text in texts:
        m = re.search(r"as of (\w+) \d{1,2}, (\d{4})", text, re.IGNORECASE)
        if m:
            month = _MONTH_MAP.get(m.group(1).lower())
            if month:
                return f"{m.group(2)}-{month}"
    return None


def _fuzzy(s1, s2):
    if not s1 or not s2:
        return 0.0
    return SequenceMatcher(None, s1.lower(), s2.lower()).ratio()


def _best_match(target, candidates, threshold=0.6):
    best, score = None, threshold
    for c in candidates:
        s = _fuzzy(target, c)
        if s > score:
            score, best = s, c
    return best


def _parse_table_to_dict(table):
    data = {}
    for row in table[1:]:
        if not row or len(row) < 3:
            continue
        label = str(row[0]).strip() if row[0] else ""
        fund  = str(row[1]).strip() if row[1] else ""
        bench = str(row[2]).strip() if row[2] else ""
        if label and label.lower() not in ("fund", "bench", "active", ""):
            data[label] = [fund, bench]
    return data


def _parse_side_by_side(table):
    rows = []
    for row in table[1:]:
        if not row or len(row) < 6:
            continue
        rows.append({
            "left":  {"label": str(row[0]).strip() if row[0] else "",
                      "fund":  str(row[1]).strip() if row[1] else "",
                      "bench": str(row[2]).strip() if row[2] else ""},
            "right": {"label": str(row[4]).strip() if len(row) > 4 and row[4] else "",
                      "fund":  str(row[5]).strip() if len(row) > 5 and row[5] else "",
                      "bench": str(row[6]).strip() if len(row) > 6 and row[6] else ""},
        })
    return rows


def _split_country_currency(parsed):
    country, currency = {}, {}
    for r in parsed:
        lbl = r["left"]["label"]
        if lbl and lbl.lower() not in ("fund", "bench", "active", ""):
            country[lbl] = [r["left"]["fund"], r["left"]["bench"]]
        lbl = r["right"]["label"]
        if lbl and lbl.lower() not in ("fund", "bench", "active", ""):
            currency[lbl] = [r["right"]["fund"], r["right"]["bench"]]
    return country, currency


def _identify_table(table, page_text):
    if not table or len(table) < 2:
        return None
    all_text = " ".join(
        str(c).lower() for row in table[:5] for c in (row or []) if c
    )
    portfolio_kw = ["portfolio statistics", "total net assets", "duration", "convexity",
                    "yield to worst", "yield to maturity", "spread duration", "average coupon"]
    if sum(1 for kw in portfolio_kw if kw in all_text) >= 3:
        if "country breakdown" not in all_text and "currency breakdown" not in all_text:
            return "portfolio_statistics"
    num_cols = len(table[0]) if table[0] else 0
    if num_cols >= 7 and "country breakdown" in all_text and "currency breakdown" in all_text:
        return "side_by_side"
    if "country breakdown" in all_text and "currency" not in all_text and num_cols < 7:
        return "country_single"
    if "currency breakdown" in all_text and "country" not in all_text and num_cols < 7:
        return "currency_single"
    return None


def _normalize(extracted_data):
    expected = {
        "Portfolio Statistics": [
            "Total Net Assets ($ millions)", "Average Price", "Duration (yrs)", "Convexity",
            "Yield to Worst (YTW) (gross)", "Yield to Maturity (YTM) (gross)",
            "Weighted Average Life (yrs)", "Average Maturity (yrs)",
            "Option Adjusted Spread (bps)", "Spread Duration (yrs)",
            "Average Coupon (gross)", "Number of Issues", "Number of Issuers",
            "SEC 30-Day Subsidized Yield", "SEC 30-Day Unsubsidized Yield",
        ],
        "Country Breakdown (%)": [
            "United States", "Non-U.S.", "Argentina", "Brazil", "Chile", "China", "Colombia",
            "Cote D'Ivoire", "Czech Republic", "Dominican Republic", "Germany", "Guatemala",
            "Hungary", "India", "Indonesia", "Jamaica", "Korea (South), Republic of", "Malaysia",
            "Mexico", "Multinational", "Panama", "Peru", "Philippines", "Poland", "Romania",
            "South Africa", "Supranational", "Thailand", "The Republic of Serbia", "Turkey",
            "Uruguay", "Cash/Cash Equivalents", "Total", "Malawi",
            "Ecuador", "El Salvador", "Oman", "Saudi Arabia", "Uganda", "Paraguay",
        ],
        "Currency Breakdown (%)": [
            "U.S. Dollar", "Non-U.S. Dollar", "Argentinian Peso", "Australian Dollar",
            "Brazilian Real", "British Pound", "Canadian Dollar", "Chilean Peso",
            "Chinese Yuan", "Chinese Offshore Yuan", "Colombian Peso", "Czech Koruna",
            "Dominican Peso", "Egyptian Pound", "Euro", "Hong Kong Dollar", "Hungarian Forint",
            "Indian Rupee", "Indonesian Rupiah", "Israeli Shekel", "Japanese Yen",
            "Malaysian Ringgit", "Mexican Peso", "New Zealand Dollar", "Norwegian Kroner",
            "Peruvian Nuevo Sol", "Philippine Peso", "Polish Zloty", "Romanian Lei",
            "Serbian Dinar", "Singapore Dollar", "South African Rand", "South Korean Won",
            "Swiss Franc", "Taiwan Dollar", "Thai Baht", "Turkish Lira", "Uruguayan Peso",
            "Nigerian Naira", "Uganda Shilling", "Paraguay Guarani", "Total",
        ],
    }
    normalized = {}
    for cat, data_dict in extracted_data.items():
        if cat not in expected:
            normalized[cat] = data_dict
            continue
        norm = {}
        for field, vals in data_dict.items():
            match = _best_match(field, expected[cat], threshold=0.65)
            if match:
                norm[match] = vals
                if match != field:
                    print(f"[mapper]   Fuzzy: '{field}' → '{match}'")
            else:
                norm[field] = vals
        normalized[cat] = norm
    return normalized


# ── core extraction ───────────────────────────────────────────────────────────

def _extract(pdf_path: str) -> dict:
    pages = _extract_tables_from_pdf(pdf_path)
    if not pages:
        raise ValueError(f"No pages extracted from {pdf_path}")

    time_period = _get_time_period([p["text"] for p in pages if p["text"]])
    if not time_period:
        raise ValueError(f"Could not determine time period from {pdf_path}")
    print(f"[mapper] Period: {time_period}")

    extracted: dict = {}

    for page in pages:
        for idx, table in enumerate(page["tables"]):
            if not table or len(table) < 2:
                continue
            ttype = _identify_table(table, page["text"])
            print(f"[mapper] Page {page['page_num']} Table {idx + 1}: {ttype!r}")

            if ttype == "portfolio_statistics":
                data = _parse_table_to_dict(table)
                if data and len(data) >= 10:
                    if "Portfolio Statistics" not in extracted or len(data) > len(extracted["Portfolio Statistics"]):
                        extracted["Portfolio Statistics"] = data

            elif ttype == "side_by_side":
                parsed = _parse_side_by_side(table)
                if parsed:
                    country, currency = _split_country_currency(parsed)
                    if country and "Country Breakdown (%)" not in extracted:
                        extracted["Country Breakdown (%)"] = country
                    if currency and "Currency Breakdown (%)" not in extracted:
                        extracted["Currency Breakdown (%)"] = currency

            elif ttype == "country_single":
                data = _parse_table_to_dict(table)
                if data:
                    if "Country Breakdown (%)" not in extracted or len(data) > len(extracted["Country Breakdown (%)"]):
                        extracted["Country Breakdown (%)"] = data

            elif ttype == "currency_single":
                data = _parse_table_to_dict(table)
                if data:
                    if "Currency Breakdown (%)" not in extracted or len(data) > len(extracted["Currency Breakdown (%)"]):
                        extracted["Currency Breakdown (%)"] = data

    for section in ["Portfolio Statistics", "Country Breakdown (%)", "Currency Breakdown (%)"]:
        if section not in extracted:
            extracted[section] = {}
            print(f"[mapper] WARNING: '{section}' not found")

    extracted = _normalize(extracted)

    values = {}
    for item in PGIM_DATA_MAPPING:
        raw = item["VALUE"](extracted)
        if raw and raw != "":
            try:
                values[item["CODE"]] = float(str(raw).replace(",", "").replace("%", "").strip())
            except (ValueError, TypeError):
                values[item["CODE"]] = raw

    print(f"[mapper] Total series extracted: {len(values)}")
    return {"period": time_period, "data": values}


# ── public interface ──────────────────────────────────────────────────────────

def _record_to_row(period: str, data: dict) -> list:
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
    existing_rows: dict = {}

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
