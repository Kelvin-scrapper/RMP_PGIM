import re
import os
import pandas as pd
import pdfplumber
from difflib import SequenceMatcher

# --- Configuration ---
# English month map for PGIM date parsing
MONTH_MAP_EN = {
    'january': '01', 'february': '02', 'march': '03', 'april': '04', 'may': '05', 'june': '06',
    'july': '07', 'august': '08', 'september': '09', 'october': '10', 'november': '11', 'december': '12'
}

# --- COMPLETE PGIM DATA MAPPING (177 Columns) ---
PGIM_DATA_MAPPING = [
    # Portfolio Statistics - Fund (15 fields)
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.TOTALNETASSET.M", "DESCRIPTION": "Portfolio Statistics: Fund - Total Net Assets ($ millions)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Total Net Assets ($ millions)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.AVERPRICE.M", "DESCRIPTION": "Portfolio Statistics: Fund - Average Price", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Average Price", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.DURATION.M", "DESCRIPTION": "Portfolio Statistics: Fund - Duration (yrs)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Duration (yrs)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.CONVEXITY.M", "DESCRIPTION": "Portfolio Statistics: Fund - Convexity", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Convexity", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.YIELDWORST.M", "DESCRIPTION": "Portfolio Statistics: Fund - Yield to Worst (YTW) (gross)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Yield to Worst (YTW) (gross)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.YIELDMATURITY.M", "DESCRIPTION": "Portfolio Statistics: Fund - Yield to Maturity (YTM) (gross)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Yield to Maturity (YTM) (gross)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.WEIGHAVERLIFE.M", "DESCRIPTION": "Portfolio Statistics: Fund - Weighted Average Life (yrs)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Weighted Average Life (yrs)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.AVERMATURITY.M", "DESCRIPTION": "Portfolio Statistics: Fund - Average Maturity (yrs)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Average Maturity (yrs)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.OPTADJUSTSPREAD.M", "DESCRIPTION": "Portfolio Statistics: Fund - Option Adjusted Spread (bps)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Option Adjusted Spread (bps)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.SPREADDURATION.M", "DESCRIPTION": "Portfolio Statistics: Fund - Spread Duration (yrs)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Spread Duration (yrs)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.AVERCOUPON.M", "DESCRIPTION": "Portfolio Statistics: Fund - Average Coupon (gross)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Average Coupon (gross)", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.NUMISSUES.M", "DESCRIPTION": "Portfolio Statistics: Fund - Number of Issues", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Number of Issues", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.NUMISSUERS.M", "DESCRIPTION": "Portfolio Statistics: Fund - Number of Issuers", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Number of Issuers", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.SEC30SUBSIDYIELD.M", "DESCRIPTION": "Portfolio Statistics: Fund - SEC Yield (30-day/Subsidized)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("SEC 30-Day Subsidized Yield", ["", ""])[0]},
    {"CODE": "RMP.PGIM.PORTSTAT.FUND.SEC30UNSUBYIELD.M", "DESCRIPTION": "Portfolio Statistics: Fund - SEC Yield (30-day/Unsubsidized)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("SEC 30-Day Unsubsidized Yield", ["", ""])[0]},
    
    # Portfolio Statistics - Bench (15 fields)
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.TOTALNETASSET.M", "DESCRIPTION": "Portfolio Statistics: Bench - Total Net Assets ($ millions)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Total Net Assets ($ millions)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.AVERPRICE.M", "DESCRIPTION": "Portfolio Statistics: Bench - Average Price", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Average Price", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.DURATION.M", "DESCRIPTION": "Portfolio Statistics: Bench - Duration (yrs)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Duration (yrs)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.CONVEXITY.M", "DESCRIPTION": "Portfolio Statistics: Bench - Convexity", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Convexity", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.YIELDWORST.M", "DESCRIPTION": "Portfolio Statistics: Bench - Yield to Worst (YTW) (gross)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Yield to Worst (YTW) (gross)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.YIELDMATURITY.M", "DESCRIPTION": "Portfolio Statistics: Bench - Yield to Maturity (YTM) (gross)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Yield to Maturity (YTM) (gross)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.WEIGHAVERLIFE.M", "DESCRIPTION": "Portfolio Statistics: Bench - Weighted Average Life (yrs)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Weighted Average Life (yrs)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.AVERMATURITY.M", "DESCRIPTION": "Portfolio Statistics: Bench - Average Maturity (yrs)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Average Maturity (yrs)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.OPTADJUSTSPREAD.M", "DESCRIPTION": "Portfolio Statistics: Bench - Option Adjusted Spread (bps)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Option Adjusted Spread (bps)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.SPREADDURATION.M", "DESCRIPTION": "Portfolio Statistics: Bench - Spread Duration (yrs)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Spread Duration (yrs)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.AVERCOUPON.M", "DESCRIPTION": "Portfolio Statistics: Bench - Average Coupon (gross)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Average Coupon (gross)", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.NUMISSUES.M", "DESCRIPTION": "Portfolio Statistics: Bench - Number of Issues", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Number of Issues", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.NUMISSUERS.M", "DESCRIPTION": "Portfolio Statistics: Bench - Number of Issuers", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("Number of Issuers", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.SEC30SUBSIDYIELD.M", "DESCRIPTION": "Portfolio Statistics: Bench - SEC Yield (30-day/Subsidized)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("SEC 30-Day Subsidized Yield", ["", ""])[1]},
    {"CODE": "RMP.PGIM.PORTSTAT.BENCH.SEC30UNSUBYIELD.M", "DESCRIPTION": "Portfolio Statistics: Bench - SEC Yield (30-day/Unsubsidized)", "VALUE": lambda data: data.get("Portfolio Statistics", {}).get("SEC 30-Day Unsubsidized Yield", ["", ""])[1]},
    
    # Country Breakdown - Fund (32 fields)
    {"CODE": "RMP.PGIM.COUNTRY.FUND.USA.M", "DESCRIPTION": "Country Breakdown: Fund - United States", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("United States", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.NONUSA.M", "DESCRIPTION": "Country Breakdown: Fund - Non-U.S.", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Non-U.S.", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.ARG.M", "DESCRIPTION": "Country Breakdown: Fund - Argentina", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Argentina", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.BRA.M", "DESCRIPTION": "Country Breakdown: Fund - Brazil", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Brazil", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.CHL.M", "DESCRIPTION": "Country Breakdown: Fund - Chile", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Chile", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.CHN.M", "DESCRIPTION": "Country Breakdown: Fund - China", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("China", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.COL.M", "DESCRIPTION": "Country Breakdown: Fund - Colombia", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Colombia", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.CIV.M", "DESCRIPTION": "Country Breakdown: Fund - Cote D'Ivoire", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Cote D'Ivoire", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.CZE.M", "DESCRIPTION": "Country Breakdown: Fund - Czech Republic", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Czech Republic", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.DOM.M", "DESCRIPTION": "Country Breakdown: Fund - Dominican Republic", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Dominican Republic", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.DEU.M", "DESCRIPTION": "Country Breakdown: Fund - Germany", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Germany", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.GTM.M", "DESCRIPTION": "Country Breakdown: Fund - Guatemala", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Guatemala", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.HUN.M", "DESCRIPTION": "Country Breakdown: Fund - Hungary", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Hungary", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.IND.M", "DESCRIPTION": "Country Breakdown: Fund - India", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("India", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.IDN.M", "DESCRIPTION": "Country Breakdown: Fund - Indonesia", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Indonesia", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.JAM.M", "DESCRIPTION": "Country Breakdown: Fund - Jamaica", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Jamaica", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.KOR.M", "DESCRIPTION": "Country Breakdown: Fund - Korea (South), Republic of", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Korea (South), Republic of", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.MYS.M", "DESCRIPTION": "Country Breakdown: Fund - Malaysia", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Malaysia", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.MEX.M", "DESCRIPTION": "Country Breakdown: Fund - Mexico", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Mexico", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.MULTNATIONAL.M", "DESCRIPTION": "Country Breakdown: Fund - Multinational", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Multinational", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.PAN.M", "DESCRIPTION": "Country Breakdown: Fund - Panama", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Panama", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.PER.M", "DESCRIPTION": "Country Breakdown: Fund - Peru", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Peru", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.PHL.M", "DESCRIPTION": "Country Breakdown: Fund - Philippines", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Philippines", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.POL.M", "DESCRIPTION": "Country Breakdown: Fund - Poland", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Poland", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.ROU.M", "DESCRIPTION": "Country Breakdown: Fund - Romania", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Romania", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.ZAF.M", "DESCRIPTION": "Country Breakdown: Fund - South Africa", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("South Africa", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.SUPNATIONAL.M", "DESCRIPTION": "Country Breakdown: Fund - Supranational", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Supranational", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.THA.M", "DESCRIPTION": "Country Breakdown: Fund - Thailand", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Thailand", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.SRB.M", "DESCRIPTION": "Country Breakdown: Fund - The Republic of Serbia", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("The Republic of Serbia", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.TUR.M", "DESCRIPTION": "Country Breakdown: Fund - Turkey", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Turkey", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.URY.M", "DESCRIPTION": "Country Breakdown: Fund - Uruguay", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Uruguay", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.CASH.M", "DESCRIPTION": "Country Breakdown: Fund - Cash/Cash Equivalents", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Cash/Cash Equivalents", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.TOTAL.M", "DESCRIPTION": "Country Breakdown: Fund - Total", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Total", ["", ""])[0]},
    {"CODE": "RMP.PGIM.COUNTRY.FUND.MWI.M", "DESCRIPTION": "Country Breakdown: Fund - Malawi", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Malawi", ["", ""])[0]},
    
    # Country Breakdown - Bench (32 fields)
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.USA.M", "DESCRIPTION": "Country Breakdown: Bench - United States", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("United States", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.NONUSA.M", "DESCRIPTION": "Country Breakdown: Bench - Non-U.S.", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Non-U.S.", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.ARG.M", "DESCRIPTION": "Country Breakdown: Bench - Argentina", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Argentina", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.BRA.M", "DESCRIPTION": "Country Breakdown: Bench - Brazil", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Brazil", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.CHL.M", "DESCRIPTION": "Country Breakdown: Bench - Chile", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Chile", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.CHN.M", "DESCRIPTION": "Country Breakdown: Bench - China", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("China", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.COL.M", "DESCRIPTION": "Country Breakdown: Bench - Colombia", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Colombia", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.CIV.M", "DESCRIPTION": "Country Breakdown: Bench - Cote D'Ivoire", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Cote D'Ivoire", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.CZE.M", "DESCRIPTION": "Country Breakdown: Bench - Czech Republic", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Czech Republic", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.DOM.M", "DESCRIPTION": "Country Breakdown: Bench - Dominican Republic", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Dominican Republic", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.DEU.M", "DESCRIPTION": "Country Breakdown: Bench - Germany", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Germany", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.GTM.M", "DESCRIPTION": "Country Breakdown: Bench - Guatemala", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Guatemala", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.HUN.M", "DESCRIPTION": "Country Breakdown: Bench - Hungary", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Hungary", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.IND.M", "DESCRIPTION": "Country Breakdown: Bench - India", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("India", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.IDN.M", "DESCRIPTION": "Country Breakdown: Bench - Indonesia", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Indonesia", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.JAM.M", "DESCRIPTION": "Country Breakdown: Bench - Jamaica", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Jamaica", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.KOR.M", "DESCRIPTION": "Country Breakdown: Bench - Korea (South), Republic of", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Korea (South), Republic of", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.MYS.M", "DESCRIPTION": "Country Breakdown: Bench - Malaysia", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Malaysia", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.MEX.M", "DESCRIPTION": "Country Breakdown: Bench - Mexico", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Mexico", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.MULTNATIONAL.M", "DESCRIPTION": "Country Breakdown: Bench - Multinational", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Multinational", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.PAN.M", "DESCRIPTION": "Country Breakdown: Bench - Panama", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Panama", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.PER.M", "DESCRIPTION": "Country Breakdown: Bench - Peru", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Peru", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.PHL.M", "DESCRIPTION": "Country Breakdown: Bench - Philippines", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Philippines", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.POL.M", "DESCRIPTION": "Country Breakdown: Bench - Poland", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Poland", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.ROU.M", "DESCRIPTION": "Country Breakdown: Bench - Romania", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Romania", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.ZAF.M", "DESCRIPTION": "Country Breakdown: Bench - South Africa", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("South Africa", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.SUPNATIONAL.M", "DESCRIPTION": "Country Breakdown: Bench - Supranational", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Supranational", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.THA.M", "DESCRIPTION": "Country Breakdown: Bench - Thailand", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Thailand", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.SRB.M", "DESCRIPTION": "Country Breakdown: Bench - The Republic of Serbia", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("The Republic of Serbia", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.TUR.M", "DESCRIPTION": "Country Breakdown: Bench - Turkey", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Turkey", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.URY.M", "DESCRIPTION": "Country Breakdown: Bench - Uruguay", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Uruguay", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.CASH.M", "DESCRIPTION": "Country Breakdown: Bench - Cash/Cash Equivalents", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Cash/Cash Equivalents", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.TOTAL.M", "DESCRIPTION": "Country Breakdown: Bench - Total", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Total", ["", ""])[1]},
    {"CODE": "RMP.PGIM.COUNTRY.BENCH.MWI.M", "DESCRIPTION": "Country Breakdown: Bench - Malawi", "VALUE": lambda data: data.get("Country Breakdown (%)", {}).get("Malawi", ["", ""])[1]},
    
    # Currency Breakdown - Fund (42 fields)
    {"CODE": "RMP.PGIM.CURRENCY.FUND.USD.M", "DESCRIPTION": "Currency Breakdown: Fund - U.S. Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("U.S. Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.NONUSD.M", "DESCRIPTION": "Currency Breakdown: Fund - Non-U.S. Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Non-U.S. Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.ARS.M", "DESCRIPTION": "Currency Breakdown: Fund - Argentinian Peso", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Argentinian Peso", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.AUD.M", "DESCRIPTION": "Currency Breakdown: Fund - Australian Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Australian Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.BRL.M", "DESCRIPTION": "Currency Breakdown: Fund - Brazilian Real", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Brazilian Real", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.GBP.M", "DESCRIPTION": "Currency Breakdown: Fund - British Pound", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("British Pound", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.CAD.M", "DESCRIPTION": "Currency Breakdown: Fund - Canadian Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Canadian Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.CLP.M", "DESCRIPTION": "Currency Breakdown: Fund - Chilean Peso", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Chilean Peso", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.CNY.M", "DESCRIPTION": "Currency Breakdown: Fund - Chinese Yuan", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Chinese Yuan", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.CNH.M", "DESCRIPTION": "Currency Breakdown: Fund - Chinese Offshore Yuan", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Chinese Offshore Yuan", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.COP.M", "DESCRIPTION": "Currency Breakdown: Fund - Colombian Peso", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Colombian Peso", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.CZK.M", "DESCRIPTION": "Currency Breakdown: Fund - Czech Koruna", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Czech Koruna", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.DOP.M", "DESCRIPTION": "Currency Breakdown: Fund - Dominican Peso", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Dominican Peso", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.EGP.M", "DESCRIPTION": "Currency Breakdown: Fund - Egyptian Pound", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Egyptian Pound", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.EUR.M", "DESCRIPTION": "Currency Breakdown: Fund - Euro", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Euro", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.HKD.M", "DESCRIPTION": "Currency Breakdown: Fund - Hong Kong Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Hong Kong Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.HUF.M", "DESCRIPTION": "Currency Breakdown: Fund - Hungarian Forint", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Hungarian Forint", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.INR.M", "DESCRIPTION": "Currency Breakdown: Fund - Indian Rupee", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Indian Rupee", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.IDR.M", "DESCRIPTION": "Currency Breakdown: Fund - Indonesian Rupiah", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Indonesian Rupiah", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.ILS.M", "DESCRIPTION": "Currency Breakdown: Fund - Israeli Shekel", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Israeli Shekel", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.JPY.M", "DESCRIPTION": "Currency Breakdown: Fund - Japanese Yen", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Japanese Yen", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.MYR.M", "DESCRIPTION": "Currency Breakdown: Fund - Malaysian Ringgit", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Malaysian Ringgit", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.MXN.M", "DESCRIPTION": "Currency Breakdown: Fund - Mexican Peso", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Mexican Peso", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.NZD.M", "DESCRIPTION": "Currency Breakdown: Fund - New Zealand Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("New Zealand Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.NOK.M", "DESCRIPTION": "Currency Breakdown: Fund - Norwegian Kroner", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Norwegian Kroner", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.PEN.M", "DESCRIPTION": "Currency Breakdown: Fund - Peruvian Nuevo Sol", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Peruvian Nuevo Sol", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.PHP.M", "DESCRIPTION": "Currency Breakdown: Fund - Philippine Peso", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Philippine Peso", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.PLN.M", "DESCRIPTION": "Currency Breakdown: Fund - Polish Zloty", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Polish Zloty", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.RON.M", "DESCRIPTION": "Currency Breakdown: Fund - Romanian Lei", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Romanian Lei", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.RSD.M", "DESCRIPTION": "Currency Breakdown: Fund - Serbian Dinar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Serbian Dinar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.SGD.M", "DESCRIPTION": "Currency Breakdown: Fund - Singapore Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Singapore Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.ZAR.M", "DESCRIPTION": "Currency Breakdown: Fund - South African Rand", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("South African Rand", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.KRW.M", "DESCRIPTION": "Currency Breakdown: Fund - South Korean Won", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("South Korean Won", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.CHF.M", "DESCRIPTION": "Currency Breakdown: Fund - Swiss Franc", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Swiss Franc", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.TWD.M", "DESCRIPTION": "Currency Breakdown: Fund - Taiwan Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Taiwan Dollar", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.THB.M", "DESCRIPTION": "Currency Breakdown: Fund - Thai Baht", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Thai Baht", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.TRY.M", "DESCRIPTION": "Currency Breakdown: Fund - Turkish Lira", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Turkish Lira", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.UYU.M", "DESCRIPTION": "Currency Breakdown: Fund - Uruguayan Peso", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Uruguayan Peso", ["", ""])[0]},
    {"CODE": "RMP.PGIM.CURRENCY.FUND.TOTAL.M", "DESCRIPTION": "Currency Breakdown: Fund - Total", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Total", ["", ""])[0]},
    
    # Currency Breakdown - Bench (42 fields)
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.USD.M", "DESCRIPTION": "Currency Breakdown: Bench - U.S. Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("U.S. Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.NONUSD.M", "DESCRIPTION": "Currency Breakdown: Bench - Non-U.S. Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Non-U.S. Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.ARS.M", "DESCRIPTION": "Currency Breakdown: Bench - Argentinian Peso", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Argentinian Peso", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.AUD.M", "DESCRIPTION": "Currency Breakdown: Bench - Australian Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Australian Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.BRL.M", "DESCRIPTION": "Currency Breakdown: Bench - Brazilian Real", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Brazilian Real", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.GBP.M", "DESCRIPTION": "Currency Breakdown: Bench - British Pound", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("British Pound", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.CAD.M", "DESCRIPTION": "Currency Breakdown: Bench - Canadian Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Canadian Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.CLP.M", "DESCRIPTION": "Currency Breakdown: Bench - Chilean Peso", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Chilean Peso", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.CNY.M", "DESCRIPTION": "Currency Breakdown: Bench - Chinese Yuan", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Chinese Yuan", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.CNH.M", "DESCRIPTION": "Currency Breakdown: Bench - Chinese Offshore Yuan", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Chinese Offshore Yuan", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.COP.M", "DESCRIPTION": "Currency Breakdown: Bench - Colombian Peso", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Colombian Peso", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.CZK.M", "DESCRIPTION": "Currency Breakdown: Bench - Czech Koruna", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Czech Koruna", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.DOP.M", "DESCRIPTION": "Currency Breakdown: Bench - Dominican Peso", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Dominican Peso", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.EGP.M", "DESCRIPTION": "Currency Breakdown: Bench - Egyptian Pound", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Egyptian Pound", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.EUR.M", "DESCRIPTION": "Currency Breakdown: Bench - Euro", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Euro", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.HKD.M", "DESCRIPTION": "Currency Breakdown: Bench - Hong Kong Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Hong Kong Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.HUF.M", "DESCRIPTION": "Currency Breakdown: Bench - Hungarian Forint", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Hungarian Forint", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.INR.M", "DESCRIPTION": "Currency Breakdown: Bench - Indian Rupee", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Indian Rupee", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.IDR.M", "DESCRIPTION": "Currency Breakdown: Bench - Indonesian Rupiah", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Indonesian Rupiah", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.ILS.M", "DESCRIPTION": "Currency Breakdown: Bench - Israeli Shekel", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Israeli Shekel", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.JPY.M", "DESCRIPTION": "Currency Breakdown: Bench - Japanese Yen", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Japanese Yen", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.MYR.M", "DESCRIPTION": "Currency Breakdown: Bench - Malaysian Ringgit", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Malaysian Ringgit", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.MXN.M", "DESCRIPTION": "Currency Breakdown: Bench - Mexican Peso", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Mexican Peso", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.NZD.M", "DESCRIPTION": "Currency Breakdown: Bench - New Zealand Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("New Zealand Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.NOK.M", "DESCRIPTION": "Currency Breakdown: Bench - Norwegian Kroner", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Norwegian Kroner", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.PEN.M", "DESCRIPTION": "Currency Breakdown: Bench - Peruvian Nuevo Sol", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Peruvian Nuevo Sol", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.PHP.M", "DESCRIPTION": "Currency Breakdown: Bench - Philippine Peso", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Philippine Peso", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.PLN.M", "DESCRIPTION": "Currency Breakdown: Bench - Polish Zloty", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Polish Zloty", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.RON.M", "DESCRIPTION": "Currency Breakdown: Bench - Romanian Lei", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Romanian Lei", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.RSD.M", "DESCRIPTION": "Currency Breakdown: Bench - Serbian Dinar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Serbian Dinar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.SGD.M", "DESCRIPTION": "Currency Breakdown: Bench - Singapore Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Singapore Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.ZAR.M", "DESCRIPTION": "Currency Breakdown: Bench - South African Rand", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("South African Rand", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.KRW.M", "DESCRIPTION": "Currency Breakdown: Bench - South Korean Won", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("South Korean Won", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.CHF.M", "DESCRIPTION": "Currency Breakdown: Bench - Swiss Franc", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Swiss Franc", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.TWD.M", "DESCRIPTION": "Currency Breakdown: Bench - Taiwan Dollar", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Taiwan Dollar", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.THB.M", "DESCRIPTION": "Currency Breakdown: Bench - Thai Baht", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Thai Baht", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.TRY.M", "DESCRIPTION": "Currency Breakdown: Bench - Turkish Lira", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Turkish Lira", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.UYU.M", "DESCRIPTION": "Currency Breakdown: Bench - Uruguayan Peso", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Uruguayan Peso", ["", ""])[1]},
    {"CODE": "RMP.PGIM.CURRENCY.BENCH.TOTAL.M", "DESCRIPTION": "Currency Breakdown: Bench - Total", "VALUE": lambda data: data.get("Currency Breakdown (%)", {}).get("Total", ["", ""])[1]},
]

# --- Helper Functions ---

def extract_tables_from_pdf(pdf_path):
    """Extracts tables from a PDF using pdfplumber."""
    tables_by_page = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                page_data = {
                    'page_num': page_num + 1,
                    'text': page.extract_text(),
                    'tables': page.extract_tables()
                }
                tables_by_page.append(page_data)
    except Exception as e:
        print(f"Error reading PDF {pdf_path}: {e}")
        return []
    return tables_by_page

def get_pgim_time_period(all_pages_text):
    """Extracts and formats the date from 'Month DD, YYYY' format - searches all pages."""
    for text in all_pages_text:
        match = re.search(r'as of (\w+) \d{1,2}, (\d{4})', text, re.IGNORECASE)
        if match:
            month_name, year = match.groups()
            month_number = MONTH_MAP_EN.get(month_name.lower())
            if month_number:
                return f"{year}-{month_number}"
    return None

def fuzzy_match_score(s1, s2):
    """Calculate similarity score between two strings (0-1)."""
    if not s1 or not s2:
        return 0.0
    return SequenceMatcher(None, s1.lower(), s2.lower()).ratio()

def find_best_match(target, candidates, threshold=0.6):
    """Find the best matching string from candidates for the target string."""
    best_match = None
    best_score = threshold

    for candidate in candidates:
        score = fuzzy_match_score(target, candidate)
        if score > best_score:
            best_score = score
            best_match = candidate

    return best_match

def parse_table_to_dict(table):
    """Converts a pdfplumber table to a dictionary {label: [fund_value, bench_value]}."""
    data = {}

    if not table or len(table) < 2:
        return data

    # Skip header row (first row usually has "Fund", "Bench", "Active")
    for row in table[1:]:
        if not row or len(row) < 3:
            continue

        label = row[0]
        fund_value = row[1]
        bench_value = row[2]

        # Clean up values
        if label:
            label = str(label).strip()
        if fund_value:
            fund_value = str(fund_value).strip()
        if bench_value:
            bench_value = str(bench_value).strip()

        # Skip empty or header rows
        if not label or label.lower() in ['fund', 'bench', 'active', '']:
            continue

        data[label] = [fund_value if fund_value else '', bench_value if bench_value else '']

    return data

def parse_side_by_side_table(table):
    """Parses a side-by-side table (Country and Currency together)."""
    data = []

    if not table or len(table) < 2:
        return data

    # Each row might have: CountryLabel, Fund, Bench, Active, CurrencyLabel, Fund, Bench, Active
    for row in table[1:]:
        if not row or len(row) < 6:
            continue

        # Extract left side (Country)
        left_label = str(row[0]).strip() if row[0] else ''
        left_fund = str(row[1]).strip() if row[1] else ''
        left_bench = str(row[2]).strip() if row[2] else ''

        # Extract right side (Currency) - usually starts at index 4 or 5
        right_label = str(row[4]).strip() if len(row) > 4 and row[4] else ''
        right_fund = str(row[5]).strip() if len(row) > 5 and row[5] else ''
        right_bench = str(row[6]).strip() if len(row) > 6 and row[6] else ''

        data.append({
            'left': {'label': left_label, 'fund': left_fund, 'bench': left_bench},
            'right': {'label': right_label, 'fund': right_fund, 'bench': right_bench}
        })

    return data

def split_country_currency_data(parsed_data):
    """Splits side-by-side table data into country and currency dictionaries."""
    country_data = {}
    currency_data = {}

    for row_data in parsed_data:
        # Left side is Country
        left = row_data['left']
        if left['label'] and left['label'].lower() not in ['fund', 'bench', 'active', '']:
            country_data[left['label']] = [left['fund'], left['bench']]

        # Right side is Currency
        right = row_data['right']
        if right['label'] and right['label'].lower() not in ['fund', 'bench', 'active', '']:
            currency_data[right['label']] = [right['fund'], right['bench']]

    return country_data, currency_data

def normalize_extracted_data(extracted_data):
    """Applies fuzzy matching to normalize field names in extracted data."""
    # Define expected field names for each category
    expected_fields = {
        "Portfolio Statistics": [
            "Total Net Assets ($ millions)", "Average Price", "Duration (yrs)", "Convexity",
            "Yield to Worst (YTW) (gross)", "Yield to Maturity (YTM) (gross)",
            "Weighted Average Life (yrs)", "Average Maturity (yrs)",
            "Option Adjusted Spread (bps)", "Spread Duration (yrs)",
            "Average Coupon (gross)", "Number of Issues", "Number of Issuers",
            "SEC 30-Day Subsidized Yield", "SEC 30-Day Unsubsidized Yield"
        ],
        "Country Breakdown (%)": [
            "United States", "Non-U.S.", "Argentina", "Brazil", "Chile", "China", "Colombia",
            "Cote D'Ivoire", "Czech Republic", "Dominican Republic", "Germany", "Guatemala",
            "Hungary", "India", "Indonesia", "Jamaica", "Korea (South), Republic of", "Malaysia",
            "Mexico", "Multinational", "Panama", "Peru", "Philippines", "Poland", "Romania",
            "South Africa", "Supranational", "Thailand", "The Republic of Serbia", "Turkey",
            "Uruguay", "Cash/Cash Equivalents", "Total", "Malawi"
        ],
        "Currency Breakdown (%)": [
            "U.S. Dollar", "Non-U.S. Dollar", "Argentinian Peso", "Australian Dollar", "Brazilian Real",
            "British Pound", "Canadian Dollar", "Chilean Peso", "Chinese Yuan", "Chinese Offshore Yuan",
            "Colombian Peso", "Czech Koruna", "Dominican Peso", "Egyptian Pound", "Euro",
            "Hong Kong Dollar", "Hungarian Forint", "Indian Rupee", "Indonesian Rupiah", "Israeli Shekel",
            "Japanese Yen", "Malaysian Ringgit", "Mexican Peso", "New Zealand Dollar", "Norwegian Kroner",
            "Peruvian Nuevo Sol", "Philippine Peso", "Polish Zloty", "Romanian Lei", "Serbian Dinar",
            "Singapore Dollar", "South African Rand", "South Korean Won", "Swiss Franc", "Taiwan Dollar",
            "Thai Baht", "Turkish Lira", "Uruguayan Peso", "Total"
        ]
    }

    normalized_data = {}

    for category, data_dict in extracted_data.items():
        if category not in expected_fields:
            normalized_data[category] = data_dict
            continue

        normalized_dict = {}
        expected = expected_fields[category]

        # Try to match each extracted field to an expected field
        for extracted_field, values in data_dict.items():
            best_match = find_best_match(extracted_field, expected, threshold=0.65)

            if best_match:
                normalized_dict[best_match] = values
                if best_match != extracted_field:
                    print(f"    Fuzzy matched: '{extracted_field}' → '{best_match}'")
            else:
                # Keep original if no good match found
                normalized_dict[extracted_field] = values
                print(f"    No match found for: '{extracted_field}' (keeping as-is)")

        normalized_data[category] = normalized_dict

    return normalized_data

# --- Core Processing Function ---

def identify_table_type(table, page_text):
    """Identifies what type of table this is based on content analysis."""
    if not table or len(table) < 2:
        return None

    # Check all cells in first few rows (not just first column)
    all_text = ' '.join([
        str(cell).lower() if cell else ''
        for row in table[:5]
        for cell in (row if row else [])
    ])

    # Portfolio Statistics detection - look for key fields
    portfolio_keywords = ['portfolio statistics', 'total net assets', 'duration', 'convexity',
                         'yield to worst', 'yield to maturity', 'spread duration', 'average coupon']
    portfolio_count = sum(1 for keyword in portfolio_keywords if keyword in all_text)

    if portfolio_count >= 3:  # If we find at least 3 portfolio keywords
        if 'country breakdown' not in all_text and 'currency breakdown' not in all_text:
            return 'portfolio_statistics'

    # Detect column count for layout type
    num_cols = len(table[0]) if table[0] else 0

    # Side-by-side Country/Currency detection (typically 7-10 columns)
    country_currency_keywords = ['country breakdown', 'currency breakdown']
    if num_cols >= 7:
        keyword_count = sum(1 for keyword in country_currency_keywords if keyword in all_text)
        if keyword_count >= 2:  # Both country and currency in same table
            return 'side_by_side'

    # Single Country table
    if 'country breakdown' in all_text or 'country' in all_text:
        if 'currency' not in all_text and num_cols < 7:
            return 'country_single'

    # Single Currency table
    if 'currency breakdown' in all_text or 'currency' in all_text:
        if 'country' not in all_text and num_cols < 7:
            return 'currency_single'

    return None

def process_pgim_pdf(pdf_path):
    """Orchestrates the data extraction and mapping for a PGIM PDF file."""
    print(f"Processing PGIM file: {pdf_path}")
    pages_data = extract_tables_from_pdf(pdf_path)

    if not pages_data:
        return

    # Search all pages for time period (not just first page)
    all_texts = [page['text'] for page in pages_data if page['text']]
    time_period = get_pgim_time_period(all_texts)
    if not time_period:
        print(f"Warning: Could not extract time period from {pdf_path}. Skipping.")
        return

    # Extract all table data from pdfplumber's extracted tables
    extracted_data = {}

    for page_data in pages_data:
        page_num = page_data['page_num']
        page_text = page_data['text']
        tables = page_data['tables']

        print(f"Page {page_num}: Found {len(tables)} table(s)")

        # Process each table on the page
        for table_idx, table in enumerate(tables):
            if not table or len(table) < 2:
                continue

            # Dynamically identify table type
            table_type = identify_table_type(table, page_text)
            print(f"  Table {table_idx + 1}: Identified as '{table_type}'")

            if table_type == 'portfolio_statistics':
                # Extract Portfolio Statistics
                data = parse_table_to_dict(table)
                # Use the largest table (avoid sub-tables)
                if data and len(data) >= 10:
                    if "Portfolio Statistics" not in extracted_data or len(data) > len(extracted_data["Portfolio Statistics"]):
                        extracted_data["Portfolio Statistics"] = data
                        print(f"  [OK] Extracted Portfolio Statistics: {len(data)} items")

            elif table_type == 'side_by_side':
                # Side-by-side Country/Currency format
                parsed = parse_side_by_side_table(table)
                print(f"  DEBUG: Parsed {len(parsed)} rows from side-by-side")

                if parsed:
                    country_data, currency_data = split_country_currency_data(parsed)
                    print(f"  DEBUG: Split into {len(country_data)} countries and {len(currency_data)} currencies")

                    if country_data and "Country Breakdown (%)" not in extracted_data:
                        extracted_data["Country Breakdown (%)"] = country_data
                        print(f"  [OK] Extracted Country Breakdown: {len(country_data)} items")

                    if currency_data and "Currency Breakdown (%)" not in extracted_data:
                        extracted_data["Currency Breakdown (%)"] = currency_data
                        print(f"  [OK] Extracted Currency Breakdown: {len(currency_data)} items")

            elif table_type == 'country_single':
                # Single Country table - prefer larger tables
                data = parse_table_to_dict(table)
                if data:
                    if "Country Breakdown (%)" not in extracted_data or len(data) > len(extracted_data["Country Breakdown (%)"]):
                        extracted_data["Country Breakdown (%)"] = data
                        print(f"  [OK] Extracted Country Breakdown (single): {len(data)} items")

            elif table_type == 'currency_single':
                # Single Currency table - prefer larger tables
                data = parse_table_to_dict(table)
                if data:
                    if "Currency Breakdown (%)" not in extracted_data or len(data) > len(extracted_data["Currency Breakdown (%)"]):
                        extracted_data["Currency Breakdown (%)"] = data
                        print(f"  [OK] Extracted Currency Breakdown (single): {len(data)} items")

    # Ensure all expected tables exist
    for section in ["Portfolio Statistics", "Country Breakdown (%)", "Currency Breakdown (%)"]:
        if section not in extracted_data:
            extracted_data[section] = {}
            print(f"Warning: Could not extract '{section}'")

    # Apply fuzzy matching to normalize field names
    print("\nNormalizing field names with fuzzy matching...")
    extracted_data = normalize_extracted_data(extracted_data)

    # --- Create DataFrame with guaranteed column order ---
    header_codes = [""] + [item["CODE"] for item in PGIM_DATA_MAPPING]
    header_descriptions = [""] + [item["DESCRIPTION"] for item in PGIM_DATA_MAPPING]
    data_row = [time_period] + [item["VALUE"](extracted_data) for item in PGIM_DATA_MAPPING]
    
    df = pd.DataFrame([data_row])

    # --- Save to CSV in an 'output' folder ---
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    base_name = os.path.basename(pdf_path)
    file_name_without_ext = os.path.splitext(base_name)[0]
    output_filename = os.path.join(output_dir, f"RMP_PGIM_DATA_{file_name_without_ext}.csv")
    
    # Write the two header rows and the data row
    with open(output_filename, 'w', newline='', encoding='utf-8') as f:
        f.write(','.join(f'"{h}"' for h in header_codes) + '\n')
        f.write(','.join(f'"{h}"' for h in header_descriptions) + '\n')
        df.to_csv(f, index=False, header=False)

    print(f"Data successfully saved to {output_filename}")
    print(f"Total columns: {len(header_codes)} (should be 177)")


# --- Main Execution Block ---

def main():
    """Scans the current directory for PGIM PDF files and processes them."""
    current_directory = '.'
    print(f"Scanning for PDF files in '{os.path.abspath(current_directory)}'...")
    pdf_files_found = 0
    for root, _, files in os.walk(current_directory):
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_path = os.path.join(root, file)
                # Simple check to see if it's a PGIM file before processing
                try:
                    with pdfplumber.open(pdf_path) as pdf:
                        if len(pdf.pages) > 0:
                            text_preview = pdf.pages[0].extract_text()
                            if text_preview and "PGIM" in text_preview and "Portfolio Statistics" in text_preview:
                                process_pgim_pdf(pdf_path)
                                pdf_files_found += 1
                except Exception as e:
                    print(f"Error processing {pdf_path}: {e}")
                    import traceback
                    traceback.print_exc()

    if pdf_files_found == 0:
        print("No PGIM PDF files were found.")
    else:
        print(f"\nFinished processing. Found and processed {pdf_files_found} PGIM PDF file(s).")

if __name__ == "__main__":
    main()