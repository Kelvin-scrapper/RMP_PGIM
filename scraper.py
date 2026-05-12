"""
RMP_PGIM scraper — PGIM EM Debt Local Currency Fund

Responsibility: DOWNLOAD ONLY.
  Downloads the EMLocal datasheet PDF from the PGIM website using
  undetected_chromedriver and saves it to downloads/.

Source: https://www.pgim.com/us/en/intermediary/investment-capabilities/
        products/mutual-funds/pgim-emerging-markets-debt-local-currency-fund#literature

Usage:
  cd RMP_PGIM
  python -c "import scraper; print(scraper.fetch_data())"
"""

import os
import random
import sys
import time
import winreg
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

PAGE_URL = (
    "https://www.pgim.com/us/en/intermediary/investment-capabilities/products/"
    "mutual-funds/pgim-emerging-markets-debt-local-currency-fund#literature"
)
_LINK_KEYWORDS = ["emlocal", "datasheet"]


# ── helpers ──────────────────────────────────────────────────────────────────

def _chrome_version():
    locations = [
        (winreg.HKEY_CURRENT_USER,  r"SOFTWARE\Google\Chrome\BLBeacon"),
        (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Google\Chrome\BLBeacon"),
        (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Wow6432Node\Google\Chrome\BLBeacon"),
    ]
    for hive, path in locations:
        try:
            key = winreg.OpenKey(hive, path)
            version, _ = winreg.QueryValueEx(key, "version")
            winreg.CloseKey(key)
            major = int(version.split(".")[0])
            print(f"[scraper] Chrome {version} (major: {major})")
            return major
        except FileNotFoundError:
            continue
    return None


def _delay(lo=0.5, hi=2.0):
    time.sleep(random.uniform(lo, hi))


def _scroll_to(driver, element):
    driver.execute_script("""
        var el = arguments[0];
        var rect = el.getBoundingClientRect();
        var top = window.pageYOffset + rect.top - window.innerHeight / 2;
        window.scrollTo({top: top, behavior: 'smooth'});
    """, element)
    _delay(1, 2)


def _click(driver, element):
    from selenium.webdriver.common.action_chains import ActionChains
    try:
        ActionChains(driver).move_to_element(element).perform()
        _delay(0.3, 0.8)
        ActionChains(driver).click(element).perform()
        _delay(0.5, 1.5)
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", element)
            _delay(0.5, 1.5)
            return True
        except Exception as e:
            print(f"[scraper] Click failed: {e}")
            return False


def _find_datasheet_link(driver):
    from selenium.webdriver.common.by import By
    links = driver.find_elements(By.CSS_SELECTOR, "a.literature-document-link")
    for link in links:
        text = link.text.lower()
        if all(kw in text for kw in _LINK_KEYWORDS):
            print(f"[scraper] Found link: '{link.text.strip()}'")
            return link
    return None


def _dismiss_modal(driver):
    from selenium.webdriver.common.by import By
    try:
        _delay(2, 4)
        buttons = driver.find_elements(
            By.XPATH,
            "//a[contains(@class,'cmp-cta__link')]//span[contains(text(),'Save')]"
        )
        if buttons and buttons[0].is_displayed():
            driver.execute_script("arguments[0].click();", buttons[0])
            print("[scraper] Modal dismissed")
            _delay(2, 3)
            return True
    except Exception:
        pass
    return False


def _extract_pdf_blob(driver, download_dir: str):
    import base64

    blob_url = None
    for _ in range(20):
        blob_url = driver.execute_script("""
            var e = document.querySelector('embed[type="application/pdf"]');
            return e ? e.src : null;
        """)
        if blob_url:
            break
        time.sleep(0.5)

    if not blob_url or not blob_url.startswith("blob:"):
        print("[scraper] No blob URL found")
        return None

    pdf_b64 = driver.execute_async_script("""
        var url = arguments[0], cb = arguments[1];
        fetch(url)
            .then(r => r.blob())
            .then(b => { var fr = new FileReader();
                         fr.onloadend = () => cb(fr.result.split(',')[1]);
                         fr.readAsDataURL(b); })
            .catch(() => cb(null));
    """, blob_url)

    if not pdf_b64:
        print("[scraper] Blob fetch failed")
        return None

    data = base64.b64decode(pdf_b64)
    if not data.startswith(b"%PDF"):
        print("[scraper] Data is not a valid PDF")
        return None

    filename = driver.execute_script("""
        var t = document.title;
        return t ? t.replace(/[^a-zA-Z0-9_-]/g, '_') + '.pdf' : null;
    """) or (blob_url.split("/")[-1] + ".pdf")

    path = os.path.join(download_dir, filename)
    with open(path, "wb") as f:
        f.write(data)
    print(f"[scraper] PDF saved: {path} ({len(data):,} bytes)")
    return path


def _download_pdf(downloads_dir: str) -> str:
    """Core download routine. Returns path to saved PDF."""
    import undetected_chromedriver as uc

    abs_dir = os.path.abspath(downloads_dir)

    # Remove stale files
    for fn in os.listdir(abs_dir):
        if "EMLocal" in fn or "Datasheet" in fn:
            try:
                os.remove(os.path.join(abs_dir, fn))
                print(f"[scraper] Removed old file: {fn}")
            except Exception:
                pass

    options = uc.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("prefs", {
        "download.default_directory": abs_dir,
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": False,
    })

    driver = None
    try:
        ver = _chrome_version()
        driver = uc.Chrome(options=options, version_main=ver) if ver else uc.Chrome(options=options)
    except Exception as e:
        raise RuntimeError(f"[scraper] Chrome failed to start: {e}") from e

    result = None
    try:
        print(f"[scraper] Navigating to {PAGE_URL}")
        driver.get(PAGE_URL)
        original_window = driver.current_window_handle
        _delay(4, 6)

        link = _find_datasheet_link(driver)
        if not link:
            raise RuntimeError("[scraper] Datasheet link not found on page")

        _scroll_to(driver, link)
        _click(driver, link)
        _delay(5, 7)

        new_window = next(
            (h for h in driver.window_handles if h != original_window), None
        )
        if not new_window:
            raise RuntimeError("[scraper] New tab did not open")

        driver.switch_to.window(new_window)
        print(f"[scraper] Switched to: {driver.current_url}")

        _dismiss_modal(driver)
        result = _extract_pdf_blob(driver, abs_dir)

    except Exception as e:
        print(f"[scraper] Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()

    if not result:
        raise FileNotFoundError("[scraper] PDF download failed")
    return result


# ── public interface ──────────────────────────────────────────────────────────

def fetch_data(downloads_dir: str = "downloads") -> str:
    """
    Download the PGIM EMLocal datasheet PDF.

    Saves the PDF to downloads_dir and returns its absolute path.
    """
    os.makedirs(downloads_dir, exist_ok=True)
    return _download_pdf(downloads_dir)
