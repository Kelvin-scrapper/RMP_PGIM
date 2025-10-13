import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import os
import time
import random
import winreg

# ==================== CONFIGURATION ====================
HEADLESS = True  # Set to False to see the browser
TARGET_URL = "https://www.pgim.com/us/en/intermediary/investment-capabilities/products/mutual-funds/pgim-emerging-markets-debt-local-currency-fund#literature"
LINK_TEXT_KEYWORDS = ['emlocal', 'datasheet']  # Keywords to find the datasheet link
DOWNLOAD_DIR = os.path.abspath(".")  # Directory to save downloaded PDF

# Human behavior delay ranges (in seconds)
DELAY_MIN = 0.5
DELAY_MAX = 2.0
SCROLL_DELAY_MIN = 1
SCROLL_DELAY_MAX = 2
# =======================================================

# --- Helper functions like get_chrome_version, human_delay, etc., remain the same ---

def get_chrome_version():
    """Auto-detect Chrome version from Windows registry."""
    try:
        # (Code unchanged)
        locations = [
            (winreg.HKEY_CURRENT_USER, r"SOFTWARE\Google\Chrome\BLBeacon"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Google\Chrome\BLBeacon"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Wow6432Node\Google\Chrome\BLBeacon"),
        ]
        for hive, key_path in locations:
            try:
                key = winreg.OpenKey(hive, key_path)
                version, _ = winreg.QueryValueEx(key, "version")
                winreg.CloseKey(key)
                major_version = int(version.split('.')[0])
                print(f"Detected Chrome version: {version} (major: {major_version})")
                return major_version
            except FileNotFoundError:
                continue
        return None
    except Exception as e:
        print(f"Error detecting Chrome version: {e}")
        return None

def human_delay(min_sec=None, max_sec=None):
    """Random delay to mimic human behavior."""
    if min_sec is None:
        min_sec = DELAY_MIN
    if max_sec is None:
        max_sec = DELAY_MAX
    time.sleep(random.uniform(min_sec, max_sec))

def human_scroll(driver, element):
    """Scroll to element like a human would."""
    driver.execute_script("""
        var element = arguments[0];
        var rect = element.getBoundingClientRect();
        var scrollTop = window.pageYOffset || document.documentElement.scrollTop;
        var targetY = rect.top + scrollTop - window.innerHeight / 2;
        window.scrollTo({ top: targetY, behavior: 'smooth' });
    """, element)
    human_delay(SCROLL_DELAY_MIN, SCROLL_DELAY_MAX)

def human_click(driver, element):
    """Click element like a human - with hover and delay using ActionChains."""
    try:
        actions = ActionChains(driver)
        actions.move_to_element(element).perform()
        human_delay(0.3, 0.8)
        actions.click(element).perform()
        human_delay(0.5, 1.5)
        print("[OK] Clicked via ActionChains (real mouse)")
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", element)
            human_delay(0.5, 1.5)
            print("[OK] Clicked via JavaScript (fallback)")
            return True
        except Exception as e2:
            print(f"[ERROR] Both click methods failed: {e2}")
            return False

def find_emlocal_datasheet_link(driver):
    """Find the datasheet link dynamically using configured keywords."""
    print("Looking for datasheet link...")
    try:
        links = driver.find_elements(By.CSS_SELECTOR, "a.literature-document-link")
        for link in links:
            link_text_lower = link.text.lower()
            # Check if all keywords are present in the link text
            if all(keyword in link_text_lower for keyword in LINK_TEXT_KEYWORDS):
                print(f"[OK] Found link: '{link.text.strip()}'")
                return link
        return None
    except Exception as e:
        print(f"Error finding datasheet link: {e}")
        return None

def dismiss_modal(driver):
    """Dismiss any modal popup by clicking SAVE button."""
    try:
        human_delay(2, 4) # Increased delay for modal to be interactable
        save_button = None
        # This XPath is more specific to the button seen in earlier logs
        buttons = driver.find_elements(By.XPATH, "//a[contains(@class, 'cmp-cta__link')]//span[contains(text(), 'Save')]")
        if buttons:
            save_button = buttons[0]
        
        if save_button and save_button.is_displayed():
            print("Clicking SAVE button...")
            # Use JS click as it's more reliable for complex elements
            driver.execute_script("arguments[0].click();", save_button)
            print("Modal dismissed!")
            human_delay(2, 3)
            return True
        else:
            print("No modal/save button found or it was not visible.")
            return False
    except Exception:
        print("No modal found, continuing...")
        return False

# --- FINAL VERSION OF THE EXTRACTION FUNCTION ---
def wait_for_pdf_load(driver):
    """Wait for PDF to fully load in the viewer."""
    print("\n[*] Waiting for PDF to fully load...")
    max_wait = 30
    start_time = time.time()

    while time.time() - start_time < max_wait:
        try:
            # Check if PDF viewer has loaded by looking for embed element
            pdf_loaded = driver.execute_script("""
                const embed = document.querySelector('embed[type="application/pdf"]');
                return embed !== null;
            """)

            if pdf_loaded:
                print("[OK] PDF viewer loaded!")
                human_delay(3, 5)  # Extra wait for PDF content to render
                return True

        except:
            pass

        time.sleep(1)

    print("[WARNING] PDF load timeout")
    return False


def capture_and_download_pdf(driver, download_dir):
    """Extract PDF blob data directly from browser."""
    print("\n[*] Extracting PDF blob from browser...")

    try:
        import base64

        # Wait for blob URL to be available
        max_attempts = 20
        blob_url = None

        for _ in range(max_attempts):
            blob_url = driver.execute_script("""
                const embed = document.querySelector('embed[type="application/pdf"]');
                if (embed && embed.src) {
                    return embed.src;
                }
                return null;
            """)

            if blob_url:
                print(f"[OK] Found blob URL: {blob_url}")
                break

            time.sleep(0.5)

        if not blob_url or not blob_url.startswith('blob:'):
            print("[ERROR] Could not find blob URL")
            return None

        # Fetch the blob data using JavaScript and convert to base64
        print("[*] Fetching blob data from browser...")
        pdf_base64 = driver.execute_async_script("""
            const blobUrl = arguments[0];
            const callback = arguments[1];

            fetch(blobUrl)
                .then(response => response.blob())
                .then(blob => {
                    const reader = new FileReader();
                    reader.onloadend = function() {
                        const base64data = reader.result.split(',')[1];
                        callback(base64data);
                    };
                    reader.readAsDataURL(blob);
                })
                .catch(error => {
                    callback(null);
                });
        """, blob_url)

        if not pdf_base64:
            print("[ERROR] Failed to fetch blob data")
            return None

        # Decode base64 to binary
        pdf_content = base64.b64decode(pdf_base64)

        if pdf_content.startswith(b'%PDF'):
            # Extract filename from page title or use default
            filename = driver.execute_script("""
                const title = document.title;
                if (title) {
                    return title.replace(/[^a-zA-Z0-9_-]/g, '_') + '.pdf';
                }
                return null;
            """)

            if not filename:
                filename = blob_url.split('/')[-1] + '.pdf'

            final_path = os.path.join(download_dir, filename)
            with open(final_path, 'wb') as f:
                f.write(pdf_content)
            print(f"[SUCCESS] PDF saved: {final_path} ({len(pdf_content)} bytes)")
            return final_path
        else:
            print(f"[ERROR] Extracted data is not a valid PDF")
            return None

    except Exception as e:
        print(f"[ERROR] Failed to extract PDF: {e}")
        import traceback
        traceback.print_exc()
        return None


def download_emlocal_datasheet():
    """
    Main function to download datasheet from PGIM website.
    Uses configuration from top of script.
    """
    # Clean up old files
    old_files = [f for f in os.listdir(DOWNLOAD_DIR) if 'EMLocal' in f or 'Datasheet' in f]
    for f in old_files:
        try: os.remove(os.path.join(DOWNLOAD_DIR, f)); print(f"Removed old file: {f}")
        except: pass

    # Configure Chrome options
    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')

    if HEADLESS:
        options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        print("[*] Running in HEADLESS mode")
    else:
        options.add_argument('--start-maximized')
        print("[*] Running in VISIBLE mode")

    # Set download preferences
    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": False,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    
    driver = None
    try:
        chrome_version = get_chrome_version()
        if chrome_version:
            driver = uc.Chrome(options=options, version_main=chrome_version)
        else:
            driver = uc.Chrome(options=options)
    except Exception as e:
        print(f"ERROR starting Chrome: {e}")
        return

    result = None
    original_window = None

    try:
        print(f"\n[*] Navigating to PGIM website...")
        driver.get(TARGET_URL)
        original_window = driver.current_window_handle
        human_delay(4, 6)

        datasheet_link = find_emlocal_datasheet_link(driver)
        if not datasheet_link:
            print("\n[ERROR] Could not find datasheet link!")
            return

        print("\n[*] Simulating a real click on datasheet link...")
        human_scroll(driver, datasheet_link)
        human_click(driver, datasheet_link)
        human_delay(5, 7) # Extra delay for new tab to process

        new_window = next((handle for handle in driver.window_handles if handle != original_window), None)
        if new_window:
            driver.switch_to.window(new_window)
            print(f"[OK] Switched to new tab: {driver.current_url}")
        else:
            print("[ERROR] New tab did not open.")
            return

        dismiss_modal(driver)

        # Capture PDF URL and download
        result = capture_and_download_pdf(driver, DOWNLOAD_DIR)

    except Exception as e:
        print(f"\n[ERROR] An error occurred during automation: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if result and os.path.exists(result):
            with open(result, 'rb') as f:
                if f.read(4) == b'%PDF':
                    print(f"\n[SUCCESS] Valid PDF downloaded!")
                    print(f"   File: {result}")
                else:
                    print("\n[WARNING] Downloaded file may not be a valid PDF.")
        else:
            print("\n[ERROR] Could not download PDF.")

        if driver:
            print("\n[*] Closing browser...")
            driver.quit()
        print("[*] Done!")

if __name__ == "__main__":
    download_emlocal_datasheet()