import main1
import map1
import os
import time

def find_latest_pdf(directory):
    """Finds the most recently modified PDF file in a directory."""
    latest_pdf = None
    latest_time = 0
    for filename in os.listdir(directory):
        if filename.lower().endswith(".pdf"):
            filepath = os.path.join(directory, filename)
            mod_time = os.path.getmtime(filepath)
            if mod_time > latest_time:
                latest_time = mod_time
                latest_pdf = filepath
    return latest_pdf

def main():
    """Orchestrates the download and processing of the PGIM datasheet."""
    print("--- Starting Orchestrator ---")

    # Step 1: Download the PDF file
    print("\n[Step 1/2] Downloading the datasheet...")
    main1.download_emlocal_datasheet()

    # Give the download a moment to finalize
    time.sleep(5)

    # Find the latest downloaded PDF
    download_dir = os.path.abspath(".")
    downloaded_pdf_path = find_latest_pdf(download_dir)

    if downloaded_pdf_path and os.path.exists(downloaded_pdf_path):
        print(f"\n[SUCCESS] Found downloaded PDF: {downloaded_pdf_path}")
        
        # Step 2: Process the downloaded PDF
        print("\n[Step 2/2] Processing the PDF to extract data...")
        try:
            map1.process_pgim_pdf(downloaded_pdf_path)
            print("\n[SUCCESS] PDF processing complete.")
        except Exception as e:
            print(f"\n[ERROR] An error occurred during PDF processing: {e}")
    else:
        print("\n[ERROR] Could not find the downloaded PDF file. Aborting processing.")

    print("\n--- Orchestrator Finished ---")

if __name__ == "__main__":
    main()