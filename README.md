# RMP PGIM Data Scraper

This project automates the process of downloading a PDF datasheet from the PGIM website, extracting data from it, and saving the data into a CSV file.

## Installation

1.  Clone the repository or download the source code.
2.  Install the required Python packages using pip:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

To run the entire process, execute the `orchestrator.py` script:

```bash
python orchestrator.py
```

This will:

1.  Launch a web browser and navigate to the PGIM website.
2.  Download the EMLocal Datasheet PDF.
3.  Process the downloaded PDF to extract data.
4.  Save the extracted data to a CSV file in the `output` directory.
