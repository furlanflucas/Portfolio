# Data Processing and Transformation Script

This repository contains a Python script to preprocess, clean, and transform a bank marketing dataset into multiple structured datasets for further analysis. The script demonstrates the use of `pandas` and `numpy` libraries to handle data manipulation tasks.

---

## Features

1. **Data Loading**:
   - Reads the dataset `bank_marketing.csv` into a pandas DataFrame.

2. **Data Splitting**:
   - Splits the dataset into three distinct DataFrames:
     - `client`: Contains client demographic and financial information.
     - `campaign`: Contains information about campaign interactions and outcomes.
     - `economics`: Contains macroeconomic indicators related to the clients.

3. **Data Cleaning**:
   - Replaces unknown values in `client` with `NaN`.
   - Converts categorical values in specific columns (`credit_default`, `mortgage`, `previous_outcome`, `campaign_outcome`) into boolean format.
   - Maps month names to numeric values using a predefined dictionary.

4. **Feature Engineering**:
   - Adds a `last_contact_date` column by combining `year`, `month`, and `day` columns.
   - Drops intermediate columns that are no longer needed.

5. **Data Validation**:
   - Includes assertions to ensure data integrity and correct column transformations throughout the script.

6. **File Export**:
   - Saves the cleaned and processed DataFrames as separate CSV files:
     - `client.csv`
     - `campaign.csv`
     - `economics.csv`

7. **Post-Processing Validation**:
   - Reloads the exported files and verifies the column structure and data integrity.

---

## Prerequisites

- Python 3.7+
- Required Libraries:
  - `pandas`
  - `numpy`

Install the required libraries using pip:

pip install pandas numpy

## Usage
Place the bank_marketing.csv file in the same directory as the script.
Run the script using the command:
- python process_data.py
After successful execution, the following output files will be created:
     - `client.csv`
     - `campaign.csv`
     - `economics.csv`
## File Structure
- Input File:
 - bank_marketing.csv: Original dataset.
 - Output Files:
 - client.csv: Cleaned and structured client data.
 - campaign.csv: Campaign interaction and outcomes data.
 - economics.csv: Macroeconomic indicators related to clients.
## Validation and Quality Assurance
The script includes multiple assertions to ensure:

- DataFrames have the correct columns.
- Data types of transformed columns are as expected.
- Exported files match the defined structure.
- If any assertion fails, the script raises an error with a descriptive message to aid debugging.

## Acknowledgments
This script was designed to provide a robust, reproducible workflow for processing the bank marketing dataset. The logic can be adapted to similar datasets with minimal adjustments.

For any questions or contributions, please open an issue or submit a pull request.
