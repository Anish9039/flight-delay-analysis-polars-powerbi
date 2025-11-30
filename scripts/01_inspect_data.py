import polars as pl
import glob
import os

# --- CONFIGURATION ---
# This looks for any Parquet file in the raw_data folder
# If you downloaded CSVs, change ".parquet" to ".csv" below!
file_pattern = "../raw_data/*.parquet"

print("--- STARTING ETL PROCESS ---")

# 1. Find the files
files = glob.glob(file_pattern)
if not files:
    print(f"ERROR: No files found in {file_pattern}")
    print("Make sure your data is in the 'raw_data' folder and the extension matches!")
    exit()
else:
    print(f"Found {len(files)} data files.")

# 2. Load Data (Lazy Mode)
# We use scan_parquet (or scan_csv) to look at the data without filling up RAM
try:
    if "parquet" in file_pattern:
        q = pl.scan_parquet(files)
    else:
        q = pl.scan_csv(files)

    # 3. Get the Schema (Column Names & Types)
    schema = q.collect_schema()
    
    print("\n--- SUCCESS! DATA CONNECTED ---")
    print(f"Total Columns: {len(schema)}")
    print("\nHere are your columns and data types:")
    print("-" * 30)
    
    # Print first 10 columns to verify
    for name, dtype in list(schema.items())[:15]: 
        print(f"{name}: {dtype}")
        
    print("-" * 30)
    print("Note: 'Int64' means number, 'String' means text.")

except Exception as e:
    print(f"An error occurred: {e}")