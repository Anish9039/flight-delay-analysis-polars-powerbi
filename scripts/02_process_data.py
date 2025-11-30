import polars as pl
import glob
import os

# --- CONFIGURATION ---
# Adjust this to match your file type (csv or parquet)
raw_file_pattern = "../raw_data/*.parquet"
output_path = "../processed_data/fact_flights_clean.parquet"

print("--- STARTING TRANSFORMATION ---")

# 1. Connect to Data
files = glob.glob(raw_file_pattern)
# Check if files exist before proceeding
if not files:
    print("Error: No files found in raw_data!")
    exit()

if "parquet" in raw_file_pattern:
    q = pl.scan_parquet(files)
else:
    q = pl.scan_csv(files)

# 2. Apply Transformations (The "Kitchen" Work)
df_clean = (
    q
    # A. Filter: We only want flights that actually flew for performance analysis
    # (We remove Cancelled flights for this specific table)
    .filter(pl.col("Cancelled") == False)
    
    # B. Select only the columns we need (Reduces file size)
    .select([
        "FlightDate", "Airline", "Origin", "Dest", 
        "CRSDepTime", "DepDelayMinutes", "ArrDelayMinutes", 
        "AirTime", "DepTime"
    ])
    
    # C. Feature Engineering (Creating new Business Columns)
    .with_columns([
        # 1. Create a Unique Route ID (e.g., "JFK-LHR")
        (pl.col("Origin") + "-" + pl.col("Dest")).alias("Route_ID"),
        
        # 2. Create a simple "Delayed" Flag (Industry standard is > 15 mins)
        pl.when(pl.col("DepDelayMinutes") > 15)
          .then(pl.lit("Delayed"))
          .otherwise(pl.lit("On-Time"))
          .alias("Departure_Status"),
          
        # 3. Fix the Time Format for Power BI
        # We extract the Hour and Minute from the integer (e.g., 1430 -> 14 and 30)
        (pl.col("CRSDepTime") // 100).alias("Scheduled_Hour"),
        (pl.col("CRSDepTime") % 100).alias("Scheduled_Minute")
    ])
    
    # D. Cleanup: Cast types to ensure Power BI reads them correctly
    .with_columns([
        pl.col("Scheduled_Hour").cast(pl.Int32),
        pl.col("Scheduled_Minute").cast(pl.Int32)
    ])
)

# 3. Execute and Save
print("Processing data... (This might take a moment)")
# .collect() actually runs the query now
df_final = df_clean.collect()

print(f"Transformation Complete! Rows processed: {df_final.height}")

# 4. Save to Parquet (Best format for Power BI)
# The corrected code on line 70
df_final.write_parquet(r'E:\aviation_project\scripts\output\processed_data.parquet')
print(f"File saved to: {df_final}") 