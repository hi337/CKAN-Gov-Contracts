from ckanapi import RemoteCKAN
import csv
import pandas as pd
import os

rc = RemoteCKAN("https://open.canada.ca/data/en/")

# Results for keyword "waste"
result_waste = rc.action.datastore_search(
    resource_id="fac950c0-00d5-4ec1-a4d3-9cbebf98a305",
    limit=10000,
    q="waste"
)

# Results for keyword "recycling"
result_recycling = rc.action.datastore_search(
    resource_id="fac950c0-00d5-4ec1-a4d3-9cbebf98a305",
    limit=10000,
    q="recycling"
)

# joining both query results into a single list
results = result_waste["records"] + result_recycling["records"]


# Define the filename for the CSV file
filename = "output.csv"

# Open the file in write mode
with open(filename, 'w', newline='', encoding="utf-8") as csvfile:
    # Create a CSV DictWriter object
    fieldnames = results[0].keys()  # Get the headers from the first dictionary
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    # Write the header
    writer.writeheader()
    
    # Write the data
    writer.writerows(results)

# Define the input and output file names
filtered_filename = 'final_output.csv'

# Read the CSV file into a DataFrame
df = pd.read_csv(filename)

# Filter the DataFrame: keep only rows where 'contract_value' > 100,000
filtered_df = df[df['contract_value'] > 100000]

# Write the filtered DataFrame to a new CSV file
filtered_df.to_csv(filtered_filename, index=False)

# Print the total number of rows in the filtered DataFrame
total_rows = len(filtered_df)
print(f"Total number of entries: {total_rows}")

# Remove the now useless input file
if os.path.exists(filename):
    os.remove(filename)