from ckanapi import RemoteCKAN
import csv
import pandas as pd
import os
from datetime import datetime

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

# Joining both query results into a single list
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

# Convert 'delivery_date' to datetime format and filter out rows with past dates
df['delivery_date'] = pd.to_datetime(df['delivery_date'], format='%Y-%m-%d', errors='coerce')
current_date = datetime.now()
df = df[df['delivery_date'] >= current_date]

# Filter the DataFrame: keep only rows where 'contract_value' > 100,000
filtered_df = df[df['contract_value'] > 100000]

# Convert DataFrame to a 2D Python list (including headers)
data_list = [filtered_df.columns.tolist()] + filtered_df.values.tolist()

# Sort the list by 'delivery_date' (assuming the delivery_date column is the third column)
# Find the index of 'delivery_date' column
delivery_date_index = data_list[0].index('delivery_date')

# Sort the data rows by the 'delivery_date' column
data_list[1:] = sorted(data_list[1:], key=lambda x: x[delivery_date_index])

# Write the sorted list back to a CSV file
with open(filtered_filename, 'w', newline='', encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(data_list)

# Print the total number of rows in the filtered DataFrame
total_rows = len(data_list) - 1  # Subtract 1 for the header row
print(f"Total number of entries: {total_rows}")

# Remove the now useless input file
if os.path.exists(filename):
    os.remove(filename)
