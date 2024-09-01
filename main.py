from ckanapi import RemoteCKAN
import csv
import pandas as pd
import os
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# Step 1: Connect to the remote CKAN instance
rc = RemoteCKAN("https://open.canada.ca/data/en/")

# Step 2: Query for results with keywords "waste" and "recycling"
result_waste = rc.action.datastore_search(
    resource_id="fac950c0-00d5-4ec1-a4d3-9cbebf98a305",
    limit=10000,
    q="waste"
)

result_recycling = rc.action.datastore_search(
    resource_id="fac950c0-00d5-4ec1-a4d3-9cbebf98a305",
    limit=10000,
    q="recycling"
)

# Step 3: Combine the query results
results = result_waste["records"] + result_recycling["records"]

# Step 4: Write results to a CSV file
filename = "output.csv"

with open(filename, 'w', newline='', encoding="utf-8") as csvfile:
    fieldnames = results[0].keys()
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)

# Step 5: Load the CSV into a DataFrame and filter the data
filtered_filename = 'final_output.csv'
df = pd.read_csv(filename)

df['delivery_date'] = pd.to_datetime(df['delivery_date'], format='%Y-%m-%d', errors='coerce')
current_date = datetime.now()
df = df[df['delivery_date'] >= current_date]
filtered_df = df[df['contract_value'] > 100000]

# Step 6: Sort the DataFrame by 'delivery_date'
filtered_df = filtered_df.sort_values(by='delivery_date')

# Step 7: Write the filtered and sorted data back to a CSV file (final_output.csv)
filtered_df.to_csv(filtered_filename, index=False)

# Step 8: Print the total number of rows
total_rows = len(filtered_df)
print(f"Total number of entries: {total_rows}")

# Step 9: Remove the temporary file (output.csv)
if os.path.exists(filename):
    os.remove(filename)

# Step 10: Calculate notification date and send email
filtered_df['notification_date'] = filtered_df['delivery_date'] - timedelta(days=30)
today = datetime.today().date()

notify_df = filtered_df[filtered_df['notification_date'] == pd.Timestamp(today)]

def send_email_notification(contracts):
    sender = 'example@hotmail.com'
    receiver = 'example@gmail.com'
    subject = 'Canadian Government Contract Notification'
    selected_columns = contracts[["_id", "reference_number", "description_en", "contract_value"]]
    body = f"The following contracts are due for delivery in 30 days:\n\n{selected_columns.to_string(index=False)}"
    
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = receiver
    
    smtp_server = 'smtp-mail.outlook.com'
    smtp_port = 587
    smtp_user = 'example@hotmail.com'
    smtp_password = 'examplepassword'
    
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(sender, receiver, msg.as_string())

if not notify_df.empty:
    send_email_notification(notify_df)
