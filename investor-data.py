import csv
import random
import pypyodbc as odbc

# Database connection settings
DRIVER_NAME= 'SQL SERVER'
SERVER_NAME='DESKTOP-CKVVUFC\\MSSQLSERVER22'
DATABASE_NAME='StockTradingDB'

connection_string=f"""
DRIVER={{{DRIVER_NAME}}};
SERVER={SERVER_NAME};
DATABASE={DATABASE_NAME};
Trust_Connection=yes;
uid=<username>;
pwd=<password>;
"""
conn=odbc.connect(connection_string)
cursor= conn.cursor()

# Fetch valid AccountIds from Trading.AccountDetails
cursor.execute("SELECT Id FROM Trading.AccountDetails;")
account_ids = [row[0] for row in cursor.fetchall()]

print(f"✅ Fetched {len(account_ids)} AccountIds from AccountDetails")

# Number of mock investors
num_rows = 1000000

# Supported currencies
currencies = ["USD", "EUR", "GBP", "JPY", "THB", "INR", "AUD"]

# Output CSV file
output_file = "mock_investors.csv"

with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    
    # No header for OPENROWSET compatibility
    for _ in range(num_rows):
        # Balance: random between 100 and 100,000 with 6 decimal places
        balance = f"{random.uniform(100, 100000):.6f}"
        
        # Currency: pick from list
        currency = random.choice(currencies)
        
        # AccountId: pick a random valid ID from AccountDetails
        account_id = random.choice(account_ids)
        
        # Write row
        writer.writerow([balance, currency, account_id])

print(f"✅ {num_rows} investors generated into {output_file} successfully!")

# Close DB connection
conn.close()