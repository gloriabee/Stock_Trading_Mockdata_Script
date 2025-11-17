import random
import csv
from datetime import datetime, timedelta, timezone
import pypyodbc as odbc

# Database connection
DRIVER_NAME = 'SQL SERVER'
SERVER_NAME = 'DESKTOP-CKVVUFC\\MSSQLSERVER22'
DATABASE_NAME = 'StockTradingDB'

connection_string = f"""
DRIVER={{{DRIVER_NAME}}};
SERVER={SERVER_NAME};
DATABASE={DATABASE_NAME};
Trust_Connection=yes;
uid=<username>;
pwd=<password>;
"""

conn = odbc.connect(connection_string)
cursor = conn.cursor()

# ✅ Fetch Instrument IDs and their ExchangeId
cursor.execute("SELECT Id, ExchangeId FROM Trading.Instruments")
instrument_data = cursor.fetchall()
instrument_map = {row[0]: row[1] for row in instrument_data}  # {InstrumentId: ExchangeId}

# ✅ Fetch Investor IDs
cursor.execute("SELECT Id FROM Trading.Investors")
investor_ids = [row[0] for row in cursor.fetchall()]

# ✅ Output CSV file
output_file = "mock_orders.csv"
num_orders = 1_000_000

# ✅ Order attributes
sides = ["buy", "sell"]
order_types = ["market", "limit", "stop"]
status_types = ["add", "cancel", "fulfill", "partialFill", "modification"]

now = datetime.now(timezone.utc)

# ✅ CSV header (no OrderId)
headers = [
    "Side", "OrderType", "Quantity", "DisplayQuantity", "Price",
    "DisplayPrice", "Status", "OrderDate", "InvestorId", "InstrumentId"
]

# ✅ Generate and write orders in batches for performance
batch_size = 10_000  # Write in chunks to avoid memory issues
written_rows = 0

with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()

    while written_rows < num_orders:
        batch = []
        for _ in range(batch_size):
            side = random.choice(sides)
            order_type = random.choice(order_types)
            status = random.choice(status_types)
            quantity = random.randint(1, 1000)
            price = round(random.uniform(10, 500), 6)

            instrument_id = random.choice(list(instrument_map.keys()))
            exchange_id = instrument_map[instrument_id]
            investor_id = random.choice(investor_ids)

            rounded_price = round(price, 2)

            # ✅ CME-style display price if ExchangeId == 9
            if exchange_id == 9:
                whole, frac = divmod(int(round(rounded_price * 100)), 100)
                display_price = f"{whole}'{frac:02d}"
            else:
                display_price = f"{rounded_price:.2f}"

            # ✅ Handle DisplayQuantity as empty string if None
            display_quantity = random.randint(1, quantity) if random.random() < 0.5 else ''
            order_date = now - timedelta(days=random.randint(0, 30), minutes=random.randint(0, 1440))
            order_date_str = order_date.strftime("%Y-%m-%d %H:%M:%S")  # SQL-friendly format

            batch.append({
                "Side": side,
                "OrderType": order_type,
                "Quantity": quantity,
                "DisplayQuantity": display_quantity,
                "Price": price,
                "DisplayPrice": display_price,
                "Status": status,
                "OrderDate": order_date_str,
                "InvestorId": investor_id,
                "InstrumentId": instrument_id
            })

            written_rows += 1
            if written_rows >= num_orders:
                break

        writer.writerows(batch)
        print(f"✅ Written {written_rows} orders so far...")

print(f"✅ Mock orders CSV file generated: {output_file}")