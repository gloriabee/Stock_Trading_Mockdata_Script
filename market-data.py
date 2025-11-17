import yfinance as yf
import random
import json
import csv
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
# for csv 
output_csv="market_data.csv"
# for json 
output_json = "market_data.json"
# for xml
output_xml= "market_data.xml"

# number of rows 
num_rows= 1_000_000
days=5


# ✅ NASDAQ symbols for IDs 1–5
nasdaq_symbols = {
    1: 'AAPL',
    2: 'TSLA',
    3: 'MSFT',
    4: 'GOOGL',
    5: 'AMZN'
}


exclude_ids = {6, 48}
all_instrument_ids = [i for i in range(1, 103) if i not in exclude_ids]


def fetch_nasdaq_data(symbol, instrument_id, days=5):
    """Fetch daily OHLCV data from yfinance"""
    try:
        df = yf.download(
            tickers=symbol,
            period=f"{days}d",
            interval="1d",
            auto_adjust=False,
            progress=False
        )
    except Exception as e:
        print(f"⚠ Error fetching {symbol}: {e}")
        return []

    if df.empty:
        print(f"⚠ No data for {symbol}")
        return []

    records = []
    for timestamp, row in df.iterrows():
        try:
            ts = timestamp.tz_convert('UTC') if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
            ts = ts.replace(hour=16, minute=0, second=0, microsecond=0)  # exchange close

            record = {
                "Timestamp": ts.strftime("%Y-%m-%d %H:%M:%S.%f").strip(),
                "OpenPrice": round(float(row["Open"]), 6),
                "HighPrice": round(float(row["High"]), 6),
                "LowPrice": round(float(row["Low"]), 6),
                "ClosePrice": round(float(row["Close"]), 6),
                "Volumes": int(row["Volumes"]),
                "InstrumentId": instrument_id
            }
            records.append(record)
        except Exception:
            continue
    return records

def generate_mock_ohlc(prev_close):
    """Generate random OHLCV data"""
    change_percent = random.uniform(-0.03, 0.03)
    open_price = prev_close * (1 + random.uniform(-0.01, 0.01))
    close_price = open_price * (1 + change_percent)
    high_price = max(open_price, close_price) * (1 + random.uniform(0, 0.01))
    low_price = min(open_price, close_price) * (1 - random.uniform(0, 0.01))
    volume = random.randint(100, 10000)
    return round(open_price, 6), round(high_price, 6), round(low_price, 6), round(close_price, 6), volume

market_data = []
now = datetime.now(timezone.utc)

# previous close price for each instrument 

prev_close_map={i: random.uniform(10,500) for i in all_instrument_ids}

# Precompute last 5 days at 4 PM UTC
days_list = []
for d in range(days):
    day = now.date() - timedelta(days=d)
    ts = datetime(day.year, day.month, day.day, 16, 0, 0, 0, tzinfo=timezone.utc)
    days_list.append(ts)


for i in range(num_rows):
    instrument_id = random.choice(all_instrument_ids)
    prev_close = prev_close_map[instrument_id]

    # Generate random OHLCV data
    o, h, l, c, v = generate_mock_ohlc(prev_close)
    prev_close_map[instrument_id] = c  # update for next tick

    # Pick one of the last 5 days
    ts = random.choice(days_list)
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S.%f")

    record = {
        "Timestamp": ts_str,
        "OpenPrice": o,
        "HighPrice": h,
        "LowPrice": l,
        "ClosePrice": c,
        "Volumes": v,
        "InstrumentId": instrument_id
    }
    market_data.append(record)
    
# ✅ Save JSON only
# with open(output_json, "w", encoding="utf-8") as f:
#     json.dump(market_data, f, indent=4)

with open(output_csv, "w", newline="", encoding="utf-8-sig") as f:
    fieldnames=["Timestamp","OpenPrice","HighPrice","LowPrice","ClosePrice","Volumes","InstrumentId"]

    writer= csv.DictWriter(f,fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(market_data)

print(f"✅ Generated {num_rows,}: {output_csv}")
    
# root= ET.Element("MarketData")
# for rec in market_data:
#     item= ET.SubElement(root,"Record")
#     for key,val in rec.items():
#         child= ET.SubElement(item,key)
#         child.text=str(val)

# tree= ET.ElementTree(root)
# tree.write(output_xml, encoding="utf-8",xml_declaration=True)



# print(f"✅ Market data JSON generated: {output_json}")
# print(f"✅ Market data exported as XML: {output_xml}")