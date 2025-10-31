"""
Incremental Market Data Updater
Updates existing JSON files with only new or changed entries
"""

import afrimarket as afm
import json
import os
import time
import pandas as pd
from datetime import datetime

BASE_DIR = 'market_data'
EXCHANGES_DIR = os.path.join(BASE_DIR, 'exchanges')
STOCKS_DIR = os.path.join(BASE_DIR, 'stocks')
SUMMARY_FILE = os.path.join(BASE_DIR, 'export_summary.json')

# Exchanges to process
exchanges = [
    'bse', 'brvm', 'gse', 'jse', 'luse', 'mse', 'nse', 'ngx', 'use', 'zse'
]

def load_json(filepath):
    """Load JSON if exists"""
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []
    return []

def save_json(filepath, data):
    """Save JSON neatly"""
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

def update_json_list(filepath, new_entries, key='Date'):
    """Append only new records (based on Date)"""
    existing = load_json(filepath)
    existing_dates = {entry.get(key) for entry in existing}
    new_clean = [e for e in new_entries if e.get(key) not in existing_dates]
    if new_clean:
        existing.extend(new_clean)
        save_json(filepath, existing)
        print(f"    ✓ Added {len(new_clean)} new entries to {os.path.basename(filepath)}")
    else:
        print(f"    No new entries for {os.path.basename(filepath)}")

def update_stock_static_data(ticker, market_code):
    """Refresh and replace daily-changing stock data"""
    print(f"  → Updating {ticker.upper()} static data...")

    stock = afm.Stock(ticker=ticker, market=market_code)
    market_stock_dir = os.path.join(STOCKS_DIR, market_code)
    os.makedirs(market_stock_dir, exist_ok=True)

    static_funcs = {
        'competitors': stock.get_competitors,
        'performance': stock.get_stock_market_performance_date,
        'growth_valuation': stock.get_growth_and_valuation,
    }

    for name, func in static_funcs.items():
        try:
            df = func()
            if df is not None and not df.empty:
                data = df.to_dict('records')
                filepath = os.path.join(market_stock_dir, f"{ticker}_{name}.json")
                save_json(filepath, data)
                print(f"    ✓ Updated {name}")
            else:
                print(f"    ✗ No data for {name}")
        except Exception as e:
            print(f"    ✗ {name} failed: {e}")

def append_stock_latest_price(ticker, market_code):
    """Append only the latest price record"""
    stock = afm.Stock(ticker=ticker, market=market_code)
    market_stock_dir = os.path.join(STOCKS_DIR, market_code)
    os.makedirs(market_stock_dir, exist_ok=True)
    filepath = os.path.join(market_stock_dir, f"{ticker}_price.json")

    try:
        df = stock.get_price()
        if df is not None and not df.empty:
            latest = df.tail(1).to_dict('records')[0]
            update_json_list(filepath, [latest])
        else:
            print(f"    ✗ No new price data for {ticker}")
    except Exception as e:
        print(f"    ✗ Price update failed for {ticker}: {e}")

def append_exchange_latest_index(market_code):
    """Append only the latest index price record"""
    exchange = afm.Exchange(market=market_code)
    filepath = os.path.join(EXCHANGES_DIR, f"{market_code}_index_price.json")

    try:
        df = exchange.get_index_price()
        if df is not None and not df.empty:
            latest = df.tail(1).to_dict('records')[0]
            update_json_list(filepath, [latest])
        else:
            print(f"  ✗ No index data for {market_code.upper()}")
    except Exception as e:
        print(f"  ✗ Index update failed for {market_code.upper()}: {e}")

def process_market_batch(markets, batch_num):
    """Process a batch of exchanges"""
    for market_code in markets:
        print(f"\n{'='*50}")
        print(f"Updating {market_code.upper()} ({batch_num})")
        print(f"{'='*50}")

        # 1. Update index latest record
        append_exchange_latest_index(market_code)

        # 2. Load tickers from listed companies file
        companies_path = os.path.join(EXCHANGES_DIR, f"{market_code}_listed_companies.json")
        companies = load_json(companies_path)
        tickers = [c['ticker'] for c in companies if c.get('ticker')]

        # 3. Process tickers in batches of 3
        for i in range(0, len(tickers), 3):
            batch = tickers[i:i + 3]
            for t in batch:
                update_stock_static_data(t, market_code)
                append_stock_latest_price(t, market_code)
                time.sleep(1)
            print("    Batch complete. Waiting 2 seconds...")
            time.sleep(2)

def append_summary_entry(start_time, end_time, processed_exchanges, total_tickers):
    """Append summary info to export_summary.json"""
    duration = (end_time - start_time).total_seconds()
    summary_entry = {
        "export_date": datetime.now().isoformat(),
        "exchanges_processed": processed_exchanges,
        "total_stocks": total_tickers,
        "duration_seconds": duration,
        "exchanges": exchanges
    }

    existing_summary = load_json(SUMMARY_FILE)
    if isinstance(existing_summary, dict):
        # Convert to list if old format
        existing_summary = [existing_summary]
    existing_summary.append(summary_entry)
    save_json(SUMMARY_FILE, existing_summary)
    print("\n✓ Summary appended to export_summary.json")

def main():
    start_time = datetime.now()
    print("="*70)
    print("AFRICAN MARKETS INCREMENTAL DATA UPDATER")
    print("="*70)
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Data directory: {os.path.abspath(BASE_DIR)}")
    print("="*70)

    total_exchanges = len(exchanges)
    total_tickers = 0
    batch_size = 3

    for i in range(0, total_exchanges, batch_size):
        batch = exchanges[i:i + batch_size]
        batch_num = i // batch_size + 1
        process_market_batch(batch, batch_num)
        total_tickers += sum(
            len(load_json(os.path.join(EXCHANGES_DIR, f"{m}_listed_companies.json"))) 
            for m in batch
        )

    end_time = datetime.now()
    append_summary_entry(start_time, end_time, total_exchanges, total_tickers)
    print(f"Completed update at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
