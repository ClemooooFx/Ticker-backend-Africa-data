"""
Market Data Updater - Incremental Updates Only
Updates existing JSON files with latest data without full re-export
"""

import afrimarket as afm
import json
import os
import time
import pandas as pd
from datetime import datetime

# Data directory structure
BASE_DIR = 'market_data'
EXCHANGES_DIR = os.path.join(BASE_DIR, 'exchanges')
STOCKS_DIR = os.path.join(BASE_DIR, 'stocks')
SUMMARY_FILE = os.path.join(BASE_DIR, 'export_summary.json')

# List of all exchanges
exchanges = {
    'bse': 'Botswana Stock Exchange',
    'brvm': 'Bourse Régionale des Valeurs Mobilières',
    'gse': 'Ghana Stock Exchange',
    'jse': 'Johannesburg Stock Exchange',
    'luse': 'Lusaka Securities Exchange',
    'mse': 'Malawi Stock Exchange',
    'nse': 'Nairobi Securities Exchange',
    'ngx': 'Nigerian Stock Exchange',
    'use': 'Uganda Securities Exchange',
    'zse': 'Zimbabwe Stock Exchange'
}

def safe_convert(value, conversion_type='float'):
    """Safely convert values to specified type"""
    try:
        if pd.isna(value) or value == '' or value is None:
            return None
        if conversion_type == 'float':
            return float(pd.to_numeric(value, errors='coerce'))
        elif conversion_type == 'str':
            return str(value)
    except:
        return None

def load_json(filepath):
    """Load existing JSON file"""
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except:
            return None
    return None

def save_json(filepath, data):
    """Save data to JSON file"""
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

def append_to_summary(summary_data):
    """Append export summary to log file"""
    summaries = []
    
    # Load existing summaries
    if os.path.exists(SUMMARY_FILE):
        try:
            with open(SUMMARY_FILE, 'r') as f:
                existing = json.load(f)
                # Check if it's a list or single object
                if isinstance(existing, list):
                    summaries = existing
                else:
                    summaries = [existing]
        except:
            summaries = []
    
    # Append new summary
    summaries.append(summary_data)
    
    # Save back
    save_json(SUMMARY_FILE, summaries)

def update_index_price(market_code):
    """Update index price with latest data point only"""
    filepath = os.path.join(EXCHANGES_DIR, f'{market_code}_index_price.json')
    
    try:
        # Get latest data
        exchange = afm.Exchange(market=market_code)
        index_df = exchange.get_index_price()
        
        if index_df is None or index_df.empty:
            print(f"      ✗ No new index data")
            return False
        
        # Get the latest record
        latest = index_df.iloc[-1]
        latest_date = str(latest['Date'])
        latest_price = float(latest['Price'])
        
        new_entry = {
            "Date": latest_date,
            "Price": latest_price
        }
        
        # Load existing data
        existing_data = load_json(filepath) or []
        
        # Check if this date already exists
        existing_dates = [entry.get('Date') for entry in existing_data]
        
        if latest_date in existing_dates:
            print(f"      ⊙ Index already up to date ({latest_date})")
            return False
        
        # Append new entry
        existing_data.append(new_entry)
        
        # Save updated data
        save_json(filepath, existing_data)
        print(f"      ✓ Index updated with {latest_date}: {latest_price}")
        return True
        
    except Exception as e:
        print(f"      ✗ Index update failed: {e}")
        return False

def update_stock_price(ticker, market_code):
    """Update stock price with latest data point only"""
    market_stock_dir = os.path.join(STOCKS_DIR, market_code)
    filepath = os.path.join(market_stock_dir, f'{ticker}_price.json')
    
    try:
        # Get latest data
        stock = afm.Stock(ticker=ticker, market=market_code)
        price_df = stock.get_price()
        
        if price_df is None or price_df.empty:
            print(f"          ✗ No price data")
            return False
        
        # Get the latest record
        latest = price_df.iloc[-1]
        latest_date = str(latest['Date'])
        latest_price = float(latest['Price'])
        
        new_entry = {
            "Date": latest_date,
            "Price": latest_price
        }
        
        # Load existing data
        existing_data = load_json(filepath) or []
        
        # Check if this date already exists
        existing_dates = [entry.get('Date') for entry in existing_data]
        
        if latest_date in existing_dates:
            return False  # Already up to date
        
        # Append new entry
        existing_data.append(new_entry)
        
        # Save updated data
        save_json(filepath, existing_data)
        print(f"          ✓ Price: {latest_date} - {latest_price}")
        return True
        
    except Exception as e:
        print(f"          ✗ Price failed: {e}")
        return False

def replace_stock_data(ticker, market_code, data_type, fetch_function):
    """Replace entire file content with fresh data (for daily-changing data)"""
    market_stock_dir = os.path.join(STOCKS_DIR, market_code)
    
    filename_map = {
        'competitors': f'{ticker}_competitors.json',
        'performance': f'{ticker}_performance.json',
        'growth': f'{ticker}_growth_valuation.json'
    }
    
    filepath = os.path.join(market_stock_dir, filename_map[data_type])
    
    try:
        data_df = fetch_function()
        
        if data_df is None or data_df.empty:
            print(f"          ✗ No {data_type} data")
            return False
        
        # Convert to dict and save (replaces entire file)
        data = data_df.to_dict('records')
        save_json(filepath, data)
        print(f"          ✓ {data_type.capitalize()} updated")
        return True
        
    except Exception as e:
        print(f"          ✗ {data_type.capitalize()} failed: {e}")
        return False

def update_exchange_data(market_code, market_name):
    """Update exchange-level data"""
    print(f"\n{'='*60}")
    print(f"Updating {market_name} ({market_code.upper()})")
    print(f"{'='*60}")
    
    # Update index price (append only)
    print(f"  → Updating index price...")
    update_index_price(market_code)
    
    # Load existing tickers from listed companies file
    companies_file = os.path.join(EXCHANGES_DIR, f'{market_code}_listed_companies.json')
    existing_companies = load_json(companies_file)
    
    if not existing_companies:
        print(f"  ✗ No existing company data found. Run comprehensive exporter first.")
        return []
    
    tickers = [company['ticker'] for company in existing_companies if company.get('ticker')]
    print(f"  → Found {len(tickers)} tickers to update")
    
    return tickers

def update_stocks_in_batches(tickers, market_code, batch_size=3):
    """Update individual stocks in batches"""
    total = len(tickers)
    if total == 0:
        print("    No tickers to update")
        return 0, 0
    
    print(f"\n  → Updating {total} stocks in batches of {batch_size}...")
    
    updates = {
        'price': 0,
        'competitors': 0,
        'performance': 0,
        'growth': 0
    }
    
    for i in range(0, total, batch_size):
        batch = tickers[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        for j, ticker in enumerate(batch, 1):
            print(f"    [{batch_num}:{j}] Updating {ticker.upper()}...")
            
            try:
                stock = afm.Stock(ticker=ticker, market=market_code)
                
                # 1. Append latest price
                if update_stock_price(ticker, market_code):
                    updates['price'] += 1
                
                # 2. Replace competitors (full file)
                if replace_stock_data(ticker, market_code, 'competitors', stock.get_competitors):
                    updates['competitors'] += 1
                
                # 3. Replace performance (full file)
                if replace_stock_data(ticker, market_code, 'performance', stock.get_stock_market_performance_date):
                    updates['performance'] += 1
                
                # 4. Replace growth & valuation (full file)
                if replace_stock_data(ticker, market_code, 'growth', stock.get_growth_and_valuation):
                    updates['growth'] += 1
                
            except Exception as e:
                print(f"          ✗ Failed to update {ticker}: {e}")
            
            time.sleep(1)  # Delay between stocks
        
        # Pause between batches
        if i + batch_size < total:
            print(f"    Batch {batch_num} complete. Pausing 2 seconds...")
            time.sleep(2)
    
    print(f"\n  ✓ Updates: Price={updates['price']}, Performance={updates['performance']}, "
          f"Growth={updates['growth']}, Competitors={updates['competitors']}")
    
    return sum(updates.values()), total * 4  # total updates / total possible

def main():
    """Main execution function"""
    start_time = datetime.now()
    
    print("=" * 70)
    print("AFRICAN MARKETS DATA UPDATER (INCREMENTAL)")
    print("=" * 70)
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Update directory: {os.path.abspath(BASE_DIR)}")
    print("=" * 70)
    
    exchange_items = list(exchanges.items())
    exchange_batch_size = 3
    total_exchanges = len(exchange_items)
    
    exchanges_updated = 0
    total_updates = 0
    total_possible = 0
    
    # Process exchanges in batches
    for start in range(0, total_exchanges, exchange_batch_size):
        batch = exchange_items[start:start + exchange_batch_size]
        batch_num = start // exchange_batch_size + 1
        
        print(f"\n{'#'*70}")
        print(f"EXCHANGE BATCH {batch_num} OF {(total_exchanges + exchange_batch_size - 1) // exchange_batch_size}")
        print(f"{'#'*70}")
        
        for code, name in batch:
            # Update exchange data and get tickers
            tickers = update_exchange_data(code, name)
            exchanges_updated += 1
            
            # Update individual stocks
            if tickers:
                updates, possible = update_stocks_in_batches(tickers, code, batch_size=3)
                total_updates += updates
                total_possible += possible
            
            print(f"  ✓ {name} update complete\n")
            time.sleep(2)  # Brief pause between exchanges
        
        # Pause between exchange batches
        if start + exchange_batch_size < total_exchanges:
            print(f"\n{'='*70}")
            print(f"Exchange batch {batch_num} complete. Pausing 2 seconds...")
            print(f"{'='*70}\n")
            time.sleep(2)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "=" * 70)
    print("UPDATE COMPLETE")
    print("=" * 70)
    print(f"Exchanges updated: {exchanges_updated}/{total_exchanges}")
    print(f"Total updates made: {total_updates}/{total_possible} possible")
    print(f"Update rate: {(total_updates/total_possible*100) if total_possible > 0 else 0:.1f}%")
    print(f"Duration: {duration}")
    print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # Append to summary log
    summary = {
        'update_date': datetime.now().isoformat(),
        'update_type': 'incremental',
        'exchanges_updated': exchanges_updated,
        'total_updates': total_updates,
        'total_possible': total_possible,
        'update_rate_percent': round((total_updates/total_possible*100) if total_possible > 0 else 0, 2),
        'duration_seconds': duration.total_seconds(),
        'exchanges': list(exchanges.keys())
    }
    
    append_to_summary(summary)
    
    print("\n✓ Update summary appended to export_summary.json")

if __name__ == "__main__":
    main()
