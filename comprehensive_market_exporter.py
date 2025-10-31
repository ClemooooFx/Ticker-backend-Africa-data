"""
Comprehensive African Markets Data Exporter
Exports exchange and individual stock data in batches
"""

import afrimarket as afm
import json
import os
import time
import pandas as pd
from datetime import datetime

# Create data directory structure
BASE_DIR = 'market_data'
EXCHANGES_DIR = os.path.join(BASE_DIR, 'exchanges')
STOCKS_DIR = os.path.join(BASE_DIR, 'stocks')

os.makedirs(EXCHANGES_DIR, exist_ok=True)
os.makedirs(STOCKS_DIR, exist_ok=True)

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

def export_exchange_data(market_code, market_name):
    """Export exchange-level data (index price and listed companies)"""
    print(f"\n{'='*60}")
    print(f"Processing {market_name} ({market_code.upper()})")
    print(f"{'='*60}")
    
    tickers = []
    
    try:
        exchange = afm.Exchange(market=market_code)
        
        # 1. Export Index Price
        print(f"  → Fetching index price...")
        try:
            index_df = exchange.get_index_price()
            if index_df is not None and not index_df.empty:
                # Convert dates to string
                index_df['Date'] = index_df['Date'].astype(str)
                index_data = index_df.to_dict('records')
                
                filepath = os.path.join(EXCHANGES_DIR, f'{market_code}_index_price.json')
                with open(filepath, 'w') as f:
                    json.dump(index_data, f, indent=2)
                print(f"    ✓ Index price exported ({len(index_data)} records)")
            else:
                print(f"    ✗ No index price data available")
        except Exception as e:
            print(f"    ✗ Index price failed: {e}")
        
        # 2. Export Listed Companies
        print(f"  → Fetching listed companies...")
        try:
            companies_df = exchange.get_listed_companies()
            if companies_df is not None and not companies_df.empty:
                companies = []
                
                for _, row in companies_df.iterrows():
                    # Handle different column structures
                    ticker = safe_convert(row.get('Ticker', row.iloc[0]), 'str')
                    name = safe_convert(row.get('Name', row.iloc[1] if len(row) > 1 else ''), 'str')
                    volume = safe_convert(row.get('Volume', 'N/A'), 'str')
                    price = safe_convert(row.get('Price', 0), 'float')
                    change = safe_convert(row.get('Change', 0), 'float')
                    
                    company_data = {
                        'ticker': ticker,
                        'name': name,
                        'volume': volume,
                        'price': price,
                        'change': change
                    }
                    companies.append(company_data)
                    
                    # Collect tickers for individual stock processing
                    if ticker:
                        tickers.append(ticker)
                
                filepath = os.path.join(EXCHANGES_DIR, f'{market_code}_listed_companies.json')
                with open(filepath, 'w') as f:
                    json.dump(companies, f, indent=2)
                print(f"    ✓ Listed companies exported ({len(companies)} companies)")
            else:
                print(f"    ✗ No listed companies data available")
        except Exception as e:
            print(f"    ✗ Listed companies failed: {e}")
        
        return tickers
        
    except Exception as e:
        print(f"✗ Failed to process {market_name}: {e}")
        return []

def export_stock_data(ticker, market_code, batch_num, stock_num):
    """Export individual stock data"""
    print(f"    [{batch_num}:{stock_num}] Processing {ticker.upper()}...")
    
    # Create market-specific directory
    market_stock_dir = os.path.join(STOCKS_DIR, market_code)
    os.makedirs(market_stock_dir, exist_ok=True)
    
    success_count = 0
    
    try:
        stock = afm.Stock(ticker=ticker, market=market_code)
        
        # 1. Historical Prices
        try:
            price_df = stock.get_price()
            if price_df is not None and not price_df.empty:
                price_df['Date'] = price_df['Date'].astype(str)
                price_data = price_df.to_dict('records')
                
                filepath = os.path.join(market_stock_dir, f'{ticker}_price.json')
                with open(filepath, 'w') as f:
                    json.dump(price_data, f, indent=2)
                print(f"        ✓ Price history ({len(price_data)} records)")
                success_count += 1
        except Exception as e:
            print(f"        ✗ Price history failed: {e}")
        
        # 2. Growth & Valuation
        try:
            growth_df = stock.get_growth_and_valuation()
            if growth_df is not None and not growth_df.empty:
                growth_data = growth_df.to_dict('records')
                
                filepath = os.path.join(market_stock_dir, f'{ticker}_growth_valuation.json')
                with open(filepath, 'w') as f:
                    json.dump(growth_data, f, indent=2)
                print(f"        ✓ Growth & valuation")
                success_count += 1
        except Exception as e:
            print(f"        ✗ Growth & valuation failed: {e}")
        
        # 3. Market Performance (Last 10 days)
        try:
            performance_df = stock.get_stock_market_performance_date()
            if performance_df is not None and not performance_df.empty:
                performance_data = performance_df.to_dict('records')
                
                filepath = os.path.join(market_stock_dir, f'{ticker}_performance.json')
                with open(filepath, 'w') as f:
                    json.dump(performance_data, f, indent=2)
                print(f"        ✓ Performance data")
                success_count += 1
        except Exception as e:
            print(f"        ✗ Performance data failed: {e}")
        
        # 4. Competitors
        try:
            competitors_df = stock.get_competitors()
            if competitors_df is not None and not competitors_df.empty:
                competitors_data = competitors_df.to_dict('records')
                
                filepath = os.path.join(market_stock_dir, f'{ticker}_competitors.json')
                with open(filepath, 'w') as f:
                    json.dump(competitors_data, f, indent=2)
                print(f"        ✓ Competitors ({len(competitors_data)} found)")
                success_count += 1
        except Exception as e:
            print(f"        ✗ Competitors failed: {e}")
        
        return success_count > 0
        
    except Exception as e:
        print(f"        ✗ Failed to process stock {ticker}: {e}")
        return False

def process_stocks_in_batches(tickers, market_code, batch_size=3):
    """Process individual stocks in batches"""
    total = len(tickers)
    if total == 0:
        print("    No tickers to process")
        return
    
    print(f"\n  → Processing {total} stocks in batches of {batch_size}...")
    success_count = 0
    
    for i in range(0, total, batch_size):
        batch = tickers[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        for j, ticker in enumerate(batch, 1):
            if export_stock_data(ticker, market_code, batch_num, j):
                success_count += 1
            time.sleep(1)  # Small delay between stocks
        
        # Pause between batches
        if i + batch_size < total:
            print(f"    Batch {batch_num} complete. Pausing 5 seconds...")
            time.sleep(5)
    
    print(f"  ✓ Stocks processed: {success_count}/{total} successful")

def main():
    """Main execution function"""
    start_time = datetime.now()
    
    print("=" * 70)
    print("AFRICAN MARKETS COMPREHENSIVE DATA EXPORTER")
    print("=" * 70)
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Export directory: {os.path.abspath(BASE_DIR)}")
    print("=" * 70)
    
    exchange_items = list(exchanges.items())
    exchange_batch_size = 3
    total_exchanges = len(exchange_items)
    
    exchanges_processed = 0
    total_stocks_processed = 0
    
    # Process exchanges in batches
    for start in range(0, total_exchanges, exchange_batch_size):
        batch = exchange_items[start:start + exchange_batch_size]
        batch_num = start // exchange_batch_size + 1
        
        print(f"\n{'#'*70}")
        print(f"EXCHANGE BATCH {batch_num} OF {(total_exchanges + exchange_batch_size - 1) // exchange_batch_size}")
        print(f"{'#'*70}")
        
        for code, name in batch:
            # Export exchange data and get tickers
            tickers = export_exchange_data(code, name)
            exchanges_processed += 1
            
            # Process individual stocks for this exchange
            if tickers:
                process_stocks_in_batches(tickers, code, batch_size=3)
                total_stocks_processed += len(tickers)
            
            print(f"  ✓ {name} complete\n")
            time.sleep(2)  # Brief pause between exchanges
        
        # Pause between exchange batches
        if start + exchange_batch_size < total_exchanges:
            print(f"\n{'='*70}")
            print(f"Exchange batch {batch_num} complete. Pausing 10 seconds...")
            print(f"{'='*70}\n")
            time.sleep(10)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "=" * 70)
    print("EXPORT COMPLETE")
    print("=" * 70)
    print(f"Exchanges processed: {exchanges_processed}/{total_exchanges}")
    print(f"Total stocks processed: {total_stocks_processed}")
    print(f"Duration: {duration}")
    print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Data location: {os.path.abspath(BASE_DIR)}")
    print("=" * 70)
    
    # Create summary file
    summary = {
        'export_date': datetime.now().isoformat(),
        'exchanges_processed': exchanges_processed,
        'total_stocks': total_stocks_processed,
        'duration_seconds': duration.total_seconds(),
        'exchanges': list(exchanges.keys())
    }
    
    with open(os.path.join(BASE_DIR, 'export_summary.json'), 'w') as f:
        json.dump(summary, f, indent=2)
    
    print("\n✓ Summary saved to export_summary.json")

if __name__ == "__main__":
    main()
