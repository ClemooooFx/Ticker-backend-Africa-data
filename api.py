from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import json
import os
from pathlib import Path

app = FastAPI(title="Market Data API", version="1.0.0")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Base path for market data
BASE_PATH = Path("market_data")

# Valid market codes
VALID_MARKETS = ["brvm", "bse", "gse", "jse", "luse", "mse", "ngx", "nse", "use", "zse"]


def load_json_file(file_path: Path):
    """Load and return JSON file content"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Data not found")
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Invalid JSON data")


@app.get("/")
def root():
    """API root endpoint with documentation"""
    return {
        "message": "Market Data API",
        "version": "1.0.0",
        "endpoints": {
            "exchange_index": "/exchange/{market}/index-price",
            "exchange_companies": "/exchange/{market}/companies",
            "stock_price": "/stock/{market}/{ticker}/price",
            "stock_growth": "/stock/{market}/{ticker}/growth-valuation",
            "stock_performance": "/stock/{market}/{ticker}/performance",
            "stock_competitors": "/stock/{market}/{ticker}/competitors"
        },
        "valid_markets": VALID_MARKETS
    }


@app.get("/exchange/{market}/index-price")
def get_index_price(market: str):
    """Get index price data for a specific market"""
    market = market.lower()
    
    if market not in VALID_MARKETS:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid market. Valid markets: {', '.join(VALID_MARKETS)}"
        )
    
    file_path = BASE_PATH / "exchanges" / f"{market}_index_price.json"
    return load_json_file(file_path)


@app.get("/exchange/{market}/companies")
def get_listed_companies(market: str):
    """Get listed companies for a specific market"""
    market = market.lower()
    
    if market not in VALID_MARKETS:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid market. Valid markets: {', '.join(VALID_MARKETS)}"
        )
    
    file_path = BASE_PATH / "exchanges" / f"{market}_listed_companies.json"
    return load_json_file(file_path)


@app.get("/stock/{market}/{ticker}/price")
def get_stock_price(market: str, ticker: str):
    """Get stock price data"""
    market = market.lower()
    ticker = ticker.upper()
    
    if market not in VALID_MARKETS:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid market. Valid markets: {', '.join(VALID_MARKETS)}"
        )
    
    file_path = BASE_PATH / "stocks" / market / f"{ticker}_price.json"
    return load_json_file(file_path)


@app.get("/stock/{market}/{ticker}/growth-valuation")
def get_stock_growth_valuation(market: str, ticker: str):
    """Get stock growth and valuation data"""
    market = market.lower()
    ticker = ticker.upper()
    
    if market not in VALID_MARKETS:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid market. Valid markets: {', '.join(VALID_MARKETS)}"
        )
    
    file_path = BASE_PATH / "stocks" / market / f"{ticker}_growth_valuation.json"
    return load_json_file(file_path)


@app.get("/stock/{market}/{ticker}/performance")
def get_stock_performance(market: str, ticker: str):
    """Get stock performance data"""
    market = market.lower()
    ticker = ticker.upper()
    
    if market not in VALID_MARKETS:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid market. Valid markets: {', '.join(VALID_MARKETS)}"
        )
    
    file_path = BASE_PATH / "stocks" / market / f"{ticker}_performance.json"
    return load_json_file(file_path)


@app.get("/stock/{market}/{ticker}/competitors")
def get_stock_competitors(market: str, ticker: str):
    """Get stock competitors data"""
    market = market.lower()
    ticker = ticker.upper()
    
    if market not in VALID_MARKETS:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid market. Valid markets: {', '.join(VALID_MARKETS)}"
        )
    
    file_path = BASE_PATH / "stocks" / market / f"{ticker}_competitors.json"
    return load_json_file(file_path)


@app.get("/stock/{market}/tickers")
def get_available_tickers(market: str):
    """Get list of available tickers for a market"""
    market = market.lower()
    
    if market not in VALID_MARKETS:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid market. Valid markets: {', '.join(VALID_MARKETS)}"
        )
    
    stocks_path = BASE_PATH / "stocks" / market
    
    if not stocks_path.exists():
        return {"market": market, "tickers": []}
    
    # Extract unique ticker names from files
    tickers = set()
    for file in stocks_path.glob("*_price.json"):
        ticker = file.stem.replace("_price", "")
        tickers.add(ticker)
    
    return {
        "market": market,
        "tickers": sorted(list(tickers))
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
