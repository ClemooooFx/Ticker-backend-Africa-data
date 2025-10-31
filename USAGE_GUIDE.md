# Usage Guide - Market Data Export System

## Overview

This system uses two scripts for efficient data management:

1. **`comprehensive_market_exporter.py`** - Full export (run once initially, then weekly)
2. **`updater_market_exporter.py`** - Incremental updates (run twice daily)

## Initial Setup

### Step 1: Full Export (First Time Only)

```bash
python comprehensive_market_exporter.py
```

**What it does:**
- Exports ALL data from all 10 exchanges
- Creates complete JSON files for all stocks
- Takes ~1 hour to complete
- Creates folder structure: `market_data/exchanges/` and `market_data/stocks/`

**Run this:**
- First time ever
- When adding new exchanges
- Once per week (Sunday) for data validation

---

## Daily Updates

### Step 2: Incremental Updates (Twice Daily)

```bash
python updater_market_exporter.py
```

**What it does:**
- **Appends** latest price to existing price files
- **Replaces** daily-changing data (competitors, performance, growth)
- Takes ~10-15 minutes
- Only updates what's changed

**Run this:**
- 6:00 AM GMT (before market open)
- 2:00 PM GMT (mid-trading day)
- Monday to Friday only

---

## Data Update Strategy

### Append-Only Data (Latest Point Added)
These add new entries to existing files:

| Data Type | File | Update Frequency |
|-----------|------|------------------|
| Index Prices | `{market}_index_price.json` | Twice daily |
| Stock Prices | `{ticker}_price.json` | Twice daily |

**Format:**
```json
[
  {"Date": "2025-10-29", "Price": 340.50},
  {"Date": "2025-10-30", "Price": 344.36},  ← Appended
  {"Date": "2025-10-31", "Price": 347.12}   ← New
]
```

### Replace-All Data (File Overwritten)
These completely replace file contents:

| Data Type | File | Update Frequency |
|-----------|------|------------------|
| Competitors | `{ticker}_competitors.json` | Twice daily |
| Performance (10 days) | `{ticker}_performance.json` | Twice daily |
| Growth & Valuation | `{ticker}_growth_valuation.json` | Twice daily |

---

## Summary Log

### `export_summary.json`

This file logs ALL export operations (never overwrites):

```json
[
  {
    "export_date": "2025-10-31T06:00:00",
    "update_type": "incremental",
    "exchanges_updated": 10,
    "total_updates": 3492,
    "duration_seconds": 847.5
  },
  {
    "export_date": "2025-10-31T14:00:00",
    "update_type": "incremental",
    "exchanges_updated": 10,
    "total_updates": 1250,
    "duration_seconds": 623.2
  }
]
```

---

## Automation Schedule

### GitHub Actions (Automatic)

**Twice Daily Updates:**
- Monday-Friday at 6:00 AM GMT
- Monday-Friday at 2:00 PM GMT
- Runs: `updater_market_exporter.py`

**Weekly Full Export:**
- Sunday at 3:00 AM GMT
- Runs: `comprehensive_market_exporter.py`

### Manual Trigger

Go to GitHub Actions → "Export African Markets Data" → Run workflow

Choose:
- **update** - Incremental update (~10 min)
- **full** - Complete re-export (~1 hour)

---

## File Structure

```
market_data/
├── export_summary.json          ← Cumulative log of all exports
├── exchanges/
│   ├── nse_index_price.json     ← Appended with latest
│   └── nse_listed_companies.json
└── stocks/
    └── nse/
        ├── safcom_price.json            ← Appended with latest
        ├── safcom_competitors.json      ← Replaced entirely
        ├── safcom_performance.json      ← Replaced entirely
        └── safcom_growth_valuation.json ← Replaced entirely
```

---

## Batch Processing

Both scripts use intelligent batching:

- **3 exchanges** at a time
- **3 stocks** at a time
- **2 seconds** pause between batches
- **1 second** pause between individual stocks

This prevents:
- Rate limiting
- Timeouts
- IP blocks

---

## Expected Durations

| Operation | Script | Duration | Updates |
|-----------|--------|----------|---------|
| Full Export | comprehensive_market_exporter.py | ~60 min | 873 stocks × 4 = 3,492 files |
| Incremental Update | updater_market_exporter.py | ~10-15 min | Only changed data |

---

## Troubleshooting

### "No existing company data found"
**Solution:** Run `comprehensive_market_exporter.py` first

### Script takes too long
**Solution:** Normal! First run takes ~1 hour. Updates take ~10 min.

### No new data appended
**Reason:** Data for that date already exists (duplicate protection)

### Rate limiting errors
**Solution:** Scripts have built-in delays. If persisting, increase wait times.

---

## Best Practices

1. **First Time:** Run full export once
2. **Ongoing:** Use updater twice daily
3. **Weekly Validation:** Run full export on weekends
4. **Monitor:** Check `export_summary.json` for success rates
5. **Backup:** Keep historical data before major changes

---

## Quick Commands

```bash
# First time setup
python comprehensive_market_exporter.py

# Daily updates (automated via GitHub Actions)
python updater_market_exporter.py

# Manual full refresh
python comprehensive_market_exporter.py

# Check what's scheduled
cat .github/workflows/export-data.yml
```

---

## Summary

- ✅ **Initial setup:** 1 hour full export
- ✅ **Daily updates:** 10-15 minutes incremental
- ✅ **Automatic:** GitHub Actions handles scheduling
- ✅ **Efficient:** Only updates what changed
- ✅ **Logged:** Complete audit trail in export_summary.json
