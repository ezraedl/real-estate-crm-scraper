# Bug Fix: Zero Properties Scraping Issue

## Problem
All scraping results in the last 2 days returned 0 properties.

## Root Causes Identified

1. **Empty listing_types**: Jobs were being created with `listing_types=[]` (empty list) instead of using default values. When `listing_types` is empty, the scraper should fall back to default types, but there was a potential edge case where empty lists weren't handled properly.

2. **Overly restrictive incremental scraping**: When jobs run very frequently (e.g., less than 24 hours apart), the incremental logic was using `updated_in_past_hours=1` (or very small values), which filtered out almost all properties since most properties don't get updated within 1 hour.

3. **HomeHarvest version**: Using an older version (0.8.7) instead of the latest (0.8.9).

## Fixes Applied

### 1. Enhanced listing_types handling (`scraper.py`)
- Added explicit check to treat empty `listing_types` lists as `None` to ensure proper fallback to defaults
- Added safety checks in `scrape_location()` and `_scrape_all_listing_types()` to ensure `listing_types_to_scrape` is never empty
- Improved logging to show when default types are being used

### 2. Fixed incremental scraping time window (`scraper.py`)
- Changed minimum `updated_in_past_hours` from 1 hour to 24 hours
- This prevents filtering out all properties when jobs run frequently
- Applied fix to all 4 locations where incremental scraping logic exists:
  - `_scrape_all_listing_types()` - forced incremental mode
  - `_scrape_all_listing_types()` - scheduled job incremental mode
  - `_scrape_listing_type()` - forced incremental mode
  - `_scrape_listing_type()` - scheduled job incremental mode

### 3. Upgraded HomeHarvest (`requirements.txt`)
- Upgraded from `homeharvest==0.8.7` to `homeharvest==0.8.9`

### 4. Created diagnostic script (`scripts/diagnose_zero_properties.py`)
- Script to analyze recent jobs and identify issues
- Checks for empty listing_types, incremental scraping parameters, and other potential problems

## Testing Recommendations

1. Run the diagnostic script to check current job status:
   ```bash
   python scripts/diagnose_zero_properties.py
   ```

2. Monitor the next scheduled job run to verify:
   - Properties are being scraped (not 0)
   - Logs show correct listing_types being used
   - Incremental scraping uses minimum 24 hours window

3. Check logs for:
   - `[TARGET] Scraping ALL property types` (when defaults are used)
   - `[INCREMENTAL] Only fetching properties updated in past 24 hours` (minimum window)

## Files Changed

- `requirements.txt` - Upgraded homeharvest to 0.8.9
- `scraper.py` - Fixed listing_types handling and incremental scraping time windows
- `scripts/diagnose_zero_properties.py` - New diagnostic script

## Branch

All changes are on branch: `bugfix/zero-properties-scraping-issue`



