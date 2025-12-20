# curl_cffi Troubleshooting Guide

## Current Issue

Production logs show:
- ✅ `curl_cffi=✅ enabled` (scraper.py can import curl_cffi)
- ❌ `[HOMEHARVEST] curl_cffi not available - using standard requests library` (homeharvest cannot import curl_cffi)
- ❌ Still getting 403 Forbidden errors

## Root Cause

The homeharvest module cannot import curl_cffi even though it's installed. This could be due to:

1. **Installation order**: curl-cffi might not be installed before homeharvest tries to import it
2. **System dependencies**: curl-cffi might need system libraries (OpenSSL, etc.) that aren't available
3. **Import path issues**: Different Python environments or import paths

## Changes Made

### 1. HomeHarvest Fork (`HomeHarvestLocal`)
- ✅ Added curl_cffi import with fallback
- ✅ Updated Session creation to use TLS fingerprinting
- ✅ Fixed `get_access_token()` to use curl_cffi
- ✅ Added detailed error logging

### 2. Scraper (`scraper.py`)
- ✅ Added diagnostic check to verify homeharvest's curl_cffi status
- ✅ Improved logging to show if homeharvest can use curl_cffi

### 3. Requirements (`requirements.txt`)
- ✅ Moved `curl-cffi>=0.5.10` to the TOP (before homeharvest)
- ✅ This ensures curl-cffi installs before homeharvest tries to import it

## Next Steps

### 1. Redeploy with Updated Requirements

The updated `requirements.txt` now has curl-cffi at the top:
```
curl-cffi>=0.5.10  # Install FIRST
git+https://github.com/ezraedl/HomeHarvestLocal.git@master
```

**Rebuild and redeploy** your Docker container or production environment.

### 2. Check Startup Logs

After redeployment, look for these log messages at startup:

**Expected (success):**
```
✅ curl_cffi is available - TLS fingerprinting should be enabled
✅ [HOMEHARVEST] curl_cffi is enabled in homeharvest with impersonate=chrome110
```

**Problem (if you see this):**
```
✅ curl_cffi is available
⚠️  [HOMEHARVEST] curl_cffi is NOT enabled in homeharvest
❌ [HOMEHARVEST] curl_cffi is available in scraper.py but NOT in homeharvest! This indicates an import issue.
```

### 3. If curl_cffi Still Fails to Import in homeharvest

Check the error message. The improved logging will show:
```
[HOMEHARVEST] curl_cffi not available - using standard requests library. ImportError: <error details>
```

**Common issues:**

#### A. System Dependencies Missing
curl-cffi might need system libraries. If using Docker, you may need:
```dockerfile
RUN apt-get update && apt-get install -y \
    libssl-dev \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*
```

#### B. Python Version Compatibility
Ensure Python 3.9+ is being used (curl-cffi requirement).

#### C. Installation Order
Ensure curl-cffi is installed BEFORE homeharvest:
```bash
pip install curl-cffi>=0.5.10
pip install git+https://github.com/ezraedl/HomeHarvestLocal.git@master
```

### 4. Verify Installation

After redeployment, you can verify:

```python
# In Python shell or test script
import curl_cffi
print(f"curl_cffi version: {curl_cffi.__version__}")

from homeharvest.core.scrapers import USE_CURL_CFFI, DEFAULT_IMPERSONATE
print(f"USE_CURL_CFFI: {USE_CURL_CFFI}")
print(f"DEFAULT_IMPERSONATE: {DEFAULT_IMPERSONATE}")
```

### 5. Test Scraping

Once curl_cffi is working in homeharvest, test a scrape:
```python
from homeharvest import scrape_property
df = scrape_property(location="46201", listing_type=["for_sale"], limit=1)
print(f"Got {len(df)} properties")
```

If you still get 403 errors, try changing the browser impersonation in `HomeHarvestLocal/homeharvest/core/scrapers/__init__.py`:
- Change `DEFAULT_IMPERSONATE = "chrome110"` to `"chrome116"`, `"chrome120"`, or `"edge99"`

## Files Modified

1. `HomeHarvestLocal/homeharvest/core/scrapers/__init__.py` - Added curl_cffi support
2. `HomeHarvestLocal/pyproject.toml` - Added curl-cffi dependency
3. `real-estate-crm-scraper/requirements.txt` - Moved curl-cffi to top
4. `real-estate-crm-scraper/scraper.py` - Added diagnostic checks

## Commit Status

All changes have been committed and pushed to:
- `HomeHarvestLocal` fork: https://github.com/ezraedl/HomeHarvestLocal.git
- `real-estate-crm-scraper` (requirements.txt and scraper.py changes need to be committed)

## Expected Outcome

After redeployment with the updated requirements.txt:
- curl-cffi should install first
- homeharvest should successfully import curl_cffi
- TLS fingerprinting should be enabled
- 403 errors should be reduced or eliminated

