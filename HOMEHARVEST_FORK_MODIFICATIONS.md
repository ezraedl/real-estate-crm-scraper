# HomeHarvest Fork Modifications for curl_cffi Support

## Your Fork
**Repository:** https://github.com/ezraedl/HomeHarvestLocal  
**Branch:** master (default)

## Files That Need Modification

Based on analysis, these files use `requests` and need to be updated:

1. `homeharvest/__init__.py`
2. `homeharvest/core/scrapers/__init__.py`

## Step-by-Step Modification Guide

### Step 1: Clone Your Fork

```bash
git clone https://github.com/ezraedl/HomeHarvestLocal.git
cd HomeHarvestLocal
```

### Step 2: Modify `homeharvest/core/scrapers/__init__.py`

This is the main file that needs changes. Find where `requests` is imported and used.

**Find this code:**
```python
import requests
from requests.adapters import HTTPAdapter
```

**Replace with:**
```python
# Try to use curl_cffi for TLS fingerprinting (anti-bot measures)
try:
    from curl_cffi import requests
    from curl_cffi.requests.adapters import HTTPAdapter
    USE_CURL_CFFI = True
    DEFAULT_IMPERSONATE = "chrome110"  # Can also try: chrome116, chrome120, edge99
except ImportError:
    import requests
    from requests.adapters import HTTPAdapter
    USE_CURL_CFFI = False
    DEFAULT_IMPERSONATE = None
```

**Find Session creation (usually `requests.Session()`):**
```python
session = requests.Session()
```

**Replace with:**
```python
if USE_CURL_CFFI:
    session = requests.Session(impersonate=DEFAULT_IMPERSONATE)
else:
    session = requests.Session()
```

**Find HTTPAdapter usage:**
```python
adapter = HTTPAdapter()
```

**Keep as is** (the import already handles curl_cffi vs requests)

### Step 3: Modify `homeharvest/__init__.py` (if needed)

Check if this file directly uses `requests`. If it only imports from other modules, you may not need to change it.

If it has:
```python
import requests
```

Replace with the same try/except pattern as above.

### Step 4: Update `pyproject.toml` or `requirements.txt`

Add `curl-cffi` as a dependency:

In `pyproject.toml`:
```toml
[project]
dependencies = [
    # ... existing dependencies ...
    "curl-cffi>=0.5.10",
]
```

Or in `requirements.txt`:
```
curl-cffi>=0.5.10
```

### Step 5: Test Locally

```bash
# Install your modified version in development mode
pip install -e .

# Test it
python -c "from homeharvest import scrape_property; df = scrape_property(location='Indianapolis, IN', listing_type=['for_sale'], limit=1); print(f'Got {len(df)} properties')"
```

### Step 6: Commit and Push

```bash
git add .
git commit -m "Add curl_cffi support for TLS fingerprinting to bypass anti-bot measures"
git push origin master
```

### Step 7: Update Your Scraper's requirements.txt

In your scraper project, update `requirements.txt`:

**Replace:**
```
homeharvest==0.8.11
```

**With:**
```
git+https://github.com/ezraedl/HomeHarvestLocal.git@master
```

Or if you create a tag:
```
git+https://github.com/ezraedl/HomeHarvestLocal.git@v0.8.11-curl-cffi
```

## Testing Browser Impersonation Versions

You can test different browser impersonation versions. Common options:
- `chrome110`, `chrome116`, `chrome120` - Chrome versions
- `safari15_3`, `safari15_5` - Safari versions  
- `edge99`, `edge101` - Edge versions

To test, modify `DEFAULT_IMPERSONATE` in the code and see which works best with Realtor.com.

## Important Notes

1. **Fallback is critical:** Always keep the fallback to regular `requests` in case `curl_cffi` is not installed
2. **Test thoroughly:** Test with actual Realtor.com requests before deploying
3. **Version compatibility:** Make sure curl_cffi version is compatible (>=0.5.10)
4. **Session reuse:** If homeharvest reuses sessions, make sure the impersonate parameter is set correctly

## Verification

After making changes, verify curl_cffi is being used:

```python
from homeharvest import scrape_property
import homeharvest.core.scrapers as scrapers

# Check if curl_cffi is being used
if hasattr(scrapers, 'USE_CURL_CFFI') and scrapers.USE_CURL_CFFI:
    print("✅ curl_cffi is enabled")
else:
    print("⚠️  Using regular requests (curl_cffi not available)")
```


