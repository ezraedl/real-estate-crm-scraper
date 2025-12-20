# Guide: Modifying HomeHarvest Fork to Use curl_cffi

## Overview
This guide explains how to modify your forked homeharvest repository to use `curl_cffi` instead of `requests` for TLS fingerprinting.

## Step 1: Clone Your Fork

```bash
git clone https://github.com/YOUR_USERNAME/homeharvest.git
cd homeharvest
```

## Step 2: Find Where `requests` is Used

The main file that needs modification is:
- `homeharvest/core/scrapers/__init__.py` (or similar scraper files)

Search for all `requests` usage:
```bash
grep -r "import requests" .
grep -r "from requests" .
```

## Step 3: Replace `requests` with `curl_cffi.requests`

### Key Changes Needed:

1. **Replace the import:**
   ```python
   # OLD:
   import requests
   from requests.adapters import HTTPAdapter
   
   # NEW:
   try:
       from curl_cffi import requests
       from curl_cffi.requests.adapters import HTTPAdapter
       USE_CURL_CFFI = True
   except ImportError:
       import requests
       from requests.adapters import HTTPAdapter
       USE_CURL_CFFI = False
   ```

2. **Update Session Creation:**
   ```python
   # OLD:
   session = requests.Session()
   
   # NEW:
   if USE_CURL_CFFI:
       session = requests.Session(impersonate="chrome110")  # or chrome116, chrome120, etc.
   else:
       session = requests.Session()
   ```

3. **Update HTTPAdapter (if used):**
   ```python
   # OLD:
   adapter = HTTPAdapter()
   
   # NEW:
   if USE_CURL_CFFI:
       from curl_cffi.requests.adapters import HTTPAdapter
       adapter = HTTPAdapter()
   else:
       from requests.adapters import HTTPAdapter
       adapter = HTTPAdapter()
   ```

## Step 4: Add curl_cffi to homeharvest's requirements

Add to `requirements.txt` or `setup.py`:
```
curl-cffi>=0.5.10
```

## Step 5: Test Your Changes

1. Install your modified version locally:
   ```bash
   pip install -e .
   ```

2. Test with a simple script:
   ```python
   from homeharvest import scrape_property
   df = scrape_property(location="Indianapolis, IN", listing_type=["for_sale"], limit=1)
   print(f"Got {len(df)} properties")
   ```

## Step 6: Commit and Push

```bash
git add .
git commit -m "Replace requests with curl_cffi for TLS fingerprinting"
git push origin main
```

## Step 7: Update Your Scraper to Use Forked Version

In `requirements.txt`, replace:
```
homeharvest==0.8.11
```

With:
```
git+https://github.com/YOUR_USERNAME/homeharvest.git@main
```

Or if you create a release:
```
homeharvest @ git+https://github.com/YOUR_USERNAME/homeharvest.git@v0.8.11-curl-cffi
```

## Important Notes

1. **Browser Impersonation:** Choose the right browser version:
   - `chrome110`, `chrome116`, `chrome120` - Chrome versions
   - `safari15_3`, `safari15_5` - Safari versions
   - `edge99`, `edge101` - Edge versions
   - Test which one works best with Realtor.com

2. **Fallback:** Always keep a fallback to regular `requests` in case `curl_cffi` is not installed

3. **Testing:** Test thoroughly before deploying to production

4. **Version Tagging:** Consider creating a version tag for your fork (e.g., `0.8.11-curl-cffi`)

## Common Issues

1. **Import Errors:** Make sure `curl_cffi` is installed before importing
2. **Session Issues:** Some homeharvest code might need session configuration updates
3. **Adapter Issues:** HTTPAdapter might need different initialization with curl_cffi



