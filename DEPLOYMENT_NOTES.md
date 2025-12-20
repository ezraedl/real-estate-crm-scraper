# HomeHarvest Fork Deployment Notes

## Issue: 403 Forbidden Errors Persist

Even though the logs show `curl_cffi=✅ enabled`, you're still getting 403 errors. This suggests that **production may not have the updated fork installed yet**.

## What Was Fixed

1. ✅ Added curl_cffi support to `homeharvest/core/scrapers/__init__.py`
2. ✅ Updated Session creation to use TLS fingerprinting
3. ✅ Fixed `get_access_token()` to use curl_cffi
4. ✅ Added logging to verify curl_cffi usage

## Deployment Steps

### 1. Verify Production Has Updated Fork

The production environment needs to reinstall homeharvest from your fork. Check your deployment process:

**If using pip/requirements.txt:**
```bash
pip install -r requirements.txt --upgrade --force-reinstall
```

**If using Docker:**
- Rebuild the Docker image to pick up the updated requirements.txt
- The requirements.txt already points to: `git+https://github.com/ezraedl/HomeHarvestLocal.git@master`

### 2. Verify Installation

After redeploying, check the logs for:
```
[HOMEHARVEST] curl_cffi enabled with impersonate=chrome110
```

If you see:
```
[HOMEHARVEST] curl_cffi not available - using standard requests library
```

Then curl_cffi is not installed in production.

### 3. Check Which Version Is Installed

You can verify which homeharvest version is installed:
```python
import homeharvest
print(homeharvest.__version__)  # Should show 0.8.11
# Or check the source location
import homeharvest.core.scrapers as scrapers
print(scrapers.__file__)  # Should point to your fork
```

### 4. Verify curl_cffi Usage

Check if curl_cffi is actually being used:
```python
from homeharvest.core.scrapers import USE_CURL_CFFI, DEFAULT_IMPERSONATE
print(f"USE_CURL_CFFI: {USE_CURL_CFFI}")
print(f"DEFAULT_IMPERSONATE: {DEFAULT_IMPERSONATE}")
```

## Current Status

- ✅ Fork updated with curl_cffi support
- ✅ `get_access_token()` now uses curl_cffi
- ✅ Added verification logging
- ⚠️ **Production needs to reinstall the package**

## Next Steps

1. **Redeploy your scraper** to pick up the updated fork
2. **Monitor logs** for the `[HOMEHARVEST] curl_cffi enabled` message
3. **Test scraping** - 403 errors should be reduced/eliminated
4. If 403s persist, try different browser impersonation:
   - Change `DEFAULT_IMPERSONATE` to `"chrome116"`, `"chrome120"`, or `"edge99"`

## Troubleshooting

If 403 errors continue after redeployment:

1. **Verify curl_cffi is installed:**
   ```bash
   pip list | grep curl-cffi
   ```

2. **Check if fork is being used:**
   ```python
   import homeharvest.core.scrapers as scrapers
   print(scrapers.USE_CURL_CFFI)  # Should be True
   ```

3. **Try different browser impersonation:**
   - Edit `homeharvest/core/scrapers/__init__.py`
   - Change `DEFAULT_IMPERSONATE = "chrome110"` to `"chrome116"` or `"edge99"`
   - Commit and push, then redeploy

