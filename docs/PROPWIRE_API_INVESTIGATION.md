# Propwire.com API Investigation Report

## Investigation Date
January 31, 2025

## Summary

Initial investigation of Propwire.com's API structure has been completed. The site uses a REST API (not GraphQL) with DataDome anti-bot protection.

## Discovered API Endpoints

### ✅ Confirmed Endpoints

1. **Autocomplete API**
   - **URL**: `POST https://api.propwire.com/api/auto_complete`
   - **Purpose**: Location/address autocomplete
   - **Status**: Confirmed via network inspection
   - **When Called**: When user types in search box

2. **Session Management**
   - **URL**: `POST https://propwire.com/session-variable`
   - **Purpose**: Session variable management
   - **Status**: Confirmed via network inspection

3. **Search Page**
   - **URL**: `GET https://propwire.com/search?filters={JSON}`
   - **Purpose**: Main search interface
   - **Status**: Confirmed
   - **Notes**: Uses URL-encoded JSON for filters

### ⚠️ Endpoints to Discover

1. **Property Search API**
   - **Expected**: `POST/GET https://api.propwire.com/api/search` or similar
   - **Status**: Not yet discovered
   - **Notes**: Search page shows "0 Results" - may require:
     - Authentication/login
     - Different endpoint pattern
     - WebSocket/SSE for real-time updates
     - Client-side filtering only

2. **Property Details API**
   - **Expected**: `GET https://api.propwire.com/api/property/{id}` or similar
   - **Status**: Not yet discovered

3. **Filter Options API**
   - **Expected**: Endpoint for available filters
   - **Status**: Not yet discovered

## API Structure

### Base URL
```
https://api.propwire.com/api/
```

### API Type
- **Format**: REST API (confirmed)
- **Not GraphQL**: No GraphQL endpoints detected

### Authentication
- **Status**: Unknown
- **Possible Methods**:
  - Session-based (cookies)
  - API key in headers
  - OAuth/JWT tokens
  - Login required for property data

## Anti-Bot Protection

### DataDome
- **Status**: ✅ Active
- **Endpoint**: `POST https://api-js.datadome.co/js/`
- **Impact**: High - Will block automated requests
- **Mitigation Strategy**:
  1. TLS fingerprinting with `curl_cffi`
  2. Browser impersonation rotation
  3. Residential proxies
  4. Conservative rate limiting (5-10 seconds)
  5. Realistic request patterns

## Technology Stack

- **Frontend**: React SPA
- **Maps**: Mapbox
- **Analytics**: Amplitude, New Relic
- **Payment**: Stripe
- **Third-party**: Facebook Pixel, Google Tag Manager

## Next Steps

### Immediate Actions

1. **Test with Authentication**
   - Create test account
   - Login and perform search
   - Monitor network requests for property data API calls
   - Document authenticated request format

2. **Deep Network Inspection**
   - Wait for property results to load
   - Check for delayed API calls
   - Inspect WebSocket connections
   - Check for Server-Sent Events

3. **JavaScript Analysis**
   - Inspect bundled JavaScript for API endpoint definitions
   - Look for API client code
   - Find request/response type definitions

4. **Reference Implementation**
   - Review: https://github.com/pim97/propwire.com-scraper
   - Note: May violate ToS - use for reference only
   - Extract API endpoint patterns if documented

### Implementation Updates Needed

Once property search endpoint is discovered:

1. **Update `queries.py`**
   - Add actual search endpoint
   - Document request format
   - Document response format
   - Add query builders

2. **Update `parsers.py`**
   - Map actual Propwire field names
   - Update all parser functions
   - Test with real API responses

3. **Update `processors.py`**
   - Map actual field structure
   - Update Property model creation
   - Handle Propwire-specific data

4. **Update `__init__.py`**
   - Implement actual API calls
   - Add authentication handling
   - Update error handling for DataDome

## Challenges Identified

1. **DataDome Protection**
   - More aggressive than Realtor.com
   - Requires sophisticated bypass techniques
   - May need CAPTCHA solving service

2. **Authentication Requirements**
   - Property data may require login
   - Need to handle session management
   - May need to maintain cookies

3. **API Discovery**
   - Property search endpoint not yet found
   - May use non-standard patterns
   - Could be WebSocket-based

4. **Legal Considerations**
   - Review Terms of Service
   - Check robots.txt
   - Ensure compliance with rate limits

## Files Updated

1. ✅ `queries.py` - Added discovered endpoints structure
2. ✅ `API_DISCOVERY.md` - Investigation notes
3. ⏳ `parsers.py` - Awaiting API response format
4. ⏳ `processors.py` - Awaiting API response format
5. ⏳ `__init__.py` - Awaiting property search endpoint

## Status

**Current Phase**: API Discovery - Partial
**Progress**: ~30% complete
**Blockers**: Property search endpoint not yet discovered

## Recommendations

1. **Continue Investigation**
   - Test with authenticated session
   - Monitor network during actual property search
   - Inspect JavaScript bundles

2. **Alternative Approach**
   - Consider browser automation (Playwright/Selenium)
   - Use stealth plugins to bypass DataDome
   - Extract data from rendered HTML

3. **Official API**
   - Contact Propwire for API access
   - Explore partnership opportunities
   - Check for developer program

---

**Note**: This investigation is ongoing. The property search API endpoint is the critical missing piece needed to complete the scraper implementation.

