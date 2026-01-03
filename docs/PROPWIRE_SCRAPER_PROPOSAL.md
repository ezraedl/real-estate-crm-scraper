# Propwire.com Scraper Implementation Proposal

## Overview

This document proposes the implementation of a Propwire.com scraper following the architecture pattern established by the existing Realtor.com scraper in HomeHarvestLocal.

## Current Realtor.com Scraper Architecture Review

### Structure

The Realtor scraper follows a modular architecture with three main components:

1. **`queries.py`**: GraphQL query definitions
   - Defines GraphQL fragments and queries
   - Handles search parameters and filters
   - Supports multiple search types (general_search, comps, area)

2. **`parsers.py`**: Data parsing and transformation
   - Converts raw API responses to structured data
   - Handles date parsing and type conversions
   - Extracts address, description, and property details

3. **`processors.py`**: Business logic and Property model creation
   - Processes parsed data into Property Pydantic models
   - Handles advertiser information (agents, brokers, offices)
   - Applies filters (MLS only, exclude pending, etc.)

### Key Features

- **GraphQL API Integration**: Uses Realtor.com's GraphQL endpoint
- **TLS Fingerprinting**: Uses `curl_cffi` with browser impersonation to bypass anti-bot measures
- **Rate Limiting**: Implements delays (3-8 seconds) between requests
- **Retry Logic**: Uses `tenacity` for exponential backoff retries
- **Parallel Processing**: ThreadPoolExecutor for concurrent property processing
- **Comprehensive Filtering**: Supports price, beds, baths, sqft, dates, property types, etc.
- **Pagination**: Handles multi-page results with parallel or sequential fetching

### Anti-Bot Measures

- Uses `curl_cffi` with rotating browser impersonation profiles
- Implements randomized delays between requests
- Uses realistic headers matching browser behavior
- Supports proxy rotation

## Propwire.com Scraper Proposal

### Challenges

Based on research, Propwire.com has implemented:
- **DataDome Protection**: AI-powered anti-scraping protection (since 2024)
- **Active Blocking**: Blocks scraping attempts that cause site outages
- **Terms of Service**: May restrict automated data collection

### Recommended Approach

#### Phase 1: Investigation & Analysis

1. **API Discovery**
   - Investigate if Propwire.com has a public API or GraphQL endpoint
   - Check browser network requests for API calls
   - Identify authentication requirements
   - Document request/response formats

2. **DataDome Bypass Strategy**
   - Test TLS fingerprinting with `curl_cffi` (similar to Realtor)
   - Implement browser fingerprint rotation
   - Test proxy rotation effectiveness
   - Consider using residential proxies

3. **Legal & Ethical Considerations**
   - Review Propwire.com's Terms of Service
   - Consider reaching out for API access or partnership
   - Ensure compliance with rate limits and usage policies

#### Phase 2: Implementation Structure

Following the Realtor scraper pattern, create:

```
homeharvest/core/scrapers/propwire/
├── __init__.py          # PropwireScraper class
├── queries.py           # API queries/requests (GraphQL or REST)
├── parsers.py           # Data parsing and transformation
├── processors.py        # Property model creation
└── introspection.json   # (Optional) API schema documentation
```

### Implementation Details

#### 1. Scraper Class (`__init__.py`)

```python
class PropwireScraper(Scraper):
    """
    Scraper for Propwire.com following Realtor scraper architecture.
    """
    API_URL = "https://api.propwire.com"  # To be determined
    NUM_PROPERTY_WORKERS = 20
    DEFAULT_PAGE_SIZE = 200
    
    def __init__(self, scraper_input):
        super().__init__(scraper_input)
        # Propwire-specific initialization
    
    def handle_location(self):
        """Resolve location to Propwire's location format"""
        pass
    
    def general_search(self, variables: dict, search_type: str):
        """Main search method"""
        pass
    
    def search(self):
        """Entry point for search operations"""
        pass
```

#### 2. Data Models

Reuse existing models from `models.py`:
- `Property`
- `Address`
- `Description`
- `Advertisers` (Agent, Broker, Office)
- `ListingType`
- `PropertyType`

May need Propwire-specific extensions if they have unique fields.

#### 3. Parsers (`parsers.py`)

Similar structure to Realtor parsers:
- `parse_address()`: Extract address components
- `parse_description()`: Extract property details (beds, baths, sqft, etc.)
- `parse_advertisers()`: Extract agent/broker information
- `parse_dates()`: Handle date parsing
- `parse_photos()`: Extract property photos

#### 4. Processors (`processors.py`)

- `process_property()`: Main property processing function
- `process_advertisers()`: Handle agent/broker data
- `process_extra_property_details()`: Fetch additional details if needed

#### 5. Queries (`queries.py`)

Depending on Propwire's API:
- If GraphQL: Define queries similar to Realtor
- If REST: Define endpoint URLs and request builders
- If HTML scraping: Define selectors and extraction logic

### Anti-Bot Strategy

1. **TLS Fingerprinting**
   ```python
   # Use curl_cffi with rotation (already in base Scraper)
   impersonate_profile = get_random_impersonate()
   session = requests.Session(impersonate=impersonate_profile)
   ```

2. **Rate Limiting**
   - Implement delays similar to Realtor (3-8 seconds)
   - Use exponential backoff for retries
   - Respect rate limits if documented

3. **Headers & User Agents**
   - Rotate user agents
   - Use realistic browser headers
   - Match Propwire's expected client format

4. **Proxy Support**
   - Support proxy rotation
   - Use residential proxies if available
   - Implement proxy health checks

### Integration Points

#### HomeHarvestLocal Integration

1. **Update `__init__.py`** to include Propwire:
   ```python
   from .propwire import PropwireScraper
   ```

2. **Update `SiteName` enum** in `models.py`:
   ```python
   class SiteName(Enum):
       ZILLOW = "zillow"
       REDFIN = "redfin"
       REALTOR = "realtor.com"
       PROPWIRE = "propwire.com"  # Add this
   ```

3. **Update main scraper factory** to support Propwire

#### Real-Estate-CRM-Scraper Integration

1. **Update `scraper.py`** to support Propwire source
2. **Add Propwire-specific error handling**
3. **Update database models** if needed for Propwire-specific fields
4. **Add Propwire to job configuration**

### Testing Strategy

1. **Unit Tests**
   - Test parsers with sample data
   - Test processors with mock responses
   - Test query builders

2. **Integration Tests**
   - Test full search flow
   - Test pagination
   - Test filtering
   - Test error handling

3. **Anti-Bot Testing**
   - Test with and without proxies
   - Test rate limiting behavior
   - Test retry logic
   - Monitor for 403/blocking errors

### Risk Mitigation

1. **Legal Risks**
   - Review Terms of Service
   - Consider official API access
   - Implement respectful rate limiting

2. **Technical Risks**
   - DataDome may block even with TLS fingerprinting
   - API structure may change
   - May require CAPTCHA solving (consider services like 2captcha)

3. **Maintenance Risks**
   - Propwire may change their protection mechanisms
   - Need to monitor for breaking changes
   - Keep scraper updated with latest anti-bot techniques

### Next Steps

1. **Immediate Actions**
   - [ ] Investigate Propwire.com's API/endpoints
   - [ ] Test DataDome bypass strategies
   - [ ] Review Terms of Service
   - [ ] Create proof-of-concept scraper

2. **Development Tasks**
   - [ ] Create propwire scraper directory structure
   - [ ] Implement location handling
   - [ ] Implement search functionality
   - [ ] Implement parsers
   - [ ] Implement processors
   - [ ] Add error handling and retries
   - [ ] Add tests

3. **Integration Tasks**
   - [ ] Integrate with HomeHarvestLocal
   - [ ] Integrate with real-estate-crm-scraper
   - [ ] Update documentation
   - [ ] Add monitoring/logging

### Alternative Approaches

If direct scraping proves too difficult:

1. **Official API**: Contact Propwire for API access
2. **Data Partnership**: Explore data licensing options
3. **Third-Party Services**: Use services that provide Propwire data
4. **Browser Automation**: Use Playwright/Selenium with stealth plugins (slower but more reliable)

### References

- Realtor scraper implementation: `HomeHarvestLocal/homeharvest/core/scrapers/realtor/`
- DataDome protection: https://datadome.co/customers-stories/propwire-secures-its-real-estate-platform-from-scraping-attacks-with-datadome/
- Existing scraper: https://github.com/pim97/propwire.com-scraper (reference only, may violate ToS)

---

**Note**: This proposal is based on the Realtor scraper architecture. Actual implementation may vary based on Propwire.com's actual API structure and requirements discovered during investigation.





