# Propwire Scraper Review Summary

## Realtor.com Scraper Architecture Review

### Key Findings

#### 1. **Modular Architecture**
The Realtor scraper uses a clean separation of concerns:
- **Queries** (`queries.py`): GraphQL query definitions and fragments
- **Parsers** (`parsers.py`): Raw data transformation and date parsing
- **Processors** (`processors.py`): Business logic and Property model creation

#### 2. **GraphQL API Integration**
- Uses Realtor.com's GraphQL endpoint: `https://www.realtor.com/frontdoor/graphql`
- Implements comprehensive query fragments for property data
- Supports multiple search types: general search, comps (radius), area search
- Handles pagination with configurable page sizes (default: 200)

#### 3. **Anti-Bot Measures**
- **TLS Fingerprinting**: Uses `curl_cffi` with browser impersonation
  - Rotates through multiple profiles: `chrome120`, `chrome116`, `safari15_3`, etc.
  - Creates unique sessions per proxy instance
- **Rate Limiting**: Random delays of 3-8 seconds between requests
- **Retry Logic**: Uses `tenacity` library with exponential backoff
- **Realistic Headers**: Matches browser behavior with proper User-Agent and headers

#### 4. **Data Processing Pipeline**
```
API Request → GraphQL Query → Raw Response → Parser → Processor → Property Model
```

#### 5. **Key Features**
- **Comprehensive Filtering**: Price, beds, baths, sqft, lot size, year built, property type
- **Date Filtering**: Supports list_date, sold_date, pending_date with hour precision
- **Status Handling**: FOR_SALE, FOR_RENT, SOLD, PENDING, OFF_MARKET
- **Parallel Processing**: ThreadPoolExecutor for concurrent property processing
- **Pagination**: Sequential or parallel page fetching
- **Error Handling**: Graceful degradation with retries

#### 6. **Property Data Model**
Extensive Property model includes:
- Basic info: address, price, status, dates
- Description: beds, baths, sqft, lot size, year built
- Location: coordinates, county, FIPS code, neighborhoods
- Financial: list price, sold price, estimates, tax history
- Advertisers: agent, broker, office, builder
- Additional: photos, open houses, HOA fees, pet policy

## Recommendations for Propwire Scraper

### 1. **Follow the Same Architecture Pattern**
Create a similar structure:
```
homeharvest/core/scrapers/propwire/
├── __init__.py          # PropwireScraper class
├── queries.py           # API queries/requests
├── parsers.py           # Data parsing
└── processors.py        # Property model creation
```

### 2. **Investigation Priorities**

#### A. API Discovery
1. **Check for Public API**
   - Inspect browser network requests on Propwire.com
   - Look for GraphQL or REST endpoints
   - Identify authentication mechanisms

2. **Data Structure Analysis**
   - Document response formats
   - Identify required vs optional fields
   - Map Propwire fields to Property model

#### B. Anti-Bot Strategy
1. **Test TLS Fingerprinting**
   - Use same `curl_cffi` approach as Realtor
   - Test different impersonation profiles
   - Monitor for 403/blocking responses

2. **Proxy Strategy**
   - Test with residential proxies
   - Implement proxy rotation
   - Add proxy health checks

3. **Rate Limiting**
   - Start with conservative delays (5-10 seconds)
   - Monitor for blocking patterns
   - Adjust based on success rate

### 3. **Implementation Phases**

#### Phase 1: Proof of Concept (1-2 weeks)
- [ ] Investigate Propwire API/endpoints
- [ ] Test basic data extraction
- [ ] Verify anti-bot bypass effectiveness
- [ ] Create minimal working scraper

#### Phase 2: Core Implementation (2-3 weeks)
- [ ] Implement location handling
- [ ] Implement search functionality
- [ ] Create parsers for Propwire data format
- [ ] Create processors for Property models
- [ ] Add error handling and retries

#### Phase 3: Integration (1 week)
- [ ] Integrate with HomeHarvestLocal
- [ ] Integrate with real-estate-crm-scraper
- [ ] Add tests
- [ ] Update documentation

#### Phase 4: Optimization (Ongoing)
- [ ] Monitor success rates
- [ ] Adjust rate limiting
- [ ] Optimize parsing performance
- [ ] Handle edge cases

### 4. **Key Differences to Consider**

#### Propwire-Specific Challenges
1. **DataDome Protection**: More aggressive than Realtor's protection
2. **API Structure**: May use REST instead of GraphQL
3. **Data Format**: Field names and structure may differ
4. **Authentication**: May require API keys or session tokens

#### Adaptations Needed
1. **Query Structure**: Adapt to Propwire's API format
2. **Parser Logic**: Map Propwire fields to Property model
3. **Error Handling**: Handle DataDome-specific errors
4. **Rate Limiting**: May need more conservative limits

### 5. **Code Reuse Opportunities**

#### Can Reuse:
- Base `Scraper` class structure
- `Property` model (may need extensions)
- TLS fingerprinting setup (`curl_cffi` integration)
- Retry logic patterns
- Parallel processing approach
- Date parsing utilities

#### Need to Customize:
- API endpoint URLs
- Query/request builders
- Field mapping (Propwire → Property model)
- Error handling for DataDome
- Rate limiting parameters

### 6. **Testing Strategy**

1. **Unit Tests**
   - Test parsers with sample Propwire data
   - Test processors with mock responses
   - Test query builders

2. **Integration Tests**
   - Test full search flow
   - Test with different locations
   - Test filtering and pagination
   - Test error scenarios

3. **Anti-Bot Tests**
   - Test with/without proxies
   - Test rate limiting
   - Monitor blocking rates
   - Test retry logic

### 7. **Risk Assessment**

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| DataDome blocking | High | High | Use TLS fingerprinting, proxies, conservative rate limiting |
| API changes | Medium | High | Implement robust error handling, monitor for changes |
| Legal issues | Low | High | Review ToS, implement rate limits, consider API access |
| Maintenance burden | Medium | Medium | Document well, create tests, monitor for changes |

### 8. **Success Criteria**

1. **Functional**
   - Successfully extract property listings
   - Handle all listing types (for sale, for rent, sold)
   - Support filtering and pagination
   - Process 100+ properties without blocking

2. **Performance**
   - Process properties at reasonable speed
   - Handle errors gracefully
   - Maintain <5% error rate

3. **Reliability**
   - Work consistently over time
   - Handle API changes gracefully
   - Provide clear error messages

## Next Steps

1. **Immediate**: Investigate Propwire.com's API structure
2. **Short-term**: Create proof-of-concept scraper
3. **Medium-term**: Full implementation following Realtor pattern
4. **Long-term**: Monitor and maintain, optimize as needed

---

**Branch**: `feature/propwire-scraper`  
**Created**: Based on review of Realtor scraper architecture  
**Status**: Proposal phase - ready for investigation and implementation

