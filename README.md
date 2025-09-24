# MLS Scraping Server

A high-performance, production-ready MLS data scraping server built with FastAPI, MongoDB, and HomeHarvest. This server provides immediate and scheduled scraping capabilities with advanced anti-bot measures and proxy support.

## Features

### 🚀 Core Functionality
- **Immediate Scraping**: High-priority on-demand property data updates
- **Scheduled Jobs**: One-time and recurring scraping jobs with cron expressions
- **MongoDB Storage**: Efficient property data storage with proper indexing
- **Job Queue System**: MongoDB-based job management with status tracking

### 🛡️ Anti-Bot Measures
- **DataImpulse Proxy Integration**: Rotating proxy support for better scraping success
- **Random User Agents**: Multiple browser user agent rotation
- **Request Delays**: Configurable delays between requests
- **Header Randomization**: Dynamic HTTP headers to avoid detection

### 📊 Monitoring & Management
- **Real-time Job Status**: Track scraping progress and completion
- **Statistics Dashboard**: Server and scraping performance metrics
- **Proxy Management**: Monitor and manage proxy performance
- **Error Handling**: Comprehensive error tracking and recovery

## Quick Start

### Prerequisites
- Python 3.9+
- MongoDB 4.4+
- DataImpulse API key (optional, for proxy support)

### Test Structure
- **`tests/`** - Permanent CI/CD tests that must work every time
- **`temp_tests/`** - Temporary development/debug tests (can be deleted after use)

Run permanent tests: `python tests/run_tests.py`

### Installation

1. **Clone and setup**:
```bash
git clone <your-repo>
cd mls-scraping-server
pip install -r requirements.txt
```

2. **Configure environment**:
```bash
cp env_example.txt .env
# Edit .env with your MongoDB and proxy settings
```

3. **Start MongoDB**:
```bash
# Make sure MongoDB is running on localhost:27017
# Or update MONGODB_URL in .env
```

4. **Run the server**:
```bash
python main.py
```

The server will start on `http://localhost:8000`

### Test the Installation

```bash
python test_server.py
```

## API Endpoints

### Immediate Scraping
```http
POST /scrape/immediate
Content-Type: application/json

{
  "locations": ["92104", "San Diego, CA"],
  "listing_type": "for_sale",
  "property_types": ["single_family", "condos"],
  "limit": 100
}
```

### Scheduled Scraping
```http
POST /scrape/scheduled
Content-Type: application/json

{
  "locations": ["San Diego, CA"],
  "listing_type": "for_rent",
  "past_days": 30,
  "scheduled_at": "2024-01-15T10:00:00Z",
  "priority": "normal"
}
```

### Recurring Jobs
```http
POST /scrape/scheduled
Content-Type: application/json

{
  "locations": ["92104"],
  "listing_type": "sold",
  "cron_expression": "0 */6 * * *",
  "priority": "low"
}
```

### Job Management
```http
GET /jobs/{job_id}          # Get job status
GET /jobs                   # List all jobs
POST /jobs/{job_id}/cancel  # Cancel a job
```

### Statistics
```http
GET /health                 # Health check
GET /stats                  # Server statistics
GET /proxies/stats          # Proxy statistics
```

## Configuration

### Environment Variables

```bash
# MongoDB Configuration
MONGODB_URL=mongodb://localhost:27017
DATABASE_NAME=mls_scraper

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000

# Scraping Configuration
MAX_CONCURRENT_JOBS=5
REQUEST_DELAY=1.0
MAX_RETRIES=3

# Rate Limiting
RATE_LIMIT_PER_MINUTE=60

# DataImpulse Proxy (optional)
DATAIMPULSE_API_KEY=your_api_key_here
```

### Supported Parameters

#### Location Formats
- ZIP codes: `"92104"`
- Cities: `"San Diego"`
- City, State: `"San Diego, CA"`
- Full addresses: `"1234 Main St, San Diego, CA 92104"`
- Radius searches: Include `radius` parameter

#### Listing Types
- `for_sale` - Properties currently for sale
- `for_rent` - Properties currently for rent
- `sold` - Recently sold properties
- `pending` - Pending/contingent sales

#### Property Types
- `single_family`
- `multi_family`
- `condos`
- `townhomes`
- `duplex_triplex`
- `farm`
- `land`
- `mobile`

## Data Schema

### Property Model
```json
{
  "property_id": "unique_identifier",
  "mls_id": "MLS123456",
  "address": {
    "street": "123 Main St",
    "city": "San Diego",
    "state": "CA",
    "zip_code": "92104",
    "formatted_address": "123 Main St, San Diego, CA 92104"
  },
  "description": {
    "beds": 3,
    "full_baths": 2,
    "half_baths": 1,
    "sqft": 1500,
    "year_built": 2020,
    "property_type": "single_family"
  },
  "financial": {
    "list_price": 750000,
    "sold_price": 720000,
    "price_per_sqft": 480
  },
  "dates": {
    "list_date": "2024-01-01T00:00:00Z",
    "last_sold_date": "2024-01-15T00:00:00Z"
  },
  "location": {
    "latitude": 32.7157,
    "longitude": -117.1611,
    "neighborhoods": ["Downtown"]
  }
}
```

## Advanced Usage

### Custom Proxy Configuration
```python
# Add custom proxies
from proxy_manager import proxy_manager

proxy_manager.add_proxy("proxy1.example.com", 8080, "username", "password")
proxy_manager.add_proxy("proxy2.example.com", 8080, "username", "password")
```

### DataImpulse Integration
```python
# Initialize DataImpulse proxies
await proxy_manager.initialize_dataimpulse_proxies("your_api_key")
```

### Custom Scraping Jobs
```python
from models import ScrapingJob, JobPriority, ListingType

job = ScrapingJob(
    job_id="custom_job_123",
    priority=JobPriority.HIGH,
    locations=["San Diego, CA", "Los Angeles, CA"],
    listing_type=ListingType.FOR_SALE,
    property_types=[PropertyType.SINGLE_FAMILY],
    past_days=30,
    limit=5000
)
```

## Monitoring & Maintenance

### Health Checks
The server provides comprehensive health monitoring:
- Database connectivity
- Scraper service status
- Proxy availability
- Job queue status

### Performance Tuning
- Adjust `MAX_CONCURRENT_JOBS` based on your server capacity
- Tune `REQUEST_DELAY` to balance speed vs. detection avoidance
- Monitor proxy success rates and rotate as needed

### Error Handling
- Automatic retry logic for failed requests
- Proxy rotation on failures
- Job status tracking with error messages
- Comprehensive logging

## Production Deployment

### Docker Support
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8000

CMD ["python", "main.py"]
```

### Environment Setup
1. Use production MongoDB instance
2. Configure proper proxy rotation
3. Set up monitoring and alerting
4. Implement log aggregation
5. Configure backup strategies

## Troubleshooting

### Common Issues

1. **MongoDB Connection Failed**
   - Check MongoDB is running
   - Verify connection string in `.env`

2. **Scraping Failures**
   - Check proxy configuration
   - Verify rate limiting settings
   - Monitor for IP blocks

3. **Job Queue Issues**
   - Check job status endpoints
   - Verify MongoDB indexes
   - Monitor server resources

### Debug Mode
```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
python main.py
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details

## Support

For issues and questions:
- Create an issue in the repository
- Check the troubleshooting section
- Review the API documentation

---

**Note**: This server is designed for legitimate real estate data collection. Please ensure compliance with all applicable terms of service and legal requirements.
