# Real Estate CRM - Scraper

Python-based MLS scraper service for the Real Estate CRM system.

## 🚀 Features

- **MLS Data Scraping** - Extract property data from multiple sources
- **FastAPI Web Service** - RESTful API for scraping operations
- **MongoDB Integration** - Store scraped property data
- **Scheduled Jobs** - Automated scraping with cron-like scheduling
- **Proxy Support** - Rotate proxies for large-scale scraping
- **Railway Deployment** - Production-ready deployment

## 🛠️ Tech Stack

- **Python 3.8+** - Programming language
- **FastAPI** - Web framework
- **MongoDB** - Database
- **Pymongo** - MongoDB driver
- **Requests** - HTTP client
- **BeautifulSoup** - HTML parsing
- **Puppeteer** - Browser automation (via Node.js)

## 📋 Prerequisites

- Python 3.8+
- MongoDB
- Node.js (for Puppeteer)

## 🔧 Installation

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install Node.js dependencies (if using Puppeteer)
npm install

# Copy environment file
cp env_example.txt .env

# Configure environment variables
# Edit .env with your settings
```

## 🚀 Development

```bash
# Start development server
python start_server.py

# Run scraper directly
python main.py

# Run tests
python -m pytest tests/

# Start scheduler
python start_scheduler.py
```

## 🔐 Environment Variables

Create a `.env` file with the following variables:

```env
# Database
MONGODB_URI=mongodb://localhost:27017/mlscraper

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000

# Scraping Configuration
MAX_CONCURRENT_JOBS=3
REQUEST_DELAY=1.0
MAX_RETRIES=3

# Rate Limiting
RATE_LIMIT_PER_MINUTE=60

# DataImpulse Configuration (Optional)
DATAIMPULSE_LOGIN=your-login
DATAIMPULSE_PASSWORD=your-password
DATAIMPULSE_ENDPOINT=your-endpoint

# Logging
LOG_LEVEL=INFO
```

## 📡 API Endpoints

### Health Check
- `GET /health` - Service health status

### Scraping Jobs
- `POST /scrape/location` - Start location-based scraping
- `GET /jobs/{job_id}` - Get job status
- `POST /jobs/{job_id}/trigger` - Trigger job execution
- `GET /jobs` - List all jobs

### Properties
- `GET /properties` - Get scraped properties
- `GET /properties/{property_id}` - Get specific property
- `DELETE /properties/{property_id}` - Delete property

## 🗄️ Database Schema

### Properties Collection
```json
{
  "_id": "ObjectId",
  "property_id": "string",
  "address": "string",
  "price": "number",
  "bedrooms": "number",
  "bathrooms": "number",
  "square_feet": "number",
  "lot_size": "number",
  "property_type": "string",
  "status": "string",
  "year_built": "number",
  "description": "string",
  "images": ["string"],
  "location": {
    "latitude": "number",
    "longitude": "number"
  },
  "scraped_at": "datetime",
  "source": "string"
}
```

### Jobs Collection
```json
{
  "_id": "ObjectId",
  "job_id": "string",
  "type": "string",
  "status": "string",
  "parameters": "object",
  "created_at": "datetime",
  "updated_at": "datetime",
  "completed_at": "datetime",
  "error": "string"
}
```

## 🚀 Deployment

### Railway

This scraper is configured for Railway deployment:

1. Connect your GitHub repository to Railway
2. Set environment variables in Railway dashboard
3. Deploy automatically on push

### Docker

```bash
# Build Docker image
docker build -t real-estate-crm-scraper .

# Run container
docker run -p 8000:8000 real-estate-crm-scraper
```

## 📝 Scripts

- `main.py` - Main FastAPI application
- `scraper.py` - Core scraping logic
- `database.py` - Database operations
- `scheduler.py` - Job scheduling
- `start_server.py` - Development server
- `start_scheduler.py` - Start job scheduler

## 🧪 Testing

```bash
# Run all tests
python -m pytest tests/

# Run specific test
python -m pytest tests/test_scraper_basic.py

# Run with coverage
python -m pytest --cov=. tests/
```

## 📊 Monitoring

- **Health Check**: `GET /health`
- **Job Status**: `GET /jobs`
- **Metrics**: Built-in FastAPI metrics
- **Logs**: Structured logging with configurable levels

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

MIT License

## 🔗 Related Repositories

- [Backend](https://github.com/ezraedl/real-estate-crm-backend) - API server
- [Frontend](https://github.com/ezraedl/real-estate-crm-frontend) - Web application
- [Chrome Extension](https://github.com/ezraedl/real-estate-crm-extension) - Browser extension