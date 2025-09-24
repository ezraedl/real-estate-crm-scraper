# MLS Scraper Integration Guide

This guide provides step-by-step instructions for integrating external applications with the MLS Scraper system.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Database Integration](#database-integration)
3. [API Integration](#api-integration)
4. [Authentication Setup](#authentication-setup)
5. [Error Handling](#error-handling)
6. [Performance Optimization](#performance-optimization)
7. [Testing](#testing)
8. [Deployment](#deployment)

## Prerequisites

### System Requirements
- MongoDB 4.4+ (local or Atlas)
- Python 3.8+ (for server)
- Node.js 14+ (for client applications)
- Java 11+ (for Java applications)

### Dependencies
- Database driver for your programming language
- HTTP client library
- JSON parsing library

## Database Integration

### 1. Connection Setup

#### Python (pymongo)
```python
from pymongo import MongoClient
from datetime import datetime
import os

# Environment variables
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
DATABASE_NAME = 'mls_scraper'

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

# Test connection
try:
    client.admin.command('ping')
    print("Connected to MongoDB successfully")
except Exception as e:
    print(f"Failed to connect to MongoDB: {e}")
```

#### Node.js (mongodb)
```javascript
const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/';
const DATABASE_NAME = 'mls_scraper';

async function connectToDatabase() {
    const client = new MongoClient(MONGO_URI);
    try {
        await client.connect();
        console.log('Connected to MongoDB successfully');
        return client.db(DATABASE_NAME);
    } catch (error) {
        console.error('Failed to connect to MongoDB:', error);
        throw error;
    }
}
```

#### Java (MongoDB Driver)
```java
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

public class DatabaseConnection {
    private static final String MONGO_URI = System.getenv("MONGO_URI") != null ? 
        System.getenv("MONGO_URI") : "mongodb://localhost:27017/";
    private static final String DATABASE_NAME = "mls_scraper";
    
    public static MongoDatabase connect() {
        MongoClient client = MongoClients.create(MONGO_URI);
        MongoDatabase database = client.getDatabase(DATABASE_NAME);
        
        // Test connection
        try {
            database.runCommand(new Document("ping", 1));
            System.out.println("Connected to MongoDB successfully");
        } catch (Exception e) {
            System.err.println("Failed to connect to MongoDB: " + e.getMessage());
            throw e;
        }
        
        return database;
    }
}
```

### 2. Data Access Patterns

#### Get Properties by Status
```python
def get_properties_by_status(db, status):
    """Get properties by status"""
    return list(db.properties.find({"status": status}))

# Usage
for_sale_properties = get_properties_by_status(db, "FOR_SALE")
pending_properties = get_properties_by_status(db, "PENDING")
```

#### Get Properties by Location
```python
def get_properties_by_location(db, location):
    """Get properties by location"""
    return list(db.properties.find({
        "location": {"$regex": location, "$options": "i"}
    }))

# Usage
indianapolis_properties = get_properties_by_location(db, "Indianapolis")
```

#### Get Properties by Price Range
```python
def get_properties_by_price_range(db, min_price, max_price):
    """Get properties by price range"""
    return list(db.properties.find({
        "price": {"$gte": min_price, "$lte": max_price}
    }))

# Usage
affordable_properties = get_properties_by_price_range(db, 200000, 400000)
```

### 3. Aggregation Queries

#### Property Statistics
```python
def get_property_statistics(db):
    """Get property statistics"""
    pipeline = [
        {
            "$group": {
                "_id": "$status",
                "count": {"$sum": 1},
                "avg_price": {"$avg": "$price"},
                "min_price": {"$min": "$price"},
                "max_price": {"$max": "$price"}
            }
        }
    ]
    return list(db.properties.aggregate(pipeline))
```

#### Location-based Statistics
```python
def get_location_statistics(db):
    """Get statistics by location"""
    pipeline = [
        {
            "$group": {
                "_id": "$location",
                "count": {"$sum": 1},
                "avg_price": {"$avg": "$price"}
            }
        },
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    return list(db.properties.aggregate(pipeline))
```

## API Integration

### 1. HTTP Client Setup

#### Python (requests)
```python
import requests
import json
from typing import Dict, Any

class MLSAPIClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def trigger_job(self, job_id: str, priority: str = "immediate") -> Dict[str, Any]:
        """Trigger an existing job"""
        response = self.session.post(
            f"{self.base_url}/scrape/trigger",
            json={"job_id": job_id, "priority": priority}
        )
        response.raise_for_status()
        return response.json()
    
    def create_immediate_job(self, locations: list, listing_type: str, **kwargs) -> Dict[str, Any]:
        """Create an immediate job"""
        data = {
            "locations": locations,
            "listing_type": listing_type,
            **kwargs
        }
        response = self.session.post(
            f"{self.base_url}/scrape/immediate",
            json=data
        )
        response.raise_for_status()
        return response.json()
    
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get job status"""
        response = self.session.get(f"{self.base_url}/jobs/{job_id}")
        response.raise_for_status()
        return response.json()
    
    def health_check(self) -> Dict[str, Any]:
        """Check server health"""
        response = self.session.get(f"{self.base_url}/health")
        response.raise_for_status()
        return response.json()

# Usage
client = MLSAPIClient()
health = client.health_check()
print(f"Server status: {health['status']}")
```

#### Node.js (axios)
```javascript
const axios = require('axios');

class MLSAPIClient {
    constructor(baseUrl = 'http://localhost:8000') {
        this.baseUrl = baseUrl;
        this.client = axios.create({
            baseURL: baseUrl,
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });
    }
    
    async triggerJob(jobId, priority = 'immediate') {
        const response = await this.client.post('/scrape/trigger', {
            job_id: jobId,
            priority: priority
        });
        return response.data;
    }
    
    async createImmediateJob(locations, listingType, options = {}) {
        const data = {
            locations,
            listing_type: listingType,
            ...options
        };
        const response = await this.client.post('/scrape/immediate', data);
        return response.data;
    }
    
    async getJobStatus(jobId) {
        const response = await this.client.get(`/jobs/${jobId}`);
        return response.data;
    }
    
    async healthCheck() {
        const response = await this.client.get('/health');
        return response.data;
    }
}

// Usage
const client = new MLSAPIClient();
client.healthCheck().then(health => {
    console.log(`Server status: ${health.status}`);
});
```

### 2. Job Management

#### Create and Monitor Jobs
```python
import time
from typing import Optional

def create_and_monitor_job(client: MLSAPIClient, locations: list, listing_type: str) -> Optional[dict]:
    """Create a job and monitor its progress"""
    
    # Create immediate job
    job_response = client.create_immediate_job(
        locations=locations,
        listing_type=listing_type,
        limit=1000
    )
    
    job_id = job_response['job_id']
    print(f"Created job: {job_id}")
    
    # Monitor job progress
    while True:
        status = client.get_job_status(job_id)
        print(f"Job status: {status['status']}")
        
        if status['status'] == 'completed':
            print(f"Job completed! Scraped {status['progress']['properties_scraped']} properties")
            return status
        elif status['status'] == 'failed':
            print(f"Job failed: {status.get('error_message', 'Unknown error')}")
            return None
        
        time.sleep(10)  # Check every 10 seconds

# Usage
client = MLSAPIClient()
result = create_and_monitor_job(
    client=client,
    locations=["Indianapolis, IN"],
    listing_type="for_sale"
)
```

### 3. Batch Operations

#### Process Multiple Locations
```python
def process_multiple_locations(client: MLSAPIClient, locations: list, listing_type: str):
    """Process multiple locations in parallel"""
    import concurrent.futures
    
    def process_location(location):
        try:
            job_response = client.create_immediate_job(
                locations=[location],
                listing_type=listing_type,
                limit=500
            )
            return {
                'location': location,
                'job_id': job_response['job_id'],
                'status': 'created'
            }
        except Exception as e:
            return {
                'location': location,
                'error': str(e),
                'status': 'failed'
            }
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_location, loc) for loc in locations]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    
    return results

# Usage
locations = ["Indianapolis, IN", "Carmel, IN", "Fishers, IN"]
results = process_multiple_locations(client, locations, "for_sale")
for result in results:
    print(f"{result['location']}: {result['status']}")
```

## Authentication Setup

### 1. API Key Authentication (Recommended)

#### Server-side Implementation
```python
from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer()

def verify_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify API key"""
    if credentials.credentials != "your-secret-api-key":
        raise HTTPException(status_code=401, detail="Invalid API key")
    return credentials.credentials

# Apply to endpoints
@app.post("/scrape/trigger")
async def trigger_job(request: TriggerJobRequest, api_key: str = Depends(verify_api_key)):
    # Your endpoint logic here
    pass
```

#### Client-side Implementation
```python
class AuthenticatedMLSAPIClient(MLSAPIClient):
    def __init__(self, base_url: str, api_key: str):
        super().__init__(base_url)
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}'
        })

# Usage
client = AuthenticatedMLSAPIClient(
    base_url="http://localhost:8000",
    api_key="your-secret-api-key"
)
```

### 2. Database Authentication

#### MongoDB with Authentication
```python
from pymongo import MongoClient
from urllib.parse import quote_plus

def connect_with_auth(username: str, password: str, host: str, database: str):
    """Connect to MongoDB with authentication"""
    username = quote_plus(username)
    password = quote_plus(password)
    
    uri = f"mongodb://{username}:{password}@{host}/{database}"
    client = MongoClient(uri)
    
    # Test connection
    try:
        client.admin.command('ping')
        return client[database]
    except Exception as e:
        print(f"Authentication failed: {e}")
        raise

# Usage
db = connect_with_auth(
    username="your-username",
    password="your-password",
    host="localhost:27017",
    database="mls_scraper"
)
```

## Error Handling

### 1. API Error Handling

#### Python
```python
import requests
from requests.exceptions import RequestException

def safe_api_call(client: MLSAPIClient, operation, *args, **kwargs):
    """Safely call API with error handling"""
    try:
        return operation(*args, **kwargs)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print("Resource not found")
        elif e.response.status_code == 422:
            print("Validation error:", e.response.json())
        elif e.response.status_code >= 500:
            print("Server error:", e.response.text)
        else:
            print(f"HTTP error {e.response.status_code}: {e.response.text}")
        return None
    except RequestException as e:
        print(f"Request failed: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

# Usage
result = safe_api_call(client, client.trigger_job, "job_id_123")
```

#### Node.js
```javascript
async function safeApiCall(operation, ...args) {
    try {
        return await operation(...args);
    } catch (error) {
        if (error.response) {
            const status = error.response.status;
            const data = error.response.data;
            
            switch (status) {
                case 404:
                    console.log('Resource not found');
                    break;
                case 422:
                    console.log('Validation error:', data);
                    break;
                case 500:
                    console.log('Server error:', data);
                    break;
                default:
                    console.log(`HTTP error ${status}:`, data);
            }
        } else {
            console.log('Request failed:', error.message);
        }
        return null;
    }
}

// Usage
const result = await safeApiCall(client.triggerJob, 'job_id_123');
```

### 2. Database Error Handling

#### Python
```python
from pymongo.errors import ConnectionFailure, OperationFailure

def safe_db_operation(operation, *args, **kwargs):
    """Safely execute database operation"""
    try:
        return operation(*args, **kwargs)
    except ConnectionFailure as e:
        print(f"Database connection failed: {e}")
        return None
    except OperationFailure as e:
        print(f"Database operation failed: {e}")
        return None
    except Exception as e:
        print(f"Unexpected database error: {e}")
        return None

# Usage
properties = safe_db_operation(db.properties.find, {"status": "FOR_SALE"})
```

## Performance Optimization

### 1. Database Optimization

#### Indexing
```python
def create_indexes(db):
    """Create database indexes for better performance"""
    # Properties collection indexes
    db.properties.create_index("status")
    db.properties.create_index("location")
    db.properties.create_index("price")
    db.properties.create_index("created_at")
    db.properties.create_index([("status", 1), ("location", 1)])
    db.properties.create_index([("price", 1), ("status", 1)])
    
    # Jobs collection indexes
    db.jobs.create_index("job_id", unique=True)
    db.jobs.create_index("status")
    db.jobs.create_index("created_at")
    db.jobs.create_index("listing_type")

# Run once during setup
create_indexes(db)
```

#### Connection Pooling
```python
from pymongo import MongoClient
from pymongo.pool import Pool

def create_optimized_client():
    """Create optimized MongoDB client"""
    client = MongoClient(
        "mongodb://localhost:27017/",
        maxPoolSize=50,
        minPoolSize=10,
        maxIdleTimeMS=30000,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=10000,
        socketTimeoutMS=20000
    )
    return client
```

### 2. API Optimization

#### Caching
```python
import redis
import json
from functools import wraps

# Redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def cache_result(expiration=300):
    """Cache API results"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = f"{func.__name__}:{hash(str(args) + str(kwargs))}"
            
            # Try to get from cache
            cached = redis_client.get(cache_key)
            if cached:
                return json.loads(cached)
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            redis_client.setex(cache_key, expiration, json.dumps(result))
            return result
        return wrapper
    return decorator

# Usage
@cache_result(expiration=600)
def get_property_statistics(db):
    """Get cached property statistics"""
    # Your expensive operation here
    pass
```

#### Batch Processing
```python
def batch_process_properties(db, batch_size=1000):
    """Process properties in batches"""
    cursor = db.properties.find({"status": "FOR_SALE"})
    
    batch = []
    for property_doc in cursor:
        batch.append(property_doc)
        
        if len(batch) >= batch_size:
            # Process batch
            process_batch(batch)
            batch = []
    
    # Process remaining items
    if batch:
        process_batch(batch)

def process_batch(batch):
    """Process a batch of properties"""
    # Your batch processing logic here
    print(f"Processing batch of {len(batch)} properties")
```

## Testing

### 1. Unit Tests

#### Python (pytest)
```python
import pytest
from unittest.mock import Mock, patch

class TestMLSAPIClient:
    def setup_method(self):
        self.client = MLSAPIClient("http://test-server")
    
    @patch('requests.Session.post')
    def test_trigger_job_success(self, mock_post):
        """Test successful job triggering"""
        mock_response = Mock()
        mock_response.json.return_value = {"job_id": "test_job", "status": "pending"}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response
        
        result = self.client.trigger_job("test_job")
        
        assert result["job_id"] == "test_job"
        assert result["status"] == "pending"
        mock_post.assert_called_once()
    
    @patch('requests.Session.post')
    def test_trigger_job_failure(self, mock_post):
        """Test job triggering failure"""
        mock_post.side_effect = requests.exceptions.HTTPError("404 Not Found")
        
        with pytest.raises(requests.exceptions.HTTPError):
            self.client.trigger_job("nonexistent_job")
```

### 2. Integration Tests

#### Database Integration Test
```python
def test_database_connection():
    """Test database connection"""
    client = MongoClient("mongodb://localhost:27017/")
    db = client["mls_scraper_test"]
    
    # Test connection
    client.admin.command('ping')
    
    # Test basic operations
    test_doc = {"test": "data", "timestamp": datetime.utcnow()}
    result = db.test_collection.insert_one(test_doc)
    assert result.inserted_id is not None
    
    # Cleanup
    db.test_collection.delete_one({"_id": result.inserted_id})
    client.close()
```

### 3. Load Testing

#### Python (locust)
```python
from locust import HttpUser, task, between

class MLSAPILoadTest(HttpUser):
    wait_time = between(1, 3)
    
    def on_start(self):
        """Setup for each user"""
        self.client.headers.update({
            'Content-Type': 'application/json'
        })
    
    @task(3)
    def health_check(self):
        """Test health check endpoint"""
        self.client.get("/health")
    
    @task(2)
    def get_job_status(self):
        """Test job status endpoint"""
        self.client.get("/jobs/test_job_id")
    
    @task(1)
    def trigger_job(self):
        """Test job triggering"""
        self.client.post("/scrape/trigger", json={
            "job_id": "test_job_id"
        })
```

## Deployment

### 1. Environment Configuration

#### Environment Variables
```bash
# Database
MONGO_URI=mongodb://localhost:27017/
DATABASE_NAME=mls_scraper

# API
API_BASE_URL=http://localhost:8000
API_KEY=your-secret-api-key

# Redis (for caching)
REDIS_URL=redis://localhost:6379/0

# Logging
LOG_LEVEL=INFO
LOG_FILE=mls_scraper.log
```

#### Configuration File
```python
import os
from dataclasses import dataclass

@dataclass
class Config:
    mongo_uri: str = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
    database_name: str = os.getenv('DATABASE_NAME', 'mls_scraper')
    api_base_url: str = os.getenv('API_BASE_URL', 'http://localhost:8000')
    api_key: str = os.getenv('API_KEY', '')
    redis_url: str = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')
    log_file: str = os.getenv('LOG_FILE', 'mls_scraper.log')

config = Config()
```

### 2. Docker Deployment

#### Dockerfile
```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["python", "start_server.py"]
```

#### Docker Compose
```yaml
version: '3.8'

services:
  mls-scraper:
    build: .
    ports:
      - "8000:8000"
    environment:
      - MONGO_URI=mongodb://mongo:27017/
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - mongo
      - redis
  
  mongo:
    image: mongo:4.4
    ports:
      - "27017:27017"
    volumes:
      - mongo_data:/data/db
  
  redis:
    image: redis:6-alpine
    ports:
      - "6379:6379"

volumes:
  mongo_data:
```

### 3. Production Considerations

#### Security
- Implement proper authentication
- Use HTTPS in production
- Validate all input data
- Implement rate limiting
- Use environment variables for secrets

#### Monitoring
- Set up logging
- Implement health checks
- Monitor database performance
- Track API usage
- Set up alerts

#### Scaling
- Use connection pooling
- Implement caching
- Consider horizontal scaling
- Monitor resource usage
- Optimize database queries

## Support

For additional support or questions:
- Check the [API Reference](./API_REFERENCE.md)
- Review the [Database Schema](./DATABASE_SCHEMA.md)
- Contact the development team
- Submit issues to the project repository
