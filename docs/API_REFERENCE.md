# MLS Scraper API Reference

## Overview

The MLS Scraper API provides RESTful endpoints for managing scraping jobs and accessing property data. This document provides comprehensive information about all available endpoints.

## Base URL

```
http://localhost:8000
```

## Authentication

The API requires JWT authentication for all endpoints except `/health`.

### Authentication Header

Include the JWT token in the Authorization header:

```
Authorization: Bearer <your-jwt-token>
```

### Obtaining a JWT Token

JWT tokens are issued by the backend service after user authentication. The token must be signed with the same `JWT_SECRET` that the scraper service is configured with.

### Public Endpoints

- `GET /health` - Health check endpoint (no authentication required)

### Protected Endpoints

All other endpoints require a valid JWT token in the Authorization header. Requests without a valid token will receive a `401 Unauthorized` response.

## Endpoints

### 1. Health Check

#### GET /health

Check the health status of the server.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-09-12T10:30:00Z",
  "version": "1.2.0"
}
```

**Status Codes:**
- `200 OK` - Server is healthy
- `500 Internal Server Error` - Server is unhealthy

---

### 2. Get Job Status

#### GET /jobs/{job_id}

Get detailed information about a specific job.

**Parameters:**
- `job_id` (string, required) - The unique job identifier

**Response:**
```json
{
  "job_id": "indianapolis_complete_core_indianapolis_for_sale_1757661723",
  "status": "running",
  "progress": {
    "total_locations": 2,
    "completed_locations": 1,
    "properties_scraped": 150,
    "properties_saved": 145,
    "progress_percentage": 50.0
  },
  "created_at": "2025-09-12T10:22:03.718Z",
  "started_at": "2025-09-12T10:22:04.233Z",
  "completed_at": null,
  "error_message": null
}
```

**Status Codes:**
- `200 OK` - Job found
- `404 Not Found` - Job not found
- `500 Internal Server Error` - Server error

---

### 3. Trigger Existing Job

#### POST /scrape/trigger

Trigger an existing scheduled job to run immediately.

**Request Body:**
```json
{
  "job_id": "indianapolis_complete_core_indianapolis_for_sale_1757661723",
  "priority": "immediate"
}
```

**Parameters:**
- `job_id` (string, required) - The job ID to trigger
- `priority` (string, optional) - Priority level: "immediate", "high", "normal", "low" (default: "immediate")

**Response:**
```json
{
  "job_id": "immediate_1757661723_abc123",
  "status": "pending",
  "message": "Triggered existing job indianapolis_complete_core_indianapolis_for_sale_1757661723 to run immediately",
  "created_at": "2025-09-12T10:30:00Z"
}
```

**Status Codes:**
- `200 OK` - Job triggered successfully
- `404 Not Found` - Original job not found
- `422 Unprocessable Entity` - Invalid request data
- `500 Internal Server Error` - Server error

---

### 4. Create Immediate Job

#### POST /scrape/immediate

Create and run an immediate scraping job.

**Request Body:**
```json
{
  "locations": ["Indianapolis, IN", "Carmel, IN"],
  "listing_type": "for_sale",
  "property_types": ["Single Family"],
  "radius": 10.0,
  "mls_only": false,
  "foreclosure": false,
  "limit": 1000
}
```

**Parameters:**
- `locations` (array, required) - List of locations to scrape (max 10)
- `listing_type` (string, required) - Property type: "for_sale", "pending", "sold", "for_rent"
- `property_types` (array, optional) - Specific property types
- `radius` (number, optional) - Search radius in miles (0.1-50.0)
- `mls_only` (boolean, optional) - Include only MLS properties (default: false)
- `foreclosure` (boolean, optional) - Include foreclosure properties (default: false)
- `limit` (number, optional) - Maximum properties to scrape (default: 100, max: 1000)

**Response:**
```json
{
  "job_id": "immediate_1757661723_def456",
  "status": "pending",
  "message": "Immediate scraping job created",
  "created_at": "2025-09-12T10:30:00Z"
}
```

**Status Codes:**
- `200 OK` - Job created successfully
- `400 Bad Request` - Invalid request data
- `422 Unprocessable Entity` - Validation error
- `500 Internal Server Error` - Server error

---

### 5. Create Scheduled Job

#### POST /scrape/scheduled

Create a scheduled or recurring scraping job.

**Request Body:**
```json
{
  "locations": ["Indianapolis, IN", "Carmel, IN"],
  "listing_type": "for_sale",
  "property_types": ["Single Family"],
  "past_days": 30,
  "date_from": "2025-01-01",
  "date_to": "2025-12-31",
  "radius": 10.0,
  "mls_only": false,
  "foreclosure": false,
  "exclude_pending": false,
  "limit": 10000,
  "scheduled_at": "2025-09-13T13:00:00Z",
  "cron_expression": "0 13 * * *"
}
```

**Parameters:**
- `locations` (array, required) - List of locations to scrape
- `listing_type` (string, required) - Property type: "for_sale", "pending", "sold", "for_rent"
- `property_types` (array, optional) - Specific property types
- `past_days` (number, optional) - Number of days to look back (1-365)
- `date_from` (string, optional) - Start date (YYYY-MM-DD)
- `date_to` (string, optional) - End date (YYYY-MM-DD)
- `radius` (number, optional) - Search radius in miles (0.1-50.0)
- `mls_only` (boolean, optional) - Include only MLS properties (default: false)
- `foreclosure` (boolean, optional) - Include foreclosure properties (default: false)
- `exclude_pending` (boolean, optional) - Exclude pending properties (default: false)
- `limit` (number, optional) - Maximum properties to scrape (default: 10000, max: 10000)
- `scheduled_at` (string, optional) - When to run one-time job (ISO 8601)
- `cron_expression` (string, optional) - Cron expression for recurring jobs

**Response:**
```json
{
  "job_id": "scheduled_1757661723_ghi789",
  "status": "pending",
  "message": "Scheduled scraping job created",
  "created_at": "2025-09-12T10:30:00Z"
}
```

**Status Codes:**
- `200 OK` - Job created successfully
- `400 Bad Request` - Invalid request data
- `422 Unprocessable Entity` - Validation error
- `500 Internal Server Error` - Server error

---

### 6. Synchronous Immediate Scraping

#### POST /scrape/immediate/sync

Create and run an immediate scraping job that returns results synchronously.

**Request Body:**
```json
{
  "locations": ["Indianapolis, IN"],
  "listing_type": "for_sale",
  "limit": 100,
  "force_rescrape": false
}
```

**Request Parameters:**
- `locations` (required): Array of location strings to scrape
- `listing_type` (optional): Type of listing to scrape (`for_sale`, `for_rent`, `sold`, `pending`)
- `limit` (optional, default: 100): Maximum number of properties to scrape
- `force_rescrape` (optional, default: false): If `true`, skips database cache and performs fresh scrape from source. Use this when you need the latest data even if properties exist in the database.

**Response:**
```json
{
  "job_id": "sync_1757661723_jkl012",
  "status": "completed",
  "properties": [
    {
      "property_id": "prop_123",
      "address": "123 Main St",
      "city": "Indianapolis",
      "state": "IN",
      "price": 250000,
      "bedrooms": 3,
      "bathrooms": 2,
      "square_feet": 1500
    }
  ],
  "total_properties": 1,
  "execution_time": 5.2,
  "created_at": "2025-09-12T10:30:00Z"
}
```

**Status Codes:**
- `200 OK` - Scraping completed successfully
- `400 Bad Request` - Invalid request data
- `422 Unprocessable Entity` - Validation error
- `500 Internal Server Error` - Server error

## Error Responses

All endpoints may return error responses in the following format:

```json
{
  "detail": "Error message description"
}
```

### Common Error Codes

- `400 Bad Request` - Invalid request data
- `404 Not Found` - Resource not found
- `422 Unprocessable Entity` - Validation error
- `500 Internal Server Error` - Server error

## Rate Limiting

Currently, no rate limiting is implemented. For production use, implement appropriate rate limiting.

## Examples

### Python (requests)

```python
import requests

# Trigger existing job
response = requests.post(
    "http://localhost:8000/scrape/trigger",
    json={"job_id": "indianapolis_complete_core_indianapolis_for_sale_1757661723"}
)
print(response.json())

# Create immediate job
response = requests.post(
    "http://localhost:8000/scrape/immediate",
    json={
        "locations": ["Indianapolis, IN"],
        "listing_type": "for_sale",
        "limit": 1000
    }
)
print(response.json())
```

### JavaScript (fetch)

```javascript
// Trigger existing job
const response = await fetch('http://localhost:8000/scrape/trigger', {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json',
    },
    body: JSON.stringify({
        job_id: 'indianapolis_complete_core_indianapolis_for_sale_1757661723'
    })
});
const data = await response.json();
console.log(data);

// Create immediate job
const response2 = await fetch('http://localhost:8000/scrape/immediate', {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json',
    },
    body: JSON.stringify({
        locations: ['Indianapolis, IN'],
        listing_type: 'for_sale',
        limit: 1000
    })
});
const data2 = await response2.json();
console.log(data2);
```

### cURL

```bash
# Trigger existing job
curl -X POST "http://localhost:8000/scrape/trigger" \
     -H "Content-Type: application/json" \
     -d '{"job_id": "indianapolis_complete_core_indianapolis_for_sale_1757661723"}'

# Create immediate job
curl -X POST "http://localhost:8000/scrape/immediate" \
     -H "Content-Type: application/json" \
     -d '{"locations": ["Indianapolis, IN"], "listing_type": "for_sale", "limit": 1000}'
```

## WebSocket Support

Currently, WebSocket support is not implemented. For real-time updates, consider implementing WebSocket endpoints for job status updates.

## API Documentation

Interactive API documentation is available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- OpenAPI Schema: `http://localhost:8000/openapi.json`
