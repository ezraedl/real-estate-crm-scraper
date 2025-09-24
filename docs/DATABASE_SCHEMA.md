# MLS Scraper Database Schema Specification

## Overview

This document provides a comprehensive specification of the MongoDB database schema used by the MLS Scraper system. It is designed for external applications that need to connect to the database and understand the data structure.

## Database Information

- **Database Name**: `mls_scraper`
- **MongoDB Version**: Compatible with MongoDB 4.4+
- **Connection**: MongoDB Atlas (cloud) or local MongoDB instance
- **Collections**: 2 main collections (`jobs`, `properties`)

## Collections

### 1. Jobs Collection (`jobs`)

The `jobs` collection stores information about scraping jobs, including scheduled, recurring, and immediate jobs.

#### Collection Details
- **Collection Name**: `jobs`
- **Indexes**: 
  - `job_id` (unique)
  - `status`
  - `created_at`
  - `listing_type`

#### Document Schema

```json
{
  "_id": "ObjectId",
  "job_id": "string (unique)",
  "priority": "string",
  "status": "string",
  "locations": ["string"],
  "listing_type": "string",
  "property_types": ["string"] | null,
  "past_days": "number" | null,
  "date_from": "string" | null,
  "date_to": "string" | null,
  "radius": "number" | null,
  "mls_only": "boolean",
  "foreclosure": "boolean",
  "exclude_pending": "boolean",
  "limit": "number",
  "scheduled_at": "ISODate" | null,
  "cron_expression": "string" | null,
  "created_at": "ISODate",
  "started_at": "ISODate" | null,
  "completed_at": "ISODate" | null,
  "error_message": "string" | null,
  "run_count": "number",
  "last_run": "ISODate" | null,
  "last_run_status": "string" | null,
  "last_run_job_id": "string" | null,
  "original_job_id": "string" | null,
  "properties_scraped": "number",
  "properties_saved": "number",
  "total_locations": "number",
  "completed_locations": "number",
  "proxy_config": "object" | null,
  "user_agent": "string" | null,
  "request_delay": "number"
}
```

#### Field Descriptions

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `_id` | ObjectId | Yes | MongoDB document ID |
| `job_id` | String | Yes | Unique job identifier (e.g., "indianapolis_complete_core_indianapolis_for_sale_1757661723") |
| `priority` | String | Yes | Job priority: "immediate", "high", "normal", "low" |
| `status` | String | Yes | Job status: "pending", "running", "completed", "failed" |
| `locations` | Array[String] | Yes | List of locations to scrape (e.g., ["Indianapolis, IN", "Marion County, IN"]) |
| `listing_type` | String | Yes | Property type: "for_sale", "pending", "sold", "for_rent" |
| `property_types` | Array[String] | No | Specific property types (if applicable) |
| `past_days` | Number | No | Number of days to look back (e.g., 14, 90) |
| `date_from` | String | No | Start date for date range filtering |
| `date_to` | String | No | End date for date range filtering |
| `radius` | Number | No | Search radius in miles |
| `mls_only` | Boolean | Yes | Whether to include only MLS properties |
| `foreclosure` | Boolean | Yes | Whether to include foreclosure properties |
| `exclude_pending` | Boolean | Yes | Whether to exclude pending properties |
| `limit` | Number | Yes | Maximum number of properties to scrape |
| `scheduled_at` | ISODate | No | When to run one-time jobs |
| `cron_expression` | String | No | Cron expression for recurring jobs (e.g., "0 13 * * *") |
| `created_at` | ISODate | Yes | When the job was created |
| `started_at` | ISODate | No | When the job started running |
| `completed_at` | ISODate | No | When the job completed |
| `error_message` | String | No | Error message if job failed |
| `run_count` | Number | Yes | Number of times recurring job has been executed |
| `last_run` | ISODate | No | Timestamp of last execution |
| `last_run_status` | String | No | Status of last execution |
| `last_run_job_id` | String | No | Job ID of last execution instance |
| `original_job_id` | String | No | Original job ID for recurring job instances |
| `properties_scraped` | Number | Yes | Total properties scraped |
| `properties_saved` | Number | Yes | Total properties saved to database |
| `total_locations` | Number | Yes | Total number of locations |
| `completed_locations` | Number | Yes | Number of completed locations |
| `proxy_config` | Object | No | Proxy configuration details |
| `user_agent` | String | No | User agent string for requests |
| `request_delay` | Number | Yes | Delay between requests in seconds |

#### Job ID Patterns

- **Scheduled Jobs**: `scheduled_{timestamp}_{random}`
- **Immediate Jobs**: `immediate_{timestamp}_{random}`
- **Indianapolis Metro**: `indianapolis_metro_{region}_{type}_{timestamp}`
- **Indianapolis Complete**: `indianapolis_complete_{region}_{type}_{timestamp}`
- **Indiana Large-Scale**: `indiana_large_{region}_{type}_{timestamp}`
- **Indiana Daily**: `indiana_daily_{type}_{timestamp}`

### 2. Properties Collection (`properties`)

The `properties` collection stores the actual property data scraped from real estate websites.

#### Collection Details
- **Collection Name**: `properties`
- **Indexes**:
  - `property_id` (unique)
  - `status`
  - `location`
  - `price`
  - `created_at`
  - `updated_at`
  - `hash` (for duplicate detection)

#### Document Schema

```json
{
  "_id": "ObjectId",
  "property_id": "string (unique)",
  "status": "string",
  "location": "string",
  "address": "string",
  "city": "string",
  "state": "string",
  "zip_code": "string",
  "county": "string",
  "price": "number",
  "price_per_sqft": "number",
  "bedrooms": "number",
  "bathrooms": "number",
  "square_feet": "number",
  "lot_size": "number",
  "year_built": "number",
  "property_type": "string",
  "listing_type": "string",
  "mls_number": "string",
  "description": "string",
  "features": ["string"],
  "images": ["string"],
  "agent_name": "string",
  "agent_phone": "string",
  "agent_email": "string",
  "brokerage": "string",
  "days_on_market": "number",
  "listing_date": "ISODate",
  "last_updated": "ISODate",
  "url": "string",
  "latitude": "number",
  "longitude": "number",
  "school_district": "string",
  "hoa_fee": "number",
  "property_tax": "number",
  "utilities": ["string"],
  "parking": "string",
  "garage": "string",
  "basement": "string",
  "pool": "boolean",
  "fireplace": "boolean",
  "central_air": "boolean",
  "heating": "string",
  "cooling": "string",
  "roof": "string",
  "exterior": "string",
  "interior": "string",
  "flooring": "string",
  "appliances": ["string"],
  "nearby_amenities": ["string"],
  "walk_score": "number",
  "transit_score": "number",
  "bike_score": "number",
  "created_at": "ISODate",
  "updated_at": "ISODate",
  "hash": "string",
  "source": "string",
  "scraped_at": "ISODate"
}
```

#### Field Descriptions

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `_id` | ObjectId | Yes | MongoDB document ID |
| `property_id` | String | Yes | Unique property identifier |
| `status` | String | Yes | Property status: "FOR_SALE", "PENDING", "SOLD", "FOR_RENT", "CONTINGENT", "OFF_MARKET" |
| `location` | String | Yes | Full location string (e.g., "Indianapolis, IN") |
| `address` | String | Yes | Street address |
| `city` | String | Yes | City name |
| `state` | String | Yes | State abbreviation (e.g., "IN") |
| `zip_code` | String | Yes | ZIP code |
| `county` | String | No | County name |
| `price` | Number | Yes | Property price |
| `price_per_sqft` | Number | No | Price per square foot |
| `bedrooms` | Number | No | Number of bedrooms |
| `bathrooms` | Number | No | Number of bathrooms |
| `square_feet` | Number | No | Square footage |
| `lot_size` | Number | No | Lot size in square feet |
| `year_built` | Number | No | Year the property was built |
| `property_type` | String | No | Type of property (e.g., "Single Family", "Condo", "Townhouse") |
| `listing_type` | String | Yes | Listing type: "for_sale", "pending", "sold", "for_rent" |
| `mls_number` | String | No | MLS listing number |
| `description` | String | No | Property description |
| `features` | Array[String] | No | Property features |
| `images` | Array[String] | No | URLs to property images |
| `agent_name` | String | No | Listing agent name |
| `agent_phone` | String | No | Agent phone number |
| `agent_email` | String | No | Agent email |
| `brokerage` | String | No | Real estate brokerage |
| `days_on_market` | Number | No | Days the property has been on market |
| `listing_date` | ISODate | No | When the property was listed |
| `last_updated` | ISODate | No | When the listing was last updated |
| `url` | String | No | URL to the property listing |
| `latitude` | Number | No | Geographic latitude |
| `longitude` | Number | No | Geographic longitude |
| `school_district` | String | No | School district |
| `hoa_fee` | Number | No | HOA fee amount |
| `property_tax` | Number | No | Annual property tax |
| `utilities` | Array[String] | No | Available utilities |
| `parking` | String | No | Parking information |
| `garage` | String | No | Garage information |
| `basement` | String | No | Basement information |
| `pool` | Boolean | No | Whether property has a pool |
| `fireplace` | Boolean | No | Whether property has a fireplace |
| `central_air` | Boolean | No | Whether property has central air |
| `heating` | String | No | Heating system type |
| `cooling` | String | No | Cooling system type |
| `roof` | String | No | Roof type |
| `exterior` | String | No | Exterior materials |
| `interior` | String | No | Interior features |
| `flooring` | String | No | Flooring type |
| `appliances` | Array[String] | No | Included appliances |
| `nearby_amenities` | Array[String] | No | Nearby amenities |
| `walk_score` | Number | No | Walk Score rating |
| `transit_score` | Number | No | Transit Score rating |
| `bike_score` | Number | No | Bike Score rating |
| `created_at` | ISODate | Yes | When the property was first scraped |
| `updated_at` | ISODate | Yes | When the property was last updated |
| `hash` | String | Yes | Hash for duplicate detection |
| `source` | String | Yes | Source website (e.g., "realtor.com") |
| `scraped_at` | ISODate | Yes | When the property was scraped |

## API Endpoints

### Base URL
```
http://localhost:8000
```

### Available Endpoints

#### 1. Health Check
```
GET /health
```
Returns server health status.

#### 2. Get Job Status
```
GET /jobs/{job_id}
```
Returns detailed information about a specific job.

#### 3. Trigger Existing Job
```
POST /scrape/trigger
Content-Type: application/json

{
  "job_id": "string",
  "priority": "string" (optional)
}
```
Triggers an existing job to run immediately.

#### 4. Create Immediate Job
```
POST /scrape/immediate
Content-Type: application/json

{
  "locations": ["string"],
  "listing_type": "string",
  "limit": "number",
  "priority": "string" (optional)
}
```
Creates and runs an immediate scraping job.

#### 5. Create Scheduled Job
```
POST /scrape/scheduled
Content-Type: application/json

{
  "locations": ["string"],
  "listing_type": "string",
  "limit": "number",
  "cron_expression": "string" (optional),
  "scheduled_at": "ISODate" (optional)
}
```
Creates a scheduled or recurring scraping job.

## Connection Examples

### Python (pymongo)
```python
from pymongo import MongoClient
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["mls_scraper"]

# Get all properties
properties = db.properties.find()

# Get properties by status
for_sale = db.properties.find({"status": "FOR_SALE"})

# Get properties by location
indianapolis = db.properties.find({"location": {"$regex": "Indianapolis"}})

# Get recent properties
recent = db.properties.find({
    "created_at": {"$gte": datetime(2025, 1, 1)}
})
```

### Node.js (mongodb)
```javascript
const { MongoClient } = require('mongodb');

async function connectToDatabase() {
    const client = new MongoClient('mongodb://localhost:27017/');
    await client.connect();
    const db = client.db('mls_scraper');
    
    // Get all properties
    const properties = await db.collection('properties').find({}).toArray();
    
    // Get properties by status
    const forSale = await db.collection('properties').find({
        status: 'FOR_SALE'
    }).toArray();
    
    return { properties, forSale };
}
```

### Java (MongoDB Driver)
```java
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

MongoClient client = MongoClients.create("mongodb://localhost:27017/");
MongoDatabase database = client.getDatabase("mls_scraper");
MongoCollection<Document> properties = database.getCollection("properties");

// Get all properties
FindIterable<Document> allProperties = properties.find();

// Get properties by status
FindIterable<Document> forSale = properties.find(
    new Document("status", "FOR_SALE")
);
```

## Query Examples

### Common Queries

#### 1. Get All Properties in Indianapolis
```javascript
db.properties.find({
    "location": {"$regex": "Indianapolis", "$options": "i"}
})
```

#### 2. Get Properties by Price Range
```javascript
db.properties.find({
    "price": {"$gte": 200000, "$lte": 500000}
})
```

#### 3. Get Properties by Bedrooms and Bathrooms
```javascript
db.properties.find({
    "bedrooms": {"$gte": 3},
    "bathrooms": {"$gte": 2}
})
```

#### 4. Get Recent Properties
```javascript
db.properties.find({
    "created_at": {"$gte": new Date("2025-01-01")}
}).sort({"created_at": -1})
```

#### 5. Get Properties by Status
```javascript
db.properties.find({
    "status": {"$in": ["FOR_SALE", "PENDING"]}
})
```

#### 6. Get Job Statistics
```javascript
db.jobs.aggregate([
    {
        $group: {
            _id: "$status",
            count: {"$sum": 1},
            totalProperties: {"$sum": "$properties_scraped"}
        }
    }
])
```

#### 7. Get Property Count by Location
```javascript
db.properties.aggregate([
    {
        $group: {
            _id: "$location",
            count: {"$sum": 1}
        }
    },
    {
        $sort: {"count": -1}
    }
])
```

## Data Relationships

### Job to Properties Relationship
- Jobs create properties through scraping
- Properties are linked to jobs via the scraping process
- Multiple properties can be created by a single job
- Properties are updated when re-scraped by recurring jobs

### Property Updates
- Properties are updated using hash-based comparison
- Only changed properties are updated in the database
- Historical data is preserved through timestamps

## Security Considerations

1. **Database Access**: Ensure proper authentication and authorization
2. **API Rate Limiting**: Implement rate limiting for API endpoints
3. **Data Validation**: Validate all input data before database operations
4. **Indexing**: Ensure proper indexing for performance
5. **Backup**: Regular database backups are recommended

## Performance Optimization

1. **Indexes**: Use appropriate indexes for common queries
2. **Aggregation**: Use MongoDB aggregation pipeline for complex queries
3. **Pagination**: Implement pagination for large result sets
4. **Caching**: Consider caching frequently accessed data
5. **Connection Pooling**: Use connection pooling for better performance

## Monitoring and Maintenance

1. **Job Status**: Monitor job status and completion rates
2. **Property Counts**: Track property counts by status and location
3. **Error Handling**: Monitor and handle job failures
4. **Database Size**: Monitor database growth and storage usage
5. **Query Performance**: Monitor slow queries and optimize as needed

## Version History

- **v1.0** - Initial schema specification
- **v1.1** - Added complete property fields and API endpoints
- **v1.2** - Added query examples and connection code samples

---

*This document is maintained by the MLS Scraper development team. For questions or updates, please contact the development team.*
