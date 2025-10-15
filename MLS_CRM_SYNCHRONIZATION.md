# MLS-CRM Property Synchronization

This document describes the new MLS-CRM property synchronization features that have been implemented.

## Overview

The system now supports automatic synchronization between MLS properties and CRM properties, ensuring that when MLS data is updated, the corresponding CRM properties are automatically updated as well.

## Features Implemented

### 1. Active Properties Re-scraping

- **Automatic Re-scraping**: Properties with `crm_status: "active"` are automatically re-scraped on a configurable schedule
- **Default Frequency**: Every hour (configurable via `ACTIVE_PROPERTIES_SCRAPING_FREQUENCY` environment variable)
- **Dynamic Location Detection**: The system automatically detects locations from active properties and scrapes those areas
- **Configurable Settings**: Frequency, limits, delays, and priority can be adjusted

### 2. MLS-CRM Property Linking

- **Bidirectional References**: MLS properties can reference multiple CRM properties, and CRM properties can reference MLS properties
- **Automatic Updates**: When MLS properties are updated, all linked CRM properties are automatically updated
- **Manual Sync**: Users can manually trigger synchronization for specific properties

### 3. Database Schema Changes

#### MLS Properties Collection

- Added `crm_property_ids` field: Array of CRM property IDs that reference this MLS property

#### CRM Properties Collection

- Added `mls_property_id` field: Reference to the MLS property_id for synchronization

## API Endpoints

### Active Properties Re-scraping Configuration

#### GET `/active-properties-config`

Get current configuration for active properties re-scraping.

**Response:**

```json
{
  "exists": true,
  "job": {
    "scheduled_job_id": "active_properties_rescraping",
    "name": "Active Properties Re-scraping",
    "status": "active",
    "cron_expression": "0 * * * *",
    "limit": 1000,
    "priority": "normal",
    "last_run_at": "2024-01-01T12:00:00Z",
    "next_run_at": "2024-01-01T13:00:00Z",
    "run_count": 24
  }
}
```

#### PUT `/active-properties-config`

Update configuration for active properties re-scraping.

**Request:**

```json
{
  "cron_expression": "0 */2 * * *",
  "limit": 500,
  "request_delay": 3.0,
  "priority": "high"
}
```

#### POST `/active-properties-config/toggle`

Enable or disable active properties re-scraping.

**Request:**

```json
{
  "enabled": true
}
```

### MLS-CRM Property Linking

#### POST `/api/properties/:id/link-mls`

Link a CRM property to an MLS property.

**Request:**

```json
{
  "mls_property_id": "abc123def456"
}
```

#### DELETE `/api/properties/:id/unlink-mls`

Unlink a CRM property from its MLS property.

#### GET `/api/properties/:id/mls-data`

Get MLS data for a linked property.

#### POST `/api/properties/:id/sync-from-mls`

Manually sync CRM property from MLS data.

### Scraper Service Endpoints

#### POST `/crm-property-reference`

Add a CRM property reference to an MLS property.

**Request:**

```json
{
  "mls_property_id": "abc123def456",
  "crm_property_id": "crm_property_789"
}
```

#### DELETE `/crm-property-reference`

Remove a CRM property reference from an MLS property.

#### POST `/sync-property`

Manually sync CRM properties from MLS data.

## Configuration

### Environment Variables

- `ACTIVE_PROPERTIES_SCRAPING_FREQUENCY`: Cron expression for scraping frequency (default: `"0 * * * *"` - every hour)

### Cron Expression Examples

- `"0 * * * *"` - Every hour
- `"0 */2 * * *"` - Every 2 hours
- `"0 */4 * * *"` - Every 4 hours
- `"0 */6 * * *"` - Every 6 hours
- `"0 */12 * * *"` - Every 12 hours
- `"0 2 * * *"` - Daily at 2 AM
- `"0 6 * * *"` - Daily at 6 AM
- `"0 12 * * *"` - Daily at 12 PM
- `"0 18 * * *"` - Daily at 6 PM

## How It Works

### 1. Active Properties Re-scraping

1. The scheduler runs every minute checking for scheduled jobs
2. When the active properties re-scraping job is due:
   - The system queries for properties with `crm_status: "active"`
   - Extracts unique locations from these properties
   - Creates a scraping job for those locations
   - Processes the job and updates MLS properties
   - Automatically updates linked CRM properties

### 2. MLS-CRM Synchronization

1. When a CRM property is linked to an MLS property:

   - The CRM property gets an `mls_property_id` field
   - The MLS property gets the CRM property ID added to its `crm_property_ids` array

2. When an MLS property is updated:
   - The system checks if it has any CRM property references
   - For each referenced CRM property, it updates the CRM property with the latest MLS data
   - Preserves CRM-specific fields (like `crm_status`, `notes`, etc.)

### 3. Update Propagation

The system uses content hashing to detect changes:

- Each MLS property has a `content_hash` field
- When properties are saved, the hash is compared
- Only properties with changed content trigger CRM updates
- This prevents unnecessary updates and preserves performance

## Testing

Run the test script to verify the implementation:

```bash
cd real-estate-crm-scraper
python test_mls_crm_integration.py
```

The test script will:

1. Create a test MLS property
2. Add CRM property references
3. Update the MLS property and verify CRM sync
4. Test manual synchronization
5. Clean up test data

## Frontend Integration

The frontend includes a new component `ActivePropertiesRescraping` that allows users to:

- View current re-scraping configuration
- Modify scraping frequency
- Enable/disable automatic re-scraping
- Monitor job status and run history

## Benefits

1. **Automatic Data Freshness**: Active properties stay up-to-date with latest MLS data
2. **Reduced Manual Work**: No need to manually refresh property data
3. **Configurable Frequency**: Adjust scraping frequency based on needs
4. **Efficient Updates**: Only properties with actual changes are updated
5. **Bidirectional Linking**: Full traceability between MLS and CRM properties
6. **Manual Override**: Users can manually sync properties when needed

## Monitoring

The system provides comprehensive logging and monitoring:

- Job execution logs
- Property update counts
- Error handling and reporting
- Performance metrics
- Sync status tracking

## Future Enhancements

Potential future improvements:

- Webhook notifications for property updates
- Advanced filtering for re-scraping (e.g., only properties with specific criteria)
- Batch processing optimizations
- Real-time sync status dashboard
- Integration with external MLS APIs
