# MLS Scraper Documentation Version

## Current Version: 1.2.0

### Version Information
- **Major Version**: 1
- **Minor Version**: 2
- **Patch Version**: 0
- **Release Date**: 2025-09-12
- **Status**: Stable

### Version History

#### v1.2.0 (2025-09-12)
- Added comprehensive Indianapolis metro area scanning
- Enhanced job scheduling with all property types
- Added trigger API for immediate job execution
- Improved database schema with run tracking
- Added complete property type coverage (FOR_SALE, PENDING, SOLD, FOR_RENT)

#### v1.1.0 (2025-09-11)
- Added enhanced scheduler with run tracking
- Implemented recurring job management
- Added property type selection capabilities
- Enhanced database schema with additional fields

#### v1.0.0 (2025-09-10)
- Initial release
- Basic scraping functionality
- MongoDB integration
- FastAPI server implementation

### Compatibility Matrix

| Version | MongoDB | FastAPI | Python | Node.js | Java |
|---------|---------|---------|--------|---------|------|
| 1.2.0   | 4.4+    | 0.68+   | 3.8+   | 14+     | 11+  |
| 1.1.0   | 4.4+    | 0.68+   | 3.8+   | 14+     | 11+  |
| 1.0.0   | 4.4+    | 0.68+   | 3.8+   | 14+     | 11+  |

### Breaking Changes

#### v1.2.0
- None

#### v1.1.0
- Added new fields to job schema (run_count, last_run, etc.)
- Enhanced property schema with additional fields

#### v1.0.0
- Initial release

### Migration Guide

#### From v1.1.0 to v1.2.0
No migration required. All changes are backward compatible.

#### From v1.0.0 to v1.1.0
1. Update database indexes
2. Add new fields to existing documents (optional)
3. Update API client code for new endpoints

### Next Version (v1.3.0)
Planned features:
- Enhanced property filtering
- Advanced analytics endpoints
- Real-time notifications
- Multi-region support
