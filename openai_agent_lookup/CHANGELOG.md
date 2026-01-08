# Changelog

## Version 1.1.0 - 2024 (Latest)

### Added
- **Email Support** - Library now returns email addresses in addition to phone numbers
- **Phone Type Field** - Phone numbers now include a `type` field (mobile, office, landline)
- **Enhanced Methods** - New helper methods:
  - `get_best_email()` - Get email with highest certainty
  - `get_mobile_numbers()` - Filter for mobile numbers only
  - `get_office_numbers()` - Filter for office/landline numbers only
  - `get_emails_above_threshold()` - Filter emails by certainty
  - `has_phone_numbers()` - Check if phone numbers exist
  - `has_emails()` - Check if emails exist

### Changed
- **Agent Instructions** - Updated to search for emails and office phone numbers in addition to mobile numbers
- **Agent Schema** - Now includes both phone numbers and emails in response
- **Result Model** - `AgentLookupResult` now includes `emails` field

### Updated Models
- `MobileNumber` - Added `type: str` field
- `AgentLookupResult` - Added `emails: List[Email]` field
- New `Email` model with `email` and `certainty` fields

## Version 1.0.0 - Initial Release

- Basic phone number lookup
- Async/sync support
- Batch processing
- Type-safe models

