# OpenAI Agent Lookup Library

A Python library for finding real estate agent contact information (phone numbers and emails) using OpenAI Agents with web search capabilities. This library leverages OpenAI's agent framework to intelligently search real estate websites and extract mobile numbers, office/landline numbers, and email addresses with confidence scores.

## Features

- ✅ **Intelligent Web Search** - Uses OpenAI Agents to search real estate websites (MIBOR, Zillow, Realtor.com, Homes.com, etc.)
- ✅ **Comprehensive Contact Data** - Returns mobile numbers, office/landline numbers, and email addresses
- ✅ **Phone Type Detection** - Distinguishes between mobile, office, and landline numbers
- ✅ **High Confidence Results** - Only returns data with 80%+ certainty
- ✅ **Structured Results** - Returns agent name, phone numbers, and emails sorted by certainty
- ✅ **Async Support** - Full async/await support for server applications
- ✅ **Batch Processing** - Look up multiple agents efficiently
- ✅ **Type Safety** - Full Pydantic models for type checking

## Installation

### Prerequisites

- **Python 3.8+** (fully tested and supported on Python 3.14)
- **OpenAI API Key** with access to the agents library
- Internet connection

**Python 3.14 Note:** This library is fully compatible with Python 3.14. All dependencies (openai-agents, pydantic, etc.) work correctly on Python 3.14.

### Quick Install

```bash
# Install from directory
cd openai_agent_lookup
pip install -r requirements.txt

# Or install as package
pip install -e .
```

### Full Installation Guide

See [INSTALL.md](INSTALL.md) for detailed installation instructions and prerequisites.

## Quick Start

### Basic Usage (Synchronous)

```python
from openai_agent_lookup import AgentLookupClient

# Initialize client
client = AgentLookupClient()

# Look up an agent
result = client.lookup("Ashley Bergen, Indianapolis, IN")

# Access results
print(f"Agent: {result.agent_name}")
print(f"Found {len(result.mobile_numbers)} phone numbers")
print(f"Found {len(result.emails)} email addresses")

# Get best results
best_number = result.get_best_number()
if best_number:
    print(f"Best number: {best_number.number} ({best_number.type}) - {best_number.certainty:.0%}")

best_email = result.get_best_email()
if best_email:
    print(f"Best email: {best_email.email} - {best_email.certainty}")
```

### Async Usage (Recommended for Servers)

```python
import asyncio
from openai_agent_lookup import AgentLookupClient

async def main():
    client = AgentLookupClient()
    
    # Look up an agent
    result = await client.lookup_async("Ashley Bergen, Indianapolis, IN")
    
    # Access results
    print(f"Agent: {result.agent_name}")
    for mobile in result.mobile_numbers:
        print(f"  {mobile.number} ({mobile.type}) - {mobile.certainty:.0%} certainty")
    
    for email in result.emails:
        print(f"  {email.email} - {email.certainty} certainty")

asyncio.run(main())
```

### Batch Processing

```python
from openai_agent_lookup import AgentLookupClient

client = AgentLookupClient()

# Look up multiple agents
queries = [
    "Ashley Bergen, Indianapolis",
    "John Smith, Chicago, IL",
    "Jane Doe, New York, NY"
]

results = client.lookup_batch(queries)

for result in results:
    print(f"{result.agent_name}: {len(result.mobile_numbers)} numbers found")
```

## API Reference

### AgentLookupClient

Main client class for looking up agents.

#### Methods

##### `lookup(query: str | LookupRequest) -> AgentLookupResult`

Synchronous lookup method. Wraps the async version.

**Parameters:**
- `query`: Search query string or LookupRequest object
  - Examples: `"Ashley Bergen, Indianapolis"`, `"Find mobile number for John Smith in Chicago"`

**Returns:** `AgentLookupResult`

##### `lookup_async(query: str | LookupRequest) -> AgentLookupResult`

Asynchronous lookup method. Recommended for server applications.

**Parameters:**
- `query`: Search query string or LookupRequest object

**Returns:** `AgentLookupResult` (awaitable)

##### `lookup_batch(queries: list[str | LookupRequest]) -> list[AgentLookupResult]`

Synchronous batch lookup.

**Parameters:**
- `queries`: List of search query strings or LookupRequest objects

**Returns:** List of `AgentLookupResult` objects

##### `lookup_batch_async(queries: list[str | LookupRequest]) -> list[AgentLookupResult]`

Asynchronous batch lookup.

**Parameters:**
- `queries`: List of search query strings or LookupRequest objects

**Returns:** List of `AgentLookupResult` objects (awaitable)

### AgentLookupResult

Result object containing agent information, phone numbers, and emails.

#### Properties

- `agent_name: str` - Name of the real estate agent
- `mobile_numbers: list[MobileNumber]` - List of phone numbers (mobile, office, landline) sorted by certainty (highest first)
- `emails: list[Email]` - List of email addresses sorted by certainty (highest first)

#### Methods

- `get_best_number() -> MobileNumber | None` - Get the phone number with highest certainty
- `get_best_email() -> Email | None` - Get the email with highest certainty
- `get_mobile_numbers() -> list[MobileNumber]` - Get only mobile phone numbers
- `get_office_numbers() -> list[MobileNumber]` - Get only office/landline phone numbers
- `get_numbers_above_threshold(threshold: float = 0.8) -> list[MobileNumber]` - Get phone numbers above certainty threshold
- `get_emails_above_threshold(threshold: Union[str, float] = 0.8) -> list[Email]` - Get emails above certainty threshold
- `has_results() -> bool` - Check if any phone numbers or emails were found
- `has_phone_numbers() -> bool` - Check if any phone numbers were found
- `has_emails() -> bool` - Check if any emails were found

### MobileNumber

Phone number with certainty score and type.

#### Properties

- `number: str` - Phone number in any format
- `certainty: float` - Certainty score between 0.0 and 1.0
- `type: str` - Phone type: "mobile", "office", "landline", etc.

### Email

Email address with certainty score.

#### Properties

- `email: str` - Email address
- `certainty: Union[str, float]` - Certainty score (can be string or float)

## Usage Examples

See [EXAMPLES.md](EXAMPLES.md) for comprehensive usage examples including:
- Server integration (FastAPI, Flask, Django)
- Error handling
- Custom configuration
- Advanced patterns

## Configuration

### Environment Variables

Set your OpenAI API key:

```bash
# PowerShell
$env:OPENAI_API_KEY="sk-your-api-key-here"

# Linux/Mac
export OPENAI_API_KEY="sk-your-api-key-here"

# Or use .env file
echo "OPENAI_API_KEY=sk-your-api-key-here" > .env
```

### Custom Workflow ID

```python
from openai_agent_lookup import AgentLookupClient

# Use custom workflow ID for tracing
client = AgentLookupClient(workflow_id="your-custom-workflow-id")
```

## How It Works

1. **Query Processing** - The library takes your query and formats it for the agent
2. **Web Search** - The OpenAI agent searches real estate websites using web search tools
3. **Intelligent Extraction** - The agent identifies mobile numbers and validates them across multiple sources
4. **Confidence Scoring** - Each number is scored based on how many sources confirm it
5. **Filtering** - Only numbers with 80%+ certainty are returned
6. **Structured Results** - Results are returned as typed Pydantic models

## Error Handling

```python
from openai_agent_lookup import AgentLookupClient

client = AgentLookupClient()

try:
    result = client.lookup("Agent Name, Location")
    if result.has_results():
        print(f"Found {len(result.mobile_numbers)} numbers")
    else:
        print("No numbers found (agent may not have been 80%+ certain)")
except Exception as e:
    print(f"Error: {e}")
```

## Performance

- **Single Lookup**: ~30-60 seconds (includes web search and validation)
- **Batch Lookup**: Parallel processing for multiple queries
- **Cost**: Varies based on OpenAI API usage (typically $0.05-0.15 per lookup)

## Limitations

- Requires OpenAI API access with agents library support
- Only returns numbers with 80%+ certainty
- Focuses on US real estate websites
- Results depend on web search availability

## License

MIT License

## Support

For issues, questions, or contributions, please open an issue on GitHub.

## Changelog

### Version 1.0.0
- Initial release
- Basic lookup functionality
- Async support
- Batch processing
- Type-safe models

