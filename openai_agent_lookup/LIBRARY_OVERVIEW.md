# Library Overview

Complete overview of the OpenAI Agent Lookup Library structure and architecture.

## Library Structure

```
openai_agent_lookup/
├── __init__.py              # Public API exports
├── client.py                # Main client class (AgentLookupClient)
├── agent.py                 # OpenAI agent configuration
├── models.py                # Pydantic data models
├── requirements.txt         # Python dependencies
├── setup.py                 # Package setup (setuptools)
├── pyproject.toml           # Modern package configuration
├── README.md                # Main documentation
├── QUICKSTART.md            # Quick start guide
├── INSTALL.md               # Installation guide
├── EXAMPLES.md              # Usage examples
├── server_example.py        # Server integration examples
└── LIBRARY_OVERVIEW.md      # This file
```

## Architecture

### Core Components

1. **AgentLookupClient** (`client.py`)
   - Main interface for the library
   - Handles async/sync operations
   - Manages agent execution
   - Provides batch processing

2. **Agent Configuration** (`agent.py`)
   - Defines the OpenAI agent
   - Configures web search tool
   - Sets up response schema
   - Configures model settings

3. **Data Models** (`models.py`)
   - `AgentLookupResult` - Main result object
   - `MobileNumber` - Phone number with certainty
   - `LookupRequest` - Request wrapper

### Data Flow

```
User Query
    ↓
AgentLookupClient.lookup()
    ↓
OpenAI Agent (with Web Search Tool)
    ↓
Web Search (MIBOR, Zillow, Realtor.com, etc.)
    ↓
Agent Analysis & Extraction
    ↓
Confidence Scoring
    ↓
Filtering (80%+ certainty)
    ↓
AgentLookupResult
```

## API Design

### Synchronous API

```python
client = AgentLookupClient()
result = client.lookup("query")
```

### Asynchronous API

```python
client = AgentLookupClient()
result = await client.lookup_async("query")
```

### Batch API

```python
client = AgentLookupClient()
results = client.lookup_batch(["query1", "query2"])
```

## Key Features

### 1. Type Safety

All data structures use Pydantic models for:
- Type checking
- Validation
- Serialization
- Documentation

### 2. Async-First Design

- Full async/await support
- Sync wrappers for convenience
- Efficient batch processing

### 3. Confidence Scoring

- Each result includes certainty (0.0-1.0)
- Only returns 80%+ certainty results
- Sorted by confidence level

### 4. Error Handling

- Graceful error handling
- Clear error messages
- Type-safe exceptions

## Integration Patterns

### Server Integration

The library is designed for server use:

```python
# FastAPI
@app.post("/api/lookup")
async def lookup(request: LookupRequest):
    result = await client.lookup_async(request.query)
    return result.model_dump()
```

### Batch Processing

Efficient batch operations:

```python
results = await client.lookup_batch_async(queries)
```

### Custom Configuration

Extensible design:

```python
client = AgentLookupClient(workflow_id="custom-id")
```

## Dependencies

### Core Dependencies

- `openai-agents` - OpenAI Agents SDK
- `openai` - OpenAI Python SDK
- `pydantic` - Data validation
- `python-dotenv` - Environment management
- `nest-asyncio` - Async support

### Optional Dependencies

- `pytest` - Testing
- `black` - Code formatting
- `flake8` - Linting

## Performance Considerations

### Single Lookup

- **Time**: 30-60 seconds
- **Cost**: ~$0.05-0.15 per lookup
- **API Calls**: Multiple (web search + agent processing)

### Batch Lookup

- **Time**: Parallel processing
- **Cost**: Linear with number of queries
- **Efficiency**: Better than sequential lookups

## Security

### API Key Management

- Never commit API keys
- Use environment variables
- Use `.env` files (gitignored)
- Rotate keys regularly

### Best Practices

```python
# Good
import os
api_key = os.getenv("OPENAI_API_KEY")

# Bad
api_key = "sk-..."  # Never hardcode
```

## Extensibility

### Custom Workflow IDs

```python
client = AgentLookupClient(workflow_id="my-workflow")
```

### Custom Trace Metadata

```python
result = await client.lookup_async(
    query,
    trace_metadata={"custom": "data"}
)
```

## Testing

### Unit Tests

```python
from openai_agent_lookup import AgentLookupClient

def test_lookup():
    client = AgentLookupClient()
    result = client.lookup("test query")
    assert result.agent_name is not None
```

### Integration Tests

```python
import pytest
from openai_agent_lookup import AgentLookupClient

@pytest.mark.asyncio
async def test_async_lookup():
    client = AgentLookupClient()
    result = await client.lookup_async("test")
    assert result.has_results()
```

## Versioning

The library follows semantic versioning:

- **Major**: Breaking changes
- **Minor**: New features, backward compatible
- **Patch**: Bug fixes

Current version: **1.0.0**

## Support

For issues, questions, or contributions:

1. Check documentation
2. Review examples
3. Open GitHub issue
4. Contact maintainers

## License

MIT License - See LICENSE file for details

