# Getting Started with OpenAI Agent Lookup Library

Welcome! This guide will help you get started with using the library in your server application.

## What Was Created

A complete, production-ready Python library for finding real estate agent mobile numbers using OpenAI Agents.

## Quick Start (3 Steps)

### 1. Install

```bash
cd openai_agent_lookup
pip install -r requirements.txt
```

### 2. Configure

Set your OpenAI API key:

```powershell
# PowerShell
$env:OPENAI_API_KEY="sk-your-api-key-here"

# Or create .env file
echo "OPENAI_API_KEY=sk-your-api-key-here" > .env
```

### 3. Use

```python
from openai_agent_lookup import AgentLookupClient

client = AgentLookupClient()
result = client.lookup("Ashley Bergen, Indianapolis, IN")

print(f"Agent: {result.agent_name}")
for mobile in result.mobile_numbers:
    print(f"  {mobile.number} - {mobile.certainty:.0%}")
```

## Documentation Files

- **[README.md](README.md)** - Complete library documentation
- **[QUICKSTART.md](QUICKSTART.md)** - 5-minute quick start
- **[INSTALL.md](INSTALL.md)** - Detailed installation guide
- **[EXAMPLES.md](EXAMPLES.md)** - Comprehensive usage examples
- **[INTEGRATION_GUIDE.md](INTEGRATION_GUIDE.md)** - Server integration guide
- **[LIBRARY_OVERVIEW.md](LIBRARY_OVERVIEW.md)** - Architecture overview
- **[server_example.py](server_example.py)** - Ready-to-use server examples

## Library Structure

```
openai_agent_lookup/
├── __init__.py              # Public API
├── client.py                # Main client class
├── agent.py                 # Agent configuration
├── models.py                # Data models
├── requirements.txt         # Dependencies
├── setup.py                 # Package setup
└── pyproject.toml           # Modern package config
```

## Key Features

✅ **Production Ready** - Clean API, error handling, type safety  
✅ **Async Support** - Full async/await for server applications  
✅ **Batch Processing** - Efficient batch lookups  
✅ **Type Safe** - Pydantic models throughout  
✅ **Well Documented** - Comprehensive docs and examples  

## Common Use Cases

### Server API Endpoint

```python
from fastapi import FastAPI
from openai_agent_lookup import AgentLookupClient

app = FastAPI()
client = AgentLookupClient()

@app.post("/api/lookup")
async def lookup(query: str):
    result = await client.lookup_async(query)
    return result.model_dump()
```

### Batch Processing

```python
queries = ["Agent 1", "Agent 2", "Agent 3"]
results = await client.lookup_batch_async(queries)
```

### Error Handling

```python
try:
    result = await client.lookup_async(query)
    if result.has_results():
        return result
except Exception as e:
    # Handle error
    pass
```

## Next Steps

1. **Read [QUICKSTART.md](QUICKSTART.md)** - Get running in 5 minutes
2. **Check [EXAMPLES.md](EXAMPLES.md)** - See real-world examples
3. **Review [INTEGRATION_GUIDE.md](INTEGRATION_GUIDE.md)** - Server integration patterns
4. **Explore [server_example.py](server_example.py)** - Copy-paste server code

## Support

- Check documentation files
- Review examples
- See troubleshooting in [INSTALL.md](INSTALL.md)

## Installation Methods

### Method 1: Use as Library (Recommended)

```bash
cd openai_agent_lookup
pip install -r requirements.txt
```

Then import:
```python
from openai_agent_lookup import AgentLookupClient
```

### Method 2: Install as Package

```bash
cd openai_agent_lookup
pip install -e .
```

### Method 3: Copy to Your Project

Copy the entire `openai_agent_lookup` folder to your project and install dependencies.

## Prerequisites

- Python 3.8+
- OpenAI API key with agents library access
- Internet connection

See [INSTALL.md](INSTALL.md) for detailed prerequisites.

## That's It!

You're ready to use the library. Start with [QUICKSTART.md](QUICKSTART.md) for the fastest path to success.

