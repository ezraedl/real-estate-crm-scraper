# Quick Start Guide

Get up and running with the OpenAI Agent Lookup Library in 5 minutes.

## Python Version

✅ **Python 3.14 is fully supported!** This library works on Python 3.8-3.14.

## Step 1: Install Dependencies

```bash
cd openai_agent_lookup
pip install -r requirements.txt
```

## Step 2: Set Your API Key

**PowerShell (Windows):**
```powershell
$env:OPENAI_API_KEY="sk-your-api-key-here"
```

**Bash (Linux/Mac):**
```bash
export OPENAI_API_KEY="sk-your-api-key-here"
```

**Or create a `.env` file:**
```env
OPENAI_API_KEY=sk-your-api-key-here
```

## Step 3: Basic Usage

### Synchronous (Simple)

```python
from openai_agent_lookup import AgentLookupClient

client = AgentLookupClient()
result = client.lookup("Ashley Bergen, Indianapolis, IN")

print(f"Agent: {result.agent_name}")
for mobile in result.mobile_numbers:
    print(f"  {mobile.number} - {mobile.certainty:.0%} certainty")
```

### Asynchronous (Recommended for Servers)

```python
import asyncio
from openai_agent_lookup import AgentLookupClient

async def main():
    client = AgentLookupClient()
    result = await client.lookup_async("Ashley Bergen, Indianapolis")
    
    if result.has_results():
        best = result.get_best_number()
        print(f"Best: {best.number} ({best.certainty:.0%})")

asyncio.run(main())
```

## Step 4: Test It

Save this as `test_lookup.py`:

```python
from openai_agent_lookup import AgentLookupClient

client = AgentLookupClient()
result = client.lookup("Ashley Bergen, Indianapolis, IN")

print(f"✓ Found agent: {result.agent_name}")
print(f"✓ Found {len(result.mobile_numbers)} mobile numbers")
```

Run it:
```bash
python test_lookup.py
```

## Next Steps

- **Full Documentation**: See [README.md](README.md)
- **Examples**: See [EXAMPLES.md](EXAMPLES.md) for server integration
- **Installation Details**: See [INSTALL.md](INSTALL.md)

## Common Issues

**"ModuleNotFoundError: No module named 'agents'"**
```bash
pip install openai-agents
```

**"OpenAI API key not found"**
- Make sure you set the environment variable
- Restart your terminal/IDE after setting it
- Or create a `.env` file

**"Invalid API key"**
- Verify your key at https://platform.openai.com/api-keys
- Make sure you have access to the agents library

## That's It!

You're ready to use the library. For more advanced usage, see the full documentation.

