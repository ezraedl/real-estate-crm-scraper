# Installation Guide

Complete installation guide for the OpenAI Agent Lookup Library.

## Prerequisites

### Python Version

- **Python 3.8 or higher** is required
- **Fully tested and supported on Python 3.14** ✅
- Tested on Python 3.8, 3.9, 3.10, 3.11, 3.12, 3.13, and 3.14

**Python 3.14 Note:** This library is fully compatible with Python 3.14. All dependencies work correctly. See [PYTHON314.md](PYTHON314.md) for details.

### System Requirements

- Internet connection (for API calls and web search)
- Valid OpenAI API key with access to the agents library
- Operating System: Windows, Linux, or macOS

### OpenAI API Access

This library requires:
- **OpenAI API Key** - Get one at https://platform.openai.com/api-keys
- **Agents Library Access** - The library uses OpenAI's `openai-agents` package
- **GPT-5-mini Model Access** - The agent uses `gpt-5-mini` model (or compatible)

## Installation Methods

### Method 1: Install from Directory (Recommended)

1. **Navigate to the library directory:**
   ```bash
   cd openai_agent_lookup
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify installation:**
   ```python
   python -c "from openai_agent_lookup import AgentLookupClient; print('OK')"
   ```

### Method 2: Install as Package (Development)

1. **Navigate to the library directory:**
   ```bash
   cd openai_agent_lookup
   ```

2. **Install in development mode:**
   ```bash
   pip install -e .
   ```

   This allows you to edit the source code and changes will be reflected immediately.

### Method 3: Install as Package (Production)

1. **Navigate to the library directory:**
   ```bash
   cd openai_agent_lookup
   ```

2. **Install the package:**
   ```bash
   pip install .
   ```

### Method 4: Copy to Your Project

1. **Copy the entire `openai_agent_lookup` folder** to your project:
   ```
   your_project/
   └── openai_agent_lookup/  ← Copy this entire folder
       ├── __init__.py
       ├── client.py
       ├── agent.py
       ├── models.py
       └── ...
   ```

2. **Install dependencies:**
   ```bash
   pip install -r openai_agent_lookup/requirements.txt
   ```

3. **Use in your code:**
   ```python
   from openai_agent_lookup import AgentLookupClient
   ```

## Dependencies

### Required Packages

The library automatically installs these dependencies:

- `openai-agents>=0.6.0` - OpenAI Agents SDK
- `openai>=2.9.0` - OpenAI Python SDK
- `pydantic>=2.12.0` - Data validation and models
- `python-dotenv>=1.0.0` - Environment variable management
- `nest-asyncio>=1.5.0` - Nested async support

### Optional Development Dependencies

For development and testing:

```bash
pip install -e ".[dev]"
```

This installs:
- `pytest>=7.0.0` - Testing framework
- `pytest-asyncio>=0.21.0` - Async testing support
- `pytest-cov>=4.0.0` - Coverage reporting
- `black>=23.0.0` - Code formatting
- `flake8>=6.0.0` - Linting

## Configuration

### Setting Up OpenAI API Key

#### Option 1: Environment Variable (Recommended)

**Windows PowerShell:**
```powershell
$env:OPENAI_API_KEY="sk-your-api-key-here"
```

**Windows Command Prompt:**
```cmd
set OPENAI_API_KEY=sk-your-api-key-here
```

**Linux/Mac:**
```bash
export OPENAI_API_KEY="sk-your-api-key-here"
```

#### Option 2: .env File

Create a `.env` file in your project root:

```env
OPENAI_API_KEY=sk-your-api-key-here
```

The library will automatically load this using `python-dotenv`.

#### Option 3: In Code (Not Recommended)

```python
import os
os.environ["OPENAI_API_KEY"] = "sk-your-api-key-here"
```

**Note:** Hardcoding API keys in code is not recommended for security reasons.

### Verifying Configuration

Test your setup:

```python
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("OPENAI_API_KEY")
if api_key:
    print(f"✓ API key found: {api_key[:8]}...")
else:
    print("✗ API key not found")
```

## Installation Verification

### Quick Test

Run this to verify everything is working:

```python
from openai_agent_lookup import AgentLookupClient

client = AgentLookupClient()
print("✓ Library imported successfully")
print("✓ Client created successfully")
```

### Full Test

```python
import asyncio
from openai_agent_lookup import AgentLookupClient

async def test():
    client = AgentLookupClient()
    result = await client.lookup_async("Ashley Bergen, Indianapolis")
    print(f"✓ Test lookup successful")
    print(f"  Agent: {result.agent_name}")
    print(f"  Numbers found: {len(result.mobile_numbers)}")

asyncio.run(test())
```

## Troubleshooting

### Import Errors

**Error: `ModuleNotFoundError: No module named 'agents'`**

Solution:
```bash
pip install openai-agents
```

**Error: `ModuleNotFoundError: No module named 'openai_agent_lookup'`**

Solution:
- Make sure you're in the correct directory
- Install the package: `pip install -e .`
- Or add the parent directory to PYTHONPATH

### API Key Issues

**Error: `OpenAI API key not found`**

Solution:
1. Verify the API key is set: `echo $OPENAI_API_KEY` (Linux/Mac) or `echo $env:OPENAI_API_KEY` (PowerShell)
2. Check `.env` file exists and contains `OPENAI_API_KEY=sk-...`
3. Restart your terminal/IDE after setting environment variables

**Error: `Invalid API key`**

Solution:
1. Verify your API key is correct at https://platform.openai.com/api-keys
2. Make sure you have access to the agents library
3. Check your OpenAI account has sufficient credits

### Version Conflicts

**Error: `pydantic` version conflicts**

Solution:
```bash
pip install --upgrade pydantic>=2.12.0
```

**Error: `openai` version conflicts**

Solution:
```bash
pip install --upgrade openai>=2.9.0 openai-agents>=0.6.0
```

### Python Version Issues

**Error: Python version not supported**

Solution:
- Upgrade to Python 3.8 or higher
- Check version: `python --version`

## Next Steps

After installation:

1. **Read the README**: See [README.md](README.md) for usage instructions
2. **Check Examples**: See [EXAMPLES.md](EXAMPLES.md) for code examples
3. **Start Using**: 
   ```python
   from openai_agent_lookup import AgentLookupClient
   client = AgentLookupClient()
   result = client.lookup("Agent Name, Location")
   ```

## Support

If you encounter issues:

1. Check this installation guide
2. Review the troubleshooting section
3. Check OpenAI API status: https://status.openai.com/
4. Open an issue on GitHub with:
   - Python version
   - Error message
   - Steps to reproduce

