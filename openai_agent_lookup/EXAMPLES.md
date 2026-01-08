# Usage Examples

Comprehensive examples for using the OpenAI Agent Lookup Library in various scenarios.

## Table of Contents

1. [Basic Usage](#basic-usage)
2. [Server Integration](#server-integration)
3. [Error Handling](#error-handling)
4. [Batch Processing](#batch-processing)
5. [Advanced Patterns](#advanced-patterns)

## Basic Usage

### Simple Synchronous Lookup

```python
from openai_agent_lookup import AgentLookupClient

client = AgentLookupClient()
result = client.lookup("Ashley Bergen, Indianapolis, IN")

print(f"Agent: {result.agent_name}")
for mobile in result.mobile_numbers:
    print(f"  {mobile.number} - {mobile.certainty:.0%} certainty")
```

### Async Lookup

```python
import asyncio
from openai_agent_lookup import AgentLookupClient

async def find_agent():
    client = AgentLookupClient()
    result = await client.lookup_async("Ashley Bergen, Indianapolis")
    
    if result.has_results():
        best = result.get_best_number()
        print(f"Best number: {best.number}")
    else:
        print("No numbers found")

asyncio.run(find_agent())
```

### Using LookupRequest

```python
from openai_agent_lookup import AgentLookupClient, LookupRequest

client = AgentLookupClient()
request = LookupRequest(query="Ashley Bergen, Indianapolis, IN")
result = client.lookup(request)
```

## Server Integration

### FastAPI Example

```python
from fastapi import FastAPI, HTTPException
from openai_agent_lookup import AgentLookupClient, LookupRequest
from pydantic import BaseModel

app = FastAPI()
client = AgentLookupClient()

class LookupQuery(BaseModel):
    query: str

@app.post("/api/lookup")
async def lookup_agent(query: LookupQuery):
    try:
        result = await client.lookup_async(query.query)
        return {
            "agent_name": result.agent_name,
            "mobile_numbers": [
                {
                    "number": m.number,
                    "certainty": m.certainty
                }
                for m in result.mobile_numbers
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/lookup/{agent_name}/{location}")
async def lookup_agent_path(agent_name: str, location: str):
    query = f"{agent_name}, {location}"
    result = await client.lookup_async(query)
    return result.model_dump()
```

### Flask Example

```python
from flask import Flask, jsonify, request
from openai_agent_lookup import AgentLookupClient
import asyncio

app = Flask(__name__)
client = AgentLookupClient()

@app.route('/api/lookup', methods=['POST'])
def lookup_agent():
    data = request.json
    query = data.get('query')
    
    if not query:
        return jsonify({"error": "query is required"}), 400
    
    try:
        result = client.lookup(query)
        return jsonify({
            "agent_name": result.agent_name,
            "mobile_numbers": [
                {
                    "number": m.number,
                    "certainty": m.certainty
                }
                for m in result.mobile_numbers
            ]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
```

### Django Example

```python
from django.http import JsonResponse
from django.views import View
from openai_agent_lookup import AgentLookupClient
import asyncio

class AgentLookupView(View):
    def __init__(self):
        self.client = AgentLookupClient()
    
    async def post(self, request):
        query = request.POST.get('query')
        if not query:
            return JsonResponse({"error": "query is required"}, status=400)
        
        try:
            result = await self.client.lookup_async(query)
            return JsonResponse({
                "agent_name": result.agent_name,
                "mobile_numbers": [
                    {
                        "number": m.number,
                        "certainty": m.certainty
                    }
                    for m in result.mobile_numbers
                ]
            })
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)
```

## Error Handling

### Basic Error Handling

```python
from openai_agent_lookup import AgentLookupClient

client = AgentLookupClient()

try:
    result = client.lookup("Agent Name, Location")
    
    if result.has_results():
        print(f"Found {len(result.mobile_numbers)} numbers")
    else:
        print("No numbers found - agent may not have been 80%+ certain")
        
except ValueError as e:
    print(f"Invalid input: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

### Advanced Error Handling with Retries

```python
import asyncio
from openai_agent_lookup import AgentLookupClient
from tenacity import retry, stop_after_attempt, wait_exponential

client = AgentLookupClient()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
async def lookup_with_retry(query: str):
    return await client.lookup_async(query)

async def main():
    try:
        result = await lookup_with_retry("Ashley Bergen, Indianapolis")
        print(result.agent_name)
    except Exception as e:
        print(f"Failed after retries: {e}")

asyncio.run(main())
```

## Batch Processing

### Simple Batch

```python
from openai_agent_lookup import AgentLookupClient

client = AgentLookupClient()

queries = [
    "Ashley Bergen, Indianapolis",
    "John Smith, Chicago, IL",
    "Jane Doe, New York, NY"
]

results = client.lookup_batch(queries)

for result in results:
    print(f"{result.agent_name}: {len(result.mobile_numbers)} numbers")
```

### Async Batch with Progress

```python
import asyncio
from openai_agent_lookup import AgentLookupClient

async def batch_lookup_with_progress():
    client = AgentLookupClient()
    
    queries = [
        "Ashley Bergen, Indianapolis",
        "John Smith, Chicago",
        "Jane Doe, New York"
    ]
    
    results = await client.lookup_batch_async(queries)
    
    for i, result in enumerate(results, 1):
        print(f"[{i}/{len(results)}] {result.agent_name}")
        if result.has_results():
            best = result.get_best_number()
            print(f"  Best: {best.number} ({best.certainty:.0%})")
        else:
            print("  No numbers found")

asyncio.run(batch_lookup_with_progress())
```

### Batch with Filtering

```python
from openai_agent_lookup import AgentLookupClient

client = AgentLookupClient()

queries = ["Agent 1", "Agent 2", "Agent 3"]
results = client.lookup_batch(queries)

# Filter results with high confidence
high_confidence = [
    r for r in results 
    if r.get_numbers_above_threshold(0.9)
]

print(f"Found {len(high_confidence)} agents with 90%+ confidence")
```

## Advanced Patterns

### Custom Workflow ID

```python
from openai_agent_lookup import AgentLookupClient

# Use custom workflow ID for tracing in OpenAI dashboard
client = AgentLookupClient(workflow_id="my-custom-workflow-id")
result = client.lookup("Ashley Bergen, Indianapolis")
```

### Integration with Database

```python
import asyncio
from openai_agent_lookup import AgentLookupClient
from typing import List

class AgentService:
    def __init__(self):
        self.client = AgentLookupClient()
    
    async def lookup_and_save(self, agent_name: str, location: str):
        query = f"{agent_name}, {location}"
        result = await self.client.lookup_async(query)
        
        # Save to database
        agent_data = {
            "name": result.agent_name,
            "mobile_numbers": [
                {
                    "number": m.number,
                    "certainty": m.certainty
                }
                for m in result.mobile_numbers
            ]
        }
        
        # Your database save logic here
        # db.save(agent_data)
        
        return agent_data

# Usage
service = AgentService()
result = asyncio.run(service.lookup_and_save("Ashley Bergen", "Indianapolis"))
```

### Caching Results

```python
from openai_agent_lookup import AgentLookupClient
from functools import lru_cache
import asyncio

client = AgentLookupClient()

# Simple in-memory cache
cache = {}

async def cached_lookup(query: str):
    if query in cache:
        return cache[query]
    
    result = await client.lookup_async(query)
    cache[query] = result
    return result

# Usage
result = asyncio.run(cached_lookup("Ashley Bergen, Indianapolis"))
```

### Rate Limiting

```python
import asyncio
from openai_agent_lookup import AgentLookupClient
from asyncio import Semaphore

class RateLimitedClient:
    def __init__(self, max_concurrent: int = 3):
        self.client = AgentLookupClient()
        self.semaphore = Semaphore(max_concurrent)
    
    async def lookup(self, query: str):
        async with self.semaphore:
            return await self.client.lookup_async(query)

# Usage - limits to 3 concurrent lookups
limited_client = RateLimitedClient(max_concurrent=3)
results = await asyncio.gather(*[
    limited_client.lookup(f"Agent {i}, Location")
    for i in range(10)
])
```

### Custom Query Formatting

```python
from openai_agent_lookup import AgentLookupClient, LookupRequest

def format_query(agent_name: str, location: str, company: str = None) -> str:
    query = f"{agent_name}, {location}"
    if company:
        query += f", {company}"
    return query

client = AgentLookupClient()

# Use formatted query
query = format_query("Ashley Bergen", "Indianapolis, IN", "Keller Williams")
result = client.lookup(query)
```

