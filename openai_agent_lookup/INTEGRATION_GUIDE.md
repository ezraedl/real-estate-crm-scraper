# Server Integration Guide

Complete guide for integrating the OpenAI Agent Lookup Library into your server application.

## Table of Contents

1. [Quick Integration](#quick-integration)
2. [FastAPI Integration](#fastapi-integration)
3. [Flask Integration](#flask-integration)
4. [Django Integration](#django-integration)
5. [Express.js (Python Bridge)](#expressjs-python-bridge)
6. [Best Practices](#best-practices)
7. [Error Handling](#error-handling)
8. [Performance Optimization](#performance-optimization)

## Quick Integration

### Minimal Example

```python
from openai_agent_lookup import AgentLookupClient

# Initialize once (reuse across requests)
client = AgentLookupClient()

# In your request handler
async def handle_lookup(query: str):
    result = await client.lookup_async(query)
    return {
        "agent_name": result.agent_name,
        "mobile_numbers": [
            {"number": m.number, "certainty": m.certainty}
            for m in result.mobile_numbers
        ]
    }
```

## FastAPI Integration

### Complete Example

```python
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from openai_agent_lookup import AgentLookupClient
from pydantic import BaseModel
from typing import Optional

app = FastAPI(title="Agent Lookup API", version="1.0.0")

# Initialize client (singleton pattern)
client = AgentLookupClient()

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request/Response models
class LookupRequest(BaseModel):
    query: str

class LookupResponse(BaseModel):
    agent_name: str
    mobile_numbers: list[dict]
    success: bool
    error: Optional[str] = None

# Endpoints
@app.post("/api/lookup", response_model=LookupResponse)
async def lookup_agent(request: LookupRequest):
    """Look up agent mobile numbers"""
    try:
        result = await client.lookup_async(request.query)
        return LookupResponse(
            agent_name=result.agent_name,
            mobile_numbers=[
                {
                    "number": m.number,
                    "certainty": m.certainty
                }
                for m in result.mobile_numbers
            ],
            success=True
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Lookup failed: {str(e)}"
        )

@app.get("/api/lookup/{agent_name}/{location}")
async def lookup_agent_path(agent_name: str, location: str):
    """Look up agent by path parameters"""
    query = f"{agent_name}, {location}"
    try:
        result = await client.lookup_async(query)
        return result.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/lookup/batch")
async def lookup_batch(queries: list[str]):
    """Batch lookup multiple agents"""
    try:
        results = await client.lookup_batch_async(queries)
        return [r.model_dump() for r in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Health check
@app.get("/health")
async def health():
    return {"status": "healthy", "service": "agent-lookup"}

# Run with: uvicorn main:app --reload
```

### With Background Tasks

```python
from fastapi import BackgroundTasks

@app.post("/api/lookup/async")
async def lookup_async(
    request: LookupRequest,
    background_tasks: BackgroundTasks
):
    """Start lookup in background"""
    result = None
    
    async def do_lookup():
        nonlocal result
        result = await client.lookup_async(request.query)
        # Save to database, send notification, etc.
    
    background_tasks.add_task(do_lookup)
    return {"status": "started", "message": "Lookup in progress"}
```

## Flask Integration

### Complete Example

```python
from flask import Flask, jsonify, request
from flask_cors import CORS
from openai_agent_lookup import AgentLookupClient
import asyncio

app = Flask(__name__)
CORS(app)

# Initialize client
client = AgentLookupClient()

@app.route('/api/lookup', methods=['POST'])
def lookup_agent():
    """Look up agent mobile numbers"""
    data = request.get_json()
    query = data.get('query') if data else request.form.get('query')
    
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
            ],
            "success": True
        })
    except Exception as e:
        return jsonify({
            "error": str(e),
            "success": False
        }), 500

@app.route('/api/lookup/<agent_name>/<location>', methods=['GET'])
def lookup_agent_path(agent_name: str, location: str):
    """Look up agent by path parameters"""
    query = f"{agent_name}, {location}"
    try:
        result = client.lookup(query)
        return jsonify(result.model_dump())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy"})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
```

## Django Integration

### views.py

```python
from django.http import JsonResponse
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.views.decorators.http import require_http_methods
from openai_agent_lookup import AgentLookupClient
import json

@method_decorator(csrf_exempt, name='dispatch')
class AgentLookupView(View):
    """Django view for agent lookup"""
    
    def __init__(self):
        self.client = AgentLookupClient()
    
    async def post(self, request):
        """Handle POST request"""
        try:
            data = json.loads(request.body)
            query = data.get('query')
            
            if not query:
                return JsonResponse(
                    {"error": "query is required"},
                    status=400
                )
            
            result = await self.client.lookup_async(query)
            return JsonResponse({
                "agent_name": result.agent_name,
                "mobile_numbers": [
                    {
                        "number": m.number,
                        "certainty": m.certainty
                    }
                    for m in result.mobile_numbers
                ],
                "success": True
            })
        except Exception as e:
            return JsonResponse(
                {"error": str(e), "success": False},
                status=500
            )
```

### urls.py

```python
from django.urls import path
from .views import AgentLookupView

urlpatterns = [
    path('api/lookup', AgentLookupView.as_view(), name='lookup'),
]
```

## Best Practices

### 1. Client Singleton

```python
# Good: Initialize once, reuse
client = AgentLookupClient()

# Bad: Creating new client for each request
def lookup():
    client = AgentLookupClient()  # Don't do this
```

### 2. Error Handling

```python
try:
    result = await client.lookup_async(query)
    if result.has_results():
        return result
    else:
        return {"error": "No results found"}
except Exception as e:
    logger.error(f"Lookup failed: {e}")
    return {"error": "Lookup service unavailable"}
```

### 3. Timeout Handling

```python
import asyncio

async def lookup_with_timeout(query: str, timeout: int = 120):
    try:
        result = await asyncio.wait_for(
            client.lookup_async(query),
            timeout=timeout
        )
        return result
    except asyncio.TimeoutError:
        return {"error": "Lookup timed out"}
```

### 4. Caching

```python
from functools import lru_cache
import hashlib

cache = {}

async def cached_lookup(query: str):
    cache_key = hashlib.md5(query.encode()).hexdigest()
    
    if cache_key in cache:
        return cache[cache_key]
    
    result = await client.lookup_async(query)
    cache[cache_key] = result
    return result
```

## Performance Optimization

### 1. Connection Pooling

```python
# Use async client for better performance
client = AgentLookupClient()

# Batch operations are more efficient
results = await client.lookup_batch_async(queries)
```

### 2. Rate Limiting

```python
from asyncio import Semaphore

class RateLimitedClient:
    def __init__(self, max_concurrent: int = 5):
        self.client = AgentLookupClient()
        self.semaphore = Semaphore(max_concurrent)
    
    async def lookup(self, query: str):
        async with self.semaphore:
            return await self.client.lookup_async(query)
```

### 3. Background Processing

```python
# Use background tasks for long-running lookups
@app.post("/api/lookup/background")
async def lookup_background(request: LookupRequest, background: BackgroundTasks):
    background.add_task(process_lookup, request.query)
    return {"status": "processing"}
```

## Error Handling

### Comprehensive Error Handling

```python
from openai_agent_lookup import AgentLookupClient
import logging

logger = logging.getLogger(__name__)
client = AgentLookupClient()

async def safe_lookup(query: str):
    try:
        result = await client.lookup_async(query)
        return {
            "success": True,
            "data": result.model_dump()
        }
    except ValueError as e:
        logger.warning(f"Invalid query: {e}")
        return {
            "success": False,
            "error": "Invalid query format"
        }
    except Exception as e:
        logger.error(f"Lookup failed: {e}", exc_info=True)
        return {
            "success": False,
            "error": "Lookup service error"
        }
```

## Testing

### Unit Tests

```python
import pytest
from openai_agent_lookup import AgentLookupClient

@pytest.fixture
def client():
    return AgentLookupClient()

@pytest.mark.asyncio
async def test_lookup(client):
    result = await client.lookup_async("Test Agent, Location")
    assert result.agent_name is not None
```

## Deployment

### Environment Variables

```bash
# Production
OPENAI_API_KEY=sk-prod-...
WORKFLOW_ID=prod-workflow-id

# Development
OPENAI_API_KEY=sk-dev-...
WORKFLOW_ID=dev-workflow-id
```

### Docker Example

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY openai_agent_lookup/ ./openai_agent_lookup/
COPY requirements.txt .

RUN pip install -r requirements.txt

ENV OPENAI_API_KEY=""
ENV PYTHONPATH=/app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

## Monitoring

### Logging

```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def monitored_lookup(query: str):
    logger.info(f"Starting lookup for: {query}")
    start_time = time.time()
    
    try:
        result = await client.lookup_async(query)
        duration = time.time() - start_time
        logger.info(f"Lookup completed in {duration:.2f}s")
        return result
    except Exception as e:
        logger.error(f"Lookup failed: {e}")
        raise
```

This completes the server integration guide. For more examples, see `server_example.py`.

