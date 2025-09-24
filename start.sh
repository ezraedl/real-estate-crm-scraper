#!/bin/bash

# Set default port if not provided
export PORT=${PORT:-8000}

# Start the FastAPI application
uvicorn main:app --host 0.0.0.0 --port $PORT
