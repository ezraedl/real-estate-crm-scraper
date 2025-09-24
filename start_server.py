#!/usr/bin/env python3
"""
Startup script for the MLS Scraping Server
"""
import asyncio
import sys
import os
from pathlib import Path

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from main import app
from database import db
from scraper import scraper
from scheduler import scheduler
from proxy_manager import proxy_manager
from config import settings
import uvicorn

async def initialize_services():
    """Initialize all services"""
    print("[INIT] Initializing MLS Scraping Server...")
    
    try:
        # Connect to database
        print("[DB] Connecting to MongoDB...")
        if not await db.connect():
            print("[ERROR] Failed to connect to MongoDB")
            return False
        print("[OK] MongoDB connected")
        
        # Initialize proxy manager
        print("[PROXY] Initializing proxy manager...")
        if settings.DATAIMPULSE_LOGIN:
            await proxy_manager.initialize_dataimpulse_proxies(settings.DATAIMPULSE_LOGIN)
            print("[OK] DataImpulse proxies initialized")
        else:
            print("[WARNING] No DataImpulse login provided - running without proxy support")
        print("[OK] Proxy manager initialized")
        
        # Start background services
        print("[SERVICES] Starting background services...")
        
        # Start scraper service
        asyncio.create_task(scraper.start())
        print("[OK] Scraper service started")
        
        # Start scheduler service
        asyncio.create_task(scheduler.start())
        print("[OK] Scheduler service started")
        
        print("[SUCCESS] All services initialized successfully!")
        return True
        
    except Exception as e:
        print(f"[ERROR] Error initializing services: {e}")
        return False

async def cleanup_services():
    """Cleanup services on shutdown"""
    print("\n[SHUTDOWN] Shutting down services...")
    
    try:
        await scraper.stop()
        await scheduler.stop()
        await db.disconnect()
        print("[OK] Services stopped gracefully")
    except Exception as e:
        print(f"[ERROR] Error during shutdown: {e}")

def main():
    """Main startup function"""
    print("=" * 60)
    print("MLS Scraping Server")
    print("=" * 60)
    print(f"Server: {settings.API_HOST}:{settings.API_PORT}")
    print(f"Database: MongoDB Atlas")
    print(f"Max Concurrent Jobs: {settings.MAX_CONCURRENT_JOBS}")
    print("=" * 60)
    
    # Check if MongoDB is accessible
    try:
        import pymongo
        client = pymongo.MongoClient(settings.MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        print("[OK] MongoDB is accessible")
    except Exception as e:
        print(f"[ERROR] MongoDB connection failed: {e}")
        print("Please ensure MongoDB is running and accessible")
        sys.exit(1)
    
    # Initialize services
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Initialize services
        if not loop.run_until_complete(initialize_services()):
            print("[ERROR] Failed to initialize services")
            sys.exit(1)
        
        # Start the server
        print(f"\n[STARTING] Starting server on http://{settings.API_HOST}:{settings.API_PORT}")
        print("[INFO] API Documentation: http://localhost:8000/docs")
        print("[INFO] Health Check: http://localhost:8000/health")
        print("\nPress Ctrl+C to stop the server")
        print("=" * 60)
        
        # Run the server
        uvicorn.run(
            app,
            host=settings.API_HOST,
            port=settings.API_PORT,
            log_level="info"
        )
        
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Received shutdown signal...")
    except Exception as e:
        print(f"[ERROR] Server error: {e}")
    finally:
        # Cleanup
        loop.run_until_complete(cleanup_services())
        loop.close()

if __name__ == "__main__":
    main()
