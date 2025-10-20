"""
Setup script to help you get MongoDB working and start scraping Indianapolis properties.
"""

import json
import os
import sys
from datetime import datetime

def check_mongodb_status():
    """Check if MongoDB is running and accessible"""
    print("=== Checking MongoDB Status ===\n")
    
    try:
        import pymongo
        print("✅ pymongo module is installed")
        
        # Try to connect to MongoDB
        client = pymongo.MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)
        server_info = client.server_info()
        print(f"✅ MongoDB is running - Version: {server_info['version']}")
        
        # Check if our database exists
        db = client["mls_scraper"]
        collections = db.list_collection_names()
        print(f"✅ Connected to database: mls_scraper")
        print(f"📊 Current collections: {collections}")
        
        # Check properties collection
        if "properties" in collections:
            count = db.properties.count_documents({})
            print(f"📊 Properties collection exists with {count} documents")
        else:
            print("❌ Properties collection does not exist")
            print("   This is normal - it will be created when you first save data")
        
        client.close()
        return True
        
    except ImportError:
        print("❌ pymongo module not installed")
        print("   Run: pip install pymongo")
        return False
        
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        print("   Make sure MongoDB is running:")
        print("   1. Install MongoDB if not installed")
        print("   2. Start MongoDB service: mongod")
        print("   3. Or use MongoDB Atlas cloud service")
        return False

def show_setup_instructions():
    """Show setup instructions"""
    print("\n=== Setup Instructions ===\n")
    
    print("1. MongoDB Setup:")
    print("   Option A - Local MongoDB:")
    print("     - Install MongoDB: https://www.mongodb.com/try/download/community")
    print("     - Start MongoDB: mongod")
    print("     - Create .env file with: MONGODB_URL=mongodb://localhost:27017")
    
    print("\n   Option B - MongoDB Atlas (Cloud):")
    print("     - Sign up at: https://www.mongodb.com/atlas")
    print("     - Create a cluster")
    print("     - Get connection string")
    print("     - Create .env file with: MONGODB_URL=your_atlas_connection_string")
    
    print("\n2. Create .env file:")
    print("   Copy env_example.txt to .env and update with your MongoDB URL")
    
    print("\n3. Install dependencies:")
    print("   pip install -r requirements.txt")
    
    print("\n4. Test the setup:")
    print("   python test_mongodb_connection.py")
    
    print("\n5. Start scraping:")
    print("   python scrape_indianapolis.py")

def create_env_file():
    """Create a .env file from the example"""
    print("\n=== Creating .env File ===\n")
    
    try:
        # Read the example file
        with open("env_example.txt", "r") as f:
            env_content = f.read()
        
        # Create .env file
        with open(".env", "w") as f:
            f.write(env_content)
        
        print("✅ Created .env file from env_example.txt")
        print("📝 Edit .env file to add your MongoDB connection string")
        
    except FileNotFoundError:
        print("❌ env_example.txt not found")
        print("   Creating basic .env file...")
        
        basic_env = """# MongoDB Configuration
MONGODB_URL=mongodb://localhost:27017
DATABASE_NAME=mls_scraper

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000

# Scraping Configuration
MAX_CONCURRENT_JOBS=5
REQUEST_DELAY=1.0
MAX_RETRIES=3
RATE_LIMIT_PER_MINUTE=60

# DataImpulse Proxy Configuration
DATAIMPULSE_API_KEY=your_dataimpulse_api_key_here
DATAIMPULSE_ENDPOINT=https://api.dataimpulse.com/v1/proxies

# Logging
LOG_LEVEL=INFO
"""
        
        with open(".env", "w") as f:
            f.write(basic_env)
        
        print("✅ Created basic .env file")

def show_quick_start():
    """Show quick start instructions"""
    print("\n=== Quick Start Guide ===\n")
    
    print("🚀 To start scraping Indianapolis properties:")
    print("\n1. Make sure MongoDB is running")
    print("2. Run: python test_mongodb_connection.py")
    print("3. Run: python scrape_indianapolis.py")
    
    print("\n🔄 To set up recurring jobs:")
    print("1. Run: python create_recurring_indianapolis_job.py")
    print("2. Run: python start_scheduler.py")
    
    print("\n📊 To check your data:")
    print("1. Connect to MongoDB: mongosh")
    print("2. Use database: use mls_scraper")
    print("3. Check properties: db.properties.find()")
    print("4. Count properties: db.properties.countDocuments()")

def main():
    """Main setup function"""
    print("🏠 Indianapolis Property Scraper Setup")
    print("=" * 50)
    
    # Check MongoDB status
    mongodb_ok = check_mongodb_status()
    
    # Create .env file if it doesn't exist
    if not os.path.exists(".env"):
        create_env_file()
    else:
        print("\n✅ .env file already exists")
    
    # Show setup instructions
    show_setup_instructions()
    
    # Show quick start
    show_quick_start()
    
    if mongodb_ok:
        print("\n🎉 MongoDB is working! You can now start scraping.")
        print("   Run: python test_mongodb_connection.py")
    else:
        print("\n⚠️  MongoDB needs to be set up before you can start scraping.")
        print("   Follow the setup instructions above.")

if __name__ == "__main__":
    main()
