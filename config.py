import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_required_env(key: str, default: str = None) -> str:
    """Get required environment variable or return default"""
    value = os.getenv(key)
    if value is None:
        if default is not None:
            return default
        raise ValueError(f"Required environment variable {key} is not set. Please check your .env file.")
    return value

def get_required_env_int(key: str) -> int:
    """Get required environment variable as int or raise error"""
    value = get_required_env(key)
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"Environment variable {key} must be an integer, got: {value}")

def get_required_env_float(key: str) -> float:
    """Get required environment variable as float or raise error"""
    value = get_required_env(key)
    try:
        return float(value)
    except ValueError:
        raise ValueError(f"Environment variable {key} must be a float, got: {value}")


class Settings:
    # MongoDB Configuration - Optional for basic functionality
    MONGODB_URI = get_required_env("MONGODB_URI", "mongodb://localhost:27017")
    
    # API Configuration - Default values for Railway
    API_HOST = get_required_env("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("PORT", "8000"))  # Use Railway's PORT variable
    
    # Scraping Configuration - Default values
    # Keep concurrency low to avoid "can't start new thread" (HomeHarvest/Playwright spawn many threads per request)
    MAX_CONCURRENT_JOBS = int(get_required_env("MAX_CONCURRENT_JOBS", "2"))
    THREAD_POOL_WORKERS = int(get_required_env("THREAD_POOL_WORKERS", "1"))
    REQUEST_DELAY = float(get_required_env("REQUEST_DELAY", "1.0"))
    MAX_RETRIES = int(get_required_env("MAX_RETRIES", "3"))
    # Location timeout: if a location hasn't added/updated properties in this many minutes, mark it as failed
    # Default: 30 minutes - if no properties are added/updated in 30 minutes, the location is likely stuck
    LOCATION_TIMEOUT_MINUTES = int(get_required_env("LOCATION_TIMEOUT_MINUTES", "30"))
    
    # Rate Limiting - Default values
    # Reduced from 60 to 10 requests per minute to avoid blocking
    # This means minimum 6 seconds between requests
    RATE_LIMIT_PER_MINUTE = int(get_required_env("RATE_LIMIT_PER_MINUTE", "10"))
    
    # DataImpulse Configuration - Optional
    # Set USE_DATAIMPULSE=false to disable DataImpulse proxy even if credentials are provided
    USE_DATAIMPULSE = os.getenv("USE_DATAIMPULSE", "true").lower() in ("true", "1", "yes", "on")
    DATAIMPULSE_LOGIN = get_required_env("DATAIMPULSE_LOGIN", "")
    DATAIMPULSE_PASSWORD = get_required_env("DATAIMPULSE_PASSWORD", "")
    DATAIMPULSE_ENDPOINT = get_required_env("DATAIMPULSE_ENDPOINT", "")
    
    # Logging Configuration - Default values
    LOG_LEVEL = get_required_env("LOG_LEVEL", "INFO")
    
    # Enrichment Configuration - Default values (kept moderate to avoid thread exhaustion)
    ENRICHMENT_WORKERS = int(get_required_env("ENRICHMENT_WORKERS", "2"))  # Number of parallel enrichment threads
    _enrichment_batch_size = os.getenv("ENRICHMENT_BATCH_SIZE")
    ENRICHMENT_BATCH_SIZE = int(_enrichment_batch_size) if _enrichment_batch_size else None  # None = process all properties from location
    
    # RentCast Configuration - Default values (kept low to avoid thread/browser exhaustion)
    RENTCAST_WORKERS = int(get_required_env("RENTCAST_WORKERS", "1"))  # Number of parallel RentCast workers (kept low to prioritize HomeHarvest)
    RENTCAST_API_TIMEOUT = int(get_required_env("RENTCAST_API_TIMEOUT", "30"))  # Timeout for direct API calls (seconds)
    RENTCAST_USE_PLAYWRIGHT_FALLBACK = os.getenv("RENTCAST_USE_PLAYWRIGHT_FALLBACK", "true").lower() in ("true", "1", "yes", "on")  # Use Playwright if API fails
    RENTCAST_API_RETRIES = int(get_required_env("RENTCAST_API_RETRIES", "2"))  # Number of retries for direct API before falling back
    
    # Additional Configuration - Default values
    PORT = int(get_required_env("PORT", "8000"))
    JWT_SECRET = get_required_env("JWT_SECRET", "default-secret-key")
    NODE_ENV = get_required_env("NODE_ENV", "production")
    GOOGLE_CLIENT_ID = get_required_env("GOOGLE_CLIENT_ID", "")
    
    # CORS Configuration
    CORS_ORIGINS = get_required_env("CORS_ORIGINS", "*")

    # Backend URL for post-scrape hooks (census backfill, etc.)
    # If not set, census backfill is skipped. Auth uses JWT signed with JWT_SECRET (aud: census-backfill).
    BACKEND_URL = (os.getenv("BACKEND_URL") or "").strip().rstrip("/")

    # Rentcast: scrape app.rentcast.io (unauthenticated) for rent estimates. Set to false to disable.
    RENTCAST_ENABLED = os.getenv("RENTCAST_ENABLED", "true").lower() in ("true", "1", "yes", "on")

    # Optional: API key for /rent-estimation/backfill. If set, X-API-Key header can be used instead of JWT.
    RENT_BACKFILL_API_KEY = (os.getenv("RENT_BACKFILL_API_KEY") or "").strip() or None

settings = Settings()
