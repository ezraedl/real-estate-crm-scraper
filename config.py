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
    API_PORT = int(get_required_env("API_PORT", "8000"))
    
    # Scraping Configuration - Default values
    MAX_CONCURRENT_JOBS = int(get_required_env("MAX_CONCURRENT_JOBS", "3"))
    REQUEST_DELAY = float(get_required_env("REQUEST_DELAY", "1.0"))
    MAX_RETRIES = int(get_required_env("MAX_RETRIES", "3"))
    
    # Rate Limiting - Default values
    RATE_LIMIT_PER_MINUTE = int(get_required_env("RATE_LIMIT_PER_MINUTE", "60"))
    
    # DataImpulse Configuration - Optional
    DATAIMPULSE_LOGIN = get_required_env("DATAIMPULSE_LOGIN", "")
    DATAIMPULSE_PASSWORD = get_required_env("DATAIMPULSE_PASSWORD", "")
    DATAIMPULSE_ENDPOINT = get_required_env("DATAIMPULSE_ENDPOINT", "")
    
    # Logging Configuration - Default values
    LOG_LEVEL = get_required_env("LOG_LEVEL", "INFO")
    
    # Additional Configuration - Default values
    PORT = int(get_required_env("PORT", "8000"))
    JWT_SECRET = get_required_env("JWT_SECRET", "default-secret-key")
    NODE_ENV = get_required_env("NODE_ENV", "production")
    GOOGLE_CLIENT_ID = get_required_env("GOOGLE_CLIENT_ID", "")

settings = Settings()
