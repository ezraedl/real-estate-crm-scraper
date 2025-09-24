from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
from enum import Enum
from bson import ObjectId
import hashlib
import json

class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class JobPriority(str, Enum):
    IMMEDIATE = "immediate"
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"

class PropertyType(str, Enum):
    SINGLE_FAMILY = "single_family"
    MULTI_FAMILY = "multi_family"
    CONDOS = "condos"
    CONDO_TOWNHOME_ROWHOUSE_COOP = "condo_townhome_rowhome_coop"
    CONDO_TOWNHOME = "condo_townhome"
    TOWNHOMES = "townhomes"
    DUPLEX_TRIPLEX = "duplex_triplex"
    FARM = "farm"
    LAND = "land"
    MOBILE = "mobile"

class ListingType(str, Enum):
    FOR_SALE = "for_sale"
    FOR_RENT = "for_rent"
    SOLD = "sold"
    PENDING = "pending"
    # Note: OFF_MARKET is not supported by homeharvest library
    # OFF_MARKET = "off_market"  # Disabled - not supported by homeharvest

class ScrapingJob(BaseModel):
    id: Optional[str] = Field(default=None, alias="_id")
    job_id: str = Field(..., description="Unique job identifier")
    priority: JobPriority = Field(default=JobPriority.NORMAL)
    status: JobStatus = Field(default=JobStatus.PENDING)
    
    # Scraping parameters
    locations: List[str] = Field(..., description="List of locations to scrape")
    listing_type: Optional[ListingType] = None  # Make listing_type optional
    property_types: Optional[List[PropertyType]] = None
    past_days: Optional[int] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    radius: Optional[float] = None
    mls_only: bool = False
    foreclosure: bool = False
    exclude_pending: bool = False
    limit: int = Field(default=10000, le=10000)
    
    # Scheduling
    scheduled_at: Optional[datetime] = None
    cron_expression: Optional[str] = None  # For recurring jobs
    
    # Execution tracking
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    
    # Run tracking for recurring jobs
    run_count: int = Field(default=0, description="Number of times this recurring job has been executed")
    last_run: Optional[datetime] = Field(default=None, description="Timestamp of the last execution")
    last_run_status: Optional[JobStatus] = Field(default=None, description="Status of the last execution")
    last_run_job_id: Optional[str] = Field(default=None, description="Job ID of the last execution instance")
    original_job_id: Optional[str] = Field(default=None, description="Original job ID for recurring job instances")
    
    # Results
    properties_scraped: int = 0
    properties_saved: int = 0
    total_locations: int = 0
    completed_locations: int = 0
    
    # Anti-bot configuration
    proxy_config: Optional[Dict[str, Any]] = None
    user_agent: Optional[str] = None
    request_delay: float = Field(default=1.0, ge=0.1, le=10.0)
    
    class Config:
        use_enum_values = True
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.isoformat()
        }

class PropertyAddress(BaseModel):
    street: Optional[str] = None
    unit: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    formatted_address: Optional[str] = None
    full_street_line: Optional[str] = None

class PropertyDescription(BaseModel):
    style: Optional[str] = None
    beds: Optional[int] = None
    full_baths: Optional[int] = None
    half_baths: Optional[int] = None
    sqft: Optional[int] = None
    year_built: Optional[int] = None
    stories: Optional[int] = None
    garage: Optional[str] = None
    lot_sqft: Optional[int] = None
    text: Optional[str] = None
    property_type: Optional[str] = None

class PropertyFinancial(BaseModel):
    list_price: Optional[float] = None
    list_price_min: Optional[float] = None
    list_price_max: Optional[float] = None
    sold_price: Optional[float] = None
    last_sold_price: Optional[float] = None
    price_per_sqft: Optional[float] = None
    estimated_value: Optional[float] = None
    tax_assessed_value: Optional[float] = None
    hoa_fee: Optional[float] = None
    tax: Optional[float] = None

class PropertyDates(BaseModel):
    list_date: Optional[datetime] = None
    pending_date: Optional[datetime] = None
    last_sold_date: Optional[datetime] = None

class PropertyLocation(BaseModel):
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    neighborhoods: Optional[List[str]] = None
    county: Optional[str] = None
    fips_code: Optional[str] = None
    parcel_number: Optional[str] = None

class PropertyAgent(BaseModel):
    agent_id: Optional[str] = None
    agent_name: Optional[str] = None
    agent_email: Optional[str] = None
    agent_phones: Optional[List[Dict[str, Any]]] = None
    agent_mls_set: Optional[str] = None
    agent_nrds_id: Optional[str] = None

class PropertyBroker(BaseModel):
    broker_id: Optional[str] = None
    broker_name: Optional[str] = None

class PropertyBuilder(BaseModel):
    builder_id: Optional[str] = None
    builder_name: Optional[str] = None

class PropertyOffice(BaseModel):
    office_id: Optional[str] = None
    office_mls_set: Optional[str] = None
    office_name: Optional[str] = None
    office_email: Optional[str] = None
    office_phones: Optional[List[Dict[str, Any]]] = None

class Property(BaseModel):
    id: Optional[str] = Field(default=None, alias="_id")
    property_id: str = Field(..., description="Unique property identifier (hash of formatted_address)")
    mls_id: Optional[str] = None
    mls: Optional[str] = None
    status: Optional[str] = None
    mls_status: Optional[str] = None
    listing_type: Optional[str] = None  # Property type: "for_sale", "pending", "sold", "for_rent"
    
    # Core data
    address: PropertyAddress
    description: PropertyDescription
    financial: PropertyFinancial
    dates: PropertyDates
    location: PropertyLocation
    
    # Contact information
    agent: Optional[PropertyAgent] = None
    broker: Optional[PropertyBroker] = None
    builder: Optional[PropertyBuilder] = None
    office: Optional[PropertyOffice] = None
    
    # URLs and references
    property_url: Optional[str] = None
    listing_id: Optional[str] = None
    permalink: Optional[str] = None
    
    # Property images
    primary_photo: Optional[str] = None
    alt_photos: Optional[List[str]] = None
    
    # Additional data
    days_on_mls: Optional[int] = None
    new_construction: Optional[bool] = None
    monthly_fees: Optional[List[Dict[str, Any]]] = None
    one_time_fees: Optional[List[Dict[str, Any]]] = None
    tax_history: Optional[List[Dict[str, Any]]] = None
    nearby_schools: Optional[List[Dict[str, Any]]] = None
    
    # Content change tracking
    content_hash: Optional[str] = None  # Hash of all content fields (excluding days_on_mls)
    last_content_updated: Optional[datetime] = None  # When content_hash last changed
    
    # Metadata
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    job_id: Optional[str] = None
    source: str = Field(default="homeharvest")
    is_comp: bool = Field(default=False, description="Whether this property is a comp for another property")
    
    def generate_property_id(self) -> str:
        """Generate property_id as hash of formatted_address"""
        if not self.address or not self.address.formatted_address:
            return None
        
        # Create hash of formatted address
        address_str = self.address.formatted_address.strip().lower()
        return hashlib.md5(address_str.encode('utf-8')).hexdigest()
    
    def generate_content_hash(self) -> str:
        """Generate hash of all content fields except days_on_mls and metadata"""
        content_data = {
            # Basic property info
            "mls_id": self.mls_id,
            "mls": self.mls,
            "status": self.status,
            "mls_status": self.mls_status,
            "listing_type": self.listing_type,
            
            # Address data
            "address": {
                "street": self.address.street if self.address else None,
                "unit": self.address.unit if self.address else None,
                "city": self.address.city if self.address else None,
                "state": self.address.state if self.address else None,
                "zip_code": self.address.zip_code if self.address else None,
                "formatted_address": self.address.formatted_address if self.address else None,
                "full_street_line": self.address.full_street_line if self.address else None,
            } if self.address else {},
            
            # Description data
            "description": {
                "style": self.description.style if self.description else None,
                "beds": self.description.beds if self.description else None,
                "full_baths": self.description.full_baths if self.description else None,
                "half_baths": self.description.half_baths if self.description else None,
                "sqft": self.description.sqft if self.description else None,
                "year_built": self.description.year_built if self.description else None,
                "stories": self.description.stories if self.description else None,
                "garage": self.description.garage if self.description else None,
                "lot_sqft": self.description.lot_sqft if self.description else None,
                "text": self.description.text if self.description else None,
                "property_type": self.description.property_type if self.description else None,
            } if self.description else {},
            
            # Financial data
            "financial": {
                "list_price": self.financial.list_price if self.financial else None,
                "list_price_min": self.financial.list_price_min if self.financial else None,
                "list_price_max": self.financial.list_price_max if self.financial else None,
                "sold_price": self.financial.sold_price if self.financial else None,
                "last_sold_price": self.financial.last_sold_price if self.financial else None,
                "price_per_sqft": self.financial.price_per_sqft if self.financial else None,
                "estimated_value": self.financial.estimated_value if self.financial else None,
                "tax_assessed_value": self.financial.tax_assessed_value if self.financial else None,
                "hoa_fee": self.financial.hoa_fee if self.financial else None,
                "tax": self.financial.tax if self.financial else None,
            } if self.financial else {},
            
            # Dates data
            "dates": {
                "list_date": self.dates.list_date.isoformat() if self.dates and self.dates.list_date else None,
                "pending_date": self.dates.pending_date.isoformat() if self.dates and self.dates.pending_date else None,
                "last_sold_date": self.dates.last_sold_date.isoformat() if self.dates and self.dates.last_sold_date else None,
            } if self.dates else {},
            
            # Location data
            "location": {
                "latitude": self.location.latitude if self.location else None,
                "longitude": self.location.longitude if self.location else None,
                "neighborhoods": self.location.neighborhoods if self.location else None,
                "county": self.location.county if self.location else None,
                "fips_code": self.location.fips_code if self.location else None,
                "parcel_number": self.location.parcel_number if self.location else None,
            } if self.location else {},
            
            # URLs and references
            "property_url": self.property_url,
            "listing_id": self.listing_id,
            "permalink": self.permalink,
            
            # Property images
            "primary_photo": self.primary_photo,
            "alt_photos": self.alt_photos,
            
            # Contact information
            "agent": {
                "agent_id": self.agent.agent_id if self.agent else None,
                "agent_name": self.agent.agent_name if self.agent else None,
                "agent_email": self.agent.agent_email if self.agent else None,
                "agent_phones": self.agent.agent_phones if self.agent else None,
                "agent_mls_set": self.agent.agent_mls_set if self.agent else None,
                "agent_nrds_id": self.agent.agent_nrds_id if self.agent else None,
            } if self.agent else {},
            
            "broker": {
                "broker_id": self.broker.broker_id if self.broker else None,
                "broker_name": self.broker.broker_name if self.broker else None,
            } if self.broker else {},
            
            "builder": {
                "builder_id": self.builder.builder_id if self.builder else None,
                "builder_name": self.builder.builder_name if self.builder else None,
            } if self.builder else {},
            
            "office": {
                "office_id": self.office.office_id if self.office else None,
                "office_mls_set": self.office.office_mls_set if self.office else None,
                "office_name": self.office.office_name if self.office else None,
                "office_email": self.office.office_email if self.office else None,
                "office_phones": self.office.office_phones if self.office else None,
            } if self.office else {},
            
            # Additional data (excluding days_on_mls)
            "new_construction": self.new_construction,
            "monthly_fees": self.monthly_fees,
            "one_time_fees": self.one_time_fees,
            "tax_history": self.tax_history,
            "nearby_schools": self.nearby_schools,
            
            # Metadata
            "is_comp": self.is_comp,
        }
        
        # Convert to JSON string and hash it
        content_json = json.dumps(content_data, sort_keys=True, default=str)
        return hashlib.sha256(content_json.encode('utf-8')).hexdigest()
    
    def update_content_tracking(self):
        """Update content hash and timestamp if content has changed"""
        new_hash = self.generate_content_hash()
        
        if self.content_hash != new_hash:
            self.content_hash = new_hash
            self.last_content_updated = datetime.utcnow()
            return True  # Content changed
        
        return False  # No change
    
    class Config:
        use_enum_values = True
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.isoformat()
        }

class ImmediateScrapeRequest(BaseModel):
    locations: List[str] = Field(..., min_items=1, max_items=10)
    listing_type: Optional[ListingType] = None  # Make listing_type optional
    property_types: Optional[List[PropertyType]] = None
    radius: Optional[float] = Field(None, ge=0.1, le=50.0)
    mls_only: bool = False
    foreclosure: bool = False
    limit: int = Field(default=100, le=1000)  # Lower limit for immediate requests
    past_days: Optional[int] = Field(None, ge=1, le=365)  # Focus on sold properties from last N days

class ScheduledScrapeRequest(BaseModel):
    locations: List[str] = Field(..., min_items=1)
    listing_type: ListingType
    property_types: Optional[List[PropertyType]] = None
    past_days: Optional[int] = Field(None, ge=1, le=365)
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    radius: Optional[float] = Field(None, ge=0.1, le=50.0)
    mls_only: bool = False
    foreclosure: bool = False
    exclude_pending: bool = False
    limit: int = Field(default=10000, le=10000)
    
    # Scheduling
    scheduled_at: Optional[datetime] = None
    cron_expression: Optional[str] = None

class TriggerJobRequest(BaseModel):
    """Request to trigger an existing job immediately"""
    job_id: str = Field(..., description="ID of the existing job to trigger")
    priority: Optional[JobPriority] = Field(default=JobPriority.IMMEDIATE, description="Priority for the immediate run")

class JobResponse(BaseModel):
    job_id: str
    status: JobStatus
    message: str
    created_at: datetime

class JobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    progress: Dict[str, Any]
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None

class ImmediateScrapeResponse(BaseModel):
    job_id: str
    status: JobStatus
    message: str
    created_at: datetime
    completed_at: Optional[datetime] = None
    execution_time_seconds: Optional[float] = None
    properties_scraped: int = 0
    properties_saved: int = 0
    properties: List[Property] = []
