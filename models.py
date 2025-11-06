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

class ScheduledJobStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PAUSED = "paused"
    DELETED = "deleted"

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

class ScheduledJob(BaseModel):
    """
    Represents a recurring scheduled job (cron job).
    This defines WHAT and WHEN to scrape, but not individual executions.
    """
    id: Optional[str] = Field(default=None, alias="_id")
    scheduled_job_id: str = Field(..., description="Unique scheduled job identifier")
    name: str = Field(..., description="Human-readable name for the scheduled job")
    description: Optional[str] = Field(None, description="Description of what this job does")
    status: ScheduledJobStatus = Field(default=ScheduledJobStatus.ACTIVE)
    
    # Cron configuration
    cron_expression: str = Field(..., description="Cron expression for scheduling")
    timezone: str = Field(default="UTC", description="Timezone for cron schedule")
    
    # Scraping parameters (template for job instances)
    locations: List[str] = Field(..., description="List of locations to scrape")
    listing_types: Optional[List[ListingType]] = Field(default=None, description="List of listing types to scrape (for_sale, sold, etc.)")
    listing_type: Optional[ListingType] = Field(default=None, description="DEPRECATED: Use listing_types instead")
    property_types: Optional[List[PropertyType]] = None
    past_days: Optional[int] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    radius: Optional[float] = None
    mls_only: bool = False
    foreclosure: bool = False
    exclude_pending: bool = False
    limit: int = Field(default=10000, le=10000)
    
    # Execution tracking
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: Optional[str] = Field(None, description="User or system that created this scheduled job")
    
    # Run history
    run_count: int = Field(default=0, description="Total number of times this job has been executed")
    last_run_at: Optional[datetime] = Field(None, description="When this job last ran")
    last_run_status: Optional[JobStatus] = Field(None, description="Status of the last run")
    last_run_job_id: Optional[str] = Field(None, description="Job ID of the last execution")
    next_run_at: Optional[datetime] = Field(None, description="Calculated next run time")
    
    # Anti-bot configuration (defaults for job instances)
    proxy_config: Optional[Dict[str, Any]] = None
    user_agent: Optional[str] = None
    request_delay: float = Field(default=1.0, ge=0.1, le=10.0)
    
    # Priority for created jobs
    priority: JobPriority = Field(default=JobPriority.NORMAL)
    
    class Config:
        use_enum_values = True
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.isoformat()
        }

class ScrapingJob(BaseModel):
    """
    Represents a single job execution (one-time or instance of a scheduled job).
    This tracks the actual execution and results of a scraping operation.
    """
    id: Optional[str] = Field(default=None, alias="_id")
    job_id: str = Field(..., description="Unique job identifier")
    priority: JobPriority = Field(default=JobPriority.NORMAL)
    status: JobStatus = Field(default=JobStatus.PENDING)
    
    # Reference to scheduled job (if this is an instance of a recurring job)
    scheduled_job_id: Optional[str] = Field(None, description="ID of the parent scheduled job (for recurring jobs)")
    
    # Scraping parameters
    locations: List[str] = Field(..., description="List of locations to scrape")
    listing_types: Optional[List[ListingType]] = Field(default=None, description="List of listing types to scrape (for_sale, sold, etc.)")
    listing_type: Optional[ListingType] = Field(default=None, description="DEPRECATED: Use listing_types instead")
    property_types: Optional[List[PropertyType]] = None
    past_days: Optional[int] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    radius: Optional[float] = None
    mls_only: bool = False
    foreclosure: bool = False
    exclude_pending: bool = False
    limit: int = Field(default=10000, le=10000)
    
    # Scheduling (for one-time scheduled jobs)
    scheduled_at: Optional[datetime] = None
    
    # Execution tracking
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    execution_time_seconds: Optional[float] = Field(None, description="How long the job took to execute")
    
    # Results
    properties_scraped: int = 0
    properties_saved: int = 0
    properties_inserted: int = Field(default=0, description="New properties added to database")
    properties_updated: int = Field(default=0, description="Existing properties updated")
    properties_skipped: int = Field(default=0, description="Properties skipped (no content change)")
    total_locations: int = 0
    completed_locations: int = 0
    
    # Detailed progress logs
    progress_logs: List[Dict[str, Any]] = Field(default_factory=list, description="Detailed logs per location and listing type")
    
    # Anti-bot configuration
    proxy_config: Optional[Dict[str, Any]] = None
    user_agent: Optional[str] = None
    request_delay: float = Field(default=1.0, ge=0.1, le=10.0)
    
    # Legacy fields for backward compatibility (deprecated)
    cron_expression: Optional[str] = Field(None, description="DEPRECATED: Use scheduled_jobs collection instead")
    run_count: int = Field(default=0, description="DEPRECATED: Moved to ScheduledJob")
    last_run: Optional[datetime] = Field(None, description="DEPRECATED: Moved to ScheduledJob")
    last_run_status: Optional[JobStatus] = Field(None, description="DEPRECATED: Moved to ScheduledJob")
    last_run_job_id: Optional[str] = Field(None, description="DEPRECATED: Moved to ScheduledJob")
    original_job_id: Optional[str] = Field(None, description="DEPRECATED: Use scheduled_job_id instead")
    
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
    
    # Contact information (nested - kept for backward compatibility)
    agent: Optional[PropertyAgent] = None
    broker: Optional[PropertyBroker] = None
    builder: Optional[PropertyBuilder] = None
    office: Optional[PropertyOffice] = None
    
    # Contact references (new - references to Contact collection in mls_scraper DB)
    agent_id: Optional[str] = None  # Reference to Contact._id
    broker_id: Optional[str] = None  # Reference to Contact._id
    builder_id: Optional[str] = None  # Reference to Contact._id
    office_id: Optional[str] = None  # Reference to Contact._id
    
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
    
    # CRM Integration
    crm_property_ids: Optional[Dict[str, List[str]]] = Field(default=None, description="Nested dictionary of CRM property IDs per platform: { 'platform_name': ['id1', 'id2'] }")
    
    def generate_property_id(self) -> str:
        """Generate property_id as hash of formatted_address"""
        if not self.address or not self.address.formatted_address:
            return None
        
        # Create hash of formatted address
        address_str = self.address.formatted_address.strip().lower()
        return hashlib.md5(address_str.encode('utf-8')).hexdigest()
    
    def generate_content_hash(self) -> str:
        """Generate hash of all content fields except days_on_mls and metadata"""
        
        def normalize_float(value):
            """Normalize float values to avoid precision issues"""
            if value is None:
                return None
            try:
                # Round to 6 decimal places to avoid precision issues
                return round(float(value), 6)
            except (ValueError, TypeError):
                return value
        
        def normalize_date(value):
            """Normalize date values to avoid timezone/precision issues"""
            if value is None:
                return None
            try:
                # Convert to date only (no time) to avoid timezone issues
                if hasattr(value, 'date'):
                    return value.date().isoformat()
                return str(value)
            except (ValueError, TypeError):
                return str(value) if value else None
        
        def normalize_list(value):
            """Normalize list values to avoid ordering issues"""
            if value is None:
                return None
            if isinstance(value, list):
                # Sort lists to ensure consistent ordering
                try:
                    return sorted(value)
                except TypeError:
                    # If sorting fails (mixed types), convert to strings and sort
                    return sorted([str(item) for item in value])
            return value
        
        def normalize_dict_list(value):
            """Normalize list of dicts to avoid ordering issues"""
            if value is None:
                return None
            if isinstance(value, list):
                # Sort by converting to string representation
                try:
                    return sorted(value, key=lambda x: str(sorted(x.items()) if isinstance(x, dict) else x))
                except TypeError:
                    return value
            return value
        
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
            
            # Financial data - normalize floats
            "financial": {
                "list_price": normalize_float(self.financial.list_price if self.financial else None),
                "list_price_min": normalize_float(self.financial.list_price_min if self.financial else None),
                "list_price_max": normalize_float(self.financial.list_price_max if self.financial else None),
                "sold_price": normalize_float(self.financial.sold_price if self.financial else None),
                "last_sold_price": normalize_float(self.financial.last_sold_price if self.financial else None),
                "price_per_sqft": normalize_float(self.financial.price_per_sqft if self.financial else None),
                "estimated_value": normalize_float(self.financial.estimated_value if self.financial else None),
                "tax_assessed_value": normalize_float(self.financial.tax_assessed_value if self.financial else None),
                "hoa_fee": normalize_float(self.financial.hoa_fee if self.financial else None),
                "tax": normalize_float(self.financial.tax if self.financial else None),
            } if self.financial else {},
            
            # Dates data - normalize dates
            "dates": {
                "list_date": normalize_date(self.dates.list_date if self.dates else None),
                "pending_date": normalize_date(self.dates.pending_date if self.dates else None),
                "last_sold_date": normalize_date(self.dates.last_sold_date if self.dates else None),
            } if self.dates else {},
            
            # Location data - normalize floats and lists
            "location": {
                "latitude": normalize_float(self.location.latitude if self.location else None),
                "longitude": normalize_float(self.location.longitude if self.location else None),
                "neighborhoods": normalize_list(self.location.neighborhoods if self.location else None),
                "county": self.location.county if self.location else None,
                "fips_code": self.location.fips_code if self.location else None,
                "parcel_number": self.location.parcel_number if self.location else None,
            } if self.location else {},
            
            # URLs and references
            "property_url": self.property_url,
            "listing_id": self.listing_id,
            "permalink": self.permalink,
            
            # Property images - normalize lists
            "primary_photo": self.primary_photo,
            "alt_photos": normalize_list(self.alt_photos),
            
            # Contact information - use IDs only (not full contact data)
            # This ensures content_hash doesn't change when contact info changes
            # Full contact data is stored in separate Contact collection
            "agent_id": self.agent.agent_id if self.agent else None,
            "broker_id": self.broker.broker_id if self.broker else None,
            "builder_id": self.builder.builder_id if self.builder else None,
            "office_id": self.office.office_id if self.office else None,
            
            # Additional data - normalize lists
            "new_construction": self.new_construction,
            "monthly_fees": normalize_dict_list(self.monthly_fees),
            "one_time_fees": normalize_dict_list(self.one_time_fees),
            "tax_history": normalize_dict_list(self.tax_history),
            "nearby_schools": normalize_dict_list(self.nearby_schools),
            
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

class PropertyIdsRequest(BaseModel):
    property_ids: List[str]

class EnrichmentRecalcRequest(BaseModel):
    """Request to recalculate enrichment scores"""
    recalc_type: str = Field(..., description="Type of recalculation: 'scores_only', 'keywords', or 'full'")
    limit: Optional[int] = Field(None, description="Max properties to update (None = all)")
    min_score: Optional[float] = Field(None, description="Only recalc properties above this score (for scores_only)")

class EnrichmentRecalcResponse(BaseModel):
    """Response from enrichment recalculation"""
    recalc_type: str
    total_processed: int
    total_updated: int
    total_errors: int
    started_at: datetime
    completed_at: datetime
    duration_seconds: float
    errors: List[str] = Field(default_factory=list, description="List of error messages if any")

class EnrichmentConfigResponse(BaseModel):
    """Response with current enrichment configuration"""
    config: Dict[str, Any]
    config_hash: str
    config_version: str
    last_modified: Optional[datetime] = None