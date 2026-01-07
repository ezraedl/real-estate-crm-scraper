"""
Data models for OpenAI Agent Lookup Library
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Union


class MobileNumber(BaseModel):
    """Phone number with certainty score and type"""
    number: str = Field(..., description="Phone number in any format")
    certainty: float = Field(..., ge=0.0, le=1.0, description="Certainty score between 0.0 and 1.0")
    type: str = Field(..., description="Phone type: 'mobile', 'office', 'landline', etc.")
    sources: List[str] = Field(default_factory=list, description="List of source URLs where the number was found")


class Email(BaseModel):
    """Email address with certainty score"""
    email: str = Field(..., description="Email address")
    certainty: Union[str, float] = Field(..., description="Certainty score (string or float)")
    sources: List[str] = Field(default_factory=list, description="List of source URLs where the email was found")


class AgentLookupResult(BaseModel):
    """Result from agent lookup"""
    agent_name: str = Field(..., description="Name of the real estate agent found")
    mobile_numbers: List[MobileNumber] = Field(
        default_factory=list,
        description="List of phone numbers (mobile, office, landline) sorted by certainty (highest first)"
    )
    emails: List[Email] = Field(
        default_factory=list,
        description="List of email addresses sorted by certainty (highest first)"
    )
    
    def get_best_number(self) -> Optional[MobileNumber]:
        """Get the phone number with highest certainty"""
        if not self.mobile_numbers:
            return None
        return self.mobile_numbers[0]
    
    def get_best_email(self) -> Optional[Email]:
        """Get the email with highest certainty"""
        if not self.emails:
            return None
        return self.emails[0]
    
    def get_mobile_numbers(self) -> List[MobileNumber]:
        """Get only mobile phone numbers"""
        return [num for num in self.mobile_numbers if num.type.lower() in ['mobile', 'cell']]
    
    def get_office_numbers(self) -> List[MobileNumber]:
        """Get only office/landline phone numbers"""
        return [num for num in self.mobile_numbers if num.type.lower() in ['office', 'landline', 'phone']]
    
    def get_numbers_above_threshold(self, threshold: float = 0.8) -> List[MobileNumber]:
        """Get all phone numbers above a certainty threshold"""
        return [num for num in self.mobile_numbers if num.certainty >= threshold]
    
    def get_emails_above_threshold(self, threshold: Union[str, float] = 0.8) -> List[Email]:
        """Get all emails above a certainty threshold"""
        def meets_threshold(email: Email) -> bool:
            if isinstance(email.certainty, str):
                try:
                    email_certainty = float(email.certainty)
                    threshold_val = float(threshold) if isinstance(threshold, str) else threshold
                    return email_certainty >= threshold_val
                except (ValueError, TypeError):
                    return False
            else:
                threshold_val = float(threshold) if isinstance(threshold, str) else threshold
                return email.certainty >= threshold_val
        
        return [email for email in self.emails if meets_threshold(email)]
    
    def has_results(self) -> bool:
        """Check if any phone numbers or emails were found"""
        return len(self.mobile_numbers) > 0 or len(self.emails) > 0
    
    def has_phone_numbers(self) -> bool:
        """Check if any phone numbers were found"""
        return len(self.mobile_numbers) > 0
    
    def has_emails(self) -> bool:
        """Check if any emails were found"""
        return len(self.emails) > 0


class LookupRequest(BaseModel):
    """Request for agent lookup"""
    query: str = Field(..., description="Search query (e.g., 'Agent Name, Location' or 'Find mobile number for Agent Name in Location')")
    
    def __str__(self) -> str:
        return self.query

