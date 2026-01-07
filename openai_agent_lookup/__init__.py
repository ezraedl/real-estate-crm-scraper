"""
OpenAI Agent Lookup Library

A library for finding real estate agent contact information (phone numbers and emails)
using OpenAI Agents with web search capabilities.

Quick Start:
    from openai_agent_lookup import AgentLookupClient
    
    client = AgentLookupClient()
    result = client.lookup("Ashley Bergen, Indianapolis, IN")
    print(result.agent_name)
    print(result.mobile_numbers)  # Phone numbers (mobile, office, landline)
    print(result.emails)  # Email addresses

Async Usage:
    from openai_agent_lookup import AgentLookupClient
    
    client = AgentLookupClient()
    result = await client.lookup_async("Ashley Bergen, Indianapolis, IN")
    print(result.get_best_number())  # Best phone number
    print(result.get_best_email())  # Best email

Documentation:
    See README.md for full documentation
    See EXAMPLES.md for usage examples
    See INSTALL.md for installation instructions
"""

from .client import AgentLookupClient
from .models import (
    AgentLookupResult,
    MobileNumber,
    Email,
    LookupRequest
)

__all__ = [
    "AgentLookupClient",
    "AgentLookupResult",
    "MobileNumber",
    "Email",
    "LookupRequest",
]

__version__ = "1.1.0"

