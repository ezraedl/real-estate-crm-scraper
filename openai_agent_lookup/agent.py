"""
OpenAI Agent configuration for real estate agent lookup
"""

from agents import WebSearchTool, Agent, ModelSettings
from openai.types.shared.reasoning import Reasoning
from pydantic import BaseModel
from typing import List, Union


class MobileNumbersItem(BaseModel):
    """Phone number item in agent response"""
    number: str
    certainty: float
    type: str  # mobile, office, landline, etc.
    sources: List[str]  # List of source URLs where the number was found


class EmailsItem(BaseModel):
    """Email item in agent response"""
    email: str
    certainty: str  # Certainty as string (may be percentage or descriptor)
    sources: List[str]  # List of source URLs where the email was found


class AgentResponseSchema(BaseModel):
    """Schema for agent response"""
    agent_name: str
    mobile_numbers: List[MobileNumbersItem]
    emails: List[EmailsItem]


# Configure web search tool
web_search_tool = WebSearchTool(
    search_context_size="medium",
    user_location={
        "country": "US",
        "type": "approximate"
    }
)

# Create the agent
listing_agent_finder = Agent(
    name="Web research agent",
    instructions="""You are a helpful assistant. Use web search to find listing agent contact data incloding and most important mobile numbers, and also office phone numbers and emails.
look for relevant labels (mobile or cell labels - for mobile, office for landline).
use preferable real estate related US web sites.
create a JSON with the agent name, and array of its numbers + array of emails, sorted by your certinity level
 make sure to look the number in several relevant sites and give me data only if your are more than 80% certain it is the right data.
look it in web sites like mibor, zillow, realtor, homes.com etc""",
    model="gpt-5-mini",
    tools=[web_search_tool],
    output_type=AgentResponseSchema,
    model_settings=ModelSettings(
        store=True,
        reasoning=Reasoning(
            effort="medium"
        )
    )
)

