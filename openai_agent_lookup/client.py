"""
Client for OpenAI Agent Lookup Library
"""

import asyncio
from typing import Optional, Union
from agents import Runner, RunConfig, TResponseInputItem, trace

from .agent import listing_agent_finder, AgentResponseSchema
from .models import AgentLookupResult, MobileNumber, Email, LookupRequest


def _normalize_certainty(certainty: Union[float, str]) -> float:
    """
    Normalize certainty value to decimal (0.0-1.0).
    
    The agent may return certainty as:
    - Percentage (0-100) -> converts to decimal (0.0-1.0)
    - Decimal (0.0-1.0) -> returns as is
    
    Args:
        certainty: Certainty value as float or string
        
    Returns:
        Normalized certainty as float between 0.0 and 1.0
    """
    if isinstance(certainty, str):
        try:
            certainty = float(certainty)
        except (ValueError, TypeError):
            return 0.0
    
    if isinstance(certainty, (int, float)):
        # If value is greater than 1.0, treat it as a percentage
        if certainty > 1.0:
            return certainty / 100.0
        # Clamp to valid range
        return max(0.0, min(1.0, certainty))
    
    return 0.0


class AgentLookupClient:
    """
    Client for looking up real estate agent mobile numbers using OpenAI Agents.
    
    Example:
        ```python
        from openai_agent_lookup import AgentLookupClient
        
        client = AgentLookupClient()
        result = await client.lookup("Ashley Bergen, Indianapolis, IN")
        print(result.agent_name)
        print(result.mobile_numbers)
        ```
    """
    
    def __init__(self, workflow_id: Optional[str] = None):
        """
        Initialize the client.
        
        Args:
            workflow_id: Optional workflow ID for tracing. If not provided, uses default.
        """
        self.workflow_id = workflow_id or "wf_695ec854e9608190922bdf5e2edd09e8025b3b6ddee3b91a"
        self.agent = listing_agent_finder
    
    async def lookup_async(
        self,
        query: Union[str, LookupRequest],
        trace_metadata: Optional[dict] = None
    ) -> AgentLookupResult:
        """
        Look up agent mobile numbers asynchronously.
        
        Args:
            query: Search query string or LookupRequest object
            trace_metadata: Optional metadata for tracing
            
        Returns:
            AgentLookupResult with agent name and mobile numbers
            
        Example:
            ```python
            result = await client.lookup_async("Ashley Bergen, Indianapolis")
            print(f"Found {len(result.mobile_numbers)} numbers")
            ```
        """
        # Convert to string if LookupRequest
        query_str = str(query) if isinstance(query, LookupRequest) else query
        
        with trace("Listing Agent finder"):
            # Build conversation history
            conversation_history: list[TResponseInputItem] = [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": query_str
                        }
                    ]
                }
            ]
            
            # Prepare trace metadata
            metadata = {
                "__trace_source__": "agent-builder",
                "workflow_id": self.workflow_id
            }
            if trace_metadata:
                metadata.update(trace_metadata)
            
            # Run the agent
            agent_result = await Runner.run(
                self.agent,
                input=conversation_history,
                run_config=RunConfig(trace_metadata=metadata)
            )
            
            # Convert agent response to our result model
            agent_output: AgentResponseSchema = agent_result.final_output
            
            # Convert phone numbers to AgentLookupResult format
            # Normalize certainty values (agent may return percentages 0-100, but model expects 0.0-1.0)
            mobile_numbers = [
                MobileNumber(
                    number=item.number,
                    certainty=_normalize_certainty(item.certainty),
                    type=item.type,
                    sources=item.sources  # List of source URLs where the number was found
                )
                for item in agent_output.mobile_numbers
            ]
            
            # Convert emails to AgentLookupResult format
            # Email certainty can be string or float
            # Normalize numeric values, preserve non-numeric strings
            def normalize_email_certainty(certainty: Union[str, float]) -> Union[str, float]:
                """Normalize email certainty, preserving non-numeric strings."""
                if isinstance(certainty, str):
                    # Try to convert numeric strings, preserve non-numeric ones
                    try:
                        numeric_value = float(certainty)
                        return _normalize_certainty(numeric_value)
                    except (ValueError, TypeError):
                        # Non-numeric string, preserve as-is
                        return certainty
                elif isinstance(certainty, (int, float)):
                    return _normalize_certainty(certainty)
                else:
                    return certainty
            
            emails = [
                Email(
                    email=item.email,
                    certainty=normalize_email_certainty(item.certainty),
                    sources=item.sources  # List of source URLs where the email was found
                )
                for item in agent_output.emails
            ]
            
            return AgentLookupResult(
                agent_name=agent_output.agent_name,
                mobile_numbers=mobile_numbers,
                emails=emails
            )
    
    def lookup(
        self,
        query: Union[str, LookupRequest],
        trace_metadata: Optional[dict] = None
    ) -> AgentLookupResult:
        """
        Look up agent mobile numbers synchronously.
        
        This is a convenience method that wraps the async version.
        Note: If you're already in an async context, use lookup_async() instead.
        
        Args:
            query: Search query string or LookupRequest object
            trace_metadata: Optional metadata for tracing
            
        Returns:
            AgentLookupResult with agent name and mobile numbers
            
        Example:
            ```python
            client = AgentLookupClient()
            result = client.lookup("Ashley Bergen, Indianapolis")
            print(f"Found {len(result.mobile_numbers)} numbers")
            ```
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If loop is already running, we need to use a different approach
                import nest_asyncio
                nest_asyncio.apply()
                return loop.run_until_complete(
                    self.lookup_async(query, trace_metadata)
                )
        except RuntimeError:
            # No event loop running, create a new one
            pass
        
        return asyncio.run(self.lookup_async(query, trace_metadata))
    
    async def lookup_batch_async(
        self,
        queries: list[Union[str, LookupRequest]],
        trace_metadata: Optional[dict] = None
    ) -> list[AgentLookupResult]:
        """
        Look up multiple agents asynchronously.
        
        Args:
            queries: List of search query strings or LookupRequest objects
            trace_metadata: Optional metadata for tracing
            
        Returns:
            List of AgentLookupResult objects
            
        Example:
            ```python
            queries = [
                "Ashley Bergen, Indianapolis",
                "John Smith, Chicago"
            ]
            results = await client.lookup_batch_async(queries)
            ```
        """
        tasks = [self.lookup_async(query, trace_metadata) for query in queries]
        return await asyncio.gather(*tasks)
    
    def lookup_batch(
        self,
        queries: list[Union[str, LookupRequest]],
        trace_metadata: Optional[dict] = None
    ) -> list[AgentLookupResult]:
        """
        Look up multiple agents synchronously.
        
        Args:
            queries: List of search query strings or LookupRequest objects
            trace_metadata: Optional metadata for tracing
            
        Returns:
            List of AgentLookupResult objects
            
        Example:
            ```python
            queries = [
                "Ashley Bergen, Indianapolis",
                "John Smith, Chicago"
            ]
            results = client.lookup_batch(queries)
            ```
        """
        return asyncio.run(self.lookup_batch_async(queries, trace_metadata))

