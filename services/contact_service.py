"""
Service for creating and managing Contact documents.
This service interacts with the backend API to create/update contacts.
"""

import httpx
import os
import asyncio
from typing import Optional, Dict, Any, List
from config import settings

class ContactService:
    """Service for managing contacts via backend API"""
    
    def __init__(self):
        # Get backend API URL from environment or use default
        # Default to port 5000 (backend) instead of 3000 (frontend)
        self.backend_url = os.getenv('BACKEND_API_URL', 'http://localhost:5000')
        self.api_base = f"{self.backend_url}/api"
    
    async def create_or_find_contact(
        self,
        contact_type: str,
        name: str,
        email: Optional[str] = None,
        phone: Optional[str] = None,
        phones: Optional[List[Dict[str, Any]]] = None,
        agent_id: Optional[str] = None,
        agent_mls_set: Optional[str] = None,
        agent_nrds_id: Optional[str] = None,
        broker_id: Optional[str] = None,
        builder_id: Optional[str] = None,
        office_id: Optional[str] = None,
        office_mls_set: Optional[str] = None,
        source: str = 'scraper'
    ) -> Optional[Dict[str, Any]]:
        """
        Create or find a contact using the backend API.
        Returns the contact document with _id, or None if creation failed.
        """
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{self.api_base}/contacts",
                    json={
                        "contact_type": contact_type,
                        "name": name,
                        "email": email,
                        "phone": phone,
                        "phones": phones,
                        "agent_id": agent_id,
                        "agent_mls_set": agent_mls_set,
                        "agent_nrds_id": agent_nrds_id,
                        "broker_id": broker_id,
                        "builder_id": builder_id,
                        "office_id": office_id,
                        "office_mls_set": office_mls_set,
                        "source": source,
                    }
                )
                
                if response.status_code == 201 or response.status_code == 200:
                    contact = response.json()
                    return contact
                else:
                    print(f"Failed to create/find contact: {response.status_code} - {response.text}")
                    return None
        except Exception as e:
            print(f"Error creating/finding contact via API: {e}")
            # If API is not available, return None (scraper will continue without contact references)
            return None
    
    async def batch_create_or_find_contacts(
        self,
        contacts: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Create or find multiple contacts in a single batch API call.
        Returns a dict mapping contact keys to contact documents.
        
        Args:
            contacts: List of contact dicts, each with:
                - key: Unique identifier for this contact (e.g., 'agent', 'broker_123')
                - contact_type: Type of contact
                - name: Contact name
                - ... (other contact fields)
        
        Returns:
            Dict mapping contact keys to contact documents (or None if failed)
        """
        if not contacts:
            return {}
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:  # Longer timeout for batch
                response = await client.post(
                    f"{self.api_base}/contacts/batch",
                    json={"contacts": contacts}
                )
                
                if response.status_code == 201:
                    result = response.json()
                    # Map contacts back to their keys
                    # The API returns contacts in order, so we can match them
                    contact_map = {}
                    returned_contacts = result.get("contacts", [])
                    
                    for i, contact_data in enumerate(contacts):
                        key = contact_data.get("key")
                        if key and i < len(returned_contacts):
                            contact_map[key] = returned_contacts[i]
                        elif i < len(returned_contacts):
                            # Fallback: use index if no key provided
                            contact_map[str(i)] = returned_contacts[i]
                    
                    return contact_map
                else:
                    print(f"Failed to batch create/find contacts: {response.status_code} - {response.text}")
                    return {}
        except Exception as e:
            print(f"Error batch creating/finding contacts via API: {e}")
            return {}
    
    async def process_property_contacts(self, property_data: Any) -> Dict[str, Optional[str]]:
        """
        Process all contacts for a property in parallel and return a dict with contact IDs.
        Returns: {
            'agent_id': Optional[str],
            'broker_id': Optional[str],
            'builder_id': Optional[str],
            'office_id': Optional[str]
        }
        """
        contact_ids = {
            'agent_id': None,
            'broker_id': None,
            'builder_id': None,
            'office_id': None
        }
        
        # Prepare all contact creation tasks to run in parallel
        tasks = []
        task_types = []
        
        # Prepare agent contact task
        if property_data.agent and property_data.agent.agent_name:
            phones = None
            phone = None
            if property_data.agent.agent_phones:
                phones = []
                for p in property_data.agent.agent_phones:
                    if isinstance(p, dict):
                        phones.append({
                            "number": p.get('number'),
                            "type": p.get('type')
                        })
                        if not phone and p.get('number'):
                            phone = p.get('number')
                    elif isinstance(p, str):
                        phones.append({"number": p})
                        if not phone:
                            phone = p
            
            tasks.append(self.create_or_find_contact(
                contact_type='agent',
                name=property_data.agent.agent_name,
                email=property_data.agent.agent_email,
                phone=phone,
                phones=phones,
                agent_id=property_data.agent.agent_id,
                agent_mls_set=property_data.agent.agent_mls_set,
                agent_nrds_id=property_data.agent.agent_nrds_id,
                source='scraper'
            ))
            task_types.append('agent')
        
        # Prepare broker contact task
        if property_data.broker and property_data.broker.broker_name:
            tasks.append(self.create_or_find_contact(
                contact_type='broker',
                name=property_data.broker.broker_name,
                source='scraper'
            ))
            task_types.append('broker')
        
        # Prepare builder contact task
        if property_data.builder and property_data.builder.builder_name:
            tasks.append(self.create_or_find_contact(
                contact_type='builder',
                name=property_data.builder.builder_name,
                source='scraper'
            ))
            task_types.append('builder')
        
        # Prepare office contact task
        if property_data.office and property_data.office.office_name:
            phones = None
            phone = None
            if property_data.office.office_phones:
                phones = []
                for p in property_data.office.office_phones:
                    if isinstance(p, dict):
                        phones.append({
                            "number": p.get('number'),
                            "type": p.get('type')
                        })
                        if not phone and p.get('number'):
                            phone = p.get('number')
                    elif isinstance(p, str):
                        phones.append({"number": p})
                        if not phone:
                            phone = p
            
            tasks.append(self.create_or_find_contact(
                contact_type='office',
                name=property_data.office.office_name,
                email=property_data.office.office_email,
                phone=phone,
                phones=phones,
                office_id=property_data.office.office_id,
                office_mls_set=property_data.office.office_mls_set,
                source='scraper'
            ))
            task_types.append('office')
        
        # Execute all contact creation tasks in parallel
        if tasks:
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for contact_type, result in zip(task_types, results):
                    if isinstance(result, Exception):
                        print(f"Error processing {contact_type} contact: {result}")
                    elif result and result.get('_id'):
                        contact_ids[f'{contact_type}_id'] = str(result['_id'])
            except Exception as e:
                print(f"Error processing contacts in parallel: {e}")
        
        return contact_ids

