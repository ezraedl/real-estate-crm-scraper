"""
Service for creating and managing Contact documents.
This service interacts with the backend API to create/update contacts.
"""

import httpx
import os
from typing import Optional, Dict, Any, List
from config import settings

class ContactService:
    """Service for managing contacts via backend API"""
    
    def __init__(self):
        # Get backend API URL from environment or use default
        self.backend_url = os.getenv('BACKEND_API_URL', 'http://localhost:3000')
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
    
    async def process_property_contacts(self, property_data: Any) -> Dict[str, Optional[str]]:
        """
        Process all contacts for a property and return a dict with contact IDs.
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
        
        # Process agent contact
        if property_data.agent and property_data.agent.agent_name:
            try:
                # Extract phone numbers
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
                
                contact = await self.create_or_find_contact(
                    contact_type='agent',
                    name=property_data.agent.agent_name,
                    email=property_data.agent.agent_email,
                    phone=phone,
                    phones=phones,
                    agent_id=property_data.agent.agent_id,
                    agent_mls_set=property_data.agent.agent_mls_set,
                    agent_nrds_id=property_data.agent.agent_nrds_id,
                    source='scraper'
                )
                
                if contact and contact.get('_id'):
                    contact_ids['agent_id'] = str(contact['_id'])
            except Exception as e:
                print(f"Error processing agent contact: {e}")
        
        # Process broker contact
        if property_data.broker and property_data.broker.broker_name:
            try:
                contact = await self.create_or_find_contact(
                    contact_type='broker',
                    name=property_data.broker.broker_name,
                    source='scraper'
                )
                
                if contact and contact.get('_id'):
                    contact_ids['broker_id'] = str(contact['_id'])
            except Exception as e:
                print(f"Error processing broker contact: {e}")
        
        # Process builder contact
        if property_data.builder and property_data.builder.builder_name:
            try:
                contact = await self.create_or_find_contact(
                    contact_type='builder',
                    name=property_data.builder.builder_name,
                    source='scraper'
                )
                
                if contact and contact.get('_id'):
                    contact_ids['builder_id'] = str(contact['_id'])
            except Exception as e:
                print(f"Error processing builder contact: {e}")
        
        # Process office contact
        if property_data.office and property_data.office.office_name:
            try:
                # Extract phone numbers
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
                
                contact = await self.create_or_find_contact(
                    contact_type='office',
                    name=property_data.office.office_name,
                    email=property_data.office.office_email,
                    phone=phone,
                    phones=phones,
                    office_id=property_data.office.office_id,
                    office_mls_set=property_data.office.office_mls_set,
                    source='scraper'
                )
                
                if contact and contact.get('_id'):
                    contact_ids['office_id'] = str(contact['_id'])
            except Exception as e:
                print(f"Error processing office contact: {e}")
        
        return contact_ids

