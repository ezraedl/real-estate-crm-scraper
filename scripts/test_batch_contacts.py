"""
Test script for batch contact processing in scraper
Run: python scripts/test_batch_contacts.py
"""

import asyncio
import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.contact_service import ContactService
from models import Property, Agent, Broker, Builder, Office

async def test_batch_contacts():
    """Test the batch contact processing"""
    print("Testing batch contact processing...")
    print("Make sure backend is running and accessible")
    print("")
    
    contact_service = ContactService()
    
    # Create test property data
    test_property = Property(
        property_id="test_property_123",
        address="123 Test St",
        city="Test City",
        state="IN",
        zip_code="46201",
        agent=Agent(
            agent_name="Test Agent",
            agent_email="agent@test.com",
            agent_phones=["+1234567890"],
            agent_id="TEST_AGENT_123"
        ),
        broker=Broker(
            broker_name="Test Broker"
        ),
        builder=Builder(
            builder_name="Test Builder"
        ),
        office=Office(
            office_name="Test Office",
            office_email="office@test.com",
            office_phones=["+1987654321"],
            office_id="TEST_OFFICE_123"
        )
    )
    
    # Prepare batch contacts
    batch_contacts = []
    contact_key_map = {}
    
    # Agent
    if test_property.agent and test_property.agent.agent_name:
        phones = None
        phone = None
        if test_property.agent.agent_phones:
            phones = []
            for p in test_property.agent.agent_phones:
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
        
        contact_key = f"{test_property.property_id}_agent"
        contact_key_map[(test_property.property_id, 'agent')] = contact_key
        batch_contacts.append({
            "key": contact_key,
            "contact_type": "agent",
            "name": test_property.agent.agent_name,
            "email": test_property.agent.agent_email,
            "phone": phone,
            "phones": phones,
            "agent_id": test_property.agent.agent_id,
            "source": "test"
        })
    
    # Broker
    if test_property.broker and test_property.broker.broker_name:
        contact_key = f"{test_property.property_id}_broker"
        contact_key_map[(test_property.property_id, 'broker')] = contact_key
        batch_contacts.append({
            "key": contact_key,
            "contact_type": "broker",
            "name": test_property.broker.broker_name,
            "source": "test"
        })
    
    # Builder
    if test_property.builder and test_property.builder.builder_name:
        contact_key = f"{test_property.property_id}_builder"
        contact_key_map[(test_property.property_id, 'builder')] = contact_key
        batch_contacts.append({
            "key": contact_key,
            "contact_type": "builder",
            "name": test_property.builder.builder_name,
            "source": "test"
        })
    
    # Office
    if test_property.office and test_property.office.office_name:
        phones = None
        phone = None
        if test_property.office.office_phones:
            phones = []
            for p in test_property.office.office_phones:
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
        
        contact_key = f"{test_property.property_id}_office"
        contact_key_map[(test_property.property_id, 'office')] = contact_key
        batch_contacts.append({
            "key": contact_key,
            "contact_type": "office",
            "name": test_property.office.office_name,
            "email": test_property.office.office_email,
            "phone": phone,
            "phones": phones,
            "office_id": test_property.office.office_id,
            "source": "test"
        })
    
    print(f"Prepared {len(batch_contacts)} contacts for batch processing:")
    for contact in batch_contacts:
        print(f"  - {contact['contact_type']}: {contact['name']}")
    print("")
    
    # Test batch processing
    try:
        print("Sending batch request to backend...")
        batch_result = await contact_service.batch_create_or_find_contacts(batch_contacts)
        
        print("")
        print("✅ Batch processing completed!")
        print(f"   Received {len(batch_result)} contact results")
        print("")
        
        # Map results back to property
        contact_ids = {
            "agent_id": None,
            "broker_id": None,
            "builder_id": None,
            "office_id": None
        }
        
        for contact_type in ['agent', 'broker', 'builder', 'office']:
            contact_key = contact_key_map.get((test_property.property_id, contact_type))
            if contact_key and contact_key in batch_result:
                contact = batch_result[contact_key]
                if contact and contact.get('_id'):
                    contact_ids[f'{contact_type}_id'] = str(contact['_id'])
                    print(f"✅ {contact_type.capitalize()} contact ID: {contact['_id']}")
                else:
                    print(f"⚠️  {contact_type.capitalize()} contact not found in results")
            else:
                print(f"⚠️  {contact_type.capitalize()} contact key not found")
        
        print("")
        print("✅ Test completed successfully!")
        return True
        
    except Exception as e:
        print("")
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    result = asyncio.run(test_batch_contacts())
    sys.exit(0 if result else 1)

