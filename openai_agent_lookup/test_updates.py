"""
Test script to verify library updates match sample.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from openai_agent_lookup import AgentLookupClient, AgentLookupResult, MobileNumber, Email

def test_models():
    """Test that models match sample.py structure"""
    print("Testing updated models...")
    
    # Test MobileNumber with type field
    mobile = MobileNumber(
        number="(317) 605-0919",
        certainty=0.92,
        type="mobile"
    )
    assert mobile.type == "mobile"
    print("[OK] MobileNumber has type field")
    
    # Test Email model
    email = Email(
        email="test@example.com",
        certainty="0.85"  # Can be string
    )
    assert email.email == "test@example.com"
    print("[OK] Email model works")
    
    # Test AgentLookupResult with emails
    result = AgentLookupResult(
        agent_name="Test Agent",
        mobile_numbers=[mobile],
        emails=[email]
    )
    
    assert result.has_phone_numbers()
    assert result.has_emails()
    assert result.get_best_email() is not None
    assert result.get_mobile_numbers() == [mobile]
    
    print("[OK] AgentLookupResult has emails field")
    print("[OK] All model tests passed!")
    
    return True

if __name__ == "__main__":
    try:
        test_models()
        print("\n[SUCCESS] All updates verified!")
    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

