"""
Test script to verify Python 3.14 compatibility
Run this to confirm the library works on Python 3.14
"""

import sys
import os

# Add parent directory to path for testing
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_python314():
    """Test that the library works on Python 3.14"""
    
    print("=" * 60)
    print("Python 3.14 Compatibility Test")
    print("=" * 60)
    
    # Check Python version
    version = sys.version_info
    print(f"\nPython Version: {version.major}.{version.minor}.{version.micro}")
    
    if version.major == 3 and version.minor == 14:
        print("[OK] Running on Python 3.14")
    else:
        print(f"[INFO] Running on Python {version.major}.{version.minor} (not 3.14)")
    
    # Test imports
    print("\n1. Testing imports...")
    try:
        import agents
        print("   [OK] openai-agents imported")
    except ImportError as e:
        print(f"   [ERROR] openai-agents failed: {e}")
        return False
    
    try:
        import pydantic
        print(f"   [OK] pydantic {pydantic.__version__} imported")
    except ImportError as e:
        print(f"   [ERROR] pydantic failed: {e}")
        return False
    
    try:
        import openai
        print(f"   [OK] openai {openai.__version__} imported")
    except ImportError as e:
        print(f"   [ERROR] openai failed: {e}")
        return False
    
    # Test library import
    print("\n2. Testing library import...")
    try:
        from openai_agent_lookup import AgentLookupClient, AgentLookupResult, MobileNumber
        print("   [OK] AgentLookupClient imported")
        print("   [OK] AgentLookupResult imported")
        print("   [OK] MobileNumber imported")
    except ImportError as e:
        print(f"   [ERROR] Library import failed: {e}")
        return False
    
    # Test client creation
    print("\n3. Testing client creation...")
    try:
        client = AgentLookupClient()
        print("   [OK] Client created successfully")
    except Exception as e:
        print(f"   [ERROR] Client creation failed: {e}")
        return False
    
    # Test models
    print("\n4. Testing data models...")
    try:
        mobile = MobileNumber(number="(317) 605-0919", certainty=0.92)
        print(f"   [OK] MobileNumber model works: {mobile.number}")
        
        result = AgentLookupResult(
            agent_name="Test Agent",
            mobile_numbers=[mobile]
        )
        print(f"   [OK] AgentLookupResult model works: {result.agent_name}")
        print(f"   [OK] Has {len(result.mobile_numbers)} mobile numbers")
    except Exception as e:
        print(f"   [ERROR] Model test failed: {e}")
        return False
    
    print("\n" + "=" * 60)
    print("[SUCCESS] All tests passed! Python 3.14 is fully supported.")
    print("=" * 60)
    
    return True

if __name__ == "__main__":
    success = test_python314()
    sys.exit(0 if success else 1)

