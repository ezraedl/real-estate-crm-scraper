"""
Script to help apply curl_cffi modifications to HomeHarvest fork.

This script can be run in the HomeHarvestLocal repository to automatically
apply the necessary changes for curl_cffi support.
"""

import sys
from pathlib import Path
import re

def modify_scrapers_init(file_path):
    """Modify homeharvest/core/scrapers/__init__.py"""
    print(f"Modifying: {file_path}")
    
    content = file_path.read_text(encoding='utf-8')
    original_content = content
    
    # Replace requests import
    requests_import_pattern = r'import\s+requests\s*\n'
    requests_from_pattern = r'from\s+requests\.adapters\s+import\s+HTTPAdapter\s*\n'
    
    # Check if already modified
    if 'curl_cffi' in content:
        print("  ⚠️  File already appears to have curl_cffi modifications")
        return False
    
    # Replace import requests
    if re.search(requests_import_pattern, content):
        replacement = '''# Try to use curl_cffi for TLS fingerprinting (anti-bot measures)
try:
    from curl_cffi import requests
    from curl_cffi.requests.adapters import HTTPAdapter
    USE_CURL_CFFI = True
    DEFAULT_IMPERSONATE = "chrome110"  # Can also try: chrome116, chrome120, edge99
except ImportError:
    import requests
    from requests.adapters import HTTPAdapter
    USE_CURL_CFFI = False
    DEFAULT_IMPERSONATE = None
'''
        content = re.sub(requests_import_pattern, replacement, content)
        # Remove the from requests.adapters line if it exists separately
        content = re.sub(requests_from_pattern, '', content)
    elif re.search(requests_from_pattern, content):
        # Only from requests.adapters exists
        replacement = '''# Try to use curl_cffi for TLS fingerprinting (anti-bot measures)
try:
    from curl_cffi import requests
    from curl_cffi.requests.adapters import HTTPAdapter
    USE_CURL_CFFI = True
    DEFAULT_IMPERSONATE = "chrome110"
except ImportError:
    import requests
    from requests.adapters import HTTPAdapter
    USE_CURL_CFFI = False
    DEFAULT_IMPERSONATE = None
'''
        content = re.sub(requests_from_pattern, replacement, content)
    
    # Replace Session() creation
    session_pattern = r'(\s*)(session\s*=\s*)requests\.Session\(\)'
    def replace_session(match):
        indent = match.group(1)
        session_var = match.group(2)
        return f"{indent}if USE_CURL_CFFI:\n{indent}    {session_var}requests.Session(impersonate=DEFAULT_IMPERSONATE)\n{indent}else:\n{indent}    {session_var}requests.Session()"
    
    content = re.sub(session_pattern, replace_session, content)
    
    if content != original_content:
        file_path.write_text(content, encoding='utf-8')
        print("  ✅ File modified successfully")
        return True
    else:
        print("  ⚠️  No changes made (file may not match expected patterns)")
        return False

def modify_init(file_path):
    """Modify homeharvest/__init__.py if needed"""
    print(f"Checking: {file_path}")
    
    if not file_path.exists():
        print("  ℹ️  File doesn't exist, skipping")
        return False
    
    content = file_path.read_text(encoding='utf-8')
    
    # Check if it directly imports requests
    if 'import requests' in content and 'curl_cffi' not in content:
        print("  ⚠️  File imports requests but modifications needed")
        print("  ⚠️  Please review manually - may need same curl_cffi pattern")
        return False
    else:
        print("  ✅ File doesn't need modification or already modified")
        return True

def update_pyproject_toml(file_path):
    """Update pyproject.toml to include curl-cffi dependency"""
    print(f"Updating: {file_path}")
    
    if not file_path.exists():
        print("  ⚠️  pyproject.toml not found")
        return False
    
    content = file_path.read_text(encoding='utf-8')
    
    # Check if curl-cffi is already in dependencies
    if 'curl-cffi' in content or 'curl_cffi' in content:
        print("  ✅ curl-cffi already in dependencies")
        return True
    
    # Try to add to dependencies section
    if '[project]' in content or '[tool.poetry.dependencies]' in content:
        # Find dependencies section
        if 'dependencies = [' in content:
            # Add curl-cffi to dependencies list
            content = re.sub(
                r'(dependencies\s*=\s*\[)',
                r'\1\n    "curl-cffi>=0.5.10",',
                content
            )
            file_path.write_text(content, encoding='utf-8')
            print("  ✅ Added curl-cffi to dependencies")
            return True
        elif 'dependencies = {' in content:
            # Poetry format
            content = re.sub(
                r'(dependencies\s*=\s*\{)',
                r'\1\n    "curl-cffi": ">=0.5.10",',
                content
            )
            file_path.write_text(content, encoding='utf-8')
            print("  ✅ Added curl-cffi to dependencies")
            return True
    
    print("  ⚠️  Could not automatically add curl-cffi - please add manually")
    print("      Add: curl-cffi>=0.5.10 to dependencies")
    return False

def main():
    """Main function"""
    print("="*70)
    print("HomeHarvest curl_cffi Patch Applicator")
    print("="*70)
    print("\nThis script modifies your HomeHarvest fork to use curl_cffi")
    print("for TLS fingerprinting.\n")
    
    # Get the repository root (should be run from HomeHarvestLocal directory)
    repo_root = Path.cwd()
    
    if not (repo_root / "homeharvest").exists():
        print("❌ Error: Not in HomeHarvestLocal repository root")
        print("   Please run this script from the HomeHarvestLocal directory")
        return 1
    
    print(f"Repository root: {repo_root}\n")
    
    # Files to modify
    scrapers_init = repo_root / "homeharvest" / "core" / "scrapers" / "__init__.py"
    main_init = repo_root / "homeharvest" / "__init__.py"
    pyproject = repo_root / "pyproject.toml"
    
    results = {}
    
    # Modify scrapers/__init__.py
    if scrapers_init.exists():
        results['scrapers_init'] = modify_scrapers_init(scrapers_init)
    else:
        print(f"⚠️  {scrapers_init} not found")
        results['scrapers_init'] = False
    
    print()
    
    # Check main __init__.py
    if main_init.exists():
        results['main_init'] = modify_init(main_init)
    else:
        print(f"⚠️  {main_init} not found")
        results['main_init'] = False
    
    print()
    
    # Update pyproject.toml
    if pyproject.exists():
        results['pyproject'] = update_pyproject_toml(pyproject)
    else:
        print(f"⚠️  {pyproject} not found")
        results['pyproject'] = False
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    
    for file_name, success in results.items():
        status = "✅ SUCCESS" if success else "⚠️  NEEDS REVIEW"
        print(f"{file_name}: {status}")
    
    print("\nNext steps:")
    print("1. Review the changes made")
    print("2. Test locally: pip install -e .")
    print("3. Test scraping: python -c \"from homeharvest import scrape_property; df = scrape_property(location='Indianapolis, IN', listing_type=['for_sale'], limit=1); print(f'Got {len(df)} properties')\"")
    print("4. Commit and push: git add . && git commit -m 'Add curl_cffi support' && git push")
    
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


