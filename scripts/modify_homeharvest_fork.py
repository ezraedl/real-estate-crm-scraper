"""
Helper script to identify what needs to be changed in homeharvest fork.

This script analyzes the installed homeharvest package and provides
a report of what files need to be modified to use curl_cffi.
"""

import sys
from pathlib import Path
import re

def find_requests_usage(package_path):
    """Find all files that use requests library"""
    requests_files = []
    
    for py_file in package_path.rglob("*.py"):
        try:
            content = py_file.read_text(encoding='utf-8', errors='ignore')
            
            # Check for requests usage
            has_requests_import = bool(re.search(r'import\s+requests|from\s+requests', content))
            has_requests_usage = 'requests.' in content or 'requests.get' in content or 'requests.post' in content
            
            if has_requests_import or has_requests_usage:
                # Count occurrences
                import_count = len(re.findall(r'import\s+requests|from\s+requests', content))
                usage_count = content.count('requests.')
                
                requests_files.append({
                    'file': py_file,
                    'import_count': import_count,
                    'usage_count': usage_count,
                    'has_import': has_requests_import,
                    'content': content
                })
        except Exception as e:
            print(f"Error reading {py_file}: {e}")
    
    return requests_files

def analyze_file(file_info):
    """Analyze a file and provide modification suggestions"""
    file_path = file_info['file']
    content = file_info['content']
    
    suggestions = []
    
    # Find import statements
    import_pattern = r'(import\s+requests|from\s+requests[^\n]*)'
    imports = re.findall(import_pattern, content)
    
    if imports:
        suggestions.append(f"  Found {len(imports)} import statement(s):")
        for imp in imports:
            suggestions.append(f"    - {imp.strip()}")
        suggestions.append("")
        suggestions.append("  Replace with:")
        suggestions.append("    try:")
        suggestions.append("        from curl_cffi import requests")
        suggestions.append("        USE_CURL_CFFI = True")
        suggestions.append("    except ImportError:")
        suggestions.append("        import requests")
        suggestions.append("        USE_CURL_CFFI = False")
        suggestions.append("")
    
    # Find Session() usage
    session_pattern = r'requests\.Session\(\)|Session\(\)'
    sessions = re.findall(session_pattern, content)
    if sessions:
        suggestions.append(f"  Found {len(sessions)} Session() usage(s)")
        suggestions.append("  Update to:")
        suggestions.append("    if USE_CURL_CFFI:")
        suggestions.append("        session = requests.Session(impersonate='chrome110')")
        suggestions.append("    else:")
        suggestions.append("        session = requests.Session()")
        suggestions.append("")
    
    # Find HTTPAdapter usage
    adapter_pattern = r'HTTPAdapter\(\)'
    adapters = re.findall(adapter_pattern, content)
    if adapters:
        suggestions.append(f"  Found {len(adapters)} HTTPAdapter() usage(s)")
        suggestions.append("  May need to update adapter imports")
        suggestions.append("")
    
    return suggestions

def main():
    """Main analysis function"""
    print("="*70)
    print("HomeHarvest Fork Modification Helper")
    print("="*70)
    print("\nThis script analyzes the installed homeharvest package")
    print("to identify what needs to be changed for curl_cffi support.\n")
    
    try:
        import homeharvest
        homeharvest_path = Path(homeharvest.__file__).parent
        print(f"✅ Found homeharvest at: {homeharvest_path}\n")
    except ImportError:
        print("❌ homeharvest is not installed")
        print("   Install it first: pip install homeharvest")
        return
    
    # Find all files using requests
    print("Searching for 'requests' usage...")
    requests_files = find_requests_usage(homeharvest_path)
    
    if not requests_files:
        print("⚠️  No files found using 'requests' library")
        print("   This is unexpected - homeharvest should use requests")
        return
    
    print(f"\n✅ Found {len(requests_files)} file(s) using 'requests':\n")
    
    # Analyze each file
    for i, file_info in enumerate(requests_files, 1):
        rel_path = file_info['file'].relative_to(homeharvest_path)
        print(f"{'='*70}")
        print(f"File {i}/{len(requests_files)}: {rel_path}")
        print(f"{'='*70}")
        print(f"  Import statements: {file_info['import_count']}")
        print(f"  Usage count: {file_info['usage_count']}")
        print()
        
        suggestions = analyze_file(file_info)
        if suggestions:
            print("  Modification suggestions:")
            for suggestion in suggestions:
                print(suggestion)
        else:
            print("  (No specific suggestions - review manually)")
        print()
    
    # Summary
    print("="*70)
    print("SUMMARY")
    print("="*70)
    print(f"Total files to modify: {len(requests_files)}")
    print("\nNext steps:")
    print("1. Clone your forked homeharvest repository")
    print("2. Modify the files listed above")
    print("3. Test your changes")
    print("4. Update requirements.txt to use your fork")
    print("\nSee MODIFY_HOMEHARVEST_FORK.md for detailed instructions")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)



