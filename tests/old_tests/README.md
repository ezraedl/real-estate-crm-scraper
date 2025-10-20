# Test Structure

## 📁 **Directory Organization**

### **`tests/` - Permanent CI/CD Tests**
These tests are part of the continuous integration/continuous deployment process and must work every time:

- **`test_database_connection.py`** - Tests database connection and basic CRUD operations
- **`test_scraper_basic.py`** - Tests basic scraper initialization and job creation
- **`test_hash_system.py`** - Tests hash-based diff system for duplicate handling
- **`test_api_endpoints.py`** - Tests all 11 API endpoints comprehensively
- **`run_tests.py`** - Test runner for all permanent tests

### **`temp_tests/` - Temporary Development Tests**
These are temporary test files used for debugging, development, or one-time testing:

- **`test_create_db_and_mock_property.py`** - Moved from tests/ (long database test)
- **`test_daily_indianapolis_import.py`** - Moved from tests/ (long scraping test)
- **`test_daily_indianapolis_with_env.py`** - Moved from tests/ (long scraping test)
- **`test_duplicate_handling.py`** - Moved from tests/ (long duplicate test)
- **`template_temp_test.py`** - Template for new temporary tests
- Use this directory for files that are created during development
- These files are not part of CI/CD and can be deleted after use
- Examples: debug scripts, one-off tests, experimental features

## 🚀 **Running Tests**

### **Permanent Tests (CI/CD)**
```bash
# Run all permanent tests
python tests/run_tests.py

# Run individual tests
python tests/test_database_connection.py
python tests/test_scraper_basic.py
python tests/test_hash_system.py
python tests/test_api_endpoints.py
```

### **Temporary Tests**
```bash
# Run temporary tests (not part of CI/CD)
python temp_tests/your_temp_test.py
```

## 📋 **Test Requirements**

### **Permanent Tests Must:**
- ✅ Work with current `.env` configuration
- ✅ Not depend on external services being available
- ✅ Clean up after themselves
- ✅ Have proper error handling
- ✅ Be deterministic (same results every time)
- ✅ Not require manual intervention

### **Temporary Tests Can:**
- ❌ Depend on specific external conditions
- ❌ Require manual cleanup
- ❌ Have hardcoded values
- ❌ Be experimental or incomplete

## 🔧 **Adding New Tests**

### **For Permanent Tests:**
1. Add to `tests/` directory
2. Update `tests/run_tests.py` to include the new test
3. Ensure it meets all permanent test requirements
4. Document the test purpose in this README

### **For Temporary Tests:**
1. Add to `temp_tests/` directory
2. No need to update test runner
3. Can be experimental or incomplete
4. Document purpose in file comments

## 📊 **Test Categories**

- **Database Tests**: Connection, CRUD operations, indexing
- **Scraping Tests**: Data retrieval, parsing, validation
- **API Tests**: Endpoint functionality, request/response handling
- **Integration Tests**: End-to-end workflows
- **Performance Tests**: Speed, memory usage, scalability
