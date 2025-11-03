# PowerShell script to run full test suite
# 1. Run migration
# 2. Restart services
# 3. Run API tests
# 4. Generate report

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Full Test Suite Execution" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Run migration
Write-Host "[1/4] Running migration..." -ForegroundColor Yellow
cd C:\Projects\Real-Estate-CRM-Repos\real-estate-crm-scraper
python scripts\migrate_remove_duplicates.py --batch-size 100
if ($LASTEXITCODE -ne 0) {
    Write-Host "Migration failed!" -ForegroundColor Red
    exit 1
}
Write-Host "Migration completed" -ForegroundColor Green
Write-Host ""

# Step 2: Restart services (if needed)
Write-Host "[2/4] Checking services..." -ForegroundColor Yellow
Write-Host "Note: Please ensure services are running:" -ForegroundColor Yellow
Write-Host "  - Scraper API: http://localhost:8000" -ForegroundColor Yellow
Write-Host "  - Backend API: http://localhost:3000" -ForegroundColor Yellow
Write-Host ""

# Step 3: Run API tests
Write-Host "[3/4] Running API tests..." -ForegroundColor Yellow
python scripts\test_api_endpoints.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "API tests failed!" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Step 4: Run consolidation tests
Write-Host "[4/4] Running consolidation tests..." -ForegroundColor Yellow
python test_consolidation.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "Consolidation tests failed!" -ForegroundColor Red
    exit 1
}
Write-Host ""

Write-Host "========================================" -ForegroundColor Green
Write-Host "All tests completed!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

