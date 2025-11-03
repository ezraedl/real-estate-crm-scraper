# Complete Test Suite - Migration, Restart Services, Test Everything
# This script:
# 1. Waits for migration to complete (if running)
# 2. Restarts services (scraper, backend)
# 3. Tests all API endpoints
# 4. Runs consolidation tests
# 5. Generates comprehensive report

param(
    [switch]$SkipMigration = $false,
    [switch]$SkipRestart = $false
)

$ErrorActionPreference = "Continue"

function Write-Header {
    param([string]$Message)
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host $Message -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
}

function Write-Step {
    param([string]$Message)
    Write-Host "> $Message" -ForegroundColor Yellow
}

function Write-Success {
    param([string]$Message)
    Write-Host "✓ $Message" -ForegroundColor Green
}

function Write-Error {
    param([string]$Message)
    Write-Host "✗ $Message" -ForegroundColor Red
}

# Step 1: Run Migration
if (-not $SkipMigration) {
    Write-Header "Step 1: Running Migration"
    Write-Step "Migrating properties to remove duplicate data..."
    
    cd C:\Projects\Real-Estate-CRM-Repos\real-estate-crm-scraper
    python scripts\migrate_remove_duplicates.py --batch-size 100
    
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Migration failed!"
        exit 1
    }
    
    Write-Success "Migration completed successfully"
    Start-Sleep -Seconds 2
}

# Step 2: Restart Services
if (-not $SkipRestart) {
    Write-Header "Step 2: Restarting Services"
    
    # Function to kill process on port
    function Stop-ProcessOnPort {
        param([int]$Port)
        $connection = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue
        if ($connection) {
            $process = Get-Process -Id $connection.OwningProcess -ErrorAction SilentlyContinue
            if ($process) {
                Write-Step "Stopping process on port $Port (PID: $($process.Id))..."
                $process.Kill()
                Start-Sleep -Seconds 2
                Write-Success "Process stopped"
            }
        }
    }
    
    # Stop existing services
    Write-Step "Checking for existing services..."
    Stop-ProcessOnPort -Port 8000
    Stop-ProcessOnPort -Port 3000
    Start-Sleep -Seconds 2
    
    # Start Scraper
    Write-Step "Starting Scraper API (port 8000)..."
    $scraperPath = "C:\Projects\Real-Estate-CRM-Repos\real-estate-crm-scraper"
    $scraperProcess = Start-Process -FilePath "python" -ArgumentList "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000" -WorkingDirectory $scraperPath -PassThru -WindowStyle Hidden
    
    if ($scraperProcess) {
        Write-Success "Scraper started (PID: $($scraperProcess.Id))"
    } else {
        Write-Error "Failed to start scraper"
    }
    
    # Wait for scraper to be ready
    Write-Step "Waiting for scraper to be ready..."
    $scraperReady = $false
    for ($i = 0; $i -lt 30; $i++) {
        try {
            $response = Invoke-WebRequest -Uri "http://localhost:8000/docs" -UseBasicParsing -TimeoutSec 2 -ErrorAction SilentlyContinue
            if ($response.StatusCode -eq 200) {
                $scraperReady = $true
                Write-Success "Scraper is ready"
                break
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }
    
    if (-not $scraperReady) {
        Write-Error "Scraper did not start in time"
        exit 1
    }
    
    # Start Backend
    Write-Step "Starting Backend API (port 3000)..."
    $backendPath = "C:\Projects\Real-Estate-CRM-Repos\real-estate-crm-backend"
    $backendProcess = Start-Process -FilePath "powershell.exe" -ArgumentList "-Command", "cd '$backendPath'; npm start" -PassThru -WindowStyle Hidden
    
    if ($backendProcess) {
        Write-Success "Backend started (PID: $($backendProcess.Id))"
    } else {
        Write-Error "Failed to start backend"
    }
    
    # Wait for backend to be ready
    Write-Step "Waiting for backend to be ready..."
    $backendReady = $false
    for ($i = 0; $i -lt 60; $i++) {
        try {
            $response = Invoke-WebRequest -Uri "http://localhost:3000/api/health" -UseBasicParsing -TimeoutSec 2 -ErrorAction SilentlyContinue
            if ($response.StatusCode -eq 200) {
                $backendReady = $true
                Write-Success "Backend is ready"
                break
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }
    
    if (-not $backendReady) {
        Write-Error "Backend did not start in time (may still be starting...)"
    }
    
    Start-Sleep -Seconds 3
}

# Step 3: Run Consolidation Tests
Write-Header "Step 3: Running Consolidation Tests"
cd C:\Projects\Real-Estate-CRM-Repos\real-estate-crm-scraper
python test_consolidation.py

if ($LASTEXITCODE -ne 0) {
    Write-Error "Consolidation tests failed!"
    exit 1
}

Write-Success "Consolidation tests passed"

# Step 4: Run API Endpoint Tests
Write-Header "Step 4: Testing API Endpoints"

# Check if services are running
Write-Step "Checking service availability..."
try {
    $scraperCheck = Invoke-WebRequest -Uri "http://localhost:8000/docs" -UseBasicParsing -TimeoutSec 2
    Write-Success "Scraper API is available"
} catch {
    Write-Error "Scraper API is not available - please start it manually"
}

try {
    $backendCheck = Invoke-WebRequest -Uri "http://localhost:3000/api/mlsproperties?limit=1" -UseBasicParsing -TimeoutSec 2
    Write-Success "Backend API is available"
} catch {
    Write-Error "Backend API is not available - please start it manually"
}

Write-Step "Running API endpoint tests..."
python scripts\test_api_endpoints.py

if ($LASTEXITCODE -ne 0) {
    Write-Error "API endpoint tests failed!"
} else {
    Write-Success "API endpoint tests passed"
}

# Step 5: Generate Final Report
Write-Header "Step 5: Test Summary"

Write-Host "All tests completed!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Review test results above" -ForegroundColor White
Write-Host "  2. Test frontend manually in browser" -ForegroundColor White
Write-Host "  3. Test filters and property displays" -ForegroundColor White
Write-Host "  4. Check browser console for errors" -ForegroundColor White
Write-Host ""
Write-Host "Service URLs:" -ForegroundColor Cyan
Write-Host "  - Scraper API: http://localhost:8000" -ForegroundColor White
Write-Host "  - Backend API: http://localhost:3000" -ForegroundColor White
Write-Host "  - Frontend: Check your frontend URL" -ForegroundColor White
Write-Host ""

