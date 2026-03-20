#!/usr/bin/env pwsh
# ─────────────────────────────────────────────────────────────────────────────
# Str:::Lab Studio — Windows PowerShell setup script
# Usage:
#   .\scripts\setup.ps1
#   .\scripts\setup.ps1 -GatewayHost my-cluster.example.com -GatewayPort 8083
#   .\scripts\setup.ps1 -Build   # build image from source instead of pulling
# ─────────────────────────────────────────────────────────────────────────────
param(
    [string]$GatewayHost = "localhost",
    [int]   $GatewayPort = 8083,
    [string]$JobManagerHost = "localhost",
    [int]   $JobManagerPort = 8081,
    [int]   $StudioPort  = 3030,
    [switch]$Build
)

$Image     = "codedstreams/flinksql-studio:latest"
$Container = "flinksql-studio"

Write-Host ""
Write-Host "╔══════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   Str:::Lab Studio — Windows Setup            ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Check Docker
try { docker info > $null 2>&1; Write-Host "[✓] Docker is running" -ForegroundColor Green }
catch { Write-Host "[✗] Docker not running — start Docker Desktop first" -ForegroundColor Red; exit 1 }

# Remove existing container
$existing = docker ps -aq --filter "name=$Container" 2>$null
if ($existing) { docker rm -f $Container > $null 2>&1 }

if ($Build) {
    Write-Host "[·] Building from source ..." -ForegroundColor Yellow
    $root = Split-Path $PSScriptRoot -Parent
    docker build -t $Image $root
} else {
    Write-Host "[·] Pulling $Image ..." -ForegroundColor Cyan
    docker pull $Image
    if ($LASTEXITCODE -ne 0) { Write-Host "[✗] Pull failed" -ForegroundColor Red; exit 1 }
}

Write-Host "[·] Starting on port $StudioPort ..." -ForegroundColor Cyan
docker run -d `
    --name $Container `
    --restart unless-stopped `
    -p "${StudioPort}:80" `
    -e "FLINK_GATEWAY_HOST=$GatewayHost" `
    -e "FLINK_GATEWAY_PORT=$GatewayPort" `
    -e "JOBMANAGER_HOST=$JobManagerHost" `
    -e "JOBMANAGER_PORT=$JobManagerPort" `
    $Image

if ($LASTEXITCODE -ne 0) { Write-Host "[✗] Failed to start" -ForegroundColor Red; exit 1 }

Write-Host ""
Write-Host "✅  Str:::Lab Studio is running!" -ForegroundColor Green
Write-Host "   Open:       http://localhost:$StudioPort" -ForegroundColor Green
Write-Host "   Gateway:    ${GatewayHost}:${GatewayPort}" -ForegroundColor Gray
Write-Host "   JobManager: ${JobManagerHost}:${JobManagerPort}" -ForegroundColor Gray
Write-Host ""
Write-Host "   Stop:  docker stop $Container" -ForegroundColor Gray
Write-Host "   Logs:  docker logs -f $Container" -ForegroundColor Gray
Write-Host ""

Start-Process "http://localhost:$StudioPort"
