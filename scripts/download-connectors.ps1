# download-connectors.ps1
# Path: <your-flink-cluster>/scripts/download-connectors.ps1
#
# Downloads the correct Flink connector JARs for ANY Flink version.
# Automatically detects your Flink version from a running container,
# or you can set it manually below.
#
# Usage:
#   .\download-connectors.ps1                      # auto-detect from running container
#   .\download-connectors.ps1 -FlinkVersion 1.19.1 # specify manually
#   .\download-connectors.ps1 -FlinkVersion 1.18.1

param(
    [string]$FlinkVersion = "",
    [string]$ConnectorsDir = ".\connectors"
)

# ── Kafka connector version map ─────────────────────────────────────────────────
# Maps Flink minor version to the latest compatible kafka connector version.
# The kafka connector follows its own versioning: <connector>-<flink-minor>
# See: https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/
$KafkaConnectorMap = @{
    "1.20" = "3.3.0-1.20"
    "1.19" = "3.3.0-1.19"
    "1.18" = "3.3.0-1.18"
    "1.17" = "3.2.0-1.17"
    "1.16" = "3.1.0-1.16"
    "1.15" = "1.15.4"
}

# ── Auto-detect Flink version if not specified ──────────────────────────────────
if (-not $FlinkVersion) {
    Write-Host "[INFO] No version specified — trying to detect from running Flink container..." -ForegroundColor Cyan
    try {
        $detected = docker exec $(docker ps --filter "name=jobmanager" --format "{{.Names}}" | Select-Object -First 1) `
            bash -c "flink --version 2>/dev/null | grep -oP '\d+\.\d+\.\d+' | head -1" 2>$null
        if ($detected -match '^\d+\.\d+\.\d+$') {
            $FlinkVersion = $detected.Trim()
            Write-Host "[INFO] Detected Flink version: $FlinkVersion" -ForegroundColor Green
        }
    } catch {}
}

if (-not $FlinkVersion) {
    Write-Host "[ERROR] Could not detect Flink version. Please specify it:" -ForegroundColor Red
    Write-Host "  .\download-connectors.ps1 -FlinkVersion 1.19.1" -ForegroundColor Yellow
    exit 1
}

# Parse minor version (e.g. "1.19.1" -> "1.19")
$MinorVersion = ($FlinkVersion -split '\.')[0..1] -join '.'

if (-not $KafkaConnectorMap.ContainsKey($MinorVersion)) {
    Write-Host "[WARN] Flink $MinorVersion not in connector map. Attempting best-effort with latest." -ForegroundColor Yellow
    $KafkaConnector = "3.3.0-$MinorVersion"
} else {
    $KafkaConnector = $KafkaConnectorMap[$MinorVersion]
}

Write-Host ""
Write-Host "=== FlinkSQL Studio Connector Downloader ===" -ForegroundColor Cyan
Write-Host "  Flink version  : $FlinkVersion"  -ForegroundColor White
Write-Host "  Kafka connector: $KafkaConnector" -ForegroundColor White
Write-Host "  Output dir     : $ConnectorsDir"  -ForegroundColor White
Write-Host ""

if (-not (Test-Path $ConnectorsDir)) {
    New-Item -ItemType Directory -Path $ConnectorsDir | Out-Null
    Write-Host "[INFO] Created $ConnectorsDir" -ForegroundColor Cyan
}

# ── Remove stale kafka connector JARs (any version that doesn't match) ──────────
Write-Host "=== Removing stale kafka connector JARs ===" -ForegroundColor Cyan
$targetFile = "flink-sql-connector-kafka-$KafkaConnector.jar"
$removed = $false
Get-ChildItem $ConnectorsDir -Filter "flink-sql-connector-kafka-*.jar" -ErrorAction SilentlyContinue | ForEach-Object {
    if ($_.Name -ne $targetFile) {
        Write-Host "[REMOVE] $($_.Name)" -ForegroundColor Red
        Remove-Item $_.FullName -Force
        $removed = $true
    }
}
if (-not $removed) { Write-Host "[OK]  No stale kafka JARs found." -ForegroundColor Green }

# ── JARs to download ────────────────────────────────────────────────────────────
$BaseUrl = "https://repo1.maven.org/maven2/org/apache/flink"

$Jars = @(
    @{
        url  = "$BaseUrl/flink-sql-connector-kafka/$KafkaConnector/flink-sql-connector-kafka-$KafkaConnector.jar"
        file = "flink-sql-connector-kafka-$KafkaConnector.jar"
        desc = "Kafka SQL connector"
    },
    @{
        url  = "$BaseUrl/flink-sql-avro-confluent-registry/$FlinkVersion/flink-sql-avro-confluent-registry-$FlinkVersion.jar"
        file = "flink-sql-avro-confluent-registry-$FlinkVersion.jar"
        desc = "Avro Confluent Schema Registry format"
    },
    @{
        url  = "$BaseUrl/flink-sql-avro/$FlinkVersion/flink-sql-avro-$FlinkVersion.jar"
        file = "flink-sql-avro-$FlinkVersion.jar"
        desc = "Avro core format"
    }
)

Write-Host ""
Write-Host "=== Downloading connector JARs ===" -ForegroundColor Cyan

foreach ($jar in $Jars) {
    $dest = Join-Path $ConnectorsDir $jar.file
    if (Test-Path $dest) {
        Write-Host "[SKIP]     $($jar.file) already exists" -ForegroundColor Yellow
    } else {
        Write-Host "[DOWNLOAD] $($jar.desc)..." -ForegroundColor Cyan
        try {
            Invoke-WebRequest -Uri $jar.url -OutFile $dest -UseBasicParsing
            $sizeKb = [math]::Round((Get-Item $dest).Length / 1KB)
            Write-Host "[OK]       $($jar.file)  ($sizeKb KB)" -ForegroundColor Green
        } catch {
            Write-Host "[ERROR]    Failed: $_" -ForegroundColor Red
            Write-Host "           Check https://repo1.maven.org/maven2/org/apache/flink/ for available versions" -ForegroundColor Yellow
            if (Test-Path $dest) { Remove-Item $dest -Force }
        }
    }
}

# ── Summary ─────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=== Connector JARs in $ConnectorsDir ===" -ForegroundColor Cyan
Get-ChildItem $ConnectorsDir -Filter "*.jar" |
    Select-Object Name, @{n="Size";e={"{0:N0} KB" -f ($_.Length/1KB)}} |
    Format-Table -AutoSize

Write-Host "=== Next steps ===" -ForegroundColor Cyan
Write-Host "  Rebuild your JM/TM images so JARs land in /opt/flink/lib/:" -ForegroundColor White
Write-Host "    docker compose down" -ForegroundColor Yellow
Write-Host "    docker compose build" -ForegroundColor Yellow
Write-Host "    docker compose up -d" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Verify:" -ForegroundColor White
Write-Host "    docker exec <taskmanager> bash -c 'ls /opt/flink/lib/ | grep kafka'" -ForegroundColor Yellow
Write-Host "  Expected: flink-sql-connector-kafka-$KafkaConnector.jar" -ForegroundColor Green
Write-Host ""
