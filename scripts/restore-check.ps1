param(
    [switch]$KeepDatabase
)

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\_common.ps1"

$projectRoot = Get-ProjectRoot
$backupDir = Join-Path $projectRoot "backups"
$archive = Get-ChildItem -LiteralPath $backupDir -Filter "fire_analytics_*.dump" -File |
    Sort-Object Name -Descending |
    Select-Object -First 1

if (-not $archive) { throw "Backup .dump tidak ditemukan di folder backups." }

$database = "restore_check_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
$containerArchive = "/tmp/$($archive.Name)"
$dbCreated = $false

$countQuery = @"
SELECT '1_raw_detections=' || COUNT(*)::text FROM public.raw_firms_detections
UNION ALL SELECT '2_staging_h3=' || COALESCE((SELECT COUNT(*)::text FROM to_regclass('staging.firms_h3_events') r CROSS JOIN staging.firms_h3_events WHERE r IS NOT NULL), '0')
UNION ALL SELECT '3_daily_h3=' || COUNT(*)::text FROM mart.fire_activity_daily
UNION ALL SELECT '4_risk_history_scd2=' || COUNT(*)::text FROM mart.h3_risk_history
UNION ALL SELECT '5_realtime_alerts=' || COALESCE((SELECT COUNT(*)::text FROM to_regclass('mart.realtime_high_risk_alerts') r CROSS JOIN mart.realtime_high_risk_alerts WHERE r IS NOT NULL), '0')
ORDER BY 1;
"@

Push-Location $projectRoot
try {
    docker compose cp $archive.FullName "postgres:$containerArchive"
    if ($LASTEXITCODE -ne 0) { throw "Gagal menyalin backup ke kontainer PostgreSQL." }

    docker compose exec -T postgres createdb -U fire_user $database
    if ($LASTEXITCODE -ne 0) { throw "Gagal membuat database uji pemulihan $database." }
    $dbCreated = $true

    docker compose exec -T postgres pg_restore --no-owner --no-privileges -U fire_user -d $database $containerArchive
    if ($LASTEXITCODE -ne 0) { throw "Pemulihan gagal pada database uji $database." }

    $restoredCounts = @(docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U fire_user -d $database -At -c $countQuery)
    if ($LASTEXITCODE -ne 0) { throw "Validasi tabel/row count gagal pada database uji $database." }

    $sourceCounts = @(docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U fire_user -d fire_analytics -At -c $countQuery)
    $matchesSource = (($restoredCounts -join ";") -eq ($sourceCounts -join ";"))

    [pscustomobject]@{
        BackupFile      = $archive.Name
        RestoreDatabase = if ($KeepDatabase) { "$database (retained)" } else { "$database (auto-dropped after check)" }
        TableCounts     = ($restoredCounts -join "; ")
        MatchesLiveDB   = $matchesSource
        RestoreCheck    = "OK"
    } | Format-List
}
finally {
    docker compose exec -T postgres rm -f $containerArchive *> $null
    if ($dbCreated -and -not $KeepDatabase) {
        docker compose exec -T postgres dropdb --if-exists -U fire_user $database *> $null
    }
    Pop-Location
}
