param(
    [int]$KeepCount = 10
)

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\_common.ps1"

$projectRoot = Get-ProjectRoot
$backupDir = Join-Path $projectRoot "backups"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$archiveName = "fire_analytics_$timestamp.dump"
$containerArchive = "/tmp/$archiveName"
$hostArchive = Join-Path $backupDir $archiveName

New-Item -ItemType Directory -Path $backupDir -Force | Out-Null
Push-Location $projectRoot
try {
    docker compose exec -T postgres pg_dump -U fire_user -d fire_analytics -Fc -f $containerArchive
    if ($LASTEXITCODE -ne 0) { throw "pg_dump gagal (kode $LASTEXITCODE). Pastikan PostgreSQL sehat." }

    docker compose exec -T postgres pg_restore --list $containerArchive | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Pemeriksaan arsip backup di kontainer gagal." }

    docker compose cp "postgres:$containerArchive" $hostArchive
    if ($LASTEXITCODE -ne 0) { throw "Gagal menyalin arsip backup ke folder lokal." }

    $file = Get-Item -LiteralPath $hostArchive
    if ($file.Length -le 0) { throw "Arsip backup lokal kosong (0 byte)." }

    if ($KeepCount -gt 0) {
        $oldBackups = @(
            Get-ChildItem -LiteralPath $backupDir -Filter "fire_analytics_*.dump" -File |
                Sort-Object Name -Descending |
                Select-Object -Skip $KeepCount
        )
        foreach ($old in $oldBackups) {
            Remove-Item -LiteralPath $old.FullName -Force -ErrorAction SilentlyContinue
        }
    }

    [pscustomobject]@{
        BackupFile   = $file.FullName
        SizeMB       = [math]::Round($file.Length / 1MB, 2)
        RetainedMax  = $KeepCount
        ArchiveCheck = "OK"
    } | Format-List
}
finally {
    docker compose exec -T postgres rm -f $containerArchive *> $null
    Pop-Location
}
