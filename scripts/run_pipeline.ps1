param()

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\_common.ps1"

$projectPath = Get-ProjectRoot
$logsPath = Join-Path $projectPath "logs"
New-Item -ItemType Directory -Force -Path $logsPath | Out-Null

$transcriptPath = Join-Path $logsPath ("pipeline-{0}.log" -f (Get-Date -Format "yyyyMMdd"))
$transcriptStarted = $false
$exitCode = 0

Push-Location $projectPath
try {
    Start-Transcript -Path $transcriptPath -Append | Out-Null
    $transcriptStarted = $true
    Write-Output "Pipeline FIRMS dimulai: $(Get-Date -Format o)"

    & docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Desktop tidak tersedia. Pastikan Docker Desktop sudah berjalan."
    }

    # Kirim flag -e langsung ke Docker Compose agar PowerShell 5.1 tidak menghapus
    # variabel kosong dan tidak jatuh kembali ke FIRMS_START_DATE backfill di .env.
    & docker compose run --rm `
        -e FIRMS_START_DATE= `
        -e FIRMS_END_DATE= `
        -e FIRMS_DAY_RANGE=5 `
        ingest
    if ($LASTEXITCODE -ne 0) {
        throw "Ingest FIRMS gagal dengan exit code $LASTEXITCODE."
    }

    & docker compose run --rm ingest python build_marts.py
    if ($LASTEXITCODE -ne 0) {
        throw "Transformasi H3/SCD Tipe 2 gagal dengan exit code $LASTEXITCODE."
    }

    Write-Output "Pipeline FIRMS selesai: $(Get-Date -Format o)"
}
catch {
    $exitCode = 1
    Write-Output "Pipeline FIRMS gagal: $($_.Exception.Message)"
}
finally {
    if ($transcriptStarted) {
        Stop-Transcript | Out-Null
    }
    Pop-Location
}

exit $exitCode
