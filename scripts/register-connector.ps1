param()

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\_common.ps1"

$projectRoot = Get-ProjectRoot
$templatePath = Join-Path $projectRoot "connect\postgres-connector.json"
$password = Get-ProjectEnvValue -Name "DEBEZIUM_PASSWORD"

$connector = Get-Content -LiteralPath $templatePath -Raw | ConvertFrom-Json
$connector.config | Add-Member -NotePropertyName "database.password" -NotePropertyValue $password -Force
$baseUri = "http://localhost:18083/connectors"

Push-Location $projectRoot
try {
    $exists = $true
    try {
        Invoke-RestMethod -Uri "$baseUri/$($connector.name)/config" -TimeoutSec 10 | Out-Null
    }
    catch {
        $statusCode = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { 0 }
        if ($statusCode -eq 404) {
            $exists = $false
        } elseif ($statusCode -eq 0) {
            throw "Kafka Connect belum merespons di $baseUri. Pastikan kontainer 'kafka' dan 'connect' sudah berjalan."
        } else {
            throw
        }
    }

    if ($exists) {
        Invoke-RestMethod -Method Put -Uri "$baseUri/$($connector.name)/config" -ContentType "application/json" -Body ($connector.config | ConvertTo-Json -Depth 20) | Out-Null
        Write-Output "Konfigurasi konektor $($connector.name) diperbarui."
    } else {
        $body = $connector | ConvertTo-Json -Depth 20
        Invoke-RestMethod -Method Post -Uri $baseUri -ContentType "application/json" -Body $body | Out-Null
        Write-Output "Konektor $($connector.name) berhasil didaftarkan."
    }
}
finally {
    Remove-Variable password, connector -ErrorAction SilentlyContinue
    Pop-Location
}
