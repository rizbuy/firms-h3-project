param()

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\_common.ps1"

$projectRoot = Get-ProjectRoot
$templatePath = Join-Path $projectRoot "connect\postgres-connector.json"
$connectorSpec = Get-Content -LiteralPath $templatePath -Raw | ConvertFrom-Json
$connectorName = $connectorSpec.name
$secret = Get-ProjectEnvValue -Name "DEBEZIUM_PASSWORD"
$baseUri = "http://localhost:18083/connectors/$connectorName"

try {
    $config = Invoke-RestMethod -Uri "$baseUri/config" -TimeoutSec 10
    $status = Invoke-RestMethod -Uri "$baseUri/status" -TimeoutSec 10
    [pscustomobject]@{
        ConnectorName      = $connectorName
        ConnectorState     = $status.connector.state
        TasksState         = (@($status.tasks | ForEach-Object { $_.state }) -join ", ")
        PasswordMatchesEnv = ($config.'database.password' -ceq $secret)
    } | Format-List
}
catch {
    $statusCode = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { 0 }
    if ($statusCode -eq 404) {
        throw "Konektor '$connectorName' belum terdaftar. Jalankan .\scripts\register-connector.ps1 terlebih dahulu."
    } elseif ($statusCode -eq 0) {
        throw "Kafka Connect tidak dapat dihubungi di http://localhost:18083. Pastikan kontainer 'connect' sedang berjalan."
    }
    throw
}
finally {
    Remove-Variable secret, config, status -ErrorAction SilentlyContinue
}
