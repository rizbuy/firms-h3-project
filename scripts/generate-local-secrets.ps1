param()

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\_common.ps1"

$projectRoot = Get-ProjectRoot
$envPath = Join-Path $projectRoot ".env"
$examplePath = Join-Path $projectRoot ".env.example"

if (-not (Test-Path -LiteralPath $envPath)) {
    Copy-Item -LiteralPath $examplePath -Destination $envPath
}

function New-Secret {
    $bytes = [byte[]]::new(32)
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    return [BitConverter]::ToString($bytes).Replace("-", "")
}

$lines = [System.Collections.Generic.List[string]]::new()
$existingLines = @(Get-Content -LiteralPath $envPath)
if ($existingLines.Count -gt 0) {
    $lines.AddRange([string[]]$existingLines)
}

$updated = $false
foreach ($name in @("POSTGRES_PASSWORD", "DEBEZIUM_PASSWORD")) {
    $pattern = "^\s*$([regex]::Escape($name))\s*=\s*(.*)$"
    $index = -1
    for ($i = 0; $i -lt $lines.Count; $i++) {
        if ($lines[$i] -match $pattern) { $index = $i }
    }

    if ($index -ge 0 -and $lines[$index] -notmatch '=\s*["'']?(replace_with_|replace_me|\s*["'']?$)') {
        continue
    }
    $entry = "$name=$(New-Secret)"
    if ($index -ge 0) { $lines[$index] = $entry } else { $lines.Add($entry) }
    $updated = $true
}

if ($updated) {
    [System.IO.File]::WriteAllLines($envPath, $lines, [System.Text.UTF8Encoding]::new($false))
    Write-Output "Rahasia lokal dibuat atau diperbarui di .env. Nilainya tidak ditampilkan."
} else {
    Write-Output "Rahasia sudah tersedia di .env; tidak ada perubahan."
}
