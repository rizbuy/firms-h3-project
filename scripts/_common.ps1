Set-StrictMode -Version Latest

function Get-ProjectRoot {
    return (Split-Path -Parent $PSScriptRoot)
}

function Get-ProjectEnvValue {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [string]$EnvPath = (Join-Path (Get-ProjectRoot) ".env"),
        [switch]$AllowEmpty
    )

    if (-not (Test-Path -LiteralPath $EnvPath)) {
        throw "File .env tidak ditemukan di $EnvPath. Jalankan .\scripts\generate-local-secrets.ps1 terlebih dahulu."
    }

    $pattern = "^\s*$([regex]::Escape($Name))\s*=\s*(.*)$"
    $match = Get-Content -LiteralPath $EnvPath | Where-Object { $_ -match $pattern } | Select-Object -Last 1
    if (-not $match) {
        if ($AllowEmpty) { return "" }
        throw "$Name belum diatur di .env."
    }

    $value = [regex]::Match($match, $pattern).Groups[1].Value.Trim()

    if ($value.Length -ge 2 -and (
        ($value.StartsWith('"') -and $value.EndsWith('"')) -or
        ($value.StartsWith("'") -and $value.EndsWith("'"))
    )) {
        $value = $value.Substring(1, $value.Length - 2)
    } else {
        # Hapus komentar inline (# ...) jika nilai tidak dibungkus tanda kutip
        $value = ($value -replace '\s+#.*$', '').Trim()
    }

    if (-not $AllowEmpty) {
        if ([string]::IsNullOrWhiteSpace($value) -or $value -eq "replace_me" -or $value.StartsWith("replace_with_")) {
            throw "$Name masih kosong atau masih memakai nilai contoh di .env."
        }
    }

    return $value
}

