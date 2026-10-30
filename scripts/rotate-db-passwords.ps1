param()

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\_common.ps1"

$projectRoot = Get-ProjectRoot
$postgresPassword = (Get-ProjectEnvValue -Name "POSTGRES_PASSWORD").Replace("'", "''")
$debeziumPassword = (Get-ProjectEnvValue -Name "DEBEZIUM_PASSWORD").Replace("'", "''")

$sql = @'
CREATE TABLE IF NOT EXISTS public.raw_firms_detections (
    detection_key text PRIMARY KEY,
    latitude double precision NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude double precision NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    bright_ti4 double precision,
    scan double precision,
    track double precision,
    acq_date date NOT NULL,
    acq_time smallint NOT NULL CHECK (acq_time BETWEEN 0 AND 2359),
    satellite text NOT NULL,
    instrument text NOT NULL,
    confidence text,
    version text,
    bright_ti5 double precision,
    frp double precision,
    daynight char(1),
    raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    ingested_at timestamptz NOT NULL DEFAULT now()
);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium_user') THEN
        CREATE ROLE debezium_user WITH LOGIN REPLICATION;
    END IF;
END
$$;
GRANT CONNECT ON DATABASE fire_analytics TO debezium_user;
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON TABLE public.raw_firms_detections TO debezium_user;
SELECT 'CREATE PUBLICATION firms_detections_publication FOR TABLE public.raw_firms_detections WITH (publish = ''insert, update, delete'')'
WHERE NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'firms_detections_publication')
\gexec
'@
$sql += "`nALTER ROLE fire_user WITH PASSWORD '$postgresPassword';"
$sql += "`nALTER ROLE debezium_user WITH LOGIN REPLICATION PASSWORD '$debeziumPassword';"

Push-Location $projectRoot
try {
    $sql | docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U fire_user -d fire_analytics
    if ($LASTEXITCODE -ne 0) { throw "Pembaruan kata sandi PostgreSQL gagal (kode $LASTEXITCODE)." }
    Write-Output "Kata sandi fire_user dan debezium_user sudah diperbarui."
    Write-Output "Catatan: Jika kontainer Kafka Connect aktif, jalankan .\scripts\register-connector.ps1 untuk menyinkronkan password konektor."
}
finally {
    Pop-Location
}
