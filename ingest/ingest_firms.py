import csv
import hashlib
import io
import os
import sys
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

import psycopg
import requests
from psycopg.types.json import Jsonb

SOURCE = os.getenv("FIRMS_SOURCE", "VIIRS_SNPP_NRT")
AREA = os.getenv("FIRMS_AREA", "94,-11,142,7")  # Indonesia bounding box: west,south,east,north
DAY_RANGE = int(os.getenv("FIRMS_DAY_RANGE", "1"))

ENSURE_RAW_TABLE_SQL = """
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

CREATE INDEX IF NOT EXISTS idx_raw_firms_detections_acq_date
    ON public.raw_firms_detections (acq_date);
"""

UPSERT_SQL = """
INSERT INTO public.raw_firms_detections (
    detection_key, latitude, longitude, bright_ti4, scan, track,
    acq_date, acq_time, satellite, instrument, confidence, version,
    bright_ti5, frp, daynight, raw_payload
) VALUES (
    %(detection_key)s, %(latitude)s, %(longitude)s, %(bright_ti4)s,
    %(scan)s, %(track)s, %(acq_date)s, %(acq_time)s, %(satellite)s,
    %(instrument)s, %(confidence)s, %(version)s, %(bright_ti5)s,
    %(frp)s, %(daynight)s, %(raw_payload)s
)
ON CONFLICT (detection_key) DO UPDATE SET
    bright_ti4 = EXCLUDED.bright_ti4,
    scan = EXCLUDED.scan,
    track = EXCLUDED.track,
    confidence = EXCLUDED.confidence,
    version = EXCLUDED.version,
    bright_ti5 = EXCLUDED.bright_ti5,
    frp = EXCLUDED.frp,
    daynight = EXCLUDED.daynight,
    raw_payload = EXCLUDED.raw_payload,
    ingested_at = now()
WHERE (
    raw_firms_detections.bright_ti4,
    raw_firms_detections.scan,
    raw_firms_detections.track,
    raw_firms_detections.confidence,
    raw_firms_detections.version,
    raw_firms_detections.bright_ti5,
    raw_firms_detections.frp,
    raw_firms_detections.daynight,
    raw_firms_detections.raw_payload
) IS DISTINCT FROM (
    EXCLUDED.bright_ti4,
    EXCLUDED.scan,
    EXCLUDED.track,
    EXCLUDED.confidence,
    EXCLUDED.version,
    EXCLUDED.bright_ti5,
    EXCLUDED.frp,
    EXCLUDED.daynight,
    EXCLUDED.raw_payload
)
"""


def numeric(row, field):
    value = row.get(field, "").strip()
    if not value:
        return None
    try:
        return float(Decimal(value))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid numeric value in {field}") from exc


def detection_key(row):
    # VIIRS coordinates are published to five decimal places. Include acquisition
    # time and satellite so nearby observations remain separate detections.
    parts = (
        SOURCE,
        row["satellite"].strip(),
        row["instrument"].strip(),
        row["acq_date"].strip(),
        str(int(row["acq_time"].strip())),
        f"{Decimal(row['latitude'].strip()):.5f}",
        f"{Decimal(row['longitude'].strip()):.5f}",
    )
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def fetch_rows(map_key, start_date=None, end_date=None):
    windows = []
    if start_date and end_date:
        cursor = start_date
        while cursor <= end_date:
            days = min(5, (end_date - cursor).days + 1)
            windows.append((cursor, days))
            cursor += timedelta(days=days)
    else:
        windows.append((None, DAY_RANGE))

    required = {
        "latitude", "longitude", "bright_ti4", "scan", "track",
        "acq_date", "acq_time", "satellite", "instrument", "confidence",
        "version", "bright_ti5", "frp", "daynight",
    }
    records_by_key = {}
    for window_start, days in windows:
        date_suffix = f"/{window_start.isoformat()}" if window_start else ""
        url = (
            "https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
            f"{map_key}/{SOURCE}/{AREA}/{days}{date_suffix}"
        )
        try:
            response = requests.get(url, timeout=60)
        except requests.RequestException:
            raise SystemExit("Gagal menghubungi FIRMS API. Periksa koneksi internet.")

        if response.status_code != 200:
            raise SystemExit(f"FIRMS API mengembalikan HTTP {response.status_code}.")

        reader = csv.DictReader(io.StringIO(response.text))
        if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
            raise SystemExit("Respons FIRMS tidak berisi kolom VIIRS yang diharapkan.")

        for row in reader:
            try:
                record = {
                    "detection_key": detection_key(row),
                    "latitude": numeric(row, "latitude"),
                    "longitude": numeric(row, "longitude"),
                    "bright_ti4": numeric(row, "bright_ti4"),
                    "scan": numeric(row, "scan"),
                    "track": numeric(row, "track"),
                    "acq_date": row["acq_date"].strip(),
                    "acq_time": int(row["acq_time"].strip()),
                    "satellite": row["satellite"].strip(),
                    "instrument": row["instrument"].strip(),
                    "confidence": row["confidence"].strip() or None,
                    "version": row["version"].strip() or None,
                    "bright_ti5": numeric(row, "bright_ti5"),
                    "frp": numeric(row, "frp"),
                    "daynight": row["daynight"].strip()[:1] or None,
                    "raw_payload": Jsonb(row),
                }
            except (KeyError, ValueError, InvalidOperation) as exc:
                raise SystemExit("Ada baris FIRMS yang formatnya tidak valid.") from exc
            records_by_key[record["detection_key"]] = record
    return list(records_by_key.values())


def run_ingestion():
    map_key = os.getenv("FIRMS_MAP_KEY", "").strip()
    if not map_key or map_key == "replace_me":
        raise SystemExit("Isi FIRMS_MAP_KEY di file .env proyek terlebih dahulu.")

    start_text = os.getenv("FIRMS_START_DATE", "").strip()
    end_text = os.getenv("FIRMS_END_DATE", "").strip()
    if bool(start_text) != bool(end_text):
        raise SystemExit("Isi FIRMS_START_DATE dan FIRMS_END_DATE bersama-sama, atau kosongkan keduanya.")

    start_date = end_date = None
    if start_text and end_text:
        try:
            start_date = date.fromisoformat(start_text)
            end_date = date.fromisoformat(end_text)
        except ValueError as exc:
            raise SystemExit("Tanggal harus memakai format YYYY-MM-DD.") from exc
        if end_date < start_date:
            raise SystemExit("FIRMS_END_DATE harus sama dengan atau setelah FIRMS_START_DATE.")
    elif not 1 <= DAY_RANGE <= 5:
        raise SystemExit("FIRMS_DAY_RANGE harus bernilai antara 1 dan 5.")

    records = fetch_rows(map_key, start_date, end_date)
    db_password = os.getenv("POSTGRES_PASSWORD", "").strip()
    if not db_password:
        raise SystemExit("POSTGRES_PASSWORD belum diatur.")
    db_url = (
        f"host={os.getenv('DB_HOST', 'postgres')} "
        f"port={os.getenv('DB_PORT', '5432')} "
        f"dbname={os.getenv('POSTGRES_DB', 'fire_analytics')} "
        f"user={os.getenv('POSTGRES_USER', 'fire_user')} "
        f"password={db_password}"
    )
    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(ENSURE_RAW_TABLE_SQL)
                cur.executemany(UPSERT_SQL, records)
                affected = cur.rowcount
    except psycopg.Error as exc:
        print(f"Gagal menyimpan data ke PostgreSQL: {exc.__class__.__name__}", file=sys.stderr)
        raise SystemExit(1)

    window_label = f"{start_date} s/d {end_date}" if start_date else f"{DAY_RANGE} hari terakhir"
    print(f"Baris FIRMS diterima: {len(records)}")
    print(f"Baris baru atau berubah di PostgreSQL: {affected}")
    print(f"Sumber: {SOURCE}; area: {AREA}; rentang: {window_label}")

    return {
        "fetched_rows": len(records),
        "upserted_rows": affected,
        "source": SOURCE,
        "area": AREA,
        "window": window_label,
    }


def main():
    run_ingestion()


if __name__ == "__main__":
    main()
