import os
import sys

import h3
import psycopg

H3_RESOLUTION = int(os.getenv("H3_RESOLUTION", "5"))

CREATE_MARTS_SQL = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS staging.firms_h3_events (
    detection_key text PRIMARY KEY,
    acq_date date NOT NULL,
    acq_time smallint NOT NULL,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    h3_index text NOT NULL,
    h3_resolution smallint NOT NULL,
    frp double precision,
    confidence text,
    satellite text NOT NULL,
    instrument text NOT NULL,
    daynight char(1),
    cdc_op char(1) NOT NULL,
    cdc_ts_ms bigint,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_staging_firms_h3_acq_date_cell
    ON staging.firms_h3_events (acq_date, h3_index);

CREATE TABLE IF NOT EXISTS mart.realtime_high_risk_alerts (
    acq_date date NOT NULL,
    h3_index text NOT NULL,
    h3_resolution smallint NOT NULL,
    detection_count bigint NOT NULL,
    total_frp double precision NOT NULL,
    max_frp double precision,
    risk_band text NOT NULL CHECK (risk_band IN ('medium', 'high')),
    first_alerted_at timestamptz NOT NULL DEFAULT now(),
    last_updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (acq_date, h3_index)
);

CREATE TABLE IF NOT EXISTS mart.fire_activity_daily (
    acq_date date NOT NULL,
    h3_index text NOT NULL,
    h3_resolution smallint NOT NULL,
    detection_count bigint NOT NULL,
    total_frp double precision NOT NULL,
    avg_frp double precision,
    max_frp double precision,
    risk_band text NOT NULL CHECK (risk_band IN ('low', 'medium', 'high')),
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (acq_date, h3_index)
);

CREATE TABLE IF NOT EXISTS mart.h3_risk_history (
    history_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    h3_index text NOT NULL,
    h3_resolution smallint NOT NULL,
    risk_band text NOT NULL CHECK (risk_band IN ('low', 'medium', 'high')),
    detection_count bigint NOT NULL,
    total_frp double precision NOT NULL,
    source_date date NOT NULL,
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    is_current boolean NOT NULL,
    CHECK ((is_current AND valid_to IS NULL) OR (NOT is_current AND valid_to IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_h3_risk_history_current
    ON mart.h3_risk_history (h3_index)
    WHERE is_current;

CREATE UNIQUE INDEX IF NOT EXISTS uq_h3_risk_history_cell_date
    ON mart.h3_risk_history (h3_index, source_date);

CREATE INDEX IF NOT EXISTS idx_h3_risk_history_dates
    ON mart.h3_risk_history (h3_index, valid_from, valid_to);
"""

FETCH_UNPROCESSED_RAW_SQL = """
SELECT
    r.detection_key,
    r.acq_date,
    r.acq_time,
    r.latitude,
    r.longitude,
    r.frp,
    r.confidence,
    r.satellite,
    r.instrument,
    r.daynight
FROM public.raw_firms_detections r
LEFT JOIN staging.firms_h3_events s
    ON s.detection_key = r.detection_key
WHERE s.detection_key IS NULL
   OR s.h3_resolution IS DISTINCT FROM %s;
"""

INSERT_FALLBACK_STAGING_SQL = """
INSERT INTO staging.firms_h3_events (
    detection_key, acq_date, acq_time, latitude, longitude,
    h3_index, h3_resolution, frp, confidence, satellite,
    instrument, daynight, cdc_op, cdc_ts_ms, processed_at
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, 'b', NULL, now()
)
ON CONFLICT (detection_key) DO UPDATE SET
    h3_index = EXCLUDED.h3_index,
    h3_resolution = EXCLUDED.h3_resolution,
    processed_at = now()
WHERE staging.firms_h3_events.h3_resolution IS DISTINCT FROM EXCLUDED.h3_resolution;
"""

UPSERT_REALTIME_ALERTS_FROM_STAGING_SQL = """
INSERT INTO mart.realtime_high_risk_alerts (
    acq_date, h3_index, h3_resolution, detection_count,
    total_frp, max_frp, risk_band, first_alerted_at, last_updated_at
)
SELECT
    acq_date,
    h3_index,
    h3_resolution,
    COUNT(*) AS detection_count,
    ROUND(COALESCE(SUM(frp), 0.0)::numeric, 4)::double precision AS total_frp,
    MAX(frp) AS max_frp,
    CASE
        WHEN COUNT(*) >= 25 THEN 'high'
        ELSE 'medium'
    END AS risk_band,
    now(),
    now()
FROM staging.firms_h3_events
WHERE h3_resolution = %s
GROUP BY acq_date, h3_index, h3_resolution
HAVING COUNT(*) >= 5
ON CONFLICT (acq_date, h3_index) DO UPDATE SET
    detection_count = EXCLUDED.detection_count,
    total_frp = EXCLUDED.total_frp,
    max_frp = EXCLUDED.max_frp,
    risk_band = EXCLUDED.risk_band,
    last_updated_at = now()
WHERE (
    realtime_high_risk_alerts.detection_count,
    realtime_high_risk_alerts.total_frp,
    realtime_high_risk_alerts.max_frp,
    realtime_high_risk_alerts.risk_band
) IS DISTINCT FROM (
    EXCLUDED.detection_count,
    EXCLUDED.total_frp,
    EXCLUDED.max_frp,
    EXCLUDED.risk_band
);
"""

PRUNE_STALE_REALTIME_ALERTS_SQL = """
DELETE FROM mart.realtime_high_risk_alerts a
WHERE NOT EXISTS (
    SELECT 1
    FROM staging.firms_h3_events s
    WHERE s.acq_date = a.acq_date
      AND s.h3_index = a.h3_index
      AND s.h3_resolution = %s
    GROUP BY s.acq_date, s.h3_index
    HAVING COUNT(*) >= 5
);
"""

UPSERT_DAILY_FROM_STAGING_SQL = """
INSERT INTO mart.fire_activity_daily (
    acq_date, h3_index, h3_resolution, detection_count,
    total_frp, avg_frp, max_frp, risk_band, refreshed_at
)
SELECT
    acq_date,
    h3_index,
    h3_resolution,
    COUNT(*) AS detection_count,
    ROUND(COALESCE(SUM(frp), 0.0)::numeric, 4)::double precision AS total_frp,
    ROUND(AVG(frp)::numeric, 4)::double precision AS avg_frp,
    MAX(frp) AS max_frp,
    CASE
        WHEN COUNT(*) >= 25 THEN 'high'
        WHEN COUNT(*) >= 5 THEN 'medium'
        ELSE 'low'
    END AS risk_band,
    now()
FROM staging.firms_h3_events
WHERE h3_resolution = %s
GROUP BY acq_date, h3_index, h3_resolution
ON CONFLICT (acq_date, h3_index) DO UPDATE SET
    h3_resolution = EXCLUDED.h3_resolution,
    detection_count = EXCLUDED.detection_count,
    total_frp = EXCLUDED.total_frp,
    avg_frp = EXCLUDED.avg_frp,
    max_frp = EXCLUDED.max_frp,
    risk_band = EXCLUDED.risk_band,
    refreshed_at = now()
WHERE (
    fire_activity_daily.h3_resolution,
    fire_activity_daily.detection_count,
    fire_activity_daily.total_frp,
    fire_activity_daily.avg_frp,
    fire_activity_daily.max_frp,
    fire_activity_daily.risk_band
) IS DISTINCT FROM (
    EXCLUDED.h3_resolution,
    EXCLUDED.detection_count,
    EXCLUDED.total_frp,
    EXCLUDED.avg_frp,
    EXCLUDED.max_frp,
    EXCLUDED.risk_band
);
"""

BUILD_TARGET_SCD2_TEMP_SQL = """
CREATE TEMP TABLE target_scd2 ON COMMIT DROP AS
WITH daily_lag AS (
    SELECT
        acq_date,
        h3_index,
        h3_resolution,
        detection_count,
        total_frp,
        risk_band,
        LAG(risk_band) OVER (PARTITION BY h3_index ORDER BY acq_date) AS prev_risk_band
    FROM mart.fire_activity_daily
),
state_islands AS (
    SELECT
        *,
        SUM(CASE WHEN prev_risk_band IS DISTINCT FROM risk_band THEN 1 ELSE 0 END)
            OVER (PARTITION BY h3_index ORDER BY acq_date) AS island_id
    FROM daily_lag
),
grouped_states AS (
    SELECT
        h3_index,
        MAX(h3_resolution)::smallint AS h3_resolution,
        risk_band,
        SUM(detection_count)::bigint AS detection_count,
        ROUND(SUM(total_frp)::numeric, 4)::double precision AS total_frp,
        MIN(acq_date) AS source_date
    FROM state_islands
    GROUP BY h3_index, island_id, risk_band
)
SELECT
    h3_index,
    h3_resolution,
    risk_band,
    detection_count,
    total_frp,
    source_date,
    (source_date::timestamp AT TIME ZONE 'UTC') AS valid_from,
    (LEAD(source_date) OVER (PARTITION BY h3_index ORDER BY source_date)::timestamp AT TIME ZONE 'UTC') AS valid_to,
    (LEAD(source_date) OVER (PARTITION BY h3_index ORDER BY source_date) IS NULL) AS is_current
FROM grouped_states;
"""

DELETE_OBSOLETE_SCD2_SQL = """
DELETE FROM mart.h3_risk_history h
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_temp.target_scd2 t
    WHERE t.h3_index = h.h3_index
      AND t.source_date = h.source_date
);
"""

CLOSE_SUPERSEDED_SCD2_SQL = """
UPDATE mart.h3_risk_history h
SET
    valid_to = t.valid_to,
    is_current = false
FROM pg_temp.target_scd2 t
WHERE h.h3_index = t.h3_index
  AND h.source_date = t.source_date
  AND h.is_current = true
  AND t.is_current = false;
"""

UPDATE_CHANGED_SCD2_SQL = """
UPDATE mart.h3_risk_history h
SET
    h3_resolution = t.h3_resolution,
    risk_band = t.risk_band,
    detection_count = t.detection_count,
    total_frp = t.total_frp,
    valid_from = t.valid_from,
    valid_to = t.valid_to,
    is_current = t.is_current
FROM pg_temp.target_scd2 t
WHERE h.h3_index = t.h3_index
  AND h.source_date = t.source_date
  AND (
      h.h3_resolution,
      h.risk_band,
      h.detection_count,
      h.total_frp,
      h.valid_from,
      h.valid_to,
      h.is_current
  ) IS DISTINCT FROM (
      t.h3_resolution,
      t.risk_band,
      t.detection_count,
      t.total_frp,
      t.valid_from,
      t.valid_to,
      t.is_current
  );
"""

INSERT_NEW_SCD2_SQL = """
INSERT INTO mart.h3_risk_history (
    h3_index, h3_resolution, risk_band, detection_count,
    total_frp, source_date, valid_from, valid_to, is_current
)
SELECT
    t.h3_index,
    t.h3_resolution,
    t.risk_band,
    t.detection_count,
    t.total_frp,
    t.source_date,
    t.valid_from,
    t.valid_to,
    t.is_current
FROM pg_temp.target_scd2 t
ON CONFLICT (h3_index, source_date) DO NOTHING;
"""


def sync_missing_staging_events(cur):
    """Fallback inkremental: hanya hitung H3 untuk baris raw yang belum diproses CDC Consumer."""
    cur.execute(FETCH_UNPROCESSED_RAW_SQL, (H3_RESOLUTION,))
    missing_rows = cur.fetchall()
    if not missing_rows:
        return 0

    staged_batch = []
    for (
        detection_key,
        acq_date,
        acq_time,
        latitude,
        longitude,
        frp,
        confidence,
        satellite,
        instrument,
        daynight,
    ) in missing_rows:
        cell = h3.latlng_to_cell(latitude, longitude, H3_RESOLUTION)
        staged_batch.append((
            detection_key,
            acq_date,
            acq_time,
            latitude,
            longitude,
            cell,
            H3_RESOLUTION,
            frp,
            confidence,
            satellite,
            instrument,
            daynight,
        ))

    cur.executemany(INSERT_FALLBACK_STAGING_SQL, staged_batch)
    return len(staged_batch)


def run_build_marts():
    if not 0 <= H3_RESOLUTION <= 15:
        raise SystemExit("H3_RESOLUTION harus bernilai antara 0 dan 15.")

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
                # Cegah race condition / deadlock jika eksekusi manual dan Prefect berjalan bersamaan
                cur.execute("SELECT pg_advisory_xact_lock(20261028);")
                cur.execute(CREATE_MARTS_SQL)

                fallback_synced = sync_missing_staging_events(cur)

                cur.execute("SELECT COUNT(*) FROM staging.firms_h3_events WHERE h3_resolution = %s", (H3_RESOLUTION,))
                total_staged = cur.fetchone()[0]
                if total_staged == 0:
                    print("Tidak ada data di staging.firms_h3_events maupun tabel raw; mart belum diperbarui.")
                    return {
                        "total_staged": 0,
                        "fallback_synced": 0,
                        "daily_upserted": 0,
                        "daily_total": 0,
                        "scd_inserted": 0,
                        "scd_updated": 0,
                        "scd_deleted": 0,
                        "latest_date": None,
                    }

                cur.execute(UPSERT_REALTIME_ALERTS_FROM_STAGING_SQL, (H3_RESOLUTION,))
                cur.execute(PRUNE_STALE_REALTIME_ALERTS_SQL, (H3_RESOLUTION,))

                cur.execute(UPSERT_DAILY_FROM_STAGING_SQL, (H3_RESOLUTION,))
                daily_upserted = cur.rowcount

                cur.execute("SELECT COUNT(*), MAX(acq_date) FROM mart.fire_activity_daily")
                daily_total, latest_date = cur.fetchone()

                cur.execute(BUILD_TARGET_SCD2_TEMP_SQL)
                cur.execute(DELETE_OBSOLETE_SCD2_SQL)
                scd_deleted = cur.rowcount

                cur.execute(CLOSE_SUPERSEDED_SCD2_SQL)
                scd_closed = cur.rowcount

                cur.execute(UPDATE_CHANGED_SCD2_SQL)
                scd_updated = scd_closed + cur.rowcount

                cur.execute(INSERT_NEW_SCD2_SQL)
                scd_inserted = cur.rowcount

    except psycopg.Error as exc:
        print(f"Gagal membangun mart: {exc.__class__.__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1)

    print(f"Total event terindeks H3 di staging: {total_staged} (backfill fallback: {fallback_synced})")
    print(f"Agregat harian H3 pada resolusi {H3_RESOLUTION}: {daily_total} (berubah/baru: {daily_upserted})")
    print(f"Tanggal observasi terbaru: {latest_date}")
    print(
        f"Transisi SCD Tipe 2 (State-Change) | baru={scd_inserted} "
        f"| diperbarui={scd_updated} | digabung/dihapus={scd_deleted}"
    )
    print("Ambang risiko contoh: low <5, medium 5-24, high >=25 deteksi per sel per hari.")

    return {
        "total_staged": total_staged,
        "fallback_synced": fallback_synced,
        "daily_upserted": daily_upserted,
        "daily_total": daily_total,
        "scd_inserted": scd_inserted,
        "scd_updated": scd_updated,
        "scd_deleted": scd_deleted,
        "latest_date": str(latest_date) if latest_date else None,
    }


def main():
    run_build_marts()


if __name__ == "__main__":
    main()