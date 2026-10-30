import argparse
import json
import os
import signal
import sys
import time
from datetime import date, timedelta

import h3
import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException

H3_RESOLUTION = int(os.getenv("H3_RESOLUTION", "5"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
CDC_TOPIC = os.getenv("CDC_TOPIC", "firms.public.raw_firms_detections")
CONSUMER_GROUP = os.getenv("CDC_CONSUMER_GROUP", "firms-h3-cdc-consumer")
BATCH_SIZE = int(os.getenv("CDC_BATCH_SIZE", "500"))
POLL_TIMEOUT_SECONDS = float(os.getenv("CDC_POLL_TIMEOUT", "2.0"))

running = True


def handle_shutdown(signum, frame):
    global running
    print(f"Sinyal berhenti diterima ({signum}); menyelesaikan batch aktif...")
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

CREATE_STREAMING_TABLES_SQL = """
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
"""

UPSERT_STAGING_SQL = """
INSERT INTO staging.firms_h3_events (
    detection_key, acq_date, acq_time, latitude, longitude,
    h3_index, h3_resolution, frp, confidence, satellite,
    instrument, daynight, cdc_op, cdc_ts_ms, processed_at
) VALUES (
    %(detection_key)s, %(acq_date)s, %(acq_time)s, %(latitude)s, %(longitude)s,
    %(h3_index)s, %(h3_resolution)s, %(frp)s, %(confidence)s, %(satellite)s,
    %(instrument)s, %(daynight)s, %(cdc_op)s, %(cdc_ts_ms)s, now()
)
ON CONFLICT (detection_key) DO UPDATE SET
    acq_date = EXCLUDED.acq_date,
    acq_time = EXCLUDED.acq_time,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    h3_index = EXCLUDED.h3_index,
    h3_resolution = EXCLUDED.h3_resolution,
    frp = EXCLUDED.frp,
    confidence = EXCLUDED.confidence,
    satellite = EXCLUDED.satellite,
    instrument = EXCLUDED.instrument,
    daynight = EXCLUDED.daynight,
    cdc_op = EXCLUDED.cdc_op,
    cdc_ts_ms = EXCLUDED.cdc_ts_ms,
    processed_at = now()
WHERE (
    firms_h3_events.h3_index,
    firms_h3_events.frp,
    firms_h3_events.confidence,
    firms_h3_events.cdc_op
) IS DISTINCT FROM (
    EXCLUDED.h3_index,
    EXCLUDED.frp,
    EXCLUDED.confidence,
    EXCLUDED.cdc_op
);
"""

REFRESH_REALTIME_ALERTS_SQL = """
INSERT INTO mart.realtime_high_risk_alerts (
    acq_date, h3_index, h3_resolution, detection_count,
    total_frp, max_frp, risk_band, first_alerted_at, last_updated_at
)
SELECT
    s.acq_date,
    s.h3_index,
    s.h3_resolution,
    COUNT(*) AS detection_count,
    ROUND(COALESCE(SUM(s.frp), 0.0)::numeric, 4)::double precision AS total_frp,
    MAX(s.frp) AS max_frp,
    CASE
        WHEN COUNT(*) >= 25 THEN 'high'
        ELSE 'medium'
    END AS risk_band,
    now(),
    now()
FROM staging.firms_h3_events s
WHERE (s.acq_date, s.h3_index) IN (
    SELECT DISTINCT unnest(%(dates)s::date[]), unnest(%(cells)s::text[])
)
GROUP BY s.acq_date, s.h3_index, s.h3_resolution
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

PRUNE_TOUCHED_ALERTS_SQL = """
DELETE FROM mart.realtime_high_risk_alerts a
WHERE (a.acq_date, a.h3_index) IN (
    SELECT DISTINCT unnest(%(dates)s::date[]), unnest(%(cells)s::text[])
)
AND NOT EXISTS (
    SELECT 1
    FROM staging.firms_h3_events s
    WHERE s.acq_date = a.acq_date
      AND s.h3_index = a.h3_index
    GROUP BY s.acq_date, s.h3_index
    HAVING COUNT(*) >= 5
);
"""


def get_db_url():
    db_password = os.getenv("POSTGRES_PASSWORD", "").strip()
    if not db_password:
        raise SystemExit("POSTGRES_PASSWORD belum diatur.")
    return (
        f"host={os.getenv('DB_HOST', 'postgres')} "
        f"port={os.getenv('DB_PORT', '5432')} "
        f"dbname={os.getenv('POSTGRES_DB', 'fire_analytics')} "
        f"user={os.getenv('POSTGRES_USER', 'fire_user')} "
        f"password={db_password}"
    )


def parse_debezium_date(raw_value):
    """Konversi format tanggal Debezium (epoch days int atau string ISO YYYY-MM-DD)."""
    if isinstance(raw_value, int):
        return date(1970, 1, 1) + timedelta(days=raw_value)
    if isinstance(raw_value, str):
        return date.fromisoformat(raw_value)
    raise ValueError(f"Format acq_date Debezium tidak dikenali: {raw_value!r}")


def parse_cdc_message(raw_bytes):
    """Ekstrak event Debezium dan perkaya dengan indeks spasial H3."""
    if raw_bytes is None:
        return None
    envelope = json.loads(raw_bytes.decode("utf-8"))
    payload = envelope.get("payload", envelope)
    if not isinstance(payload, dict):
        return None

    op = payload.get("op")
    # 'c' = create (insert), 'u' = update, 'r' = read (initial snapshot), 'd' = delete
    if op == "d":
        before = payload.get("before") or {}
        return {"op": "d", "detection_key": before.get("detection_key")}

    if op not in ("c", "u", "r"):
        return None

    after = payload.get("after")
    if not after:
        return None

    lat = float(after["latitude"])
    lon = float(after["longitude"])
    cell = h3.latlng_to_cell(lat, lon, H3_RESOLUTION)

    return {
        "op": op,
        "record": {
            "detection_key": after["detection_key"],
            "acq_date": parse_debezium_date(after["acq_date"]),
            "acq_time": int(after["acq_time"]),
            "latitude": lat,
            "longitude": lon,
            "h3_index": cell,
            "h3_resolution": H3_RESOLUTION,
            "frp": float(after["frp"]) if after.get("frp") is not None else None,
            "confidence": after.get("confidence"),
            "satellite": after["satellite"],
            "instrument": after["instrument"],
            "daynight": after.get("daynight"),
            "cdc_op": op,
            "cdc_ts_ms": payload.get("ts_ms"),
        },
    }


def process_micro_batch(conn, messages):
    upserts = {}
    deletes = set()
    touched_pairs = set()

    for msg in messages:
        parsed = parse_cdc_message(msg.value())
        if not parsed:
            continue
        if parsed["op"] == "d":
            key = parsed.get("detection_key")
            if key:
                deletes.add(key)
                upserts.pop(key, None)
        else:
            rec = parsed["record"]
            key = rec["detection_key"]
            deletes.discard(key)
            upserts[key] = rec
            touched_pairs.add((rec["acq_date"], rec["h3_index"]))

    with conn.cursor() as cur:
        if deletes:
            cur.execute(
                "DELETE FROM staging.firms_h3_events "
                "WHERE detection_key = ANY(%s) "
                "RETURNING acq_date, h3_index",
                (list(deletes),),
            )
            for deleted_date, deleted_cell in cur.fetchall():
                touched_pairs.add((deleted_date, deleted_cell))
        if upserts:
            cur.executemany(UPSERT_STAGING_SQL, list(upserts.values()))
        if touched_pairs:
            dates = [pair[0] for pair in touched_pairs]
            cells = [pair[1] for pair in touched_pairs]
            params = {"dates": dates, "cells": cells}
            cur.execute(REFRESH_REALTIME_ALERTS_SQL, params)
            cur.execute(PRUNE_TOUCHED_ALERTS_SQL, params)
    conn.commit()
    return len(upserts), len(deletes), len(touched_pairs)


def ensure_db_connection(conn, db_url):
    """Pastikan koneksi PostgreSQL aktif; sambung ulang otomatis jika terputus."""
    if conn is not None and not conn.closed:
        return conn
    while running:
        try:
            new_conn = psycopg.connect(db_url)
            with new_conn.cursor() as cur:
                cur.execute(CREATE_STREAMING_TABLES_SQL)
            new_conn.commit()
            return new_conn
        except psycopg.OperationalError as exc:
            print(f"Menunggu koneksi PostgreSQL pulih ({exc.__class__.__name__})...", file=sys.stderr)
            time.sleep(3)
    raise SystemExit(0)


def run_consumer(once=False):
    db_url = get_db_url()
    conn = ensure_db_connection(None, db_url)

    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([CDC_TOPIC])
    print(
        f"CDC Consumer aktif | broker={KAFKA_BOOTSTRAP_SERVERS} "
        f"| topic={CDC_TOPIC} | group={CONSUMER_GROUP} | H3_res={H3_RESOLUTION}"
    )

    empty_polls = 0
    total_processed = 0

    try:
        while running:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT_SECONDS)
            valid_messages = []
            for msg in messages:
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() in (
                        KafkaError._PARTITION_EOF,
                        KafkaError.UNKNOWN_TOPIC_OR_PART,
                    ):
                        continue
                    raise KafkaException(msg.error())
                valid_messages.append(msg)

            if not valid_messages:
                empty_polls += 1
                if once and empty_polls >= 3:
                    print(f"Mode --once selesai. Total event CDC diproses: {total_processed}")
                    break
                continue

            empty_polls = 0
            try:
                conn = ensure_db_connection(conn, db_url)
                upserted, deleted, touched_cells = process_micro_batch(conn, valid_messages)
            except psycopg.OperationalError:
                if conn and not conn.closed:
                    conn.close()
                conn = ensure_db_connection(None, db_url)
                upserted, deleted, touched_cells = process_micro_batch(conn, valid_messages)

            # Manual offset commit HANYA setelah transaksi PostgreSQL berhasil (At-Least-Once)
            consumer.commit(asynchronous=False)
            total_processed += upserted + deleted
            print(
                f"Batch CDC berhasil | upsert={upserted} | delete={deleted} "
                f"| sel_h3_dievaluasi={touched_cells} | total={total_processed}"
            )
    finally:
        if conn and not conn.closed:
            conn.close()
        consumer.close()
    return total_processed


def main():
    parser = argparse.ArgumentParser(description="Kafka CDC Consumer untuk deteksi FIRMS H3.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Baca semua pesan yang tersedia di topik Kafka lalu keluar.",
    )
    args = parser.parse_args()
    run_consumer(once=args.once)


if __name__ == "__main__":
    main()
