import os
import socket

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.schedules import Cron

from build_marts import run_build_marts
from cdc_consumer import run_consumer
from ingest_firms import run_ingestion


def is_kafka_reachable():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092").split(",")[0].strip()
    host, _, port_str = bootstrap.partition(":")
    port = int(port_str) if port_str else 19092
    try:
        with socket.create_connection((host, port), timeout=2.0):
            return True
    except OSError:
        return False


@task(
    name="Fetch NASA FIRMS and load PostgreSQL",
    retries=2,
    retry_delay_seconds=60,
    timeout_seconds=1800,
)
def ingest_recent_detections():
    return run_ingestion()


@task(
    name="Drain Kafka CDC events and index H3",
    retries=1,
    retry_delay_seconds=30,
    timeout_seconds=600,
)
def process_cdc_events():
    if not is_kafka_reachable():
        print("Broker Kafka tidak aktif; tahap CDC dilewati dan akan ditangani oleh SQL fallback di build_marts.")
        return {"cdc_status": "Skipped (SQL Fallback Active)", "cdc_processed": 0}
    processed = run_consumer(once=True)
    return {"cdc_status": "Active (Kafka Consumer)", "cdc_processed": processed}


@task(
    name="Build H3 aggregates and SCD Type 2 history",
    retries=1,
    retry_delay_seconds=60,
    timeout_seconds=1800,
)
def build_analytics_marts():
    return run_build_marts()


@flow(
    name="NASA FIRMS daily pipeline",
    description="Fetch the latest five days of FIRMS detections, process CDC stream, then refresh H3 and SCD Type 2 marts.",
    log_prints=True,
)
def nasa_firms_daily_pipeline():
    ingest_stats = ingest_recent_detections()
    cdc_stats = process_cdc_events()
    mart_stats = build_analytics_marts()

    report_md = f"""# Laporan Eksekusi Pipeline Harian NASA FIRMS H3

## 1. Ingestion (NASA FIRMS API -> PostgreSQL)
| Metrik | Nilai |
| :--- | :--- |
| **Sumber Satelit** | `{ingest_stats['source']}` |
| **Rentang Waktu** | `{ingest_stats['window']}` |
| **Deteksi Diterima dari API** | `{ingest_stats['fetched_rows']:,}` |
| **Baris Baru/Berubah (Upserted)** | `{ingest_stats['upserted_rows']:,}` |

## 2. Streaming CDC (Debezium -> Kafka -> Consumer)
| Metrik | Nilai |
| :--- | :--- |
| **Status Consumer CDC** | `{cdc_stats['cdc_status']}` |
| **Event CDC Diproses pada Run Ini** | `{cdc_stats['cdc_processed']:,}` |

## 3. Transformasi Spasial H3 & SCD Tipe 2 (Set-Based SQL)
| Metrik | Nilai |
| :--- | :--- |
| **Total Event di `staging.firms_h3_events`** | `{mart_stats['total_staged']:,}` |
| **Sinkronisasi Fallback (Non-CDC)** | `{mart_stats['fallback_synced']:,}` |
| **Total Agregat Harian (`mart.fire_activity_daily`)** | `{mart_stats['daily_total']:,}` |
| **Agregat Harian Diperbarui** | `{mart_stats['daily_upserted']:,}` |
| **Transisi Baru SCD Tipe 2** | `{mart_stats['scd_inserted']:,}` |
| **Transisi SCD Tipe 2 Diperbarui** | `{mart_stats['scd_updated']:,}` |
| **Tanggal Observasi Terbaru** | `{mart_stats['latest_date']}` |
"""
    create_markdown_artifact(
        key="firms-daily-pipeline-report",
        markdown=report_md,
        description="Ringkasan metrik ingest NASA FIRMS, indeks spasial H3, dan riwayat SCD Tipe 2.",
    )
    return {"ingest": ingest_stats, "cdc": cdc_stats, "marts": mart_stats}


if __name__ == "__main__":
    run_on_startup = os.getenv("PREFECT_RUN_ON_STARTUP", "true").strip().lower() in ("1", "true", "yes")
    if run_on_startup:
        print("PREFECT_RUN_ON_STARTUP=true: Menjalankan sinkronisasi awal sebelum menunggu jadwal 07:00 WIB...")
        try:
            nasa_firms_daily_pipeline()
        except Exception as exc:
            print(f"Run awal saat startup mengalami kendala ({exc}); jadwal harian 07:00 WIB tetap didaftarkan.")

    nasa_firms_daily_pipeline.serve(
        name="nasa-firms-daily-0700-wib",
        schedules=[Cron("0 7 * * *", timezone="Asia/Jakarta")],
        tags=["nasa-firms", "daily", "h3", "scd2"],
        pause_on_shutdown=False,
        limit=1,
    )
