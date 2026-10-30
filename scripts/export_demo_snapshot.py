import csv
import gzip
import os
from pathlib import Path

import psycopg

ROOT_DIR = Path(__file__).resolve().parents[1]
DEMO_DIR = ROOT_DIR / "dashboard" / "demo_data"


def load_env_file():
    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def main():
    load_env_file()
    db_password = os.getenv("POSTGRES_PASSWORD", "").strip()
    if not db_password:
        raise SystemExit("POSTGRES_PASSWORD tidak ditemukan di environment atau .env")

    db_url = (
        f"host={os.getenv('DB_HOST', '127.0.0.1')} "
        f"port={os.getenv('DB_PORT', '5434')} "
        f"dbname={os.getenv('POSTGRES_DB', 'fire_analytics')} "
        f"user={os.getenv('POSTGRES_USER', 'fire_user')} "
        f"password={db_password}"
    )

    DEMO_DIR.mkdir(parents=True, exist_ok=True)

    queries = {
        "fire_activity_daily.csv.gz": (
            "SELECT acq_date, h3_index, h3_resolution, detection_count, "
            "total_frp, avg_frp, max_frp, risk_band "
            "FROM mart.fire_activity_daily ORDER BY acq_date ASC, detection_count DESC"
        ),
        "realtime_high_risk_alerts.csv.gz": (
            "SELECT acq_date, h3_index, detection_count, "
            "round(total_frp::numeric, 2) AS total_frp_mw, "
            "round(max_frp::numeric, 2) AS max_frp_mw, "
            "risk_band, first_alerted_at, last_updated_at "
            "FROM mart.realtime_high_risk_alerts ORDER BY acq_date ASC, detection_count DESC"
        ),
        "h3_risk_history.csv.gz": (
            "SELECT h3_index, risk_band, detection_count, "
            "round(total_frp::numeric, 2) AS total_frp_mw, "
            "source_date, valid_from, valid_to, is_current "
            "FROM mart.h3_risk_history ORDER BY h3_index ASC, valid_from DESC"
        ),
    }

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            for filename, sql in queries.items():
                cur.execute(sql)
                cols = [desc.name for desc in cur.description]
                rows = cur.fetchall()
                out_path = DEMO_DIR / filename
                with gzip.open(out_path, "wt", encoding="utf-8", newline="") as gz:
                    writer = csv.writer(gz)
                    writer.writerow(cols)
                    writer.writerows(rows)
                size_kb = out_path.stat().st_size / 1024
                print(f"Diekspor: {out_path.relative_to(ROOT_DIR)} ({len(rows):,} baris, {size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
