import csv
import io
import os
from datetime import date
from pathlib import Path

import altair as alt
import h3
import pandas as pd
import psycopg
import pydeck as pdk
import streamlit as st
from psycopg.rows import dict_row

APP_DIR = Path(__file__).resolve().parent
DEMO_DIR = APP_DIR / "demo_data"


def get_secret_or_env(key, default=""):
    val = os.getenv(key, "").strip()
    if val:
        return val
    try:
        if key in st.secrets:
            return str(st.secrets[key]).strip()
    except Exception:
        pass
    return default


DB_PASSWORD = get_secret_or_env("POSTGRES_PASSWORD")
DATABASE_URL = get_secret_or_env("DATABASE_URL")

if DATABASE_URL:
    DB_URL = DATABASE_URL
elif DB_PASSWORD:
    DB_URL = (
        f"host={get_secret_or_env('DB_HOST', 'postgres')} "
        f"port={get_secret_or_env('DB_PORT', '5432')} "
        f"dbname={get_secret_or_env('POSTGRES_DB', 'fire_analytics')} "
        f"user={get_secret_or_env('POSTGRES_USER', 'fire_user')} "
        f"password={DB_PASSWORD}"
    )
else:
    DB_URL = None

RISK_COLORS = {
    "low": [46, 204, 113, 195],
    "medium": [243, 156, 18, 215],
    "high": [231, 76, 60, 235],
}

st.set_page_config(page_title="FIRMS H3 Fire Activity", page_icon="🔥", layout="wide")


def has_demo_snapshot():
    return (DEMO_DIR / "fire_activity_daily.csv.gz").exists()


@st.cache_data(ttl=3600)
def load_demo_snapshot():
    """Muat snapshot terkompresi (.csv.gz) untuk mode Live Demo publik (Streamlit Cloud)."""
    daily_df = pd.read_csv(DEMO_DIR / "fire_activity_daily.csv.gz")
    daily_df["acq_date"] = daily_df["acq_date"].apply(lambda x: date.fromisoformat(str(x)[:10]))

    alerts_path = DEMO_DIR / "realtime_high_risk_alerts.csv.gz"
    if alerts_path.exists():
        alerts_df = pd.read_csv(alerts_path)
        alerts_df["acq_date"] = alerts_df["acq_date"].apply(lambda x: date.fromisoformat(str(x)[:10]))
    else:
        alerts_df = pd.DataFrame()

    scd2_path = DEMO_DIR / "h3_risk_history.csv.gz"
    if scd2_path.exists():
        scd2_df = pd.read_csv(scd2_path)
        scd2_df["source_date"] = scd2_df["source_date"].apply(lambda x: date.fromisoformat(str(x)[:10]))
        scd2_df["valid_to"] = scd2_df["valid_to"].where(scd2_df["valid_to"].notna(), None)
        scd2_df["is_current"] = scd2_df["is_current"].astype(bool)
    else:
        scd2_df = pd.DataFrame()

    return daily_df, alerts_df, scd2_df


def enrich_activity_rows(raw_rows):
    enriched = []
    for row in raw_rows:
        center_lat, center_lon = h3.cell_to_latlng(row["h3_index"])
        band = row["risk_band"]
        avg_val = row.get("avg_frp")
        max_val = row.get("max_frp")
        enriched.append({
            "acq_date": row["acq_date"],
            "acq_date_iso": row["acq_date"].isoformat(),
            "h3_index": str(row["h3_index"]),
            "h3_resolution": int(row["h3_resolution"]),
            "risk_band": band,
            "detection_count": int(row["detection_count"]),
            "total_frp": float(row["total_frp"]),
            "total_frp_mw": round(float(row["total_frp"]), 2),
            "avg_frp_mw": round(float(avg_val), 2) if avg_val is not None and not pd.isna(avg_val) else None,
            "max_frp_mw": round(float(max_val), 2) if max_val is not None and not pd.isna(max_val) else None,
            "cell_center_latitude": round(center_lat, 5),
            "cell_center_longitude": round(center_lon, 5),
            "color": RISK_COLORS[band],
        })
    return enriched


@st.cache_data(ttl=60)
def load_daily_trend(use_demo=False):
    if use_demo:
        daily_df, _, _ = load_demo_snapshot()
        grouped = (
            daily_df.groupby("acq_date", as_index=False)
            .agg(total_detections=("detection_count", "sum"), total_frp=("total_frp", "sum"))
            .sort_values("acq_date", ascending=True)
        )
        grouped["total_frp"] = grouped["total_frp"].round(1)
        return grouped.to_dict("records")

    with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT acq_date, "
                "SUM(detection_count)::bigint AS total_detections, "
                "ROUND(SUM(total_frp)::numeric, 1)::double precision AS total_frp "
                "FROM mart.fire_activity_daily "
                "GROUP BY acq_date "
                "ORDER BY acq_date ASC"
            )
            return cur.fetchall()


@st.cache_data(ttl=60)
def load_activity(selected_date, use_demo=False):
    """Ambil agregat harian dan hitung koordinat pusat H3 serta pembulatan di dalam cache."""
    if use_demo:
        daily_df, _, _ = load_demo_snapshot()
        sub = daily_df[daily_df["acq_date"] == selected_date].sort_values(
            "detection_count", ascending=False
        )
        return enrich_activity_rows(sub.to_dict("records"))

    with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT acq_date, h3_index, h3_resolution, detection_count, total_frp, "
                "avg_frp, max_frp, risk_band "
                "FROM mart.fire_activity_daily WHERE acq_date = %s ORDER BY detection_count DESC",
                (selected_date,),
            )
            raw_rows = cur.fetchall()
    return enrich_activity_rows(raw_rows)


@st.cache_data(ttl=30)
def load_realtime_alerts(selected_date, use_demo=False):
    if use_demo:
        _, alerts_df, _ = load_demo_snapshot()
        if alerts_df.empty:
            return []
        sub = (
            alerts_df[alerts_df["acq_date"] == selected_date]
            .sort_values(["detection_count", "total_frp_mw"], ascending=[False, False])
            .head(25)
        )
        return sub.to_dict("records")

    with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT acq_date, h3_index, detection_count, "
                "round(total_frp::numeric, 2) AS total_frp_mw, "
                "round(max_frp::numeric, 2) AS max_frp_mw, "
                "risk_band, first_alerted_at, last_updated_at "
                "FROM mart.realtime_high_risk_alerts "
                "WHERE acq_date = %s "
                "ORDER BY detection_count DESC, total_frp DESC LIMIT 25",
                (selected_date,),
            )
            return cur.fetchall()


@st.cache_data(ttl=60)
def load_cell_scd2_history(h3_index, use_demo=False):
    if use_demo:
        _, _, scd2_df = load_demo_snapshot()
        if scd2_df.empty:
            return []
        sub = scd2_df[scd2_df["h3_index"] == h3_index].sort_values("valid_from", ascending=False)
        return sub.to_dict("records")

    with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT h3_index, risk_band, detection_count, "
                "round(total_frp::numeric, 2) AS total_frp_mw, "
                "source_date, valid_from, valid_to, is_current "
                "FROM mart.h3_risk_history "
                "WHERE h3_index = %s "
                "ORDER BY valid_from DESC",
                (h3_index,),
            )
            return cur.fetchall()


st.title("🔥 NASA FIRMS — Geospatial Fire Activity & CDC Analytics")
st.caption(
    "Deteksi titik panas VIIRS dikelompokkan ke grid heksagonal Uber H3 (Resolusi 5) melalui pipeline "
    "Streaming CDC (Debezium/Kafka) & Set-Based SQL SCD Tipe 2."
)

use_demo_mode = False
if DB_URL:
    try:
        trend_rows = load_daily_trend(use_demo=False)
    except psycopg.Error as exc:
        if has_demo_snapshot():
            use_demo_mode = True
            trend_rows = load_daily_trend(use_demo=True)
        else:
            st.error(
                f"Tidak dapat membaca mart PostgreSQL ({exc.__class__.__name__}). "
                "Pastikan transformasi H3 sudah dijalankan."
            )
            st.stop()
else:
    if has_demo_snapshot():
        use_demo_mode = True
        trend_rows = load_daily_trend(use_demo=True)
    else:
        st.error("POSTGRES_PASSWORD belum diatur dan snapshot demo tidak ditemukan.")
        st.stop()

if use_demo_mode:
    st.info(
        "🌐 **Mode Live Demo Publik (Cloud Snapshot)**: Menampilkan hasil nyata pipeline 30 hari "
        "NASA FIRMS VIIRS (252.196 deteksi titik panas, 37.880 agregat sel H3 harian, dan histori SCD Tipe 2). "
        "Saat dijalankan melalui Docker Compose, dashboard otomatis terhubung secara *real-time* ke PostgreSQL & Kafka CDC."
    )

if not trend_rows:
    st.info("Belum ada agregat. Jalankan `docker compose run --rm ingest python build_marts.py` terlebih dahulu.")
    st.stop()

dates_asc = [row["acq_date"] for row in trend_rows]
min_date = dates_asc[0]
max_date = dates_asc[-1]


def sync_date_widgets(target_date):
    st.session_state["selected_date"] = target_date
    st.session_state["date_picker"] = target_date
    st.session_state["date_slider"] = target_date


if "selected_date" not in st.session_state or st.session_state["selected_date"] not in dates_asc:
    sync_date_widgets(max_date)


def on_calendar_change():
    picked = st.session_state.get("date_picker", max_date)
    if picked in dates_asc:
        sync_date_widgets(picked)
    else:
        nearest = min(dates_asc, key=lambda d: abs((d - picked).days))
        sync_date_widgets(nearest)


def on_slider_change():
    sync_date_widgets(st.session_state["date_slider"])


def step_date(offset):
    current = st.session_state["selected_date"]
    idx = dates_asc.index(current)
    new_idx = max(0, min(len(dates_asc) - 1, idx + offset))
    sync_date_widgets(dates_asc[new_idx])


current_idx = dates_asc.index(st.session_state["selected_date"])

# --- Top Control Bar dalam Container Berbingkai ---
with st.container(border=True):
    ctrl_prev, ctrl_cal, ctrl_next, ctrl_filter, ctrl_theme = st.columns([0.9, 1.6, 0.9, 3.2, 1.4])

    with ctrl_prev:
        st.write("")
        st.button(
            "◀ H-1",
            on_click=step_date,
            args=(-1,),
            disabled=(current_idx == 0),
            use_container_width=True,
            help="Mundur ke tanggal observasi sebelumnya",
        )

    with ctrl_cal:
        st.date_input(
            "Tanggal Akuisisi (Kalender)",
            min_value=min_date,
            max_value=max_date,
            key="date_picker",
            on_change=on_calendar_change,
        )

    with ctrl_next:
        st.write("")
        st.button(
            "H+1 ▶",
            on_click=step_date,
            args=(1,),
            disabled=(current_idx == len(dates_asc) - 1),
            use_container_width=True,
            help="Maju ke tanggal observasi berikutnya",
        )

    with ctrl_filter:
        filters = st.multiselect(
            "Filter Kategori Risiko",
            ["high", "medium", "low"],
            default=["high", "medium", "low"],
        )

    with ctrl_theme:
        map_theme = st.selectbox(
            "Tema Basemap 3D",
            options=["dark", "light"],
            index=0,
            format_func=lambda t: "🌙 Dark Matter" if t == "dark" else "☀️ Positron Light",
        )

    selected_date = st.session_state["selected_date"]

    if len(dates_asc) > 1:
        col_slider, col_trend = st.columns([2.2, 2.8])
        with col_slider:
            st.select_slider(
                "Geser Timeline Kronologis",
                options=dates_asc,
                format_func=lambda value: value.isoformat(),
                key="date_slider",
                on_change=on_slider_change,
            )
            st.caption(
                f"Menampilkan hari ke-**{current_idx + 1}** dari **{len(dates_asc)}** hari observasi "
                f"(`{min_date.isoformat()}` s/d `{max_date.isoformat()}`)."
            )
        with col_trend:
            trend_df = pd.DataFrame([
                {
                    "Tanggal": r["acq_date"].isoformat(),
                    "Deteksi": int(r["total_detections"]),
                    "Total FRP (MW)": float(r["total_frp"]),
                    "Status": "Terpilih" if r["acq_date"] == selected_date else "Hari Lain",
                }
                for r in trend_rows
            ])
            trend_chart = (
                alt.Chart(trend_df)
                .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                .encode(
                    x=alt.X("Tanggal:N", title=None, axis=alt.Axis(labels=False, ticks=False)),
                    y=alt.Y("Deteksi:Q", title="Deteksi/Hari"),
                    color=alt.Color(
                        "Status:N",
                        scale=alt.Scale(domain=["Terpilih", "Hari Lain"], range=["#e74c3c", "#4a6fa5"]),
                        legend=None,
                    ),
                    tooltip=["Tanggal:N", "Deteksi:Q", "Total FRP (MW):Q"],
                )
                .properties(height=95)
            )
            st.altair_chart(trend_chart, use_container_width=True)

try:
    rows = load_activity(selected_date, use_demo=use_demo_mode)
except psycopg.Error as exc:
    st.error(f"Gagal membaca data tanggal tersebut ({exc.__class__.__name__}).")
    st.stop()

if not rows:
    st.warning("Tidak ada agregat untuk tanggal yang dipilih.")
    st.stop()

filtered = [row for row in rows if row["risk_band"] in filters]

total_detections = sum(row["detection_count"] for row in filtered)
total_frp = sum(row["total_frp"] for row in filtered)
high_cells = sum(1 for row in filtered if row["risk_band"] == "high")

# --- Perhitungan Delta terhadap Hari Observasi Sebelumnya (H-1) ---
delta_cells_str = None
delta_det_str = None
delta_frp_str = None
delta_high_str = None

if current_idx > 0:
    prev_date = dates_asc[current_idx - 1]
    prev_rows = load_activity(prev_date, use_demo=use_demo_mode)
    prev_filtered = [row for row in prev_rows if row["risk_band"] in filters]

    prev_cells = len(prev_filtered)
    prev_detections = sum(row["detection_count"] for row in prev_filtered)
    prev_frp = sum(row["total_frp"] for row in prev_filtered)
    prev_high = sum(1 for row in prev_filtered if row["risk_band"] == "high")

    delta_cells_str = f"{len(filtered) - prev_cells:+,} vs {prev_date.isoformat()}"
    delta_det_str = f"{total_detections - prev_detections:+,} vs {prev_date.isoformat()}"
    delta_frp_str = f"{total_frp - prev_frp:+,.1f} MW vs {prev_date.isoformat()}"
    delta_high_str = f"{high_cells - prev_high:+,} vs {prev_date.isoformat()}"

k1, k2, k3, k4 = st.columns(4)
with k1:
    with st.container(border=True):
        st.metric("Sel H3 Ditampilkan", f"{len(filtered):,}", delta=delta_cells_str, delta_color="inverse")
with k2:
    with st.container(border=True):
        st.metric("Deteksi Aktif (VIIRS)", f"{total_detections:,}", delta=delta_det_str, delta_color="inverse")
with k3:
    with st.container(border=True):
        st.metric("Total Radiative Power", f"{total_frp:,.1f} MW", delta=delta_frp_str, delta_color="inverse")
with k4:
    with st.container(border=True):
        st.metric("Sel Risiko Tinggi (High)", f"{high_cells:,}", delta=delta_high_str, delta_color="inverse")

if filtered:
    layer = pdk.Layer(
        "H3HexagonLayer",
        data=filtered,
        get_hexagon="h3_index",
        get_fill_color="color",
        get_elevation="detection_count",
        elevation_scale=150,
        extruded=True,
        high_precision=True,
        pickable=True,
        stroked=True,
        get_line_color=[255, 255, 255, 60] if map_theme == "dark" else [35, 40, 48, 100],
        opacity=0.82,
    )
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=pdk.ViewState(
            latitude=-2.5,
            longitude=117.5,
            zoom=3.6,
            pitch=38,
            bearing=0,
        ),
        tooltip={
            "html": (
                "<div style='font-family:sans-serif;font-size:13px;line-height:1.5'>"
                "<b>H3 Index:</b> <code>{h3_index}</code><br/>"
                "<b>Kategori Risiko:</b> <span style='text-transform:uppercase;font-weight:bold'>{risk_band}</span><br/>"
                "<b>Jumlah Deteksi:</b> {detection_count} titik<br/>"
                "<b>Total FRP:</b> {total_frp_mw} MW (Maks: {max_frp_mw} MW)<br/>"
                "<b>Pusat Sel:</b> {cell_center_latitude}, {cell_center_longitude}"
                "</div>"
            ),
            "style": {
                "backgroundColor": "#111827",
                "color": "#f9fafb",
                "border": "1px solid #374151",
                "borderRadius": "6px",
                "padding": "8px 10px",
            },
        },
        map_style=map_theme,
    )
    st.pydeck_chart(deck, use_container_width=True)
else:
    st.info("Pilih setidaknya satu kategori risiko untuk menampilkan peta.")

col_alerts, col_scd2 = st.columns(2)

with col_alerts:
    with st.container(border=True):
        st.subheader("🚨 Live CDC High-Risk Alerts")
        st.caption("Sel H3 berisiko `high`/`medium` dari stream CDC Debezium (`mart.realtime_high_risk_alerts`).")
        try:
            alerts = load_realtime_alerts(selected_date, use_demo=use_demo_mode)
            if alerts:
                st.dataframe(
                    alerts,
                    use_container_width=True,
                    hide_index=True,
                    height=310,
                    column_config={
                        "acq_date": st.column_config.DateColumn("Tanggal"),
                        "h3_index": st.column_config.TextColumn("Indeks H3"),
                        "risk_band": st.column_config.TextColumn("Risiko"),
                        "detection_count": st.column_config.NumberColumn("Deteksi", format="%d"),
                        "total_frp_mw": st.column_config.NumberColumn("Total FRP", format="%.2f MW"),
                        "max_frp_mw": st.column_config.NumberColumn("Maks FRP", format="%.2f MW"),
                        "first_alerted_at": st.column_config.DatetimeColumn("Pertama Terdeteksi", format="YYYY-MM-DD HH:mm"),
                        "last_updated_at": st.column_config.DatetimeColumn("Update Terakhir", format="YYYY-MM-DD HH:mm"),
                    },
                )
            else:
                st.info("Tidak ada sel berisiko medium/high pada tanggal ini.")
        except psycopg.Error:
            st.info("Tabel `mart.realtime_high_risk_alerts` belum tersedia.")

with col_scd2:
    with st.container(border=True):
        st.subheader("🕒 Inspector Riwayat Risiko (SCD Tipe 2)")
        st.caption("Lacak transisi perubahan status `risk_band` dari waktu ke waktu (`mart.h3_risk_history`).")
        if filtered:
            cell_options = [
                f"{r['h3_index']} ({r['risk_band'].upper()} — {r['detection_count']} deteksi)"
                for r in filtered
            ]
            selected_label = st.selectbox("Pilih sel H3 untuk melihat histori SCD Tipe 2", cell_options)
            selected_cell = selected_label.split(" ")[0]
            try:
                history_rows = load_cell_scd2_history(selected_cell, use_demo=use_demo_mode)
                if history_rows:
                    st.dataframe(
                        history_rows,
                        use_container_width=True,
                        hide_index=True,
                        height=240,
                        column_config={
                            "h3_index": st.column_config.TextColumn("Indeks H3"),
                            "risk_band": st.column_config.TextColumn("Status Risiko"),
                            "detection_count": st.column_config.NumberColumn("Akumulasi Deteksi", format="%d"),
                            "total_frp_mw": st.column_config.NumberColumn("Akumulasi FRP", format="%.2f MW"),
                            "source_date": st.column_config.DateColumn("Mulai Tanggal"),
                            "valid_from": st.column_config.DatetimeColumn("Valid From (UTC)", format="YYYY-MM-DD"),
                            "valid_to": st.column_config.DatetimeColumn("Valid To (UTC)", format="YYYY-MM-DD"),
                            "is_current": st.column_config.CheckboxColumn("Aktif?"),
                        },
                    )
                else:
                    st.info("Belum ada riwayat SCD Tipe 2 untuk sel ini.")
            except psycopg.Error:
                st.info("Tabel `mart.h3_risk_history` belum tersedia.")
        else:
            st.info("Pilih kategori risiko pada filter untuk menginspeksi sel H3.")

with st.container(border=True):
    header_col, dl_col = st.columns([3.5, 1.5])
    with header_col:
        st.subheader("📋 Tabel Agregat Spasial H3 & Ekspor Data")
        st.caption("Memuat seluruh sel H3 sesuai filter kategori pada tanggal terpilih beserta titik pusat koordinatnya.")

    export_rows = [
        {
            "acq_date": row["acq_date_iso"],
            "h3_index": row["h3_index"],
            "h3_resolution": row["h3_resolution"],
            "risk_band": row["risk_band"],
            "detection_count": row["detection_count"],
            "total_frp_mw": row["total_frp_mw"],
            "avg_frp_mw": row["avg_frp_mw"],
            "max_frp_mw": row["max_frp_mw"],
            "cell_center_latitude": row["cell_center_latitude"],
            "cell_center_longitude": row["cell_center_longitude"],
        }
        for row in filtered
    ]

    if export_rows:
        csv_buffer = io.StringIO()
        csv_columns = list(export_rows[0].keys())
        writer = csv.DictWriter(csv_buffer, fieldnames=csv_columns)
        writer.writeheader()
        writer.writerows(export_rows)
        with dl_col:
            st.write("")
            st.download_button(
                "📥 Unduh CSV (Kompatibel Excel)",
                data=csv_buffer.getvalue().encode("utf-8-sig"),
                file_name=f"firms_h3_{selected_date.isoformat()}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        st.dataframe(
            export_rows,
            use_container_width=True,
            hide_index=True,
            height=480,
            column_config={
                "acq_date": st.column_config.TextColumn("Tanggal Akuisisi"),
                "h3_index": st.column_config.TextColumn("Indeks H3"),
                "h3_resolution": st.column_config.NumberColumn("Resolusi", format="%d"),
                "risk_band": st.column_config.TextColumn("Kategori Risiko"),
                "detection_count": st.column_config.NumberColumn("Jumlah Deteksi", format="%d"),
                "total_frp_mw": st.column_config.NumberColumn("Total FRP", format="%.2f MW"),
                "avg_frp_mw": st.column_config.NumberColumn("Rata-rata FRP", format="%.2f MW"),
                "max_frp_mw": st.column_config.NumberColumn("Maks FRP", format="%.2f MW"),
                "cell_center_latitude": st.column_config.NumberColumn("Lat Pusat Sel", format="%.5f"),
                "cell_center_longitude": st.column_config.NumberColumn("Lon Pusat Sel", format="%.5f"),
            },
        )
    else:
        st.info("Pilih minimal satu kategori risiko untuk menampilkan tabel dan mengunduh CSV.")
