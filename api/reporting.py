"""
Report generation — PDF (fpdf2) and CSV exports from Trino data.
"""

import csv
import io
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from fpdf import FPDF

sys.path.insert(0, str(Path(__file__).parent.parent / "dashboard"))
from core.trino_client import TrinoClient

client = TrinoClient()
REPORT_DIR = Path(__file__).parent.parent / "reports"
REPORT_DIR.mkdir(exist_ok=True)


# ─── helpers ────────────────────────────────────────────────

def _query(sql):
    try:
        df = client.query(sql)
        return df if df is not None and not df.empty else None
    except Exception:
        return None


def _summary_data():
    sql = """
    SELECT
        COUNT(*)                                           AS total_events,
        ROUND(AVG(speed_kmh), 1)                           AS avg_speed,
        ROUND(AVG(CASE WHEN is_congestion = 1 THEN 1 ELSE 0 END) * 100, 1) AS congestion_pct,
        ROUND(AVG(CASE WHEN is_incident = 1 THEN 1 ELSE 0 END) * 100, 1)  AS incident_pct,
        COUNT(DISTINCT segment_key)                        AS active_segments,
        COUNT(DISTINCT vehicle_id)                         AS unique_vehicles
    FROM delta.gold.fact_traffic_events
    """
    return _query(sql)


def _segments_data():
    sql = """
    SELECT segment_key, COUNT(*) AS event_count,
        ROUND(AVG(speed_kmh), 1) AS avg_speed,
        ROUND(AVG(is_congestion), 2) AS congestion_rate,
        ROUND(AVG(is_incident), 2)  AS incident_rate
    FROM delta.gold.fact_traffic_events
    GROUP BY segment_key ORDER BY event_count DESC
    """
    return _query(sql)


def _timeline_data(limit=48):
    sql = f"""
    SELECT CONCAT(CAST(t.year AS VARCHAR), '-', LPAD(CAST(t.month AS VARCHAR), 2, '0'), '-',
        LPAD(CAST(t.day AS VARCHAR), 2, '0'), ' ', LPAD(CAST(t.hour AS VARCHAR), 2, '0'), ':00') AS period,
        COUNT(*) AS event_count, ROUND(AVG(f.speed_kmh), 1) AS avg_speed,
        ROUND(AVG(f.is_congestion), 2) AS congestion_rate
    FROM delta.gold.fact_traffic_events f
    JOIN delta.gold.dim_time t ON f.time_key = t.time_key
    GROUP BY t.year, t.month, t.day, t.hour
    ORDER BY t.year, t.month, t.day, t.hour DESC LIMIT {limit}
    """
    return _query(sql)


def _anomalies_data(limit=100):
    sql = f"""
    SELECT event_timestamp, segment_key, speed_kmh, is_congestion, is_incident,
        speed_category
    FROM delta.gold.fact_traffic_events
    WHERE is_congestion = 1 OR speed_kmh < 20 OR speed_kmh > 120
    ORDER BY event_timestamp DESC LIMIT {limit}
    """
    return _query(sql)


# ─── CSV generation ─────────────────────────────────────────

def generate_csv(report_type: str = "summary") -> str:
    """Generate a CSV report and return the file path."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = REPORT_DIR / f"rapport_{report_type}_{ts}.csv"

    if report_type == "summary":
        df = _summary_data()
        if df is None:
            df = _segments_data()
            report_type = "segments"
        else:
            df = _segments_data()
    else:
        df = _segments_data()

    if df is not None:
        df.to_csv(path, index=False, encoding="utf-8-sig")
    else:
        path.write_text("Aucune donnée disponible\n", encoding="utf-8")
    return str(path)


def generate_csv_bytes(report_type: str = "summary") -> tuple[bytes, str]:
    """Return (csv_bytes, filename)."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"rapport_trafic_{report_type}_{ts}.csv"
    buf = io.StringIO()
    writer = csv.writer(buf)

    if report_type == "summary":
        df = _summary_data()
        if df is not None:
            df.to_csv(buf, index=False, encoding="utf-8-sig")
        else:
            writer.writerow(["Aucune donnée"])
    elif report_type == "segments":
        df = _segments_data()
        if df is not None:
            df.to_csv(buf, index=False, encoding="utf-8-sig")
        else:
            writer.writerow(["Aucune donnée"])
    elif report_type == "anomalies":
        df = _anomalies_data()
        if df is not None:
            df.to_csv(buf, index=False, encoding="utf-8-sig")
        else:
            writer.writerow(["Aucune donnée"])
    elif report_type == "timeline":
        df = _timeline_data(168)
        if df is not None:
            df.to_csv(buf, index=False, encoding="utf-8-sig")
        else:
            writer.writerow(["Aucune donnée"])
    else:
        writer.writerow(["Type de rapport inconnu"])

    return buf.getvalue().encode("utf-8-sig"), filename


# ─── PDF generation ─────────────────────────────────────────

def _plot_to_image(fig) -> str:
    """Save a matplotlib figure to a temp PNG and return the path."""
    tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
    fig.savefig(tmp.name, dpi=120, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return tmp.name


class TrafficReportPDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(100, 100, 100)
        self.cell(0, 6, "Tanger Smart Traffic - Rapport Automatique", align="L")
        self.ln(8)

    def footer(self):
        self.set_y(-12)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(150, 150, 150)
        self.cell(0, 8, f"Page {self.page_no()}/{{nb}} | Genere le {datetime.now().strftime('%d/%m/%Y %H:%M')}", align="C")


def generate_pdf(report_type: str = "complet") -> str:
    """Generate a PDF report and return the file path."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = REPORT_DIR / f"rapport_trafic_{report_type}_{ts}.pdf"

    pdf = TrafficReportPDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=18)

    # ── Page 1: Title ──
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 24)
    pdf.set_text_color(37, 99, 235)
    pdf.cell(0, 14, "Rapport de Trafic", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 12)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 8, "Tanger Smart Traffic - Plateforme d'Analytics Temps Reel", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, f"Genere le {datetime.now().strftime('%d/%m/%Y a %H:%M')}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)

    # ── Summary KPIs ──
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(30, 41, 59)
    pdf.cell(0, 10, "Indicateurs cles (KPI)", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    summary = _summary_data()
    if summary is not None:
        row = summary.iloc[0]
        labels = [
            ("Total evenements", f"{int(row['total_events'])}"),
            ("Vitesse moyenne", f"{row['avg_speed']} km/h"),
            ("Taux congestion", f"{row['congestion_pct']}%"),
            ("Taux incidents", f"{row['incident_pct']}%"),
            ("Segments actifs", f"{int(row['active_segments'])}"),
            ("Vehicules uniques", f"{int(row['unique_vehicles'])}"),
        ]
        pdf.set_font("Helvetica", "", 10)
        for label, value in labels:
            pdf.set_text_color(60, 60, 60)
            pdf.cell(70, 7, label)
            pdf.set_text_color(37, 99, 235)
            pdf.cell(0, 7, value, new_x="LMARGIN", new_y="NEXT")
    else:
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(150, 150, 150)
        pdf.cell(0, 7, "Donnees non disponibles", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(6)

    # ── Congestion chart ──
    segs = _segments_data()
    if segs is not None and len(segs) > 0:
        fig, ax = plt.subplots(figsize=(6, 2.5))
        names = segs["segment_key"].tolist()[:10]
        speeds = segs["avg_speed"].tolist()[:10]
        colors = ["#22c55e" if s > 60 else "#eab308" if s > 40 else "#ef4444" for s in speeds]
        ax.barh(names[::-1], speeds[::-1], color=colors[::-1], edgecolor="white", height=0.6)
        ax.set_xlabel("Vitesse moyenne (km/h)", fontsize=9)
        ax.tick_params(labelsize=8)
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.grid(axis="x", alpha=0.3)
        img_path = _plot_to_image(fig)
        pdf.image(img_path, x=12, w=180)
        os.unlink(img_path)
        pdf.ln(3)

    # ── Timeline chart ──
    tl = _timeline_data(48)
    if tl is not None and len(tl) > 1:
        tl = tl.iloc[::-1]
        fig, ax = plt.subplots(figsize=(6, 2.2))
        ax.fill_between(range(len(tl)), tl["avg_speed"], alpha=0.15, color="#3b82f6")
        ax.plot(range(len(tl)), tl["avg_speed"], color="#3b82f6", linewidth=1.5, marker="o", markersize=2)
        ax.set_ylabel("km/h", fontsize=8)
        ax.set_xlabel("Periodes", fontsize=8)
        ax.tick_params(labelsize=7)
        step = max(1, len(tl) // 12)
        ax.set_xticks(range(0, len(tl), step))
        ax.set_xticklabels([str(t)[11:16] for t in tl["period"].iloc[::step]], rotation=30, fontsize=6)
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.grid(alpha=0.3)
        img_path = _plot_to_image(fig)
        pdf.image(img_path, x=12, w=180)
        os.unlink(img_path)
        pdf.ln(3)

    # ── Per-segment table ──
    if segs is not None and len(segs) > 0:
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_text_color(30, 41, 59)
        pdf.cell(0, 9, "Details par segment", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
        col_w = [40, 30, 30, 30, 30, 30]
        headers = ["Segment", "Evenements", "Vitesse", "Congestion", "Incidents"]
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_fill_color(37, 99, 235)
        pdf.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            pdf.cell(col_w[i], 6, h, border=1, fill=True, align="C")
        pdf.ln()
        pdf.set_font("Helvetica", "", 8)
        pdf.set_text_color(30, 30, 30)
        for _, row in segs.iterrows():
            vals = [
                str(row["segment_key"])[:18],
                str(int(row["event_count"])),
                f"{row['avg_speed']} km/h",
                f"{row['congestion_rate']*100:.0f}%",
                f"{row['incident_rate']*100:.0f}%",
            ]
            for i, v in enumerate(vals):
                pdf.cell(col_w[i], 5.5, v, border=1, align="C")
            pdf.ln()

    pdf.output(str(path))
    return str(path)


def generate_pdf_bytes(report_type: str = "complet") -> tuple[bytes, str]:
    """Return (pdf_bytes, filename)."""
    path = generate_pdf(report_type)
    data = Path(path).read_bytes()
    filename = Path(path).name
    return data, filename
