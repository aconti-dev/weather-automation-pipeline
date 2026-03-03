#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Weather Underground / Weather.com PWS (API) -> daily JSON + monthly history JSON + per-city PDF + per-region HTML.

GitHub-friendly:
- NO hardcoded API keys
- NO hardcoded server paths
- Output in ./outputs (configurable via OUTPUT_BASE_DIR)
- Public links in HTML configurable via PUBLIC_BASE_URL + relative WP-like paths

Env:
- WU_API_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx   (required)
- OUTPUT_BASE_DIR=./outputs                    (optional)
- PUBLIC_BASE_URL=                             (optional; e.g. https://tuodominio.it)
- STATIONS_FILE=stations_by_region.sample.json (optional)
"""

import os
import json
import time
import html
import random
import hashlib
import asyncio
import aiohttp
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, Tuple, List, Union, Optional

from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.colors import HexColor
from reportlab.lib.enums import TA_CENTER
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import VerticalBarChart


Number = Union[int, float]
RainValue = Union[Number, str]

RETRY_HTTP_STATUSES = {429, 500, 502, 503, 504}


# =========================
# Config
# =========================

@dataclass(frozen=True)
class Config:
    # API Weather.com / Wunderground PWS
    api_base: str = "https://api.weather.com/v2/pws"
    api_key_env: str = "WU_API_KEY"

    # Timezone
    tz_name: str = "Europe/Rome"

    # Inputs
    stations_file_env: str = "STATIONS_FILE"
    stations_file_default: str = "stations_by_region.sample.json"

    # Outputs base dir (repo-friendly)
    output_base_dir_env: str = "OUTPUT_BASE_DIR"
    output_base_dir_default: str = "outputs"

    # Public URL (optional) used in HTML links
    public_base_url_env: str = "PUBLIC_BASE_URL"
    public_base_url_default: str = ""  # if empty -> keep relative links

    # WP-like relative paths used ONLY for link building (NOT filesystem)
    public_pdf_current_rel: str = "/wp-content/uploads/pdf_stazioni"
    public_pdf_prev_rel: str = "/wp-content/uploads/pdf_stazioni/storico_precedente"
    public_region_rel: str = "/wp-content/uploads/regioni"  # optional, not used internally

    # Concurrency / networking
    concurrent_limit: int = 20
    timeout_total_sec: int = 20
    min_delay_s: float = 0.20
    max_delay_s: float = 0.60
    retries: int = 2
    backoff_base_s: float = 0.5
    backoff_cap_s: float = 8.0

    # Data sanity
    max_daily_mm_reasonable: float = 400.0

    # Thresholds (coerenti)
    over_threshold_mm: float = 200.0  # >=200 "over" fucsia
    t150_mm: float = 150.0
    t120_mm: float = 120.0
    t100_mm: float = 100.0
    t90_mm: float = 90.0
    t80_mm: float = 80.0
    t40_mm: float = 40.0
    t25_mm: float = 25.0

    # Lock file (anti overlap)
    lock_file: str = "/tmp/weather_pipeline.lock"
    lock_stale_seconds: int = 60 * 60 * 3  # 3 hours


CFG = Config()


# =========================
# Helpers: filesystem, atomic writes, hashing
# =========================

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def atomic_write_text(path: str, text: str) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def atomic_write_json(path: str, obj: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def sha1_json(obj: Any) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def mm_fmt(v: RainValue) -> str:
    if isinstance(v, (int, float)):
        return f"{float(v):.2f}"
    return str(v)


def join_url(base: str, path: str) -> str:
    """Join base url with a path. If base is empty, return path (relative)."""
    if not base:
        return path
    return base.rstrip("/") + "/" + path.lstrip("/")


# =========================
# Helpers: lockfile
# =========================

def acquire_lock_or_exit(lock_path: str, stale_seconds: int) -> None:
    now = time.time()
    if os.path.exists(lock_path):
        try:
            mtime = os.path.getmtime(lock_path)
            age = now - mtime
            if age > stale_seconds:
                os.remove(lock_path)
            else:
                raise SystemExit(f"⛔ Cron già in esecuzione (lock presente, età ~{int(age)}s). Esco.")
        except FileNotFoundError:
            pass

    ensure_dir(os.path.dirname(lock_path) or "/tmp")
    with open(lock_path, "w", encoding="utf-8") as f:
        f.write(f"pid={os.getpid()} started={datetime.now(timezone.utc).isoformat()}Z\n")


def release_lock(lock_path: str) -> None:
    try:
        os.remove(lock_path)
    except FileNotFoundError:
        pass


# =========================
# Utils
# =========================

def inches_to_mm(x: float) -> float:
    return round(x * 25.4, 2)


def normalize_city_name(city: str) -> str:
    return (
        city.replace("🏅", "")
            .replace("🔴", "")
            .replace("(", "")
            .replace(")", "")
            .replace(",", "")
            .replace("’", "")
            .replace("-", "")
            .replace(" ", "_")
            .strip()
    )


def month_label_it(month_label: str) -> str:
    """YYYY-MM -> 'Febbraio 2026'"""
    try:
        y, m = month_label.split("-")
        month_it = {
            1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
            5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
            9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
        }.get(int(m))
        return f"{month_it} {y}" if month_it else month_label
    except Exception:
        return month_label


def threshold_class_daily(mm: Number) -> str:
    v = float(mm)
    if v >= CFG.over_threshold_mm:
        return "high-daily-over"
    if v >= CFG.t150_mm:
        return "high-daily-150"
    if v >= CFG.t120_mm:
        return "high-daily-120"
    if v >= CFG.t100_mm:
        return "high-daily-100"
    if v >= CFG.t90_mm:
        return "high-daily-90"
    if v >= CFG.t80_mm:
        return "high-daily-80"
    if v >= CFG.t40_mm:
        return "high-daily-40"
    if v >= CFG.t25_mm:
        return "high-daily-25"
    return ""


# =========================
# PDF generation
# =========================

def genera_pdf_stazione(
    city_name: str,
    data_giornaliera: Dict[str, Number],
    output_folder: str,
    month_label: str,
    update_time: str,
    current_month: str,
    filename_base: str,
) -> str:
    ensure_dir(output_folder)
    filename = os.path.join(output_folder, f"{filename_base}.pdf")
    mese_label = month_label_it(month_label)

    doc = SimpleDocTemplate(
        filename,
        pagesize=A4,
        leftMargin=1.5 * cm,
        rightMargin=1.5 * cm,
        topMargin=1.5 * cm,
        bottomMargin=1.5 * cm,
        title=f"{city_name} - {mese_label}",
    )

    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        "TitleCustom",
        parent=styles["Title"],
        alignment=TA_CENTER,
        fontName="Helvetica-Bold",
        fontSize=16,
        leading=20,
        spaceAfter=6,
    )

    subtitle_style = ParagraphStyle(
        "SubTitleCustom",
        parent=styles["Normal"],
        alignment=TA_CENTER,
        fontName="Helvetica",
        fontSize=11,
        textColor=HexColor("#444444"),
        spaceAfter=8,
    )

    small_style = ParagraphStyle(
        "SmallCustom",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9,
        textColor=HexColor("#666666"),
    )

    update_centered_style = ParagraphStyle(
        "UpdateCentered",
        parent=small_style,
        alignment=TA_CENTER,
        spaceAfter=10,
    )

    elements = []
    elements.append(Paragraph(city_name, title_style))
    elements.append(Paragraph(f"Pioggia giornaliera – {mese_label}", subtitle_style))

    if month_label == current_month:
        elements.append(Paragraph(f"Aggiornato: {update_time}", update_centered_style))

    elements.append(Spacer(1, 6))

    # Mini bar chart
    giorni: List[str] = []
    valori: List[float] = []
    for giorno, valore in sorted(data_giornaliera.items()):
        if isinstance(valore, (int, float)):
            giorni.append(str(giorno)[-2:])
            valori.append(float(valore))

    if valori:
        table_width = 11 * cm
        drawing = Drawing(table_width, 120)
        drawing.hAlign = "CENTER"

        chart = VerticalBarChart()
        chart.x = 0
        chart.y = 10
        chart.height = 90
        chart.width = table_width
        chart.data = [valori]
        chart.strokeColor = colors.black

        max_v = max(valori) if max(valori) > 0 else 1
        chart.valueAxis.valueMin = 0
        chart.valueAxis.valueMax = max_v * 1.2
        chart.valueAxis.valueStep = max(1, int(max_v / 5))

        chart.categoryAxis.categoryNames = giorni
        chart.categoryAxis.labels.boxAnchor = "n"
        chart.categoryAxis.labels.fontSize = 7

        chart.bars[0].fillColor = HexColor("#007bff")
        chart.bars[0].strokeColor = HexColor("#0056b3")

        drawing.add(chart)
        elements.append(drawing)
        elements.append(Spacer(1, 12))

    # Table
    table_data = [["Giorno", "Pioggia (mm)"]]
    totale_mese = 0.0
    cell_bg_cmds = []
    cell_txt_cmds = []

    for giorno, valore in sorted(data_giornaliera.items()):
        if not isinstance(valore, (int, float)):
            continue

        v = float(valore)
        totale_mese += v
        giorno_label = str(giorno)[-2:]
        row_idx = len(table_data)
        table_data.append([giorno_label, f"{v:.2f}"])

        bg = None
        fg = None
        if v >= CFG.over_threshold_mm:
            bg = HexColor("#FF66FF"); fg = colors.black
        elif v >= CFG.t150_mm:
            bg = HexColor("#8C1C6C"); fg = colors.white
        elif v >= CFG.t120_mm:
            bg = HexColor("#595959"); fg = colors.white
        elif v >= CFG.t100_mm:
            bg = HexColor("#0070CD"); fg = colors.white
        elif v >= CFG.t90_mm:
            bg = HexColor("#375623"); fg = colors.white
        elif v >= CFG.t80_mm:
            bg = HexColor("#548235"); fg = colors.white
        elif v >= CFG.t40_mm:
            bg = HexColor("#A9D08E"); fg = colors.black
        elif v >= CFG.t25_mm:
            bg = HexColor("#E2EFDA"); fg = colors.black

        if bg is not None:
            cell_bg_cmds.append(("BACKGROUND", (1, row_idx), (1, row_idx), bg))
            cell_txt_cmds.append(("TEXTCOLOR", (1, row_idx), (1, row_idx), fg))
            cell_txt_cmds.append(("FONTNAME", (1, row_idx), (1, row_idx), "Helvetica-Bold"))

    tbl = Table(table_data, colWidths=[5 * cm, 6 * cm], hAlign="CENTER")

    header_blue = HexColor("#007bff")
    zebra = HexColor("#f2f2f2")

    base_style = [
        ("BACKGROUND", (0, 0), (-1, 0), header_blue),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 11),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
        ("TOPPADDING", (0, 0), (-1, 0), 8),

        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 10),
        ("ALIGN", (0, 1), (-1, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.5, HexColor("#dddddd")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, zebra]),
        ("TOPPADDING", (0, 1), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 6),
    ]

    tbl.setStyle(TableStyle(base_style + cell_bg_cmds + cell_txt_cmds))
    elements.append(tbl)
    elements.append(Spacer(1, 12))

    elements.append(
        Paragraph(
            f"<b>Totale mese:</b> {totale_mese:.2f} mm",
            ParagraphStyle(
                "TotalStyle",
                parent=styles["Normal"],
                alignment=TA_CENTER,
                fontName="Helvetica-Bold",
                fontSize=11,
                spaceBefore=6,
            )
        )
    )

    doc.build(elements)
    return filename


# =========================
# API fetch with retries/backoff + concurrency semaphore
# =========================

async def backoff_sleep(attempt: int) -> None:
    base = CFG.backoff_base_s * (2 ** attempt)
    base = min(base, CFG.backoff_cap_s)
    await asyncio.sleep(base + random.uniform(0.0, 0.35))


async def fetch_weather_data(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    api_base: str,
    api_key: str,
    station_id: str,
    city: str,
) -> Tuple[str, str, RainValue]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://www.wunderground.com/",
    }

    current_url = (
        f"{api_base}/observations/current"
        f"?apiKey={api_key}&stationId={station_id}"
        f"&numericPrecision=decimal&format=json&units=e"
    )

    async with sem:
        await asyncio.sleep(random.uniform(CFG.min_delay_s, CFG.max_delay_s))

        attempt = 0
        while True:
            try:
                async with session.get(
                    current_url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=CFG.timeout_total_sec),
                ) as r:
                    if r.status == 200:
                        try:
                            data = await r.json()
                        except Exception:
                            return station_id, city, "Dati non disponibili"

                        obs = (data.get("observations") or [])
                        if obs:
                            imperial = obs[0].get("imperial") or {}
                            precip_in = imperial.get("precipTotal")
                            if isinstance(precip_in, (int, float)):
                                daily_mm = inches_to_mm(float(precip_in))
                                if daily_mm > CFG.max_daily_mm_reasonable:
                                    return station_id, city, "⚠️ Errore nella stazione"
                                return station_id, city, daily_mm

                        return station_id, city, "Dati non disponibili"

                    if r.status in (204, 404):
                        return station_id, city, "Dati non disponibili"

                    if r.status in RETRY_HTTP_STATUSES and attempt < CFG.retries:
                        await backoff_sleep(attempt)
                        attempt += 1
                        continue

                    return station_id, city, "Dati non disponibili"

            except asyncio.TimeoutError:
                if attempt < CFG.retries:
                    await backoff_sleep(attempt)
                    attempt += 1
                    continue
                return station_id, city, "⏱️ Timeout"
            except Exception:
                if attempt < CFG.retries:
                    await backoff_sleep(attempt)
                    attempt += 1
                    continue
                return station_id, city, "❌ Errore"


# =========================
# HTML builder (escaping + station_id aware, wp-linkable)
# =========================

def build_region_html(
    region: str,
    stations_data: List[Tuple[str, RainValue, str]],  # (city, daily, station_id)
    storico_data: Dict[str, Dict[str, Number]],
    pdfs_presenti: set[str],
    previous_pdf_dir_abs: str,
    now_it: datetime,
    update_time: str,
    public_base_url: str,
) -> str:
    region_esc = html.escape(region)
    cache_bust = now_it.strftime("%Y%m%d%H%M")

    html_out = f"""<!DOCTYPE html>
<html lang='it'>
<head>
  <meta charset='UTF-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1.0'>
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <meta http-equiv="Pragma" content="no-cache">
  <meta http-equiv="Expires" content="0">
  <title>Pioggia - {region_esc}</title>

  <style>
    body {{
      font-family: Arial, sans-serif;
      background: #f4f4f4;
      margin: 20px;
      text-align: center;
    }}

    h1 {{
      color: #333;
      font-size: 28px;
      text-transform: uppercase;
      margin-bottom: 20px;
    }}

    .search-box {{
      margin-bottom: 20px;
    }}

    input[type="text"] {{
      width: 40%;
      padding: 8px;
      font-size: 16px;
      border: 1px solid #ccc;
      border-radius: 5px;
    }}

    .table-full {{
      width: 100%;
    }}

    .table-wrap {{
      overflow-x: auto;
      -webkit-overflow-scrolling: touch;
    }}

    table {{
      margin: 0 auto 20px auto;
      border-collapse: collapse;
      width: 100%;
      background: #fff;
      border-radius: 10px;
      box-shadow: 0px 4px 8px rgba(0,0,0,0.2);
      overflow: hidden;
    }}

    th {{
      background: #007bff;
      color: white;
      padding: 12px;
      font-size: 16px;
    }}

    td, th {{
      border: 1px solid #ddd;
      padding: 10px;
      text-align: center;
    }}

    tr:nth-child(even) {{
      background: #f2f2f2;
    }}

    td {{
      font-size: 15px;
      color: #333;
    }}

    /* ===== COLORI DAILY ===== */
    .high-daily-25 {{ background-color: #E2EFDA; font-weight: bold; }}
    .high-daily-40 {{ background-color: #A9D08E; font-weight: bold; }}
    .high-daily-80 {{ background-color: #548235; font-weight: bold; color: white; }}
    .high-daily-90 {{ background-color: #375623; font-weight: bold; color: white; }}
    .high-daily-100 {{ background-color: #0070CD; font-weight: bold; color: white; }}
    .high-daily-120 {{ background-color: #595959; font-weight: bold; color: white; }}
    .high-daily-150 {{ background-color: #8C1C6C; font-weight: bold; color: white; }}
    .high-daily-over {{ background-color: #FF66FF; font-weight: bold; color: black; }}

    /* ===== NON DISPONIBILE ===== */
    .na-cell {{ color: #888; font-style: italic; }}

    /* ===== MOBILE FULL WIDTH ===== */
    @media screen and (max-width: 768px) {{
      body {{
        margin: 0;
        padding: 0;
      }}

      h1 {{
        font-size: 22px;
        margin: 16px 0;
      }}

      input[type="text"] {{
        width: 90%;
      }}

      .table-full {{
        width: 100vw;
        margin-left: calc(-50vw + 50%);
      }}

      table {{
        border-radius: 0;
      }}

      th, td {{
        padding: 6px;
        font-size: 11px;
      }}
    }}
  </style>
</head>

<body>
  <h1>{region_esc} - Pioggia</h1>

  <p style='font-size:16px; color:#444; margin-top:-10px;'>
    <span style="font-size:18px;">&#128337;</span>
    Ultimo aggiornamento: <strong>{html.escape(update_time)}</strong>
  </p>

  <div class="search-box">
    <input type="text" id="searchInput" onkeyup="filterTable()" placeholder="🔍 Cerca località o sigla (es)...">
  </div>

  <div class="table-full">
    <div class="table-wrap">
      <table id="rainTable">
        <tr>
          <th>Località</th>
          <th>Pioggia Giornaliera (mm)</th>
          <th>PDF Mese Corrente</th>
          <th>PDF Mese Precedente</th>
        </tr>
"""

    for city, today_rain, station_id in stations_data:
        # offline if no numeric values in current month history
        is_offline = city not in storico_data or not any(
            isinstance(v, (int, float)) for v in storico_data.get(city, {}).values()
        )
        city_display = f"🔴 {city}" if is_offline else city
        city_display_esc = html.escape(city_display)

        # filenames (new + legacy)
        legacy_base = normalize_city_name(city)
        new_base = f"{legacy_base}_{station_id}"

        # current month PDF link: prefer new, fallback to legacy
        new_pdf_name = f"{new_base}.pdf"
        legacy_pdf_name = f"{legacy_base}.pdf"

        chosen_pdf_name = new_pdf_name if new_pdf_name in pdfs_presenti else legacy_pdf_name
        pdf_rel = f"{CFG.public_pdf_current_rel}/{chosen_pdf_name}?v={cache_bust}"
        pdf_link = join_url(public_base_url, pdf_rel)
        pdf_cell = (
            f"<a href='{pdf_link}' target='_blank'>📄 Scarica</a>"
            if chosen_pdf_name in pdfs_presenti
            else "<span style='color:#bbb;' title='PDF non disponibile'>📁</span>"
        )

        # previous month PDF (filesystem existence check)
        prev_new_abs = os.path.join(previous_pdf_dir_abs, new_pdf_name)
        prev_legacy_abs = os.path.join(previous_pdf_dir_abs, legacy_pdf_name)

        if os.path.exists(prev_new_abs):
            prev_name = new_pdf_name
            prev_rel = f"{CFG.public_pdf_prev_rel}/{prev_name}?v={cache_bust}"
            prev_link = join_url(public_base_url, prev_rel)
            prev_cell = f"<a href='{prev_link}' target='_blank'>📄 Scarica</a>"
        elif os.path.exists(prev_legacy_abs):
            prev_name = legacy_pdf_name
            prev_rel = f"{CFG.public_pdf_prev_rel}/{prev_name}?v={cache_bust}"
            prev_link = join_url(public_base_url, prev_rel)
            prev_cell = f"<a href='{prev_link}' target='_blank'>📄 Scarica</a>"
        else:
            prev_cell = "<span style='color:#bbb;' title='PDF non disponibile'>📁</span>"

        # daily value formatting
        if isinstance(today_rain, (int, float)):
            daily_class = threshold_class_daily(today_rain)
            daily_value_str = f"{float(today_rain):.2f}"
        else:
            daily_class = "na-cell"
            daily_value_str = "Dati non disponibili"

        html_out += (
            f"<tr>"
            f"<td>{city_display_esc}</td>"
            f"<td class='{daily_class}'>{html.escape(daily_value_str)}</td>"
            f"<td>{pdf_cell}</td>"
            f"<td>{prev_cell}</td>"
            f"</tr>"
        )

    html_out += """
      </table>
    </div>
  </div>

  <script>
    function filterTable() {
      var input = document.getElementById("searchInput");
      var filter = input.value.toLowerCase();
      var table = document.getElementById("rainTable");
      var tr = table.getElementsByTagName("tr");

      for (var i = 1; i < tr.length; i++) {
        var td = tr[i].getElementsByTagName("td")[0];
        if (td) {
          var txtValue = td.textContent || td.innerText;
          tr[i].style.display = txtValue.toLowerCase().indexOf(filter) > -1 ? "" : "none";
        }
      }
    }
  </script>
</body>
</html>
"""
    return html_out


# =========================
# Main pipeline
# =========================

async def main() -> None:
    acquire_lock_or_exit(CFG.lock_file, CFG.lock_stale_seconds)
    started = time.time()

    try:
        api_key = os.getenv(CFG.api_key_env)
        if not api_key:
            raise RuntimeError(f"{CFG.api_key_env} non impostata in ambiente.")

        script_dir = os.path.dirname(os.path.abspath(__file__))
        stations_file = os.getenv(CFG.stations_file_env, CFG.stations_file_default)
        stations_path = os.path.join(script_dir, stations_file)

        with open(stations_path, "r", encoding="utf-8") as f:
            stations_by_region: Dict[str, Dict[str, str]] = json.load(f)

        if not isinstance(stations_by_region, dict) or not stations_by_region:
            raise ValueError("stations_by_region.json non valido o vuoto: deve essere {regione: {station_id: city}}")

        # Dates
        italy_tz = ZoneInfo(CFG.tz_name)
        now_it = datetime.now(italy_tz)
        update_time = now_it.strftime("%d/%m/%Y - %H:%M")
        today = now_it.date()

        current_month = today.strftime("%Y-%m")
        previous_month = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
        today_str = today.strftime("%Y-%m-%d")

        # Outputs
        output_base_dir = os.getenv(CFG.output_base_dir_env, CFG.output_base_dir_default)
        output_base_dir = os.path.abspath(os.path.join(script_dir, output_base_dir))

        uploads_dir = os.path.join(output_base_dir, "uploads")
        backup_json_dir = os.path.join(uploads_dir, "mensili")
        pdf_output_dir = os.path.join(uploads_dir, "pdf_stazioni")
        daily_archive_dir = os.path.join(uploads_dir, "giornalieri")
        regioni_output_dir = os.path.join(uploads_dir, "regioni")

        ensure_dir(output_base_dir)
        ensure_dir(uploads_dir)
        ensure_dir(backup_json_dir)
        ensure_dir(pdf_output_dir)
        ensure_dir(daily_archive_dir)
        ensure_dir(regioni_output_dir)

        previous_pdf_dir = os.path.join(pdf_output_dir, "storico_precedente")
        ensure_dir(previous_pdf_dir)

        pdf_meta_dir = os.path.join(pdf_output_dir, "_meta")
        ensure_dir(pdf_meta_dir)
        html_meta_dir = os.path.join(regioni_output_dir, "_meta")
        ensure_dir(html_meta_dir)

        public_base_url = os.getenv(CFG.public_base_url_env, CFG.public_base_url_default)

        # Files
        daily_json_file = os.path.join(backup_json_dir, f"{today_str}.json")
        storico_json_file = os.path.join(backup_json_dir, f"{current_month}_storico.json")
        daily_history_file = os.path.join(daily_archive_dir, f"{current_month}_giornaliero.json")
        previous_storico_json = os.path.join(backup_json_dir, f"{previous_month}_storico.json")

        # Load previous month storico (for prev PDFs)
        previous_storico_data: Dict[str, Dict[str, Number]] = {}
        if os.path.exists(previous_storico_json):
            try:
                with open(previous_storico_json, "r", encoding="utf-8") as f:
                    tmp = json.load(f)
                if isinstance(tmp, dict):
                    previous_storico_data = tmp
            except Exception:
                previous_storico_data = {}

        # Build mappings
        value_to_region: Dict[Tuple[str, str], str] = {}  # (station_id, city) -> region
        all_stations: List[Tuple[str, str, str]] = []     # (region, station_id, city)

        for region, stations in stations_by_region.items():
            if not isinstance(stations, dict):
                continue
            for station_id, city in stations.items():
                all_stations.append((region, station_id, city))
                value_to_region[(station_id, city)] = region

        # Fetch API
        sem = asyncio.Semaphore(CFG.concurrent_limit)
        connector = aiohttp.TCPConnector(limit=CFG.concurrent_limit * 2, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [
                fetch_weather_data(session, sem, CFG.api_base, api_key, station_id, city)
                for (_region, station_id, city) in all_stations
            ]
            results: List[Tuple[str, str, RainValue]] = await asyncio.gather(*tasks)

        # Organize per region
        data_by_region: Dict[str, List[Tuple[str, RainValue, str]]] = {r: [] for r in stations_by_region.keys()}
        for station_id, city, daily_rain in results:
            region = value_to_region.get((station_id, city))
            if region:
                data_by_region.setdefault(region, []).append((city, daily_rain, station_id))

        # Daily backup (region -> city -> mm) numeric only
        daily_data: Dict[str, Dict[str, Number]] = {}
        for region, station_list in data_by_region.items():
            for city, daily_rain, _station_id in station_list:
                if isinstance(daily_rain, (int, float)):
                    daily_data.setdefault(region, {})[city] = float(daily_rain)

        atomic_write_json(daily_json_file, daily_data)

        # Daily history (city -> {YYYY-MM-DD: mm})
        daily_history: Dict[str, Dict[str, Number]] = {}
        if os.path.exists(daily_history_file):
            try:
                with open(daily_history_file, "r", encoding="utf-8") as f:
                    tmp = json.load(f)
                if isinstance(tmp, dict):
                    daily_history = tmp
            except Exception:
                daily_history = {}

        for region_cities in daily_data.values():
            for city, rain_mm in region_cities.items():
                if isinstance(rain_mm, (int, float)):
                    daily_history.setdefault(city, {})[today_str] = float(rain_mm)

        atomic_write_json(daily_history_file, daily_history)

        # Monthly storico for PDFs (city -> {YYYY-MM-DD: mm})
        storico_data: Dict[str, Dict[str, Number]] = {}
        if os.path.exists(storico_json_file):
            try:
                with open(storico_json_file, "r", encoding="utf-8") as f:
                    raw_data = json.load(f)
                if isinstance(raw_data, dict):
                    for city, giorni in raw_data.items():
                        if isinstance(giorni, dict):
                            giorni_validi = {
                                str(d): float(v) for d, v in giorni.items()
                                if str(d).startswith(current_month) and isinstance(v, (int, float))
                            }
                            if giorni_validi:
                                storico_data[str(city)] = giorni_validi
            except Exception:
                storico_data = {}

        # insert today's value from daily history
        for city, daily_values in daily_history.items():
            v = daily_values.get(today_str)
            if isinstance(v, (int, float)):
                storico_data.setdefault(city, {})[today_str] = float(v)

        # keep only current month, remove empties
        for city in list(storico_data.keys()):
            storico_data[city] = {
                d: v for d, v in storico_data[city].items()
                if str(d).startswith(current_month) and isinstance(v, (int, float))
            }
            if not storico_data[city]:
                del storico_data[city]

        atomic_write_json(storico_json_file, storico_data)

        # Build PDFs current month (SKIP if unchanged via hash)
        generated_pdfs = 0
        removed_pdfs = 0
        valid_pdf_names: set[str] = set()

        for region, station_list in data_by_region.items():
            for city, _daily_rain, station_id in station_list:
                giorni = storico_data.get(city) or {}
                giorni_mese = {
                    d: v for d, v in giorni.items()
                    if str(d).startswith(current_month) and isinstance(v, (int, float))
                }

                legacy_base = normalize_city_name(city)
                new_base = f"{legacy_base}_{station_id}"
                new_pdf_path = os.path.join(pdf_output_dir, f"{new_base}.pdf")
                legacy_pdf_path = os.path.join(pdf_output_dir, f"{legacy_base}.pdf")

                if not giorni_mese:
                    for p in (new_pdf_path, legacy_pdf_path):
                        if os.path.exists(p):
                            os.remove(p)
                            removed_pdfs += 1
                    meta_path = os.path.join(pdf_meta_dir, f"{new_base}.sha1")
                    if os.path.exists(meta_path):
                        os.remove(meta_path)
                    continue

                meta_path = os.path.join(pdf_meta_dir, f"{new_base}.sha1")
                h = sha1_json(giorni_mese)
                old = None
                if os.path.exists(meta_path):
                    try:
                        with open(meta_path, "r", encoding="utf-8") as f:
                            old = f.read().strip()
                    except Exception:
                        old = None

                if old == h and os.path.exists(new_pdf_path):
                    valid_pdf_names.add(f"{new_base}.pdf")
                    continue

                genera_pdf_stazione(
                    city_name=city,
                    data_giornaliera=giorni_mese,
                    output_folder=pdf_output_dir,
                    month_label=current_month,
                    update_time=update_time,
                    current_month=current_month,
                    filename_base=new_base,
                )
                atomic_write_text(meta_path, h)
                generated_pdfs += 1
                valid_pdf_names.add(f"{new_base}.pdf")

        # Remove only new-style orphan PDFs (safe)
        for fn in os.listdir(pdf_output_dir):
            p = os.path.join(pdf_output_dir, fn)
            if os.path.isdir(p) or not fn.endswith(".pdf"):
                continue
            if fn in {"storico_precedente"}:
                continue
            if "_" in fn and fn not in valid_pdf_names:
                try:
                    os.remove(p)
                    removed_pdfs += 1
                except Exception:
                    pass

        # Build previous month PDFs (legacy name is fine)
        for city, giorni in (previous_storico_data or {}).items():
            if isinstance(giorni, dict) and giorni:
                legacy_base = normalize_city_name(city)
                genera_pdf_stazione(
                    city_name=str(city),
                    data_giornaliera={str(k): float(v) for k, v in giorni.items() if isinstance(v, (int, float))},
                    output_folder=previous_pdf_dir,
                    month_label=previous_month,
                    update_time=update_time,
                    current_month=current_month,
                    filename_base=legacy_base,
                )

        # Compute present PDFs for HTML linking
        pdfs_presenti = {fn for fn in os.listdir(pdf_output_dir) if fn.endswith(".pdf")}

        # Build region HTML (SKIP if unchanged via hash)
        generated_html = 0
        for region, station_list in data_by_region.items():
            stations_for_region = []
            for (city, today_rain, station_id) in station_list:
                giorni = storico_data.get(city) or {}
                offline = not any(isinstance(v, (int, float)) for v in giorni.values())

                legacy_base = normalize_city_name(city)
                new_name = f"{legacy_base}_{station_id}.pdf"
                legacy_name = f"{legacy_base}.pdf"
                current_pdf_present = (new_name in pdfs_presenti) or (legacy_name in pdfs_presenti)

                prev_new_abs = os.path.join(previous_pdf_dir, new_name)
                prev_legacy_abs = os.path.join(previous_pdf_dir, legacy_name)
                prev_present = os.path.exists(prev_new_abs) or os.path.exists(prev_legacy_abs)

                stations_for_region.append([
                    city,
                    mm_fmt(today_rain),
                    station_id,
                    int(offline),
                    int(current_pdf_present),
                    int(prev_present),
                ])

            region_hash_obj = {"region": region, "update_time": update_time, "stations": stations_for_region}
            region_hash = sha1_json(region_hash_obj)

            meta_path = os.path.join(html_meta_dir, f"{region.lower().replace(' ', '_')}.sha1")
            old = None
            if os.path.exists(meta_path):
                try:
                    with open(meta_path, "r", encoding="utf-8") as f:
                        old = f.read().strip()
                except Exception:
                    old = None

            file_name = region.lower().replace(" ", "_") + ".html"
            output_path = os.path.join(regioni_output_dir, file_name)

            if old == region_hash and os.path.exists(output_path):
                continue

            html_content = build_region_html(
                region=region,
                stations_data=station_list,
                storico_data=storico_data,
                pdfs_presenti=pdfs_presenti,
                previous_pdf_dir_abs=previous_pdf_dir,
                now_it=now_it,
                update_time=update_time,
                public_base_url=public_base_url,
            )
            atomic_write_text(output_path, html_content)
            atomic_write_text(meta_path, region_hash)
            generated_html += 1

        # Optional: if day 1, generate previous month .json from yesterday daily backup
        if today.day == 1:
            yesterday = (now_it - timedelta(days=1)).date()
            yesterday_file = os.path.join(backup_json_dir, f"{yesterday.strftime('%Y-%m-%d')}.json")
            monthly_file = os.path.join(backup_json_dir, f"{yesterday.strftime('%Y-%m')}.json")
            if os.path.exists(yesterday_file):
                try:
                    with open(yesterday_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    atomic_write_json(monthly_file, data)
                except Exception:
                    pass

        # Summary
        elapsed = time.time() - started
        total = len(all_stations)
        numeric = sum(1 for _sid, _c, v in results if isinstance(v, (int, float)))
        errs = total - numeric
        print(
            f"✅ Done | stazioni={total} | numeriche={numeric} | non-numeriche={errs} "
            f"| pdf_gen={generated_pdfs} pdf_rm={removed_pdfs} | html_gen={generated_html} | t={elapsed:.1f}s"
        )

    finally:
        release_lock(CFG.lock_file)


if __name__ == "__main__":
    asyncio.run(main())