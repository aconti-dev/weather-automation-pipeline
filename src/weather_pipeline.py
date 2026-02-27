#!/usr/bin/env python3
"""
Weather Underground / Weather.com PWS (API) -> daily JSON + monthly history JSON + per-city PDF + per-region HTML.

✅ Repo/GitHub friendly:
- NO hardcoded API keys
- NO hardcoded server paths (/home/customer/...)
- NO hardcoded domain
- Output in ./outputs/ by default
- Public links in HTML are configurable via env

Requisiti:
- Python 3.11+
- aiohttp
- reportlab

Env:
- WU_API_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
- PUBLIC_BASE_URL="" (opzionale; es: https://tuodominio.it)
- OUTPUT_BASE_DIR="./outputs" (opzionale)
"""

import os
import json
import random
import asyncio
import aiohttp
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.colors import HexColor
from reportlab.lib.enums import TA_CENTER
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import VerticalBarChart


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
    stations_file: str = "stations_by_region.sample.json"

    # Outputs base dir (repo-friendly)
    output_base_dir_env: str = "OUTPUT_BASE_DIR"
    output_base_dir_default: str = "outputs"

    # Public base URL (optional) used in HTML links
    public_base_url_env: str = "PUBLIC_BASE_URL"
    public_base_url_default: str = ""  # keep relative links by default

    # WP-like relative paths used in HTML (customizable if you want)
    # These are ONLY for link building (NOT filesystem).
    # If you don't want WP paths, change these strings.
    public_pdf_current_rel: str = "/wp-content/uploads/pdf_stazioni"
    public_pdf_prev_rel: str = "/wp-content/uploads/pdf_stazioni/storico_precedente"

    # Networking
    timeout_total_sec: int = 20
    min_delay_s: float = 0.25
    max_delay_s: float = 0.7
    concurrent_limit: int = 30  # limit concurrency to be gentle with API


CFG = Config()


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
    """month_label: YYYY-MM -> 'Febbraio 2026' """
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


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def join_url(base: str, path: str) -> str:
    """Join base url with a path. If base is empty, return path (relative)."""
    if not base:
        return path
    return base.rstrip("/") + "/" + path.lstrip("/")


# =========================
# PDF generation
# =========================

def genera_pdf_stazione(
    city_name: str,
    data_giornaliera: dict,
    output_folder: str,
    month_label: str,
    update_time: str,
    current_month: str,
) -> str:
    """
    Create per-city PDF with:
    - title + subtitle
    - optional 'Aggiornato' only for current month
    - mini bar chart + table with colored thresholds
    """
    pdf_name = normalize_city_name(city_name)
    filename = os.path.join(output_folder, f"{pdf_name}.pdf")

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

    # ---- Mini bar chart ----
    valori = []
    giorni = []
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

    # ---- Table ----
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

        # thresholds (same as your HTML)
        bg = None
        fg = None
        if v > 150:
            bg = HexColor("#FF66FF"); fg = colors.black
        elif v >= 150:
            bg = HexColor("#8C1C6C"); fg = colors.white
        elif v >= 120:
            bg = HexColor("#595959"); fg = colors.white
        elif v >= 100:
            bg = HexColor("#0070CD"); fg = colors.white
        elif v >= 90:
            bg = HexColor("#375623"); fg = colors.white
        elif v >= 80:
            bg = HexColor("#548235"); fg = colors.white
        elif v >= 40:
            bg = HexColor("#A9D08E"); fg = colors.black
        elif v >= 25:
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
    print("✅ PDF creato:", filename)
    return filename


# =========================
# API fetch (PWS current)
# =========================

async def fetch_weather_data(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    api_base: str,
    api_key: str,
    station_id: str,
    city: str,
) -> tuple[str, float | str]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://www.wunderground.com/",
    }

    daily_rain: float | str = "Dati non disponibili"

    current_url = (
        f"{api_base}/observations/current"
        f"?apiKey={api_key}&stationId={station_id}"
        f"&numericPrecision=decimal&format=json&units=e"
    )

    try:
        async with sem:
            await asyncio.sleep(random.uniform(CFG.min_delay_s, CFG.max_delay_s))
            async with session.get(
                current_url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=CFG.timeout_total_sec),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    obs = (data.get("observations") or [])
                    if obs:
                        imperial = obs[0].get("imperial") or {}
                        precip_in = imperial.get("precipTotal")
                        if isinstance(precip_in, (int, float)):
                            daily_mm = inches_to_mm(float(precip_in))
                            if daily_mm > 400:
                                print(f"⚠️ Valore anomalo DAILY {station_id} ({city}): {daily_mm} mm")
                                daily_rain = "⚠️ Errore nella stazione"
                            else:
                                daily_rain = daily_mm
                else:
                    print(f"⚠️ HTTP {r.status} DAILY {station_id}")
    except asyncio.TimeoutError:
        print(f"⏱️ Timeout API per {station_id}")
    except Exception as e:
        print(f"❌ Errore API per {station_id}: {e}")

    return city, daily_rain


# =========================
# HTML region builder
# =========================

def build_region_html(
    region: str,
    stations_data: list[tuple[str, float | str]],
    storico_data: dict,
    pdfs_presenti: set[str],
    previous_pdf_dir_abs: str,
    now_it: datetime,
    update_time: str,
    public_base_url: str,
) -> str:
    """Return full HTML string for a region page (responsive, mobile full width)."""

    html = f"""<!DOCTYPE html>
<html lang='it'>
<head>
  <meta charset='UTF-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1.0'>
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <meta http-equiv="Pragma" content="no-cache">
  <meta http-equiv="Expires" content="0">
  <title>Pioggia - {region}</title>

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
  <h1>{region} - Pioggia</h1>

  <p style='font-size:16px; color:#444; margin-top:-10px;'>
    <span style="font-size:18px;">&#128337;</span>
    Ultimo aggiornamento: <strong>{update_time}</strong>
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

    cache_bust = now_it.strftime("%Y%m%d%H%M")

    for city, today_rain in stations_data:
        # offline if no numeric values in current month history
        is_offline = city not in storico_data or not any(
            isinstance(v, (int, float)) for v in storico_data.get(city, {}).values()
        )
        city_display = f"🔴 {city}" if is_offline else city

        # current month PDF
        pdf_filename = normalize_city_name(city) + ".pdf"
        pdf_rel = f"{CFG.public_pdf_current_rel}/{pdf_filename}?v={cache_bust}"
        pdf_link = join_url(public_base_url, pdf_rel)
        pdf_cell = (
            f"<a href='{pdf_link}' target='_blank'>📄 Scarica</a>"
            if pdf_filename in pdfs_presenti
            else "<span style='color:#bbb;' title='PDF non disponibile'>📁</span>"
        )

        # previous month PDF (existence check is filesystem)
        pdf_prec_filename = normalize_city_name(city) + ".pdf"
        pdf_prec_abs = os.path.join(previous_pdf_dir_abs, pdf_prec_filename)
        pdf_prec_rel = f"{CFG.public_pdf_prev_rel}/{pdf_prec_filename}?v={cache_bust}"
        pdf_prec_link = join_url(public_base_url, pdf_prec_rel)
        pdf_prec_cell = (
            f"<a href='{pdf_prec_link}' target='_blank'>📄 Scarica</a>"
            if os.path.exists(pdf_prec_abs)
            else "<span style='color:#bbb;' title='PDF non disponibile'>📁</span>"
        )

        # daily color class
        daily_class = ""
        if isinstance(today_rain, (int, float)):
            if today_rain > 150:
                daily_class = "high-daily-over"
            elif today_rain >= 150:
                daily_class = "high-daily-150"
            elif today_rain >= 120:
                daily_class = "high-daily-120"
            elif today_rain >= 100:
                daily_class = "high-daily-100"
            elif today_rain >= 90:
                daily_class = "high-daily-90"
            elif today_rain >= 80:
                daily_class = "high-daily-80"
            elif today_rain >= 40:
                daily_class = "high-daily-40"
            elif today_rain >= 25:
                daily_class = "high-daily-25"

        html += (
            f"<tr>"
            f"<td>{city_display}</td>"
            f"<td class='{daily_class}'>{today_rain}</td>"
            f"<td>{pdf_cell}</td>"
            f"<td>{pdf_prec_cell}</td>"
            f"</tr>"
        )

    html += """
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
    return html


# =========================
# Main
# =========================

async def main() -> None:
    # ---- API KEY ----
    api_key = os.getenv(CFG.api_key_env)
    if not api_key:
        raise ValueError(
            f"{CFG.api_key_env} non impostata. "
            "Crea un file .env (non committarlo) oppure imposta la variabile d'ambiente."
        )

    # ---- paths ----
    script_dir = os.path.dirname(os.path.abspath(__file__))
    stations_path = os.path.join(script_dir, CFG.stations_file)

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

    # ---- public base url (optional) ----
    public_base_url = os.getenv(CFG.public_base_url_env, CFG.public_base_url_default)

    # ---- load stations ----
    with open(stations_path, "r", encoding="utf-8") as f:
        stations_by_region = json.load(f)

    if not isinstance(stations_by_region, dict) or not stations_by_region:
        raise ValueError("stations_by_region.json non valido o vuoto: deve essere {regione: {station_id: city}}")

    # ---- dates ----
    italy_tz = ZoneInfo(CFG.tz_name)
    now_it = datetime.now(italy_tz)
    update_time = now_it.strftime("%d/%m/%Y - %H:%M")
    today = now_it.date()

    current_month = today.strftime("%Y-%m")
    previous_month = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    today_str = today.strftime("%Y-%m-%d")

    previous_json_file = os.path.join(backup_json_dir, f"{previous_month}_storico.json")
    if not os.path.exists(previous_json_file):
        previous_json_file = os.path.join(backup_json_dir, f"{previous_month}.json")

    daily_json_file = os.path.join(backup_json_dir, f"{today_str}.json")
    storico_json_file = os.path.join(backup_json_dir, f"{current_month}_storico.json")
    daily_history_file = os.path.join(daily_archive_dir, f"{current_month}_giornaliero.json")
    previous_storico_json = os.path.join(backup_json_dir, f"{previous_month}_storico.json")

    # ---- optional load previous month data (kept for compatibility / logic) ----
    previous_month_data: dict[str, float] = {}
    if os.path.exists(previous_json_file):
        with open(previous_json_file, "r", encoding="utf-8") as f:
            json_data = json.load(f)

        if isinstance(json_data, dict) and all(isinstance(v, dict) for v in json_data.values()):
            for _region, cities in json_data.items():
                for city, mm in cities.items():
                    if isinstance(mm, (int, float)):
                        previous_month_data[str(city).strip()] = float(mm)
        elif isinstance(json_data, dict):
            for city, daily_values in json_data.items():
                if isinstance(daily_values, dict):
                    total = sum(v for v in daily_values.values() if isinstance(v, (int, float)))
                    previous_month_data[str(city).strip()] = round(float(total), 2)

        print(f"✅ File JSON caricato: {previous_json_file} | Città lette: {len(previous_month_data)}")

    previous_storico_data = {}
    if os.path.exists(previous_storico_json):
        with open(previous_storico_json, "r", encoding="utf-8") as f:
            previous_storico_data = json.load(f)

    # ---- fetch API ----
    data_by_region: dict[str, list[tuple[str, float | str]]] = {r: [] for r in stations_by_region.keys()}
    value_to_region: dict[str, str] = {}
    for region, stations in stations_by_region.items():
        for _station_id, city in stations.items():
            value_to_region[city] = region

    sem = asyncio.Semaphore(CFG.concurrent_limit)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for region, stations in stations_by_region.items():
            for station_id, city in stations.items():
                tasks.append(fetch_weather_data(session, sem, CFG.api_base, api_key, station_id, city))
        results = await asyncio.gather(*tasks)

    for (city, daily_rain) in results:
        region = value_to_region.get(city)
        if region:
            data_by_region[region].append((city, daily_rain))

    # ---- daily backup (region -> city -> mm) numeric only ----
    daily_data: dict[str, dict[str, float]] = {}
    for region, station_list in data_by_region.items():
        for city, daily_rain in station_list:
            if isinstance(daily_rain, (int, float)):
                daily_data.setdefault(region, {})[city] = float(daily_rain)

    with open(daily_json_file, "w", encoding="utf-8") as f:
        json.dump(daily_data, f, ensure_ascii=False, indent=2)
    print(f"📅 Backup giornaliero salvato: {daily_json_file}")

    # ---- daily history (city -> {YYYY-MM-DD: mm}) ----
    daily_history: dict[str, dict[str, float]] = {}
    if os.path.exists(daily_history_file):
        with open(daily_history_file, "r", encoding="utf-8") as f:
            daily_history = json.load(f)

    for region_cities in daily_data.values():
        for city, rain_mm in region_cities.items():
            daily_history.setdefault(city, {})[today_str] = float(rain_mm)

    with open(daily_history_file, "w", encoding="utf-8") as f:
        json.dump(daily_history, f, ensure_ascii=False, indent=2)
    print(f"📘 Storico giornaliero aggiornato: {daily_history_file}")

    # ---- monthly storico for PDFs (city -> {YYYY-MM-DD: mm}) ----
    storico_data: dict[str, dict[str, float]] = {}
    if os.path.exists(storico_json_file):
        with open(storico_json_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        for city, giorni in raw_data.items():
            if isinstance(giorni, dict):
                giorni_validi = {
                    d: v for d, v in giorni.items()
                    if str(d).startswith(current_month) and isinstance(v, (int, float))
                }
                if giorni_validi:
                    storico_data[city] = {str(k): float(v) for k, v in giorni_validi.items()}

    # insert today's value from daily history
    for city, daily_values in daily_history.items():
        v = daily_values.get(today_str)
        if isinstance(v, (int, float)):
            storico_data.setdefault(city, {})[today_str] = float(v)

    # keep only current month
    for city in list(storico_data.keys()):
        storico_data[city] = {
            d: v for d, v in storico_data[city].items()
            if str(d).startswith(current_month) and isinstance(v, (int, float))
        }
        if not storico_data[city]:
            del storico_data[city]

    with open(storico_json_file, "w", encoding="utf-8") as f:
        json.dump(storico_data, f, ensure_ascii=False, indent=2)
    print(f"📘 Storico mensile aggiornato: {storico_json_file}")

    # ---- build current month PDFs ----
    for city, giorni in storico_data.items():
        giorni_mese = {
            d: v for d, v in giorni.items()
            if str(d).startswith(current_month) and isinstance(v, (int, float))
        }
        pdf_path = os.path.join(pdf_output_dir, normalize_city_name(city) + ".pdf")
        if giorni_mese:
            genera_pdf_stazione(city, giorni_mese, pdf_output_dir, current_month, update_time, current_month)
        else:
            if os.path.exists(pdf_path):
                os.remove(pdf_path)

    # remove orphan PDFs
    valid_pdf_names = {normalize_city_name(city) + ".pdf" for city in storico_data.keys()}
    for filename in os.listdir(pdf_output_dir):
        if filename.endswith(".pdf"):
            path = os.path.join(pdf_output_dir, filename)
            if os.path.isfile(path) and filename not in valid_pdf_names and filename != "storico_precedente":
                os.remove(path)

    # ---- build previous month PDFs (if available) ----
    if isinstance(previous_storico_data, dict):
        for city, giorni in previous_storico_data.items():
            if isinstance(giorni, dict) and giorni:
                genera_pdf_stazione(city, giorni, previous_pdf_dir, previous_month, update_time, current_month)

    pdfs_presenti = set(fn for fn in os.listdir(pdf_output_dir) if fn.endswith(".pdf"))

    # ---- build region HTML files ----
    for region, stations_data in data_by_region.items():
        html_content = build_region_html(
            region=region,
            stations_data=stations_data,
            storico_data=storico_data,
            pdfs_presenti=pdfs_presenti,
            previous_pdf_dir_abs=previous_pdf_dir,
            now_it=now_it,
            update_time=update_time,
            public_base_url=public_base_url,
        )

        file_name = region.lower().replace(" ", "_") + ".html"
        output_path = os.path.join(regioni_output_dir, file_name)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        print(f"✅ HTML salvato per {region}: {output_path}")

    # ---- optional: if day 1, build previous month .json from yesterday daily backup ----
    if today.day == 1:
        yesterday = (now_it - timedelta(days=1)).date()
        yesterday_file = os.path.join(backup_json_dir, f"{yesterday.strftime('%Y-%m-%d')}.json")
        monthly_file = os.path.join(backup_json_dir, f"{yesterday.strftime('%Y-%m')}.json")

        if os.path.exists(yesterday_file):
            with open(yesterday_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            with open(monthly_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"✅ File mensile generato da backup del giorno precedente: {monthly_file}")
        else:
            print(f"⚠️ Nessun backup giornaliero trovato per ieri ({yesterday_file})")


if __name__ == "__main__":
    asyncio.run(main())