# 🌧 Weather Automation Pipeline (Python + API + PDF + HTML)

Pipeline automatizzata per la raccolta di dati meteo da Weather.com / Wunderground (PWS API), con:

- Backup JSON giornaliero
- Storico mensile per stazione
- Report PDF dinamici (tabella + grafico + soglie colore)
- Pagine HTML regionali responsive (mobile-first)
- Automazione asincrona con asyncio + aiohttp

Progetto sviluppato come esempio reale di automazione data-driven e workflow scalabile.

---

## 🚀 Funzionalità principali

- Integrazione API REST (Weather.com PWS)
- Fetch asincrono con asyncio
- Validazione dati e gestione anomalie
- Persistenza JSON strutturata (giornaliero + mensile)
- Generazione PDF dinamici con ReportLab
- Generazione HTML responsive con ricerca live
- Struttura repository GitHub-ready (no chiavi hardcoded)

---

## 🧱 Struttura del progetto

weather-automation-pipeline/
├─ src/
│  └─ weather_pipeline.py
├─ stations_by_region.sample.json
├─ .env.example
├─ .gitignore
└─ README.md

---

## ✅ Requisiti

- Python 3.10+
- API Key Weather.com / Wunderground PWS
- Connessione internet

---

## ⚙️ Setup (Windows)

Apri PowerShell nella cartella del progetto.

### 1️⃣ Crea ambiente virtuale

python -m venv .venv  
.\.venv\Scripts\Activate.ps1

Se PowerShell blocca l’attivazione:

Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

---

### 2️⃣ Installa dipendenze

pip install aiohttp reportlab python-dotenv

---

## 🔐 Configurazione

### 1️⃣ Crea il file .env

Copia `.env.example` e rinominalo in `.env`.

Inserisci la tua chiave:

WU_API_KEY=INCOLLA_LA_TUA_API_KEY

Il file `.env` non viene caricato su GitHub.

---

### 2️⃣ Configura le stazioni

Duplica `stations_by_region.sample.json` e rinominalo in:

stations_by_region.json

Formato esempio:

{
  "Lombardia": {
    "ISTATIONID1": "Como",
    "ISTATIONID2": "Milano"
  },
  "Veneto": {
    "ISTATIONID3": "Verona"
  }
}

---

## ▶️ Esecuzione

python .\src\weather_pipeline.py

---

## 📦 Output generato

- JSON giornalieri (backup)
- Storico mensile per stazione
- PDF dinamici per stazione
- HTML regionali responsive

---

## 🔒 Sicurezza

- Nessuna API key hardcoded
- Configurazione tramite variabili ambiente
- File sensibili ignorati via .gitignore

---

## 📈 Possibili miglioramenti futuri

- requirements.txt ufficiale
- Logging strutturato
- Dockerizzazione
- Task Scheduler / cron automation
- Test automatici

---

## 📄 Licenza

MIT (consigliata se vuoi renderlo riutilizzabile)

---

## 👨‍💻 Autore

Alfonso Conti  
Digital Automation & Data Workflow Engineer  

Python | API Integration | Data Pipelines | AI Automation  

📍 Como, Italy  
🔗 LinkedIn: https://www.linkedin.com/in/alfonso-conti-8580603b2/