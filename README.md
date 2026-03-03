# 🌧 Weather Automation Pipeline  
Python · Async API · JSON Storage · Dynamic PDF · Responsive HTML

Pipeline automatizzata per la raccolta di dati meteo da Weather.com / Wunderground (PWS API) con generazione di report PDF e pagine HTML regionali.

Progetto sviluppato come esempio reale di automazione data-driven e workflow scalabile.

------------------------------------------------------------

🚀 FUNZIONALITÀ PRINCIPALI

- Integrazione API REST (Weather.com PWS)
- Fetch asincrono con asyncio + aiohttp
- Gestione retry, backoff e rate limiting
- Validazione dati e gestione anomalie
- Backup JSON giornaliero
- Storico mensile per stazione
- Generazione PDF dinamici (ReportLab)
- Generazione HTML responsive (mobile-first)
- Hash-based update (rigenera solo se cambiano i dati)
- Nessuna API key hardcoded

------------------------------------------------------------

🧱 STRUTTURA DEL PROGETTO

weather-automation-pipeline/
│
├─ src/
│   └─ weather_pipeline.py
│
├─ stations_by_region.sample.json
├─ .env.example
├─ .gitignore
├─ requirements.txt
└─ README.md

------------------------------------------------------------

✅ REQUISITI

- Python 3.11+
- API Key Weather.com / Wunderground (PWS API)
- Connessione internet

------------------------------------------------------------

⚙️ SETUP (Windows)

Apri PowerShell nella cartella del progetto.

1) Crea ambiente virtuale

    python -m venv .venv
    .\.venv\Scripts\Activate.ps1

Se PowerShell blocca l’attivazione:

    Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

2) Installa dipendenze

    pip install -r requirements.txt

------------------------------------------------------------

🔐 CONFIGURAZIONE

1) Crea il file .env

Copia .env.example e rinominalo in .env.

Inserisci la tua chiave API:

    WU_API_KEY=INCOLLA_LA_TUA_API_KEY

Il file .env è ignorato dal .gitignore e non viene caricato su GitHub.

2) Configura le stazioni

Duplica:

    stations_by_region.sample.json

e rinominalo in:

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

------------------------------------------------------------

▶️ ESECUZIONE

    python .\src\weather_pipeline.py

------------------------------------------------------------

📦 OUTPUT GENERATO

Il sistema genera automaticamente:

- JSON giornalieri (backup)
- Storico mensile per stazione
- PDF dinamici per stazione (con grafico + soglie colore)
- HTML regionali responsive con ricerca live

------------------------------------------------------------

🛡 SICUREZZA

- Nessuna API key hardcoded
- Configurazione tramite variabili d’ambiente
- File sensibili ignorati via .gitignore
- Lock file anti-overlap per cron
- Retry automatici su errori API

------------------------------------------------------------

⚡ ARCHITETTURA TECNICA

- asyncio + aiohttp per chiamate concorrenti controllate
- Backoff esponenziale con jitter
- Hash SHA1 per evitare rigenerazioni inutili
- Scrittura atomica file (anti-corruzione)
- Separazione tra:
  - Fetch API
  - Persistenza dati
  - Generazione PDF
  - Generazione HTML

------------------------------------------------------------

🔄 AUTOMAZIONE

Può essere schedulato con:

- Windows Task Scheduler
- Cron (Linux)
- Hosting provider cron jobs

------------------------------------------------------------

📈 POSSIBILI MIGLIORAMENTI FUTURI

- Logging strutturato (logging module)
- Dockerizzazione
- Test automatici
- CI/CD GitHub Actions
- Dashboard di monitoraggio

------------------------------------------------------------

📄 LICENZA

MIT (consigliata se vuoi renderlo riutilizzabile)

------------------------------------------------------------

👨‍💻 AUTORE

Alfonso Conti  
Digital Automation & Data Workflow Engineer  
Python · API Integration · Data Pipelines · AI Automation  
📍 Como, Italy  
🔗 LinkedIn: https://www.linkedin.com/in/alfonso-conti-8580603b2/