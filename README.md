# 🌧 Weather Automation Pipeline  
Python | Async API Integration | PDF Generation | HTML Automation

Pipeline automatizzata per raccolta dati meteorologici tramite Weather.com / Wunderground (PWS API) con generazione automatica di:

- Backup JSON giornaliero
- Storico mensile aggregato
- Report PDF dinamici per singola stazione
- Pagine HTML regionali responsive (mobile-first)
- Automazione asincrona con asyncio + aiohttp

Progetto dimostrativo di automazione data-driven e workflow scalabile.

---

## 🚀 Funzionalità principali

- Integrazione API REST (Weather.com PWS)
- Fetch asincrono con asyncio
- Controllo concorrenza tramite Semaphore
- Validazione dati e gestione anomalie
- Persistenza JSON strutturata
- Generazione PDF dinamici con ReportLab (grafici + soglie colore)
- Generazione HTML responsive con ricerca dinamica
- Struttura GitHub-friendly (no chiavi hardcoded, no percorsi server)

---

## 📦 Struttura del progetto

project-root/
│
├── weather_pipeline.py
├── stations_by_region.sample.json
├── .env.example
├── .gitignore
└── README.md

Output generato automaticamente nella cartella:

outputs/

---

## ⚙️ Installazione

1. Clona il repository:

git clone https://github.com/tuo-username/weather-automation-pipeline.git  
cd weather-automation-pipeline  

2. Crea ambiente virtuale:

python -m venv .venv  

Attivazione:

Windows:
.venv\Scripts\activate  

Mac / Linux:
source .venv/bin/activate  

3. Installa dipendenze:

pip install aiohttp reportlab  

---

## 🔐 Configurazione API

Copia `.env.example` in `.env` e inserisci la tua chiave:

WU_API_KEY=LA_TUA_API_KEY  

Oppure imposta variabile ambiente manualmente.

---

## 🗂 Configurazione stazioni

Copia:

stations_by_region.sample.json  

Rinominalo in:

stations_by_region.json  

Struttura richiesta:

{
  "NomeRegione": {
    "STATION_ID": "Nome Città"
  }
}

---

## ▶️ Esecuzione

python weather_pipeline.py

I file generati verranno creati automaticamente in:

outputs/

---

## 🧠 Architettura tecnica

- Async I/O per gestione concorrente di molte stazioni
- Persistenza incrementale giornaliera e mensile
- Generazione PDF dinamici con grafico integrato
- Generazione HTML server-side responsive
- Configurazione tramite variabili ambiente
- Separazione completa tra codice e configurazione

---

## 📌 Note

- Nessuna API key è presente nel codice
- Nessun percorso server hardcoded
- Dataset completo non incluso nel repository (versione demo fornita)
- Struttura compatibile con deploy su WordPress o static hosting

---

## 👨‍💻 Autore

Alfonso Conti  
Digital Automation & Data Workflow  
Python | API Integration | AI Automation