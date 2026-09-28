# Phase 1: Project setup

## 1. Objective
Get a working development environment on Windows: tools installed, repository cloned, Python virtual environment created, dependencies installed, and a `.env` file holding every setting and secret.

## 2. Architecture position
Foundation for every later phase. Nothing flows yet.

## 3. Prerequisites

| Tool | Version | Install | Check |
|---|---|---|---|
| **Python** | 3.11 (3.10–3.12 work) | https://www.python.org/downloads/windows/ (tick **"Add python.exe to PATH"**) | `py -3.11 --version` |
| **Docker Desktop** | Latest, WSL 2 backend | https://www.docker.com/products/docker-desktop/ (restart after installing) | `docker --version` and `docker compose version` |
| **Git** | Any | https://git-scm.com/download/win | `git --version` |
| **VS Code** (recommended) | Any | https://code.visualstudio.com/ + the Python extension | |
| **OpenWeatherMap account** | Free | https://home.openweathermap.org/users/sign_up, then **API keys** tab | New keys can take **up to 2 hours** to activate |
| **Snowflake trial** | 30 days / $400 credit | https://signup.snowflake.com/ (Standard edition, any cloud/region near you) | Needed from Phase 7 |
| **Power BI Desktop** | Latest | Microsoft Store | Needed from Phase 9 |

> **You do NOT need Java, Spark or Kafka installed on Windows.** They run in Docker. Java 17 is only needed if you want to run the optional Spark unit tests locally.

Docker Desktop settings: **Settings → Resources**, give it at least **4 GB RAM** (Spark + Kafka together use about 2–3 GB).

## 4. Folder / file structure

```
weather-etl-pipeline/
├── .env.example            template for .env (committed)
├── .env                    YOUR secrets (git-ignored, never committed)
├── .gitignore
├── requirements.txt        Python deps for ingestion + tests
├── requirements-dev.txt    + pyspark, for optional local Spark tests
├── config/
│   └── cities.json         list of cities to poll
├── docker/
│   └── docker-compose.yml  Kafka, Kafka UI, Spark
├── ingestion/              Phase 2-3: API -> Kafka
├── spark/                  Phase 5-7: Kafka -> Snowflake
├── snowflake/              Phase 7-8: SQL scripts
├── powerbi/                Phase 9-10: DAX + dashboard design
├── scripts/                helper scripts for testing
├── tests/                  unit tests
└── docs/                   this guide
```

## 5. Commands (PowerShell)

```powershell
# 1. Get the code
cd $HOME\Documents
git clone https://github.com/SravanChappidi/SravanChappidi.git
cd SravanChappidi\weather-etl-pipeline

# 2. Create and activate a virtual environment
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
#   If you get "running scripts is disabled on this system", run this once, then retry:
#   Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned

# 3. Install dependencies (prompt now starts with (.venv))
python -m pip install --upgrade pip
pip install -r requirements.txt

# 4. Create your .env from the template and edit it
copy .env.example .env
notepad .env
```

In `.env`, set at least `WEATHER_API_KEY` for now. Leave the Snowflake values until Phase 7.

## 6. Code

`.env.example` holds every setting the project uses: API, Kafka, Spark, Snowflake.

`ingestion/config.py` is the only place Python reads settings:

```python
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")          # loads .env into os.environ

def _required(name):
    value = os.getenv(name, "").strip()
    if not value or value.startswith("replace"):
        raise ConfigError(f"Environment variable {name} is not set. Add it to your .env file.")
    return value
```

`config/cities.json` holds the city list. To add a city, add a line; no code changes needed:

```json
{"cities": [{"name": "Delhi", "country": "IN"}, {"name": "Mumbai", "country": "IN"}]}
```

## 7. Explanation
* **Why `.env`?** Secrets never go into code or git. `python-dotenv` loads the file for Python. Docker Compose passes the same file to the Spark container (`env_file: ../.env`), so there is one source of truth.
* **Why a venv?** It isolates this project's packages from other Python projects on your laptop.
* **Why is the city list a JSON file and not code?** It's configuration. Changing which cities you poll shouldn't require editing code.
* `config.py` validates early: a missing API key fails at startup with a clear message, not 7 cities later with a 401.

## 8. Expected output

```
(.venv) PS C:\...\weather-etl-pipeline> pip list | findstr /i "kafka requests dotenv"
confluent-kafka    2.6.1
python-dotenv      1.0.1
requests           2.32.3
```

## 9. Testing steps

```powershell
python -c "from ingestion.config import load_settings; s = load_settings(); print(len(s.cities), 'cities,', s.kafka_topic, s.poll_interval_seconds)"
# -> 7 cities, weather-data 300

pytest -q tests/test_weather_api.py
# -> 8 passed
```

## 10. Common errors and fixes

| Error | Fix |
|---|---|
| `py : The term 'py' is not recognized` | Reinstall Python and tick "Add to PATH", or use `python` instead of `py -3.11`. |
| `Activate.ps1 cannot be loaded because running scripts is disabled` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |
| `pip install confluent-kafka` tries to compile / needs librdkafka | You're on a Python version without a prebuilt wheel. Use Python 3.11 or 3.12. |
| `ConfigError: Environment variable WEATHER_API_KEY is not set` | `.env` is missing, still named `.env.example`, or still has the placeholder value. It must sit in `weather-etl-pipeline\`, next to `requirements.txt`. |
| `ModuleNotFoundError: No module named 'ingestion'` | Run commands from the `weather-etl-pipeline` folder, not from inside `ingestion\`. |
| Docker Desktop: "WSL 2 installation is incomplete" | Run `wsl --install` in an admin PowerShell, reboot. |

## 11. Verify before moving on
- [ ] `(.venv)` shows in your prompt and `pip list` shows the three packages
- [ ] `.env` exists, has your API key, and `git status` does **not** list it
- [ ] `docker compose version` works
- [ ] Settings load and ingestion tests pass (step 9)
