# Phase 2: Weather API ingestion

## 1. Objective
Call OpenWeatherMap for each configured city, handle failures properly, and turn the nested API response into a **flat, clean JSON event**. There's no Kafka yet: we print events to the screen (`--dry-run`).

## 2. Architecture position
```
[Weather API] ──► [Python ingestion]  ─ ─ ► Kafka (Phase 3)
                     ▲ you are here
```

## 3. Prerequisites
* Phase 1 complete, venv active
* Your OpenWeatherMap key is **active**. Test it in a browser:
  `https://api.openweathermap.org/data/2.5/weather?q=Delhi,IN&units=metric&appid=YOUR_KEY`
  You should see JSON. `{"cod":401, ...}` means the key isn't active yet; wait, or use `--mock`.

## 4. Folder / file structure
```
ingestion/
├── __init__.py
├── config.py         settings from .env   (Phase 1)
├── weather_api.py    HTTP client + response -> event mapping   ◄ this phase
├── mock_weather.py   fake API for offline testing              ◄ this phase
└── main.py           polling loop + CLI flags                  ◄ this phase
```

## 5. Commands
```powershell
# One polling cycle, print the events, don't send anywhere
python -m ingestion.main --once --dry-run

# No API key yet? Same thing with generated data
python -m ingestion.main --once --dry-run --mock

# More detail (shows retry attempts)
python -m ingestion.main --once --dry-run --log-level DEBUG
```

## 6. Code (key parts)

**Retries + timeout**: `ingestion/weather_api.py`
```python
retry = Retry(
    total=max_retries,                  # API_MAX_RETRIES (default 3)
    backoff_factor=1,                   # waits 1s, 2s, 4s between attempts
    status_forcelist=(429, 500, 502, 503, 504),
    respect_retry_after_header=True,    # honour the API's rate-limit hint
)
self.session.mount("https://", HTTPAdapter(max_retries=retry))
...
resp = self.session.get(self.base_url, params=params, timeout=self.timeout)   # API_TIMEOUT_SECONDS
```

**Error classification**
```python
if resp.status_code == 401: raise WeatherAPIError("401 Unauthorized - check WEATHER_API_KEY ...")
if resp.status_code == 404: raise WeatherAPIError("404 city not found - check config/cities.json")
```

**Required vs optional fields**: `to_weather_event()`
```python
missing = [name for name, value in (("coord.lat", coord.get("lat")), ("coord.lon", coord.get("lon")),
                                    ("dt", raw.get("dt")), ("main.temp", main.get("temp"))) if value is None]
if missing:
    raise InvalidWeatherResponse(f"{city}: response missing required field(s): {', '.join(missing)}")
...
"rain_1h": (raw.get("rain") or {}).get("1h"),      # optional -> None when absent
```

**Polling loop**: `ingestion/main.py`
```python
while not _stop:
    run_cycle(cycle, settings, client, producer)
    if args.once: break
    while not _stop and time.monotonic() - started < settings.poll_interval_seconds:
        time.sleep(1)      # 1-second steps so Ctrl+C stops quickly
```

## 7. Explanation

| Requirement | How it's handled |
|---|---|
| Request timeout | `timeout=API_TIMEOUT_SECONDS` on every call. Without it, one hung connection freezes the whole loop forever. |
| Retry handling | `urllib3.Retry` with exponential backoff for **transient** errors only (connection errors, 429, 5xx). 401/404 aren't retried because retrying a wrong key never helps. |
| API errors | `WeatherAPIError` is logged and that city is skipped. **The other cities still run.** One bad city never kills the cycle. |
| Invalid response | `InvalidWeatherResponse` if the body isn't a dict or lacks `coord`/`dt`/`main.temp`. Without these the record is useless. |
| Missing fields | Optional fields (`rain`, `wind.gust`, `visibility`...) become `None`. We don't invent values here. Spark decides that "no rain block" means 0 mm. |
| Ingestion timestamp | `ingestion_timestamp` (UTC) is added to every event, separate from the API's own observation time `api_timestamp`. |
| Configurable interval | `POLL_INTERVAL_SECONDS` in `.env` (minimum 60). |
| Logging | One line per city, plus a **CYCLE SUMMARY** line with counts. |

**Why 300 seconds?** OpenWeatherMap updates current weather roughly every 10 minutes. Polling faster just produces duplicates (which we handle, see Phase 7). Polling every 5 minutes means you never miss an update by more than about 5 minutes. 7 cities × 12 polls/hour = 84 calls/hour, far below the free limit of 60 per **minute**.

**Why keep our own city name?** The API's `name` field can differ from what you asked for (a locality name, or a different spelling). Our configured `city` is the business key. The API's name is kept as `api_location_name` for reference.

## 8. Expected output
```
2026-09-28 08:52:18,500 INFO    ingestion - Polling 7 cities every 300s
2026-09-28 08:52:18,700 INFO    ingestion - Fetched Delhi      temp= 31.1C humidity=62% condition=Haze
{
  "schema_version": 1,
  "source": "openweathermap",
  "city": "Delhi",
  "country": "IN",
  ...
  "ingestion_timestamp": "2026-09-28T08:52:18.500113+00:00"
}
...
2026-09-28 08:52:20,100 INFO    ingestion - CYCLE 1 SUMMARY cities=7 fetched=7 api_failures=0 invalid=0 published=7 delivered=0 delivery_failed=0 pending=0 duration=1.6s
```

## 9. Testing steps
1. `pytest -q tests/test_weather_api.py`: mapping, rain, missing required fields, and non-dict responses.
2. **Failure test (bad key):** set `WEATHER_API_KEY=abc` in `.env`, run `--once --dry-run`. You should see 7 `API failure: ... 401 Unauthorized` lines and `api_failures=7`, and the program exits cleanly. Put the real key back.
3. **Failure test (bad city):** add `{"name": "Atlantisxyz", "country": "IN"}` to `cities.json`. That city logs `404 city not found` and the other 7 still work. Remove it.
4. **Network failure test:** disconnect Wi-Fi and run with `--log-level DEBUG`. You'll see the retries, then `request failed after retries: ConnectionError` for each city, and the cycle still completes.

## 10. Common errors and fixes

| Error | Fix |
|---|---|
| `401 Unauthorized` | Key not active yet (wait up to 2h), or copied with a space. Use `--mock` meanwhile. |
| `404 city not found` | Spelling. Try the name in the browser URL above. |
| `429 Too Many Requests` | You're polling too fast or running two copies. Check `POLL_INTERVAL_SECONDS`. |
| `SSLError` on a corporate network | Proxy/SSL inspection. Try from a home network, or set `REQUESTS_CA_BUNDLE` to your company CA file. |

## 11. Verify before moving on
- [ ] `--once --dry-run` prints 7 events with sensible temperatures
- [ ] `api_timestamp` and `ingestion_timestamp` are both present and in UTC (`+00:00`)
- [ ] The bad-key test shows 7 failures without crashing
- [ ] You can explain the difference between `api_timestamp` and `ingestion_timestamp`
