"""Offline stand-in for WeatherClient.

Useful while your new API key is still activating (can take ~2 hours), or for
testing Kafka/Spark without spending API calls. Returns payloads shaped exactly
like OpenWeatherMap's /data/2.5/weather response.
"""
import random
import time
import zlib

_COORDS = {
    "Delhi": (28.6667, 77.2167), "Mumbai": (19.0144, 72.8479), "Hyderabad": (17.3753, 78.4744),
    "Bengaluru": (12.9762, 77.6033), "Chennai": (13.0878, 80.2785), "Kolkata": (22.5697, 88.3697),
    "Pune": (18.5196, 73.8553),
}
_CONDITIONS = [("Clear", "clear sky"), ("Clouds", "scattered clouds"), ("Clouds", "overcast clouds"),
               ("Haze", "haze"), ("Rain", "light rain"), ("Rain", "moderate rain"), ("Thunderstorm", "thunderstorm")]


class MockWeatherClient:
    def fetch_current(self, city, country=""):
        lat, lon = _COORDS.get(city, (20.0, 78.0))
        condition, description = random.choice(_CONDITIONS)
        temp = round(random.uniform(22, 40), 2)
        now = int(time.time())
        payload = {
            "coord": {"lon": lon, "lat": lat},
            "weather": [{"id": 800, "main": condition, "description": description, "icon": "01d"}],
            "main": {"temp": temp, "feels_like": round(temp + random.uniform(-1, 5), 2),
                     "temp_min": round(temp - 1.5, 2), "temp_max": round(temp + 1.5, 2),
                     "pressure": random.randint(998, 1015), "humidity": random.randint(30, 95)},
            "visibility": random.choice([3000, 5000, 8000, 10000]),
            "wind": {"speed": round(random.uniform(0.5, 9), 2), "deg": random.randint(0, 359)},
            "clouds": {"all": random.randint(0, 100)},
            "dt": now - now % 600,  # like the real API: observation time changes ~every 10 min
            "sys": {"country": country or "IN", "sunrise": now - 20000, "sunset": now + 20000},
            "timezone": 19800,
            "id": zlib.crc32(city.encode()) % 1_000_000,
            "name": city,
            "cod": 200,
        }
        if condition in ("Rain", "Thunderstorm"):
            payload["rain"] = {"1h": round(random.uniform(0.1, 8), 2)}
        return payload
