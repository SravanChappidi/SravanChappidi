# Dashboard design (4 pages)

Canvas: 16:9 (1280 × 720). Top band on every page (height ~60 px): title on the left, the `[Data Status]` card plus the `[Last Refreshed (UTC)]` card on the right. Use a consistent colour per city (Format → Data colors) so Delhi is the same colour everywhere.

**Slicers used across pages:** `City[CITY]` (dropdown, multi-select), `Date[DATE]` (between slider), `Weather[WEATHER_CONDITION]` (dropdown). Use **View → Sync slicers** so City and Date stay the same on pages 1–3. Page 4 has its own single-select City slicer.

---

## Page 1: Weather Overview
*Question it answers: what's the weather right now, everywhere?*

```
┌─────────────────────────────────────────────────────────────────────┐
│ Weather Overview                          [🟢 Live] [Data as of …]  │
├──────────┬──────────┬──────────┬──────────┬─────────────────────────┤
│ Latest   │ Latest   │ Latest   │ Latest   │  Slicers: City | Date   │
│ Temp °C  │ Humidity │ Wind m/s │ Obs time │                         │
├──────────┴──────────┴──────────┴──────────┼─────────────────────────┤
│ Clustered bar: Latest Temperature by City │  Map (City lat/long,    │
│ (sorted desc, colour = Latest Temp Color) │  bubble size = Latest   │
│                                           │  Temperature, tooltip = │
├───────────────────────────────────────────┤  condition)             │
│ Table: City | Latest Temp | Feels like |  │                         │
│ Humidity | Wind | Condition | Obs time    │                         │
│ (source: Latest table + City)             │                         │
└───────────────────────────────────────────┴─────────────────────────┘
```

| Visual | Fields |
|---|---|
| 4 cards | `[Latest Temperature]`, `[Latest Humidity]`, `[Latest Wind Speed]`, `[Latest Observation Time]` |
| Clustered bar | Y: `City[CITY]`; X: `[Latest Temperature]`; bar colour: conditional formatting by `[Latest Temp Color]` |
| Map (or Azure Map) | Latitude/Longitude: `City[LATITUDE]`/`City[LONGITUDE]`; Size: `[Latest Temperature]`; Tooltips: `Latest[WEATHER_DESCRIPTION]`, `[Latest Humidity]` |
| Table | `City[CITY]`, `Latest[TEMPERATURE]`, `Latest[FEELS_LIKE]`, `Latest[HUMIDITY]`, `Latest[WIND_SPEED]`, `Latest[WEATHER_CONDITION]`, `Latest[OBSERVATION_TS_LOCAL]`, `Latest[HEAT_INDEX_CATEGORY]` |

> Map visuals need *File → Options → Security → Use Map and Filled Map visuals* enabled. If your tenant disables them, use a scatter chart (lon on X, lat on Y) instead.

## Page 2: Temperature Trends
*Question: how is temperature changing over time and between cities?*

| Visual | Fields |
|---|---|
| Line chart (full width) | X: `Weather[OBSERVATION_TS_LOCAL]` (continuous axis); Y: `[Average Temperature]`; Legend: `City[CITY]`. Analytics pane → **Trend line** on |
| Clustered column | X: `Date[DATE]`; Y: `[Average Temperature]`; Legend: `City[CITY]` (daily average) |
| Line + clustered column (or area) | X: `Date[DATE]`; Y: `[Minimum Temperature]`, `[Maximum Temperature]`; one city at a time via the slicer |
| Matrix | Rows: `City[CITY]`; Columns: `Date[DATE]`; Values: `[Average Temperature]` with a background colour scale (heatmap) |
| KPI cards | `[Maximum Temperature]`, `[Minimum Temperature]`, `[Temp Change vs Previous Day]`, `[Temp 7-Day Moving Avg]` |

## Page 3: Weather Conditions
*Question: what kind of weather are we seeing, and how are humidity, pressure, wind and rain behaving?*

| Visual | Fields |
|---|---|
| Donut | Legend: `Weather[WEATHER_CONDITION]`; Values: `[Observation Count]` |
| 100% stacked bar | Y: `City[CITY]`; X: `[Observation Count]`; Legend: `Weather[WEATHER_CONDITION]` |
| Line chart | X: `Weather[OBSERVATION_TS_LOCAL]`; Y: `[Average Humidity]`; Legend: City |
| Line chart | X: `Weather[OBSERVATION_TS_LOCAL]`; Y: `[Average Pressure]`; Legend: City |
| Line chart | X: `Weather[OBSERVATION_TS_LOCAL]`; Y: `[Average Wind Speed]`; Legend: City |
| Clustered column | X: `City[CITY]`; Y: `[Rainy Observations]`; tooltip `[Rainy Observation %]` |
| Slicer | `Weather[WEATHER_CONDITION]` |

## Page 4: City Detail
*Question: tell me everything about one city.*

| Visual | Fields |
|---|---|
| Slicer (single select, tile style) | `City[CITY]`. **Edit interactions**/sync so it only affects this page |
| Title text box | Dynamic title: a card with `[Selected City Title]` |
| Cards | `[Latest Temperature]`, `[Latest Humidity]`, `[Latest Wind Speed]`, `[Latest Pressure]`, `[Latest Condition]`, `City[STATE]`, `City[REGION]` |
| Line chart | Temperature history: X `OBSERVATION_TS_LOCAL`, Y `[Average Temperature]` and `[Average Feels Like]` |
| Line chart | Humidity history |
| Line chart | Wind speed history |
| Line chart | Pressure history |
| Stacked column | X: `Date[DATE]`, Y: `[Observation Count]`, Legend: `Weather[WEATHER_CONDITION]` (conditions per day) |

**Drill-through (nice touch):** on Page 4, add `City[CITY]` to the *Drill through* well. Users can then right-click any city on pages 1–3 → *Drill through → City Detail*.

---

## Finishing checklist
- [ ] All temperatures formatted `0.0 "°C"`, humidity `0"%"`, wind `0.0 "m/s"`
- [ ] Axis titles removed where the visual title says it all
- [ ] Tooltips show local time, not UTC
- [ ] Every page's visuals respond to the slicers as intended (test with Delhi only)
- [ ] Page navigation buttons (Insert → Buttons → Navigator → Page navigator)
- [ ] Screenshots of each page saved in `docs/images/` for the README
