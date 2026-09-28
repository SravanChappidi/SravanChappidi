# DAX measures

Model tables (see Phase 9):

| Power BI table | Snowflake source | Role |
|---|---|---|
| `Weather` | `VW_WEATHER_OBSERVATIONS` | Fact: one row per city per observation |
| `City` | `VW_CITY` | Dimension: slicer, map |
| `Date` | `VW_DATE` | Dimension: marked as date table |
| `Latest` | `VW_LATEST_WEATHER` | Latest row per city (overview page) |
| `Pipeline Health` | `VW_PIPELINE_HEALTH` | One row of load statistics |

Relationships: `City[CITY]` 1→* `Weather[CITY]`, `Date[DATE]` 1→* `Weather[OBSERVATION_DATE]`, `City[CITY]` 1→1 `Latest[CITY]`.

Create a table `_Measures` (Home → Enter data → Load) and add each measure below with **New measure**. The display folders are only a suggestion.

---

## Core

```DAX
Observation Count = COUNTROWS ( Weather )
```
```DAX
Average Temperature = AVERAGE ( Weather[TEMPERATURE] )
```
```DAX
Maximum Temperature = MAX ( Weather[TEMP_MAX] )
```
```DAX
Minimum Temperature = MIN ( Weather[TEMP_MIN] )
```
```DAX
Average Feels Like = AVERAGE ( Weather[FEELS_LIKE] )
```
```DAX
Average Humidity = AVERAGE ( Weather[HUMIDITY] )
```
```DAX
Average Wind Speed = AVERAGE ( Weather[WIND_SPEED] )
```
```DAX
Average Pressure = AVERAGE ( Weather[PRESSURE] )
```
```DAX
Rainy Observations =
CALCULATE ( [Observation Count], Weather[IS_RAINY] = TRUE () )
```
```DAX
Rainy Observation % = DIVIDE ( [Rainy Observations], [Observation Count] )
```
Format: Percentage, 1 decimal.
```DAX
Total Rain (mm) = SUM ( Weather[RAIN_1H] )
```
> `RAIN_1H` is "rain in the last hour" at each observation. Summing several observations within the same hour over-counts, so treat this as an *indicator* (did it rain a lot?), not exact rainfall.

## Latest values (correct in any filter context)

Pattern: for each city in the current filter, find that city's latest observation time and take the value at that time, then average across cities (for one city, that's just its value).

```DAX
Latest Observation Time = MAX ( Weather[OBSERVATION_TS_LOCAL] )
```
```DAX
Latest Temperature =
AVERAGEX (
    VALUES ( City[CITY] ),
    VAR LastTs = CALCULATE ( MAX ( Weather[OBSERVATION_TS_LOCAL] ) )
    RETURN CALCULATE ( MAX ( Weather[TEMPERATURE] ), Weather[OBSERVATION_TS_LOCAL] = LastTs )
)
```
```DAX
Latest Humidity =
AVERAGEX (
    VALUES ( City[CITY] ),
    VAR LastTs = CALCULATE ( MAX ( Weather[OBSERVATION_TS_LOCAL] ) )
    RETURN CALCULATE ( MAX ( Weather[HUMIDITY] ), Weather[OBSERVATION_TS_LOCAL] = LastTs )
)
```
```DAX
Latest Wind Speed =
AVERAGEX (
    VALUES ( City[CITY] ),
    VAR LastTs = CALCULATE ( MAX ( Weather[OBSERVATION_TS_LOCAL] ) )
    RETURN CALCULATE ( MAX ( Weather[WIND_SPEED] ), Weather[OBSERVATION_TS_LOCAL] = LastTs )
)
```
```DAX
Latest Pressure =
AVERAGEX (
    VALUES ( City[CITY] ),
    VAR LastTs = CALCULATE ( MAX ( Weather[OBSERVATION_TS_LOCAL] ) )
    RETURN CALCULATE ( MAX ( Weather[PRESSURE] ), Weather[OBSERVATION_TS_LOCAL] = LastTs )
)
```
```DAX
Latest Condition =
IF (
    HASONEVALUE ( City[CITY] ),
    VAR LastTs = MAX ( Weather[OBSERVATION_TS_LOCAL] )
    RETURN CALCULATE ( MAX ( Weather[WEATHER_DESCRIPTION] ), Weather[OBSERVATION_TS_LOCAL] = LastTs ),
    "Select a city"
)
```

## Trends

```DAX
Avg Temp Previous Day =
CALCULATE ( [Average Temperature], DATEADD ( 'Date'[DATE], -1, DAY ) )
```
```DAX
Temp Change vs Previous Day =
VAR CurrentAvg = [Average Temperature]
VAR PreviousAvg = [Avg Temp Previous Day]
RETURN IF ( NOT ISBLANK ( CurrentAvg ) && NOT ISBLANK ( PreviousAvg ), CurrentAvg - PreviousAvg )
```
Format: `+0.0 °C;-0.0 °C;0.0 °C`.
```DAX
Daily Temperature Range =
AVERAGEX (
    VALUES ( 'Date'[DATE] ),
    CALCULATE ( MAX ( Weather[TEMP_MAX] ) - MIN ( Weather[TEMP_MIN] ) )
)
```
```DAX
Temp 7-Day Moving Avg =
VAR LastDate = MAX ( 'Date'[DATE] )
RETURN
CALCULATE (
    [Average Temperature],
    DATESINPERIOD ( 'Date'[DATE], LastDate, -7, DAY )
)
```

## Freshness / pipeline

```DAX
Minutes Since Last Observation =
VAR LastUtc = CALCULATE ( MAX ( Weather[OBSERVATION_TS_UTC] ), REMOVEFILTERS () )
RETURN DATEDIFF ( LastUtc, UTCNOW (), MINUTE )
```
> In Import mode this is measured against the time the visual is *rendered*, so it grows between refreshes. That's useful: a large number means "refresh me" or "pipeline stopped".
```DAX
Data Status =
SWITCH (
    TRUE (),
    [Minutes Since Last Observation] <= 30, "🟢 Live",
    [Minutes Since Last Observation] <= 120, "🟡 Delayed",
    "🔴 Stale"
)
```
```DAX
Last Refreshed (UTC) = "Data as of " & FORMAT ( CALCULATE ( MAX ( Weather[LOADED_AT] ), REMOVEFILTERS () ), "dd-mmm-yyyy hh:nn" ) & " UTC"
```

## Presentation helpers

```DAX
Selected City Title =
"Weather details – " & SELECTEDVALUE ( City[CITY], "select a city" )
```
```DAX
Latest Temp Color =
VAR t = [Latest Temperature]
RETURN
SWITCH (
    TRUE (),
    ISBLANK ( t ), "#9E9E9E",
    t >= 38, "#C62828",
    t >= 30, "#EF6C00",
    t >= 20, "#F9A825",
    t >= 10, "#2E7D32",
    "#1565C0"
)
```
Use it via *Format → Conditional formatting → Field value* on cards, bars or table cells.

---

### Why these patterns
* **`AVERAGEX(VALUES(City[CITY]), ...)` for "latest"**: a plain `MAX(timestamp)` at the total level only picks cities that reported at the exact global max time. Iterating cities gives each city its own latest reading.
* **`CALCULATE` inside the iterator** triggers *context transition*: the current city becomes a filter.
* **`DATEADD` / `DATESINPERIOD`** need the `Date` table marked as a date table with a contiguous range. `VW_DATE` provides 2025–2028.
* All measures work in **Import and DirectQuery** (no calculated tables or columns required).
