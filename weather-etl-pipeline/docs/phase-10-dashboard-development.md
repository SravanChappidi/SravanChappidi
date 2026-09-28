# Phase 10: Dashboard development

## 1. Objective
Build the 4-page report (Overview, Temperature Trends, Weather Conditions, City Detail) with slicers, conditional formatting and drill-through.

## 2. Architecture position
```
Snowflake views ──► Power BI model (Phase 9) ──► [REPORT PAGES]
                                                    ▲ you are here
```

## 3. Prerequisites
Phase 9 model with all measures. Ideally **at least 1–2 days of data** so the trend and daily charts have something to show. Start the producer early and leave it running (with the cost notes in mind).

## 4. Folder / file structure
```
powerbi/dashboard_design.md   exact layout, visual types and fields per page   ◄ follow this
powerbi/weather_dashboard.pbix
docs/images/                  screenshots for the README
```

## 5. Steps
Follow [`powerbi/dashboard_design.md`](../powerbi/dashboard_design.md) page by page. Suggested order:
1. **Theme first:** View → Themes → pick one (e.g. *Executive*), so you don't restyle later.
2. **Build Page 1** fully, including the header band (title + `[Data Status]` + `[Last Refreshed (UTC)]`).
3. **Duplicate Page 1** (right-click tab → Duplicate) to reuse the header, then build Pages 2–4 from the copy.
4. **Sync slicers:** View → Sync slicers → City and Date synced on pages 1–3.
5. **Drill-through:** Page 4 → Drill through well ← `City[CITY]`.
6. **Navigation:** Insert → Buttons → Navigator → Page navigator on each page.
7. **Formats:** set formats on the *measures* (Measure tools → Format), not per visual, so they're consistent everywhere.

## 6. Code
Conditional colour for temperature bars and cards uses the `[Latest Temp Color]` measure:
*Format visual → Bars → Color → fx → Format style: Field value → What field: `[Latest Temp Color]`.*

Dynamic page title on City Detail: a card visual showing `[Selected City Title]` with the category label turned off.

## 7. Explanation
* **Overview uses `Latest` + measures.** The `Latest` table (from `VW_LATEST_WEATHER`) is precomputed in Snowflake. It's cheap and easy to show in a table. The DAX "latest" measures give the same answer and respect slicers.
* **Trends use `OBSERVATION_TS_LOCAL`** on a continuous axis for intraday detail, and `Date[DATE]` for daily aggregates (the daily axis must come from the date dimension so time intelligence works).
* **Slicers on dimensions** (`City`, `Date`) filter every fact visual consistently. Slicing on `Weather[CITY]` instead would not filter `Latest`.
* **Conditions page** combines categorical (donut, 100% bars) and time-series visuals. That's the classic "what" and "when".
* **City detail** is a focused drill-down: one city, all metrics, full history.

## 8. Expected output
Four pages. On Page 1, 7 bars sorted by temperature with warm cities in red/orange; the map shows bubbles across India; the table shows one row per city with the latest observation time (local).

## 9. Testing steps
1. Pick **Delhi** in the City slicer. Every visual on pages 1–3 shows only Delhi.
2. Pick a single date. The trend charts collapse to that day, and `[Temp Change vs Previous Day]` shows a number (if the previous day has data).
3. Right-click Mumbai on the Page 1 bar → Drill through → City Detail. The page opens filtered to Mumbai.
4. Hover the map bubbles. The tooltips show the condition and humidity.
5. Cross-check one number with SQL, e.g. Delhi's average temperature for a day vs:
   ```sql
   SELECT AVG_TEMPERATURE FROM VW_DAILY_CITY_WEATHER WHERE CITY='Delhi' AND OBSERVATION_DATE='2026-09-28';
   ```

## 10. Common errors and fixes

| Problem | Fix |
|---|---|
| Line chart shows one point per day | The X axis is `Date[DATE]` or set to *Categorical*. Use `OBSERVATION_TS_LOCAL`, axis type *Continuous*. |
| Line chart looks like zig-zag spaghetti | 7 cities on one chart. Use the City slicer, or small multiples (Format → Small multiples: City). |
| Map empty | Map visuals disabled in Options/tenant, or lat/long not set to *Latitude*/*Longitude* data categories (Column tools → Data category). |
| Donut shows "Count of …" instead of your measure | Drag the `[Observation Count]` measure, not the column. |
| Drill-through doesn't appear on right-click | The field in the drill-through well must be the *same* column the source visual uses (`City[CITY]`). |

## 11. Verify before moving on
- [ ] All 4 pages are built and slicers behave as expected
- [ ] Numbers reconcile with Snowflake views
- [ ] Screenshots are saved to `docs/images/`
