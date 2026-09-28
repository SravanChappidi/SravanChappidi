# Phase 9: Power BI connection and data model

## 1. Objective
Connect Power BI Desktop to Snowflake, load the reporting views, build a small star schema, choose a storage mode, and add the DAX measures.

## 2. Architecture position
```
Snowflake views (VW_*) ──► Power BI semantic model ──► report (Phase 10)
```

## 3. Prerequisites
* Phase 7–8 done; `WEATHER_FACT` has at least a few hours of data (let the producer run)
* Power BI Desktop installed (Windows)
* In Snowflake, let your own login use the read-only BI role:
  ```sql
  USE ROLE ACCOUNTADMIN;
  GRANT ROLE WEATHER_BI_ROLE TO USER <YOUR_LOGIN_NAME>;
  ```

## 4. Folder / file structure
```
powerbi/
├── dax_measures.md        every measure, copy-paste ready     ◄ this phase
├── dashboard_design.md    page-by-page layout (Phase 10)
└── weather_dashboard.pbix (you save this yourself)
```

## 5. Steps
1. **Home → Get data → More… → Snowflake → Connect.**
2. **Server:** `abcdefg-xy12345.snowflakecomputing.com` (your account + `.snowflakecomputing.com`)
   **Warehouse:** `WEATHER_WH`
   **Advanced options → Role:** `WEATHER_BI_ROLE`
   **Data connectivity mode:** **Import** (see the trade-off below)
3. **Authentication:** *Snowflake* tab → your username/password. If your login requires MFA/SSO, use the *Microsoft Account* tab if your org has Entra ID SSO set up, or use a PAT as the password.
4. **Navigator:** WEATHER_DB → WEATHER → tick:
   `VW_WEATHER_OBSERVATIONS`, `VW_CITY`, `VW_DATE`, `VW_LATEST_WEATHER`, `VW_PIPELINE_HEALTH` → **Load**.
5. **Rename tables** (double-click in the Data pane):
   `VW_WEATHER_OBSERVATIONS` → **Weather**, `VW_CITY` → **City**, `VW_DATE` → **Date**, `VW_LATEST_WEATHER` → **Latest**, `VW_PIPELINE_HEALTH` → **Pipeline Health**.
6. **Model view → relationships** (delete any auto-detected ones first):
   * `City[CITY]` 1 → * `Weather[CITY]` (single direction)
   * `Date[DATE]` 1 → * `Weather[OBSERVATION_DATE]` (single direction)
   * `City[CITY]` 1 → 1 `Latest[CITY]` (for the overview page; set cross-filter to *single*, City filters Latest)
7. **Mark `Date` as date table:** select the Date table → Table tools → *Mark as date table* → column `DATE`.
8. **Sort columns:** `Date[MONTH_NAME]` sort by `MONTH_NUMBER`, `Date[DAY_NAME]` sort by `DAY_OF_WEEK_NUMBER`.
9. **Hide** technical columns from report view: `EVENT_ID`, `OBSERVATION_TS_UTC`, `LOADED_AT`, and the foreign-key `Weather[CITY]` / `Weather[OBSERVATION_DATE]` (users slice with `City[CITY]` and `Date[DATE]`).
10. **Create a measures table:** Home → Enter data → name it `_Measures` → Load. Add all measures from `powerbi/dax_measures.md` (Home → New measure).
11. **Save** as `powerbi/weather_dashboard.pbix`.

## 6. Code
All measures are in [`powerbi/dax_measures.md`](../powerbi/dax_measures.md). The key pattern, "latest value per city", works in any filter context:
```DAX
Latest Temperature =
AVERAGEX (
    VALUES ( City[CITY] ),
    VAR LastTs = CALCULATE ( MAX ( Weather[OBSERVATION_TS_LOCAL] ) )
    RETURN CALCULATE ( MAX ( Weather[TEMPERATURE] ), Weather[OBSERVATION_TS_LOCAL] = LastTs )
)
```

## 7. Explanation: Import vs DirectQuery

| | **Import** (recommended for this POC) | **DirectQuery** |
|---|---|---|
| Where data lives | Copied into the .pbix (compressed, in memory) | Stays in Snowflake; every visual sends SQL |
| Speed | Very fast visuals | Each click = query + possibly a warehouse resume (1–3 s) |
| Freshness | As of the last refresh | Near real time (on interaction or automatic page refresh) |
| Snowflake cost | Warehouse runs only during refresh | Warehouse wakes for **every** interaction/page refresh, so it can burn credits |
| DAX | Everything supported | Some functions/time intelligence are limited or slower |
| Refresh | Desktop: manual button. Service: scheduled; **8/day on Pro**, 48/day on Premium Per User | Automatic page refresh: any interval in Desktop; **≥ 30 min in the Service on shared capacity**, lower only on Premium/Fabric capacity |

**Recommendation:** use **Import**. The data is small (7 cities × about 144 unique observations/day ≈ 1,000 rows/day), visuals are instant, and cost is predictable. For the live demo, click **Refresh** in Desktop. It takes a few seconds and shows the newest observations.

**Optional "live" demo mode:** switch `Latest` to DirectQuery (a composite model: history in Import, latest in DirectQuery) and turn on **Automatic page refresh** on the overview page. That gives near-real-time cards without making the whole model DirectQuery. Mention the cost trade-off if you present this.

Why a star schema: `Weather` is the fact; `City` and `Date` are dimensions. Slicers built on dimensions filter all facts consistently, and DAX time intelligence needs a proper date table.

## 8. Expected output
Model view: `City` and `Date` at the top, `Weather` below them with two many-to-one relationships, and `Latest` joined 1:1 to City. The Data pane shows measures under `_Measures`.

A test card with `[Observation Count]` shows the same number as:
```sql
SELECT COUNT(*) FROM WEATHER_DB.WEATHER.WEATHER_FACT;
```

## 9. Testing steps
1. Card `[Observation Count]` = Snowflake fact count.
2. Table visual: `City[CITY]`, `[Latest Temperature]`, `[Latest Observation Time]` matches `SELECT * FROM VW_LATEST_WEATHER`.
3. Add a `City` slicer, pick Delhi. All measures change to Delhi-only.
4. Wait for a new producer cycle, click **Refresh**. The count and latest time increase.

## 10. Common errors and fixes

| Problem | Fix |
|---|---|
| `We couldn't authenticate with the credentials provided` | Wrong user/password; or MFA required (use SSO/PAT). Clear old creds: File → Options → Data source settings → Clear permissions. |
| `The ODBC driver ... not found` / connector error | Update Power BI Desktop (it bundles the Snowflake driver). |
| Navigator shows no views | The role lacks grants: run the `GRANT SELECT ON ALL VIEWS ... TO ROLE WEATHER_BI_ROLE` line in `03_views.sql`, and check the Role in advanced options. |
| Measures show the same value for every city | Relationship missing or inactive: check Model view. The slicer must use `City[CITY]`, not `Weather[CITY]`. |
| Time-intelligence measure blank | `Date` not marked as a date table, or the relationship is on the wrong column. |
| Times look 5h30 off | You used `OBSERVATION_TS_UTC`. Use `OBSERVATION_TS_LOCAL` for display. |

## 11. Verify before moving on
- [ ] Model has the relationships above; Date is marked as a date table
- [ ] All measures from `dax_measures.md` are created without errors
- [ ] Counts reconcile with Snowflake
- [ ] You can explain why Import was chosen and the trade-off vs DirectQuery
