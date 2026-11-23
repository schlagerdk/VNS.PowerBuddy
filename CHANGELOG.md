# Changelog

All notable changes to VNS PowerBuddy. Versions are tagged `vX.Y.Z` and published as GitHub releases.

## 1.0.28 (unreleased)

### Docs
- Added this changelog, with the history of every release so far.

## 1.0.27 (2026-11-19)

### Changed
- The price bars show the full price per kWh: spot + supplier markup + network tariff + Energinet + electricity
  tax + VAT, exactly the price the planner optimises. The network tariff was missing before, so the expensive
  17-21 peak window could not be seen and correct decisions could look like holds in expensive hours.
- One shared price model (`tariff_service.total_tariff_ore_24h`) is used by the planner, the chart, `/prices`,
  `/tariff` and the chart data.
- The electricity tax default is now 0.8 øre/kWh (EU minimum for 2026-2027). Check
  `POWERBUDDY_TARIFF_ELAFGIFT_ORE` if it is set in `.env`.

### Removed
- `POWERBUDDY_PRICE_TRANSPORT_FIXED_ORE` is no longer used.

## 1.0.26 (2026-11-19)

### Changed
- `hold` is only planned when it saves at least 10 øre per held hour (was 1 øre): a hold is a bet on the
  forecast, and a gain of a few øre is not worth the risk if consumption turns out lower. Marginal holds in
  expensive hours disappear; holds in cheap hours with a real gain stay.
- The threshold is set with `POWERBUDDY_HOLD_MIN_GAIN_ORE`.

## 1.0.25 (2026-11-19)

### Changed
- New cost-optimal planner: dynamic programming over battery SOC minimises the real bill (spot, tariff, VAT,
  feed-in and battery wear) with the same model as the simulation in the chart.
- The battery runs `auto` when there is enough energy; `hold` is only planned when it saves money, so only in
  the day's cheap hours.
- The default requirement of at least 45 % battery at 17-21 is removed (it forced holds in expensive
  afternoon hours).
- The winter rule (the 4 cheapest hours plus 2 extra night hours) still applies, but an intraday replan no
  longer forces charging in the evening.
- Energy left at midnight is valued at the next day's cheapest price when it is known.
- Claude proposals are only used when they save at least 25 øre per day (`POWERBUDDY_CLAUDE_MIN_SAVING_ORE`);
  changes that save nothing are rejected.

## 1.0.24 (2026-11-10)

### Changed
- New versions are found much faster: the server checks GitHub every 2 minutes, and at most every 10 seconds
  when the menu is opened. An open dashboard checks every 3 minutes and when its tab becomes active again.
- The check uses conditional requests that do not count against GitHub's rate limit; if the limit is hit
  anyway, PowerBuddy waits until it resets.

## 1.0.23 (2026-11-10)

### Changed
- The self-update flow uses a PowerBuddy dialog instead of the browser's confirm/alert: it shows the version
  change, a link to the release notes and clear actions; failures and timeouts use the same dialog.

## 1.0.22 (2026-11-10)

### Changed
- With a Claude API key, Claude gets the same basis as the local planner: hourly network tariff, expected
  consumption and solar production, starting SOC, battery capacity, limits, efficiencies and wear cost, VAT,
  feed-in price, reserve window, the local plan with projected SOC and grid use, and up to 7 days of KPIs.
- Hours with manual or locked actions are only marked as locked. Telemetry, location, inverter addresses and
  keys are never sent.
- `POWERBUDDY_CLAUDE_SHARE_HOUSEHOLD_CONTEXT=false` sends only public spot prices and general rules.

## 1.0.21 (2026-11-09)

### Fixed
- The settings dialog scrolls on small screens while its header and close button stay visible, and the
  dashboard keeps its scroll position when the dialog closes. The scrollbar matches the dark theme.

## 1.0.20 (2026-11-09)

### Changed
- Claude proposals and the local plan are compared after the same processing and with the same snapshot of
  consumption, solar, starting SOC and tariffs. Safe parts of a proposal can be used when the whole proposal
  fails the local check; manual, locked and winter charge periods are protected.
- The simulation handles the rest of the day, partial periods, solar surplus and battery limits more precisely.
- The log and settings dialogs close only with their X button.

## 1.0.19 (2026-11-09)

### Fixed
- The update check caches GitHub's answer for 10 minutes and checks again when the menu opens.
- Status messages in Settings disappear after about 3.5 seconds; messages about running actions stay.

## 1.0.18 (2026-11-09)

### Fixed
- The Settings status message has its own box and is hidden when empty; shorter placeholder in the Claude key
  field once a key is saved.

## 1.0.17 (2026-11-09)

### Added
- Self-update: the menu shows the running version and, when a newer GitHub release exists, an update button
  that installs it and restarts the service.
- Optional Claude planning: a Claude API key in Settings or `.env` (`POWERBUDDY_CLAUDE_API_KEY`). Proposals are
  validated locally and fall back to the built-in planner. The fetch/plan buttons report on how many days the
  AI proposal was used.

## 1.0.16 (2026-10-24)

### Added
- Multi-hour edits: choose an end time in the planning dialog to apply one action to several whole hours, with
  a spinner while saving.

### Changed
- Full-battery protection: the active charge period (hour or half hour, manual or planned) becomes hold when
  the battery is full; later charge points are evaluated when they become active.
- Stripes mark every non-auto action, planned or manual.

### Fixed
- Internal Server Error on the dashboard on DST fall-back days (duplicate 02:00 hour in the price data).

## 1.0.15 (2026-10-24)

### Changed
- Choosing an action for a whole hour saves immediately; split half-hour choices still use the save button.
- Every planned action other than auto is shown with stripes.
- Full-battery charge conversion changes only the active period to hold.

### Fixed
- Internal Server Error on the dashboard on DST fall-back days.

## 1.0.14 (2026-10-20)

### Added
- Half-hour planning: each hour can hold one action or two different ones (first/second half).
- Dashboard languages: Danish, English and German (`POWERBUDDY_UI_LANGUAGE`, labels in `locales/*.json`).

### Changed
- Winter rule: the four cheapest charge slots plus two extra cheap slots in the night window are guaranteed.

## 1.0.13 (2026-09-21)

### Changed
- The Easee EV charger integration is replaced by Fronius Wattpilot Flex Pro status
  (`POWERBUDDY_WATTPILOT_*`).

## 1.0.12 (2026-09-01)

### Added
- Provisional (dummy) prices for tomorrow until the day-ahead prices are published; stale dummy prices are
  cleaned up when the real ones arrive.

## 1.0.11 (2026-08-31)

### Changed
- Reworked dashboard, price fetching and scheduler timing; log file path handling.

## 1.0.10 (2026-08-02)

### Changed
- Planner and planning sanity updates, including a fallback for manual planning while tomorrow's prices are
  still unpublished; dashboard update.

## 1.0.9 (2026-06-16)

### Changed
- The battery ETA only shows +1/+2 beyond 24 hours.
- Chart edge markers are only shown on desktop with more than 17 bars.

## 1.0.8 (2026-06-15)

### Changed
- BYD HVM power table adjusted (13.8 kWh: 6.80 kW, 16.6 kWh: 8.20 kW) and the detected battery capacity is used
  as is; dashboard and planner updates.

## 1.0.7 (2026-05-28)

### Added
- Overnight auto window: tuning for keeping `auto` overnight and choosing the cheapest night hours
  (`POWERBUDDY_OVERNIGHT_*`).

## 1.0.6 (2026-05-26)

### Changed
- Planner converts hold to auto when cheaper slots come before expensive ones; tunable with
  `POWERBUDDY_PRECHEAP_AUTO_CONVERT_MIN_DELTA_ORE`.

## 1.0.5 (2026-05-20)

### Changed
- The native dashboard serves its packaged CSS, JS and icons under `/powerbuddy/static`; price badges for
  cheapest/most expensive hours.
- The chart bars show only the current hour and forward.

## 1.0.3 (2026-04-20)

### Added
- Battery capacity is discovered at startup and mapped to BYD HVM power limits (`auto`).

### Changed
- SOC bounds are fixed in code; legacy env keys still work.
- Faster realtime data through inverter response caching.

## 1.0.2 (2026-04-18)

### Changed
- `/prices` and `/planning` follow all released future prices; future days are planned day by day from the
  previous day's projected SOC.

## 1.0.1 (2026-04-17)

### Changed
- A daily quality check after day-ahead publication refreshes tomorrow's plan automatically.
- Stronger planning sanity checks, multi-variant plan evaluation, and one price basis for the dashboard and the
  planner.

## 1.0.0 (2026-04-16)

- First stable release.
