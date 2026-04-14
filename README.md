# VNS.PowerBuddy

<p align="left">
  <img src="assets/powerbuddy-icon.svg" alt="PowerBuddy icon" width="56" />
</p>

VNS.PowerBuddy er en Linux-klar backend service til smart energistyring:

- Henter spotpriser loebende.
- Henter realtime data fra inverter (default: Fronius Gen24 API).
- Laver en dagsplan for batteri (`charge` / `hold` / `discharge`).
- Tager hoejde for forventet doegnforbrug i planlaegning/simulation.
- Gemmer priser, snapshots, plan og simulation i SQLite.
- Understotter manuelle overrides af planlagt adfaerd.
- Fast dagsplan: ingen automatisk intraday replan medmindre du manuelt aendrer plan/action.

## Arkitektur

- `src/powerbuddy/services/pricing.py`: Pris-provider interface + EnergiDataService implementation.
- `src/powerbuddy/services/inverter.py`: Inverter interface + Fronius implementation.
- `src/powerbuddy/services/planner.py`: Planlaegnings- og simulationsmotor.
- `src/powerbuddy/repositories.py`: Data access lag.
- `src/powerbuddy/main.py`: FastAPI endpoints.
- `src/powerbuddy/services/scheduler.py`: Automatisk daglig drift.

## Hurtig start (Linux)

1. Opret virtuelt miljoe og installer:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

2. Opret miljøfil:

```bash
cp .env.example .env
```

Din aktive config-fil er `/.env` i projektroden.

3. Start API:

```bash
uvicorn powerbuddy.main:app --host 0.0.0.0 --port 8000
```

4. Test:

```bash
curl http://localhost:8000/health
```

5. Aabn Swagger dokumentation:

```bash
open http://localhost:8000/swagger
```

## Build og release

Lokal build med Makefile:

```bash
make install
make run
```

Versionering:
- `VERSION` er release-version.
- `pyproject.toml`, `src/powerbuddy/__init__.py` og FastAPI app-version holdes synkroniseret.

CI:
- GitHub Actions workflow findes i `.github/workflows/ci.yml`.

## API oversigt

Swagger/OpenAPI:
- `GET /swagger`
- `GET /openapi.json`
- `GET /redoc`

Udvalgte endpoints:
- `GET /health`
- `GET /config`
- `POST /prices/fetch?target_date=YYYY-MM-DD`
- `GET /prices?target_date=YYYY-MM-DD`
- `GET /prices` (default: fra nuvaerende time og 24 timer frem)
- `GET /tariff`
- `PUT /tariff/config`
- `PUT /tariff/manual-hours`
- `DELETE /tariff/manual-hours`
- `GET /inverter/realtime`
- `POST /planning/generate?target_date=YYYY-MM-DD`
- `GET /planning?target_date=YYYY-MM-DD`
- `PUT /planning`
- `PUT /planning/action/{action_id}`
- `DELETE /planning/action/{action_id}`
- `POST /planning/override`
- `POST /planning/simulate?target_date=YYYY-MM-DD`
- `GET /planning/now`
- `GET /planning/chart-data?target_date=YYYY-MM-DD`

## Runtime config

- `GET /config` viser de effektive runtime settings, som indlaeses fra `.env` ved opstart.
- Runtime config opdateres ikke via database eller API.
- Aendringer i `.env` kraever restart af servicen for at traede i kraft.
- `.env` er lokal og skal ikke commit'es.

Pris-rytmer i scheduler:
- Spotpris timecadence: aktiv pris gaelder i 1 time, service refresher loebende (`POWERBUDDY_PRICE_RECHECK_INTERVAL_MINUTES`, default 60 i .env.example).
- Day-ahead publicering: kommende doegns timepriser forventes omkring kl. 13 lokal tid (`POWERBUDDY_DAY_AHEAD_PUBLISH_HOUR_LOCAL`, default 13).
- Planhorisont: service planlaegger mindst 48 timer frem (`POWERBUDDY_PLANNING_HORIZON_HOURS`, default 48) når en dag mangler plan.

Planstrategi:
- Der genereres kun automatisk plan for en dag, hvis planen mangler.
- Eksisterende dagsplan overskrives ikke automatisk.
- Afvigelser sker kun ved manuelle API-aendringer (`/planning/action/*`, `/planning`, `/planning/override`).

## Forbrug: init + dynamisk model

- Init-vaerdi: `POWERBUDDY_EXPECTED_DAILY_CONSUMPTION_KWH` i `.env` (fx 60).
- Dynamisk model: hvis `POWERBUDDY_DYNAMIC_CONSUMPTION_ENABLED=true`, bruges et rullende gennemsnit af historiske dagsforbrug fra `power_snapshots`.
- Lookback-vindue: `POWERBUDDY_DYNAMIC_CONSUMPTION_LOOKBACK_DAYS`.
- Datakrav pr. dag: `POWERBUDDY_DYNAMIC_CONSUMPTION_MIN_SAMPLES_PER_DAY`.

## Eksempel: manuel override

```json
{
  "date": "2026-04-14",
  "start_time": "2026-04-14T16:00:00+02:00",
  "end_time": "2026-04-14T17:00:00+02:00",
  "action": "hold",
  "target_soc": 65,
  "reason": "bevar SOC i spidslast"
}
```

## Næste roadmap

- Bedre forecast for husforbrug og PV produktion.
- Tariffer, afgifter og nettarif-vinduer.
- Profitoptimering med eksportpris/importpris.
- UI/dashboard med grafer og redigering af plan.
- Flere adapters: Deye, Huawei, Tesla Powerwall m.fl.
