# ETF_US

Bloomberg-style U.S. ETF holdings monitor for:

- Active universe view
- All universe view
- Daily holdings snapshots
- Membership changes
- Aggregate buy/sell flow ranking
- Optional Telegram alerts

## Main Files

- `us_etf_tracker.py` - collector, database writer, dashboard renderer, Telegram sender
- `us_etf_config.example.json` - tracker configuration
- `run_us_etf_tracker_once.bat` - manual run
- `run_us_etf_tracker_silent.bat` - silent run for scheduler
- `register_us_etf_tracker_schedule.bat` - Windows Task Scheduler registration
- `echo_us_etf_telegram_env.bat` - Telegram environment variable check
- `US_ETF_TRACKER.md` - project notes

## Output

- Local dashboard: `output/us_etf_weight_dashboard.html`
- GitHub Pages entry: `docs/index.html`

## GitHub Pages

This project writes the latest dashboard to `docs/index.html` on every `render` or `update` run.

After pushing to GitHub:

1. Open repository `Settings`
2. Open `Pages`
3. Set source to `Deploy from a branch`
4. Select branch `main`
5. Select folder `/docs`

GitHub will then publish the dashboard as a web link.

## Run

```powershell
python .\us_etf_tracker.py update --config .\us_etf_config.example.json
```

or run:

```powershell
.\run_us_etf_tracker_once.bat
```
