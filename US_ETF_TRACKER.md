# US Active ETF Tracker

Tracks daily holdings weight changes for a ranked universe of U.S. active ETFs across:

- `US Large Cap Active`
- `US Technology Active`
- `US Small Cap Active`

The dashboard is rendered in a dark terminal-style layout inspired by Bloomberg-like market monitors.

## Exact vs Proxy

The tracker now separates ETF sources into two transparency modes:

- `EXACT`: daily full holdings are published directly by the issuer
- `PROXY`: the issuer publishes a semi-transparent proxy portfolio instead of the exact live portfolio

Both modes are shown in the dashboard, but they should not be interpreted the same way.
`EXACT` is best for true holdings change analysis, while `PROXY` is best for monitoring directional positioning in semi-transparent ETFs.

## Run

```powershell
python .\us_etf_tracker.py update --config .\us_etf_config.example.json
```

Outputs:

- SQLite tables in `data/market_dashboard.sqlite`
- HTML dashboard at `output/us_etf_weight_dashboard.html`
- GitHub Pages entry at `docs/index.html`
- Optional Telegram summary when Telegram config is enabled

## GitHub Pages

Every `update` and `render` run now also copies the latest dashboard into:

- `docs/index.html`
- `docs/.nojekyll`

That means this folder is ready for GitHub Pages publishing as soon as it is pushed to a GitHub repository.

Recommended setup:

1. Create or connect this folder to a GitHub repository.
2. Push the project to the `main` branch.
3. In GitHub, open `Settings -> Pages`.
4. Set `Build and deployment` to `Deploy from a branch`.
5. Choose `main` and `/docs`.
6. Save, then open the published URL that GitHub shows.

## Status Model

- `LIVE`: source works and the latest snapshot is stored
- `ERROR`: source is configured but parsing or download failed
- `PENDING`: universe row exists, but direct holdings source is not configured yet

## Telegram

Set these environment variables before running:

```powershell
$env:TELEGRAM_BOT_USA_TOKEN="123456:..."
$env:TELEGRAM_CHAT_USA_ID="123456789"
```

Then change `telegram.enabled` to `true` in `us_etf_config.example.json`.
Set `send_document` to `true` if the HTML dashboard should also be sent as a file.

## Currently Verified Source Families

- `JEPI`, `JEPQ`: J.P. Morgan official product-data JSON
- `ARKK`, `ARKW`, `ARKF`, `ARKQ`: ARK official CSV
- `DYNF`, `BGRO`, `BLCR`, `BLCV`, `BAI`, `TEK`: iShares official ajax CSV export
- `CGDV`, `CGGR`, `CGUS`, `CGMM`: Capital Group official daily holdings XLSX
- `TCAF`, `TDVG`, `TCHP`, `TSPA`, `TGRW`, `TGRT`, `TACU`, `TEQI`, `TTEQ`, `TNXT`, `TMSL`: T. Rowe Price semi-transparent proxy portfolio data on official ETF pages
- `DFAS`, `DFAT`, `DFSV`: Dimensional official daily CSV download link on ETF pages
- `AVUV`, `AVSC`: Avantis official page embedded daily ETF holdings
