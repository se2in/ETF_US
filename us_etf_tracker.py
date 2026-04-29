from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import mimetypes
import os
import re
import shutil
import sqlite3
import sys
from io import BytesIO
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

import pdfplumber
import requests


KST = ZoneInfo("Asia/Seoul")
US_ET = ZoneInfo("America/New_York")
DEFAULT_CONFIG = "us_etf_config.example.json"


SCHEMA = """
CREATE TABLE IF NOT EXISTS us_etf_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    status TEXT NOT NULL,
    message TEXT
);

CREATE TABLE IF NOT EXISTS us_etf_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    issuer TEXT,
    as_of_date TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_type TEXT NOT NULL,
    raw_hash TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    fetched_at TEXT NOT NULL,
    status TEXT NOT NULL,
    message TEXT,
    UNIQUE(ticker, as_of_date, raw_hash)
);

CREATE TABLE IF NOT EXISTS us_etf_holdings (
    snapshot_id INTEGER NOT NULL,
    ticker TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    holding_key TEXT NOT NULL,
    holding_ticker TEXT,
    holding_name TEXT NOT NULL,
    cusip TEXT,
    isin TEXT,
    sedol TEXT,
    sector TEXT,
    country TEXT,
    asset_class TEXT,
    shares REAL,
    market_value REAL,
    weight_pct REAL,
    currency TEXT,
    raw_json TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, holding_key),
    FOREIGN KEY (snapshot_id) REFERENCES us_etf_snapshots(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_us_etf_holdings_lookup
ON us_etf_holdings (ticker, as_of_date, holding_key);

CREATE TABLE IF NOT EXISTS us_etf_membership_events (
    current_snapshot_id INTEGER NOT NULL,
    previous_snapshot_id INTEGER,
    ticker TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    prev_as_of_date TEXT,
    holding_key TEXT NOT NULL,
    holding_ticker TEXT,
    holding_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    weight_pct REAL,
    prev_weight_pct REAL,
    delta_pct REAL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (current_snapshot_id, event_type, holding_key),
    FOREIGN KEY (current_snapshot_id) REFERENCES us_etf_snapshots(id) ON DELETE CASCADE,
    FOREIGN KEY (previous_snapshot_id) REFERENCES us_etf_snapshots(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_us_etf_membership_events_lookup
ON us_etf_membership_events (ticker, as_of_date, event_type);

CREATE TABLE IF NOT EXISTS us_etf_aggregate_flows (
    run_id INTEGER NOT NULL,
    view_name TEXT NOT NULL,
    as_of_anchor TEXT,
    holding_key TEXT NOT NULL,
    holding_ticker TEXT,
    holding_name TEXT NOT NULL,
    total_delta_pct REAL NOT NULL,
    contributor_count INTEGER NOT NULL,
    direction TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, view_name, direction, holding_key),
    FOREIGN KEY (run_id) REFERENCES us_etf_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_us_etf_aggregate_flows_lookup
ON us_etf_aggregate_flows (view_name, run_id, direction);
"""


@dataclass
class Holding:
    holding_key: str
    holding_ticker: str | None
    holding_name: str
    cusip: str | None
    isin: str | None
    sedol: str | None
    sector: str | None
    country: str | None
    asset_class: str | None
    shares: float | None
    market_value: float | None
    weight_pct: float | None
    currency: str | None
    raw: dict[str, Any]


class TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._in_table = False
        self._in_cell = False
        self._table: list[list[str]] = []
        self._row: list[str] = []
        self._cell: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self._in_table = True
            self._table = []
        elif self._in_table and tag == "tr":
            self._row = []
        elif self._in_table and tag in {"td", "th"}:
            self._in_cell = True
            self._cell = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "table" and self._in_table:
            if self._table:
                self.tables.append(self._table)
            self._in_table = False
        elif self._in_table and tag == "tr":
            if self._row:
                self._table.append(self._row)
        elif self._in_table and tag in {"td", "th"} and self._in_cell:
            self._row.append(clean_text(" ".join(self._cell)))
            self._in_cell = False

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell.append(data)


def now_kst() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def today_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def connect(path: str | Path) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def load_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    config = json.loads(config_path.read_text(encoding="utf-8"))
    base_dir = config_path.resolve().parent

    def resolve_local(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        if not value:
            return value
        parsed = urllib.parse.urlparse(value)
        if parsed.scheme or parsed.netloc:
            return value
        return str((base_dir / value).resolve())

    for key in ("database_path", "output_dir", "github_pages_dir"):
        if key in config:
            config[key] = resolve_local(config[key])
    krx = config.get("krx")
    if isinstance(krx, dict) and "raw_pdf_dir" in krx:
        krx["raw_pdf_dir"] = resolve_local(krx["raw_pdf_dir"])
    return config


def fetch_bytes(url: str, user_agent: str) -> bytes:
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/csv,application/json,text/html,application/vnd.ms-excel,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=45) as response:
        return response.read()


def append_query_params(url: str, params: dict[str, str]) -> str:
    parsed = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    existing = dict(query)
    for key, value in params.items():
        if value and key not in existing:
            query.append((key, value))
    return urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(query)))


def format_template_value(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return value.format(**context)
    if isinstance(value, dict):
        return {key: format_template_value(item, context) for key, item in value.items()}
    if isinstance(value, list):
        return [format_template_value(item, context) for item in value]
    return value


def fetch_dimensional_csv(ticker: str, user_agent: str) -> tuple[str, bytes]:
    today_us = datetime.now(US_ET).date()
    for offset in range(0, 10):
        as_of = today_us - timedelta(days=offset)
        url = f"https://tools-blob.dimensional.com/etf/{as_of:%Y%m%d}/{ticker.upper()}.csv"
        try:
            content = fetch_bytes(url, user_agent)
        except urllib.error.URLError:
            continue
        if content.lstrip().lower().startswith(b"date,etf_ticker,"):
            return url, content
    raise ValueError(f"No recent Dimensional CSV found for {ticker}.")


class KrxMarketDataClient:
    def __init__(self, config: dict[str, Any], user_agent: str) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/pdf,application/octet-stream,text/html,application/json,*/*",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )
        self._authenticated = False

    def authenticate(self) -> None:
        if self._authenticated:
            return
        cookie_env = str(self.config.get("cookie_env", "")).strip()
        if cookie_env:
            cookie_value = os.getenv(cookie_env, "").strip()
            if cookie_value:
                self.session.headers["Cookie"] = cookie_value
                self._authenticated = True
                return

        login_post_url = str(self.config.get("login_post_url", "")).strip()
        username_env = str(self.config.get("username_env", "KRX_MARKETDATA_ID")).strip()
        password_env = str(self.config.get("password_env", "KRX_MARKETDATA_PASSWORD")).strip()
        username = os.getenv(username_env, "").strip()
        password = os.getenv(password_env, "").strip()

        if not login_post_url:
            self._authenticated = True
            return
        if not username or not password:
            raise ValueError(
                f"Missing KRX credentials. Set environment variables {username_env} and {password_env}."
            )

        login_page_url = str(self.config.get("login_page_url", "")).strip()
        if login_page_url:
            self.session.get(login_page_url, timeout=30)

        payload = format_template_value(self.config.get("login_payload", {}), {})
        payload[str(self.config.get("username_field", "loginId"))] = username
        payload[str(self.config.get("password_field", "loginPwd"))] = password

        headers = {}
        if login_page_url:
            headers["Referer"] = login_page_url

        response = self.session.post(login_post_url, data=payload, headers=headers, timeout=30)
        response.raise_for_status()
        self._authenticated = True

    def fetch_pdf(self, etf: dict[str, Any], trade_date: str | None) -> tuple[str, bytes]:
        self.authenticate()
        context = {
            "ticker": etf["ticker"].upper(),
            "date": (trade_date or today_kst()).replace("-", ""),
            "date_dash": trade_date or today_kst(),
        }
        source_url = str(format_template_value(etf.get("source_url", ""), context)).strip()
        if not source_url:
            raise ValueError("KRX PDF source_url is not configured.")

        method = str(etf.get("source_method", "GET")).upper()
        payload = format_template_value(etf.get("source_payload", {}), context)
        headers = {}
        referer = str(format_template_value(etf.get("source_referer", self.config.get("source_referer", "")), context)).strip()
        if referer:
            headers["Referer"] = referer

        if method == "POST":
            response = self.session.post(source_url, data=payload, headers=headers, timeout=60)
        else:
            response = self.session.get(source_url, params=payload or None, headers=headers, timeout=60)
        response.raise_for_status()
        content = response.content
        content_type = response.headers.get("content-type", "").lower()
        if not content.lstrip().startswith(b"%PDF") and "pdf" not in content_type:
            raise ValueError("KRX response is not a PDF. Check login/session and source_url settings.")
        return response.url, content


def normalize_krx_header(value: str) -> str:
    return re.sub(r"[\s_\-/()%]+", "", clean_text(value)).lower()


def canonical_krx_header(value: str) -> str | None:
    text = normalize_krx_header(value)
    if not text:
        return None
    aliases = {
        "ticker": ["종목코드", "단축코드", "코드", "표준코드", "구성종목코드", "종목번호", "symbol", "ticker"],
        "name": ["종목명", "한글종목명", "한글종목약명", "자산명", "구성종목명", "종목", "name", "securityname"],
        "shares": ["수량", "보유수량", "주식수", "편입수량", "주수", "quantity", "shares"],
        "market_value": ["평가금액", "평가금액원", "시가평가액", "평가액", "금액", "amount", "marketvalue", "value"],
        "weight": ["비중", "편입비중", "구성비", "비율", "weight", "portfolioweight"],
        "currency": ["통화", "currency"],
    }
    for canonical, candidates in aliases.items():
        if text in {normalize_krx_header(item) for item in candidates}:
            return canonical
    return None


def parse_krx_as_of_date(text: str, fallback: str | None) -> str:
    patterns = [
        r"기준일\s*[:：]?\s*([0-9]{4}[./-][0-9]{1,2}[./-][0-9]{1,2})",
        r"작성기준일\s*[:：]?\s*([0-9]{4}[./-][0-9]{1,2}[./-][0-9]{1,2})",
        r"기준일\s*[:：]?\s*([0-9]{4}년\s*[0-9]{1,2}월\s*[0-9]{1,2}일)",
        r"작성기준일\s*[:：]?\s*([0-9]{4}년\s*[0-9]{1,2}월\s*[0-9]{1,2}일)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        raw = match.group(1)
        raw = raw.replace("년", "-").replace("월", "-").replace("일", "").replace(".", "-").replace("/", "-")
        try:
            return datetime.strptime(re.sub(r"\s+", "", raw), "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            continue
    return fallback or today_kst()


def extract_krx_pdf_rows(content: bytes, fallback_date: str | None) -> tuple[str, list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    first_text = ""
    with pdfplumber.open(BytesIO(content)) as pdf:
        if pdf.pages:
            first_text = pdf.pages[0].extract_text() or ""
        for page in pdf.pages:
            for table in page.extract_tables() or []:
                if not table or len(table) < 2:
                    continue
                header_index = -1
                headers: list[str | None] = []
                for index, candidate in enumerate(table[:6]):
                    mapped = [canonical_krx_header(cell or "") for cell in candidate]
                    if "name" in mapped and ("weight" in mapped or "market_value" in mapped):
                        header_index = index
                        headers = mapped
                        break
                if header_index < 0:
                    continue
                for raw_row in table[header_index + 1 :]:
                    if not raw_row:
                        continue
                    item: dict[str, Any] = {}
                    for idx, header in enumerate(headers):
                        if not header or idx >= len(raw_row):
                            continue
                        value = clean_text(raw_row[idx] or "")
                        if value:
                            item[header] = value
                    if not item.get("name") or is_total_row(str(item["name"])):
                        continue
                    rows.append(item)
    if not rows:
        raise ValueError("KRX PDF table parsing failed. No holdings rows were detected.")
    return parse_krx_as_of_date(first_text, fallback_date), rows


def fetch_krx_pdf_holdings(etf: dict[str, Any], config: dict[str, Any]) -> tuple[str, str, list[Holding]]:
    krx = config.get("krx", {})
    trade_date = str(etf.get("trade_date") or krx.get("trade_date") or today_kst())
    client = KrxMarketDataClient(krx, config.get("user_agent", "US ETF Weight Monitor contact@example.com"))
    source_url, content = client.fetch_pdf(etf, trade_date)
    raw_dir = Path(str(krx.get("raw_pdf_dir", Path(config["database_path"]).parent / "krx_pdfs")))
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_name = f'{etf["ticker"].upper()}_{trade_date.replace("-", "")}.pdf'
    (raw_dir / raw_name).write_bytes(content)
    as_of_date, rows = extract_krx_pdf_rows(content, trade_date)
    holdings = normalize_holdings(rows)
    if not holdings:
        raise ValueError("KRX PDF was downloaded but holdings normalization returned no rows.")
    return source_url, as_of_date, holdings


def detect_source_type(source_type: str, url: str, content: bytes) -> str:
    if source_type != "auto":
        return source_type
    suffix = Path(urllib.parse.urlparse(url).path).suffix.lower()
    if suffix in {".csv", ".txt"}:
        return "csv"
    if suffix in {".json"}:
        return "json"
    text_start = content[:512].lstrip().lower()
    if text_start.startswith(b"{") or text_start.startswith(b"["):
        return "json"
    if content.startswith(b"PK\x03\x04"):
        return "xlsx"
    if b"<html" in text_start or b"<table" in text_start or b"<!doctype html" in text_start:
        return "html"
    return "csv"


def parse_rows(source_type: str, content: bytes) -> tuple[str | None, list[dict[str, Any]]]:
    if source_type == "xlsx":
        rows = rows_from_xlsx(content)
        return infer_as_of_from_rows(rows), rows
    text = decode_bytes(content)
    as_of = parse_as_of_date(text)
    if source_type == "avantis_js":
        rows = rows_from_avantis_js(text)
        as_of = parse_as_of_date(text) or infer_as_of_from_rows(rows)
    elif source_type == "dimensional_csv":
        rows = rows_from_csv(text)
    elif source_type == "jpm_product_data":
        rows = rows_from_jpm_product_data(json.loads(text))
        as_of = infer_as_of_from_rows(rows)
    elif source_type == "trowe_embedded":
        rows = rows_from_trowe_html(text)
        as_of = parse_as_of_date(text) or infer_as_of_from_rows(rows)
    elif source_type == "json":
        rows = rows_from_json(json.loads(text))
    elif source_type == "html":
        rows = rows_from_html(text)
    else:
        rows = rows_from_csv(text)
    return as_of or infer_as_of_from_rows(rows), rows


def decode_bytes(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-16", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def rows_from_csv(text: str) -> list[dict[str, Any]]:
    lines = [line for line in text.splitlines() if line.strip()]
    best: list[dict[str, Any]] = []
    for delimiter in (",", "\t", ";"):
        parsed = list(csv.reader(lines, delimiter=delimiter))
        for header_index, row in enumerate(parsed[:40]):
            headers = [normalize_header(cell) for cell in row]
            if not looks_like_holdings_header(headers):
                continue
            rows: list[dict[str, Any]] = []
            for item in parsed[header_index + 1 :]:
                if not any(cell.strip() for cell in item):
                    continue
                padded = item + [""] * (len(row) - len(item))
                rows.append({row[i].strip(): padded[i].strip() for i in range(len(row))})
            if len(rows) > len(best):
                best = rows
    return best


def rows_from_xlsx(content: bytes) -> list[dict[str, Any]]:
    best: list[dict[str, Any]] = []
    with zipfile.ZipFile(io_bytes(content)) as archive:
        shared_strings = read_xlsx_shared_strings(archive)
        sheet_names = sorted(name for name in archive.namelist() if name.startswith("xl/worksheets/sheet"))
        for sheet_name in sheet_names:
            matrix = read_xlsx_sheet(archive, sheet_name, shared_strings)
            rows = rows_from_matrix(matrix)
            if len(rows) > len(best):
                best = rows
    return best


def rows_from_matrix(matrix: list[list[str]]) -> list[dict[str, Any]]:
    best: list[dict[str, Any]] = []
    for header_index, row in enumerate(matrix[:80]):
        headers = [normalize_header(cell) for cell in row]
        if not looks_like_holdings_header(headers):
            continue
        rows = []
        for item in matrix[header_index + 1 :]:
            if not any(cell.strip() for cell in item):
                continue
            padded = item + [""] * (len(row) - len(item))
            rows.append({row[i].strip(): padded[i].strip() for i in range(len(row))})
        if len(rows) > len(best):
            best = rows
    return best


def io_bytes(content: bytes):
    import io

    return io.BytesIO(content)


def read_xlsx_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ElementTree.fromstring(archive.read("xl/sharedStrings.xml"))
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    values = []
    for item in root.findall("x:si", ns):
        texts = [node.text or "" for node in item.findall(".//x:t", ns)]
        values.append("".join(texts))
    return values


def read_xlsx_sheet(archive: zipfile.ZipFile, sheet_name: str, shared_strings: list[str]) -> list[list[str]]:
    root = ElementTree.fromstring(archive.read(sheet_name))
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    matrix: list[list[str]] = []
    for row in root.findall(".//x:sheetData/x:row", ns):
        values: dict[int, str] = {}
        for cell in row.findall("x:c", ns):
            ref = cell.attrib.get("r", "A1")
            col = column_index(ref)
            values[col] = read_xlsx_cell(cell, shared_strings, ns)
        if values:
            width = max(values) + 1
            matrix.append([values.get(index, "") for index in range(width)])
    return matrix


def read_xlsx_cell(cell: ElementTree.Element, shared_strings: list[str], ns: dict[str, str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell.findall(".//x:t", ns)).strip()
    node = cell.find("x:v", ns)
    if node is None or node.text is None:
        return ""
    if cell_type == "s":
        index = int(node.text)
        return shared_strings[index] if index < len(shared_strings) else ""
    return node.text.strip()


def column_index(ref: str) -> int:
    letters = re.match(r"[A-Z]+", ref.upper())
    if not letters:
        return 0
    value = 0
    for char in letters.group(0):
        value = value * 26 + (ord(char) - ord("A") + 1)
    return value - 1


def rows_from_avantis_js(text: str) -> list[dict[str, Any]]:
    match = re.search(r"etfHoldings:\[(.*?)\],\s*[a-zA-Z_]+:", text, flags=re.S)
    if not match:
        match = re.search(r"etfHoldings:\[(.*?)]", text, flags=re.S)
    if not match:
        return []
    rows: list[dict[str, Any]] = []
    for object_text in re.findall(r"\{(.*?)\}", match.group(1), flags=re.S):
        row: dict[str, Any] = {}
        for key, value in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\s*:\s*\"((?:\\.|[^\"])*)\"", object_text):
            row[key] = value.encode("utf-8").decode("unicode_escape")
        if row:
            rows.append(row)
    return rows


def rows_from_trowe_html(text: str) -> list[dict[str, Any]]:
    decoded = html.unescape(text)
    candidate_markers = ['"proxyPortfolio":', '"holdings":[', '"individualHoldings":[']
    arrays: list[list[dict[str, Any]]] = []
    for marker in candidate_markers:
        start = 0
        while True:
            idx = decoded.find(marker, start)
            if idx == -1:
                break
            bracket_start = decoded.find("[", idx)
            if bracket_start == -1:
                break
            chunk = extract_json_array(decoded, bracket_start)
            start = bracket_start + 1
            if not chunk:
                continue
            try:
                data = json.loads(chunk)
            except json.JSONDecodeError:
                continue
            if isinstance(data, list) and data and isinstance(data[0], dict):
                arrays.append(data)
    if not arrays:
        return []
    best = max(
        arrays,
        key=lambda rows: (
            sum(1 for row in rows if any(key in row for key in ("tickerSymbol", "percentageTotalNetAssets", "marketValue"))),
            len(rows),
        ),
    )
    normalized_rows = []
    for row in best:
        normalized_rows.append(
            {
                "Ticker": row.get("tickerSymbol") or "",
                "Name": row.get("name") or "",
                "Sector": row.get("sectorName") or "",
                "Industry": row.get("industryName") or "",
                "Country": row.get("countryName") or "",
                "Market Value": row.get("marketValue"),
                "Weight (%)": row.get("percentageTotalNetAssets"),
                "Shares": row.get("shareQuantity"),
                "CUSIP": row.get("cusip") or row.get("prioritizedIdentifier") or "",
                "As Of Date": row.get("effectiveDate") or "",
            }
        )
    return normalized_rows


def rows_from_jpm_product_data(data: Any) -> list[dict[str, Any]]:
    fund_data = data.get("fundData", {}) if isinstance(data, dict) else {}
    daily = fund_data.get("dailyHoldingsAll") or fund_data.get("dailyHoldings") or {}
    effective_date = daily.get("effectiveDate") or ""
    rows = []
    for row in daily.get("data") or []:
        rows.append(
            {
                "Ticker": row.get("securityTicker") or "",
                "Name": row.get("securityDescription") or "",
                "Sector": row.get("sector") or "",
                "Industry": row.get("industry") or "",
                "Country": row.get("country") or "",
                "Market Value": row.get("marketValue"),
                "Weight (%)": row.get("marketValuePercent") if row.get("marketValuePercent") is not None else row.get("netAssetValuePercent"),
                "Shares": row.get("shares"),
                "CUSIP": row.get("securityId") or row.get("securityCusip") or "",
                "ISIN": row.get("securityIsin") or "",
                "SEDOL": row.get("securitySedol") or "",
                "Currency": row.get("currencyCode") or "",
                "Asset Class": row.get("securityType") or row.get("assetType") or "",
                "As Of Date": effective_date,
            }
        )
    return rows


def extract_json_array(text: str, start_index: int) -> str | None:
    depth = 0
    in_string = False
    escaped = False
    for index in range(start_index, len(text)):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return text[start_index : index + 1]
    return None


def rows_from_html(text: str) -> list[dict[str, Any]]:
    parser = TableParser()
    parser.feed(text)
    best: list[dict[str, Any]] = []
    for table in parser.tables:
        for header_index, row in enumerate(table[:10]):
            headers = [normalize_header(cell) for cell in row]
            if not looks_like_holdings_header(headers):
                continue
            rows = []
            for item in table[header_index + 1 :]:
                if not any(item):
                    continue
                padded = item + [""] * (len(row) - len(item))
                rows.append({row[i].strip(): padded[i].strip() for i in range(len(row))})
            if len(rows) > len(best):
                best = rows
    return best


def rows_from_json(data: Any) -> list[dict[str, Any]]:
    candidates: list[list[dict[str, Any]]] = []

    def walk(value: Any) -> None:
        if isinstance(value, list) and value and all(isinstance(item, dict) for item in value):
            headers = {normalize_header(key) for row in value for key in row.keys()}
            if looks_like_holdings_header(list(headers)):
                candidates.append(value)
        elif isinstance(value, dict):
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(data)
    return max(candidates, key=len, default=[])


def looks_like_holdings_header(headers: list[str]) -> bool:
    has_name = bool({"name", "company", "securityname", "holding", "description"} & set(headers))
    has_weight = any("weight" in header or "marketvalue" in header or header in {"percentoffund", "pct"} for header in headers)
    return has_name and has_weight


def normalize_holdings(rows: list[dict[str, Any]]) -> list[Holding]:
    holdings: list[Holding] = []
    for row in rows:
        lookup = {normalize_header(key): value for key, value in row.items()}
        name = first_value(
            lookup,
            "company",
            "name",
            "asset",
            "securityname",
            "holding",
            "description",
            "fundname",
            "issuer",
        )
        if not name or is_total_row(name):
            continue
        ticker = first_value(lookup, "ticker", "symbol", "holdingticker", "tickersymbol")
        cusip = first_value(lookup, "cusip")
        isin = first_value(lookup, "isin")
        sedol = first_value(lookup, "sedol")
        weight = parse_number(
            first_value(
                lookup,
                "weight",
                "weight%",
                "weightpercentage",
                "marketweight",
                "notionalweight",
                "%marketvalue",
                "percentoffund",
                "percentofnetassets",
                "pct",
            )
        )
        if weight is None:
            weight = parse_number(first_value(lookup, "marketvalueweight", "netassets"))
        holding_key = make_holding_key(ticker, cusip, isin, name)
        holdings.append(
            Holding(
                holding_key=holding_key,
                holding_ticker=empty_to_none(ticker),
                holding_name=name.strip(),
                cusip=empty_to_none(cusip),
                isin=empty_to_none(isin),
                sedol=empty_to_none(sedol),
                sector=empty_to_none(first_value(lookup, "sector", "industry")),
                country=empty_to_none(first_value(lookup, "country", "location", "geography")),
                asset_class=empty_to_none(first_value(lookup, "assetclass", "assettype", "securitytype")),
                shares=parse_number(first_value(lookup, "shares", "sharesnumber", "quantity", "sharesprincipalnotionalamount")),
                market_value=parse_number(first_value(lookup, "marketvalue", "marketvalue$", "value", "notionalvalue", "basemarketvalue")),
                weight_pct=weight,
                currency=empty_to_none(first_value(lookup, "currency", "marketcurrency")),
                raw=row,
            )
        )
    return holdings


def save_snapshot(
    conn: sqlite3.Connection,
    etf: dict[str, Any],
    source_type: str,
    raw_hash: str,
    as_of_date: str,
    holdings: list[Holding],
    status: str,
    message: str,
) -> int:
    existing = conn.execute(
        """
        SELECT id FROM us_etf_snapshots
        WHERE ticker = ? AND as_of_date = ? AND raw_hash = ?
        """,
        (etf["ticker"], as_of_date, raw_hash),
    ).fetchone()
    if existing:
        snapshot_id = int(existing["id"])
        conn.execute("DELETE FROM us_etf_holdings WHERE snapshot_id = ?", (snapshot_id,))
    else:
        cur = conn.execute(
            """
            INSERT INTO us_etf_snapshots (
                ticker, issuer, as_of_date, source_url, source_type, raw_hash,
                row_count, fetched_at, status, message
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                etf["ticker"],
                etf.get("issuer"),
                as_of_date,
                etf["source_url"],
                source_type,
                raw_hash,
                len(holdings),
                now_kst(),
                status,
                message,
            ),
        )
        snapshot_id = int(cur.lastrowid)

    conn.executemany(
        """
        INSERT OR REPLACE INTO us_etf_holdings (
            snapshot_id, ticker, as_of_date, holding_key, holding_ticker,
            holding_name, cusip, isin, sedol, sector, country, asset_class,
            shares, market_value, weight_pct, currency, raw_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                snapshot_id,
                etf["ticker"],
                as_of_date,
                holding.holding_key,
                holding.holding_ticker,
                holding.holding_name,
                holding.cusip,
                holding.isin,
                holding.sedol,
                holding.sector,
                holding.country,
                holding.asset_class,
                holding.shares,
                holding.market_value,
                holding.weight_pct,
                holding.currency,
                json.dumps(holding.raw, ensure_ascii=False),
            )
            for holding in holdings
        ],
    )
    conn.commit()
    return snapshot_id


def update_all(config: dict[str, Any]) -> dict[str, Any]:
    conn = connect(config["database_path"])
    init_db(conn)
    run_id = start_run(conn)
    results = []
    tracked_etfs = list(config.get("etfs", [])) + list(config.get("kr_etfs", []))
    for etf in tracked_etfs:
        if not etf.get("enabled", True):
            results.append({"ticker": etf["ticker"].upper(), "status": "SKIPPED", "row_count": 0, "message": "Source pending"})
            continue
        results.append(update_one(conn, etf, config))
    status = "OK" if all(item["status"] in {"OK", "SKIPPED"} for item in results) else "PARTIAL"
    message = "; ".join(f"{item['ticker']}={item['status']}({item['row_count']})" for item in results)
    finish_run(conn, run_id, status, message)
    output_path = render_dashboard(conn, config)
    summary = build_telegram_summary(conn, results, output_path, run_id, config)
    if config.get("telegram", {}).get("enabled"):
        send_telegram(config, summary, output_path)
    conn.close()
    return {"status": status, "results": results, "output_path": output_path, "summary": summary}


def update_one(conn: sqlite3.Connection, etf: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    ticker = etf["ticker"].upper()
    try:
        user_agent = config.get("user_agent", "US ETF Weight Monitor contact@example.com")
        requested_source_type = etf.get("source_type", "auto")
        source_url = etf.get("source_url", "")
        if requested_source_type == "dimensional_csv":
            source_url, content = fetch_dimensional_csv(ticker, user_agent)
            source_type = requested_source_type
        elif requested_source_type == "fmp_holdings":
            api_env = etf.get("api_key_env") or config.get("fmp_api_key_env", "FMP_API_KEY")
            api_key = os.getenv(api_env, "").strip()
            if not api_key:
                raise ValueError(f"Missing API key in environment variable: {api_env}")
            source_url = source_url or f"https://financialmodelingprep.com/stable/etf/holdings?symbol={ticker}"
            source_url = source_url.format(ticker=ticker)
            source_url = append_query_params(source_url, {"apikey": api_key})
            content = fetch_bytes(source_url, user_agent)
            source_type = "json"
        elif requested_source_type == "krx_marketdata_pdf":
            source_url, as_of, holdings = fetch_krx_pdf_holdings({**etf, "ticker": ticker}, config)
            raw_hash = hashlib.sha256(
                json.dumps([holding.raw for holding in holdings], ensure_ascii=False, sort_keys=True).encode("utf-8")
            ).hexdigest()
            snapshot_id = save_snapshot(
                conn,
                {**etf, "ticker": ticker, "source_url": source_url},
                requested_source_type,
                raw_hash,
                as_of,
                holdings,
                "OK",
                "",
            )
            persist_membership_events(conn, ticker, snapshot_id, as_of)
            return {"ticker": ticker, "status": "OK", "row_count": len(holdings), "snapshot_id": snapshot_id}
        else:
            if not source_url:
                raise ValueError("Source URL is not configured.")
            content = fetch_bytes(source_url, user_agent)
            source_type = detect_source_type(requested_source_type, source_url, content)
        raw_hash = hashlib.sha256(content).hexdigest()
        as_of, rows = parse_rows(source_type, content)
        holdings = normalize_holdings(rows)
        if not holdings:
            raise ValueError("No holdings rows found. This source may require a direct CSV/API URL.")
        snapshot_id = save_snapshot(
            conn,
            {**etf, "ticker": ticker, "source_url": source_url},
            source_type,
            raw_hash,
            as_of or today_kst(),
            holdings,
            "OK",
            "",
        )
        persist_membership_events(conn, ticker, snapshot_id, as_of or today_kst())
        return {"ticker": ticker, "status": "OK", "row_count": len(holdings), "snapshot_id": snapshot_id}
    except Exception as exc:
        save_failed_snapshot(conn, {**etf, "ticker": ticker}, str(exc))
        return {"ticker": ticker, "status": "ERROR", "row_count": 0, "message": str(exc)}


def save_failed_snapshot(conn: sqlite3.Connection, etf: dict[str, Any], message: str) -> None:
    raw_hash = hashlib.sha256(f"{etf['ticker']}:{now_kst()}:{message}".encode()).hexdigest()
    conn.execute(
        """
        INSERT INTO us_etf_snapshots (
            ticker, issuer, as_of_date, source_url, source_type, raw_hash,
            row_count, fetched_at, status, message
        )
        VALUES (?, ?, ?, ?, ?, ?, 0, ?, 'ERROR', ?)
        """,
        (
            etf["ticker"],
            etf.get("issuer"),
            today_kst(),
            etf.get("source_url", ""),
            etf.get("source_type", "auto"),
            raw_hash,
            now_kst(),
            message,
        ),
    )
    conn.commit()


def start_run(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        "INSERT INTO us_etf_runs (started_at, status) VALUES (?, 'RUNNING')",
        (now_kst(),),
    )
    conn.commit()
    return int(cur.lastrowid)


def finish_run(conn: sqlite3.Connection, run_id: int, status: str, message: str) -> None:
    conn.execute(
        "UPDATE us_etf_runs SET ended_at = ?, status = ?, message = ? WHERE id = ?",
        (now_kst(), status, message, run_id),
    )
    conn.commit()


def render_dashboard(conn: sqlite3.Connection, config: dict[str, Any]) -> Path:
    output_dir = Path(config.get("output_dir", "output"))
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / config.get("output_file", "us_etf_weight_dashboard.html")
    us_etfs = list(config.get("etfs", []))
    kr_etfs = list(config.get("kr_etfs", []))
    all_tracked = us_etfs + kr_etfs
    active_etfs = etfs_for_view(us_etfs, "active")
    every_etf = etfs_for_view(us_etfs, "all")
    kr_view_etfs = etfs_for_view(kr_etfs, "kr")
    for etf in all_tracked:
        current = latest_snapshot(conn, etf["ticker"].upper(), ok_only=True)
        if current:
            persist_membership_events(conn, etf["ticker"].upper(), int(current["id"]), str(current["as_of_date"]))
    freshest_as_of_date = latest_source_date(conn, all_tracked)
    latest_run = conn.execute(
        "SELECT * FROM us_etf_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    run_id = int(latest_run["id"]) if latest_run else 0
    if run_id:
        persist_view_aggregate_flows(conn, run_id, "active", active_etfs, freshest_as_of_date)
        persist_view_aggregate_flows(conn, run_id, "all", every_etf, freshest_as_of_date)
        persist_view_aggregate_flows(conn, run_id, "kr", kr_view_etfs, freshest_as_of_date)
    active_groups = group_etfs(active_etfs)
    all_groups = group_etfs(every_etf)
    kr_groups = group_etfs(kr_view_etfs)
    view_panels = {
        "active": {
            "label": "Active",
            "sections": "\n".join(
                render_group_section(conn, group_name, rows, freshest_as_of_date) for group_name, rows in active_groups
            ),
            "stats": dashboard_stats(conn, active_etfs),
            "event_summary": render_view_event_summary(conn, active_etfs),
            "flow_summary": render_view_flow_summary(conn, run_id, "active"),
        },
        "all": {
            "label": "All",
            "sections": "\n".join(
                render_group_section(conn, group_name, rows, freshest_as_of_date) for group_name, rows in all_groups
            ),
            "stats": dashboard_stats(conn, every_etf),
            "event_summary": render_view_event_summary(conn, every_etf),
            "flow_summary": render_view_flow_summary(conn, run_id, "all"),
        },
        "kr": {
            "label": "KR",
            "sections": "\n".join(
                render_group_section(conn, group_name, rows, freshest_as_of_date) for group_name, rows in kr_groups
            ),
            "stats": dashboard_stats(conn, kr_view_etfs),
            "event_summary": render_view_event_summary(conn, kr_view_etfs),
            "flow_summary": render_view_flow_summary(conn, run_id, "kr"),
        },
    }
    updated = latest_run["ended_at"] if latest_run and latest_run["ended_at"] else now_kst()
    output_path.write_text(
        build_html(config.get("brand", "US Active ETF Monitor"), updated, view_panels),
        encoding="utf-8",
    )
    sync_github_pages_output(output_path, config)
    return output_path


def sync_github_pages_output(output_path: Path, config: dict[str, Any]) -> Path:
    docs_dir = Path(config.get("github_pages_dir", "docs"))
    docs_dir.mkdir(parents=True, exist_ok=True)
    index_path = docs_dir / config.get("github_pages_file", "index.html")
    shutil.copyfile(output_path, index_path)
    (docs_dir / ".nojekyll").write_text("", encoding="utf-8")
    return index_path


def render_group_section(
    conn: sqlite3.Connection, group_name: str, etfs: list[dict[str, Any]], freshest_as_of_date: str | None
) -> str:
    rows = "\n".join(render_overview_row(conn, etf, freshest_as_of_date) for etf in etfs)
    details = "\n".join(render_etf_section(conn, etf, freshest_as_of_date) for etf in etfs)
    tracked = sum(1 for etf in etfs if etf.get("enabled", True))
    exact_count = sum(1 for etf in etfs if collector_mode(etf) == "exact")
    proxy_count = sum(1 for etf in etfs if collector_mode(etf) == "proxy")
    return f"""
    <section class="group-band">
      <div class="group-head">
        <div>
          <h2>{esc(group_name)}</h2>
          <p>{len(etfs)} funds · {tracked} collectors · {exact_count} exact · {proxy_count} proxy</p>
        </div>
      </div>
      <div class="terminal-table">
        <table>
          <thead>
            <tr>
              <th>Rank</th><th>Ticker</th><th>Mode</th><th>Issuer</th><th>Strategy</th><th>Feature</th><th>Status</th><th>As Of</th><th>Rows</th><th>Top Move</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
      <div class="detail-grid">{details}</div>
    </section>
    """


def render_etf_section(conn: sqlite3.Connection, etf: dict[str, Any], freshest_as_of_date: str | None) -> str:
    ticker = etf["ticker"].upper()
    mode_badge = mode_chip(collector_mode(etf))
    current = latest_snapshot(conn, ticker, ok_only=False)
    if not etf.get("enabled", True):
        return f"""
        <article class="detail-card pending">
          <div class="detail-head"><div><h3>{esc(ticker)}</h3>{mode_badge}</div><span class="status pending">PENDING</span></div>
          <p class="meta">{esc(etf.get("issuer", ""))} · {esc(etf.get("strategy", ""))}</p>
          <p class="muted">{esc(etf.get("feature", ""))}</p>
          <p class="muted">Source URL pending. Universe row is included, but collection is paused.</p>
        </article>
        """
    if not current:
        return f"""
        <article class="detail-card pending">
          <div class="detail-head"><div><h3>{esc(ticker)}</h3>{mode_badge}</div><span class="status pending">NO DATA</span></div>
          <p class="meta">{esc(etf.get("issuer", ""))} · {esc(etf.get("strategy", ""))}</p>
          <p class="muted">{esc(etf.get("feature", ""))}</p>
          <p class="muted">Collector is enabled, but no snapshot has been stored yet.</p>
        </article>
        """
    if current["status"] != "OK":
        return f"""
        <article class="detail-card warning">
          <div class="detail-head"><div><h3>{esc(ticker)}</h3>{mode_badge}</div><span class="status error">ERROR</span></div>
          <p class="meta">{esc(etf.get("issuer", ""))} · {esc(etf.get("strategy", ""))}</p>
          <p class="muted">{esc(etf.get("feature", ""))}</p>
          <p>{esc(current["message"] or "Unknown error")}</p>
        </article>
        """
    previous = previous_snapshot(conn, ticker, int(current["id"]), str(current["as_of_date"]))
    stale = is_source_date_stale(str(current["as_of_date"]), freshest_as_of_date)
    changes = holding_changes(conn, int(current["id"]), int(previous["id"]) if previous else None)
    comparable = previous is not None
    positive_changes = [item for item in changes if comparable and item["delta"] is not None and item["delta"] > 0]
    negative_changes = [item for item in changes if comparable and item["delta"] is not None and item["delta"] < 0]
    avg_up = average_delta(positive_changes)
    avg_down = average_delta(negative_changes)
    top_up = positive_changes[:8]
    top_down = negative_changes[-8:]
    new_rows = [item for item in changes if comparable and item["change_type"] == "NEW"][:8]
    sold_rows = [item for item in changes if comparable and item["change_type"] == "REMOVED"][:8]
    holdings = conn.execute(
        """
        SELECT holding_ticker, holding_name, weight_pct, shares, market_value
        FROM us_etf_holdings
        WHERE snapshot_id = ?
        ORDER BY COALESCE(weight_pct, -999999) DESC
        """,
        (current["id"],),
    ).fetchall()
    return f"""
    <article class="detail-card">
      <div class="detail-head">
        <div>
          <h3>{esc(ticker)}</h3>
          {mode_badge}
          <p class="meta">{esc(current["issuer"] or etf.get("issuer", ""))} · {esc(etf.get("strategy", ""))}</p>
        </div>
        <span class="status {'stale' if stale else 'ok'}">{'SOURCE DATE STALE' if stale else 'LIVE'}</span>
      </div>
      <div class="meta-strip">
        <span>{esc(etf.get("feature", ""))}</span>
        <span>As of {esc(current["as_of_date"])}</span>
        <span>Checked {esc(fmt_checked_at(current["fetched_at"]))}</span>
        <span>{int(current["row_count"])} holdings</span>
        {f'<span class="status stale">SOURCE DATE STALE · Last source update {esc(current["as_of_date"])}</span>' if stale else ''}
      </div>
      <div class="kpi-grid">
        {kpi("Top 증가", fmt_delta(top_up[0]["delta"]) if top_up else "-")}
        {kpi("Top 감소", fmt_delta(top_down[0]["delta"]) if top_down else "-")}
        {kpi("Avg Up", fmt_delta(avg_up))}
        {kpi("Avg Down", fmt_delta(avg_down))}
      </div>
      <div class="change-grid">
        {change_table("비중 증가", top_up, avg_up)}
        {change_table("비중 감소", list(reversed(top_down)), avg_down)}
        {change_table("신규 편입", new_rows)}
        {change_table("제외", sold_rows)}
      </div>
      <h4>Top Holdings</h4>
      {holdings_table(holdings)}
    </article>
    """


def latest_snapshot(conn: sqlite3.Connection, ticker: str, ok_only: bool = True) -> sqlite3.Row | None:
    status_clause = "AND status = 'OK'" if ok_only else ""
    return conn.execute(
        f"""
        SELECT * FROM us_etf_snapshots
        WHERE ticker = ? {status_clause}
        ORDER BY fetched_at DESC, id DESC
        LIMIT 1
        """,
        (ticker,),
    ).fetchone()


def snapshot_by_id(conn: sqlite3.Connection, snapshot_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM us_etf_snapshots WHERE id = ?", (snapshot_id,)).fetchone()


def previous_snapshot(conn: sqlite3.Connection, ticker: str, current_id: int, current_as_of_date: str) -> sqlite3.Row | None:
    prior_day = conn.execute(
        """
        SELECT * FROM us_etf_snapshots
        WHERE ticker = ? AND status = 'OK' AND as_of_date < ?
        ORDER BY as_of_date DESC, fetched_at DESC, id DESC
        LIMIT 1
        """,
        (ticker, current_as_of_date),
    ).fetchone()
    if prior_day:
        return prior_day
    return conn.execute(
        """
        SELECT * FROM us_etf_snapshots
        WHERE ticker = ? AND status = 'OK' AND id <> ?
        ORDER BY fetched_at DESC, id DESC
        LIMIT 1
        """,
        (ticker, current_id),
    ).fetchone()


def latest_source_date(conn: sqlite3.Connection, etfs: list[dict[str, Any]]) -> str | None:
    dates: list[str] = []
    for etf in etfs:
        ticker = etf["ticker"].upper()
        row = latest_snapshot(conn, ticker, ok_only=True)
        if row and row["as_of_date"]:
            dates.append(str(row["as_of_date"]))
    return max(dates) if dates else None


def is_source_date_stale(as_of_date: str | None, freshest_as_of_date: str | None) -> bool:
    return bool(as_of_date and freshest_as_of_date and as_of_date < freshest_as_of_date)


def holding_changes(conn: sqlite3.Connection, current_id: int, previous_id: int | None) -> list[dict[str, Any]]:
    current_rows = {
        row["holding_key"]: dict(row)
        for row in conn.execute("SELECT * FROM us_etf_holdings WHERE snapshot_id = ?", (current_id,))
    }
    previous_rows = (
        {
            row["holding_key"]: dict(row)
            for row in conn.execute("SELECT * FROM us_etf_holdings WHERE snapshot_id = ?", (previous_id,))
        }
        if previous_id
        else {}
    )
    changes = []
    for key in sorted(set(current_rows) | set(previous_rows)):
        current = current_rows.get(key)
        previous = previous_rows.get(key)
        if current and previous:
            current_weight = current["weight_pct"]
            previous_weight = previous["weight_pct"]
            delta = None if current_weight is None or previous_weight is None else current_weight - previous_weight
            change_type = "CHANGED"
        elif current:
            current_weight = current["weight_pct"]
            previous_weight = None
            delta = current_weight
            change_type = "NEW"
        else:
            current_weight = None
            previous_weight = previous["weight_pct"]
            delta = -previous_weight if previous_weight is not None else None
            change_type = "REMOVED"
        row = current or previous
        changes.append(
            {
                "holding_key": row["holding_key"],
                "holding_ticker": row["holding_ticker"],
                "holding_name": row["holding_name"],
                "weight": current_weight,
                "prev_weight": previous_weight,
                "delta": delta,
                "change_type": change_type,
            }
        )
    changes.sort(key=lambda item: abs(item["delta"] or 0), reverse=True)
    return changes


def persist_membership_events(conn: sqlite3.Connection, ticker: str, current_snapshot_id: int, current_as_of_date: str) -> None:
    current = snapshot_by_id(conn, current_snapshot_id)
    previous = previous_snapshot(conn, ticker, current_snapshot_id, current_as_of_date)
    conn.execute("DELETE FROM us_etf_membership_events WHERE current_snapshot_id = ?", (current_snapshot_id,))
    if not current or not previous:
        conn.commit()
        return
    changes = holding_changes(conn, current_snapshot_id, int(previous["id"]))
    event_rows = [item for item in changes if item["change_type"] in {"NEW", "REMOVED"}]
    if not event_rows:
        conn.commit()
        return
    conn.executemany(
        """
        INSERT OR REPLACE INTO us_etf_membership_events (
            current_snapshot_id, previous_snapshot_id, ticker, as_of_date, prev_as_of_date,
            holding_key, holding_ticker, holding_name, event_type, weight_pct, prev_weight_pct,
            delta_pct, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                current_snapshot_id,
                int(previous["id"]),
                ticker,
                str(current["as_of_date"]),
                str(previous["as_of_date"]),
                make_holding_key(item["holding_ticker"], None, None, item["holding_name"]),
                item["holding_ticker"],
                item["holding_name"],
                item["change_type"],
                item["weight"],
                item["prev_weight"],
                item["delta"],
                now_kst(),
            )
            for item in event_rows
        ],
    )
    conn.commit()


def aggregate_view_flows(conn: sqlite3.Connection, etfs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for etf in etfs:
        ticker = etf["ticker"].upper()
        current = latest_snapshot(conn, ticker, ok_only=True)
        if not current:
            continue
        previous = previous_snapshot(conn, ticker, int(current["id"]), str(current["as_of_date"]))
        if not previous:
            continue
        for change in holding_changes(conn, int(current["id"]), int(previous["id"])):
            delta = change.get("delta")
            if delta is None or delta == 0:
                continue
            holding_key = str(change["holding_key"])
            bucket = grouped.setdefault(
                holding_key,
                {
                    "holding_key": holding_key,
                    "holding_ticker": change.get("holding_ticker"),
                    "holding_name": change.get("holding_name"),
                    "total_delta_pct": 0.0,
                    "contributors": set(),
                },
            )
            bucket["total_delta_pct"] += float(delta)
            bucket["contributors"].add(ticker)
            if not bucket.get("holding_ticker") and change.get("holding_ticker"):
                bucket["holding_ticker"] = change.get("holding_ticker")
    rows: list[dict[str, Any]] = []
    for item in grouped.values():
        total_delta = float(item["total_delta_pct"])
        if total_delta == 0:
            continue
        rows.append(
            {
                "holding_key": item["holding_key"],
                "holding_ticker": item.get("holding_ticker"),
                "holding_name": item.get("holding_name"),
                "total_delta_pct": total_delta,
                "contributor_count": len(item["contributors"]),
                "direction": "BUY" if total_delta > 0 else "SELL",
            }
        )
    rows.sort(key=lambda item: abs(item["total_delta_pct"]), reverse=True)
    return rows


def persist_view_aggregate_flows(
    conn: sqlite3.Connection,
    run_id: int,
    view_name: str,
    etfs: list[dict[str, Any]],
    as_of_anchor: str | None,
) -> None:
    conn.execute(
        "DELETE FROM us_etf_aggregate_flows WHERE run_id = ? AND view_name = ?",
        (run_id, view_name),
    )
    rows = aggregate_view_flows(conn, etfs)
    if rows:
        conn.executemany(
            """
            INSERT INTO us_etf_aggregate_flows (
                run_id, view_name, as_of_anchor, holding_key, holding_ticker, holding_name,
                total_delta_pct, contributor_count, direction, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    view_name,
                    as_of_anchor,
                    row["holding_key"],
                    row["holding_ticker"],
                    row["holding_name"],
                    row["total_delta_pct"],
                    row["contributor_count"],
                    row["direction"],
                    now_kst(),
                )
                for row in rows
            ],
        )
    conn.commit()


def aggregate_flow_rows(
    conn: sqlite3.Connection, run_id: int, view_name: str, direction: str, limit: int = 20
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT holding_ticker, holding_name, total_delta_pct, contributor_count
        FROM us_etf_aggregate_flows
        WHERE run_id = ? AND view_name = ? AND direction = ?
        ORDER BY ABS(total_delta_pct) DESC, holding_name ASC
        LIMIT ?
        """,
        (run_id, view_name, direction, limit),
    ).fetchall()


def build_telegram_summary(
    conn: sqlite3.Connection,
    results: list[dict[str, Any]],
    output_path: Path,
    run_id: int,
    config: dict[str, Any],
) -> str:
    ok_count = sum(1 for item in results if item["status"] == "OK")
    error_count = sum(1 for item in results if item["status"] == "ERROR")
    us_etfs = list(config.get("etfs", []))
    kr_etfs = list(config.get("kr_etfs", []))
    active_etfs = etfs_for_view(us_etfs, "active")
    all_etfs = etfs_for_view(us_etfs, "all")
    kr_view_etfs = etfs_for_view(kr_etfs, "kr")

    def format_new_entries(view_etfs: list[dict[str, Any]], limit: int = 10) -> str:
        rows = latest_membership_events_for_view(conn, view_etfs, "NEW")[:limit]
        if not rows:
            return "None"
        return ", ".join(f'{row["ticker"]}:{row["holding_ticker"] or row["holding_name"]}' for row in rows)

    def format_aggregate(direction: str, view_name: str, limit: int = 10) -> str:
        rows = aggregate_flow_rows(conn, run_id, view_name, direction, limit=limit)
        if not rows:
            return "None"
        return ", ".join(
            f'{row["holding_ticker"] or row["holding_name"]} {fmt_delta(row["total_delta_pct"])} ({int(row["contributor_count"])}ETF)'
            for row in rows
        )

    lines = [
        "ETF holdings update",
        f"Collection result: OK {ok_count} / ERROR {error_count}",
        f"Dashboard: {output_path}",
        "",
        "[Active] New entries",
        format_new_entries(active_etfs, 10),
        "",
        "[Active] Aggregate buys Top 10",
        format_aggregate("BUY", "active", 10),
        "",
        "[Active] Aggregate sells Top 10",
        format_aggregate("SELL", "active", 10),
        "",
        "[All] New entries",
        format_new_entries(all_etfs, 10),
        "",
        "[All] Aggregate buys Top 10",
        format_aggregate("BUY", "all", 10),
        "",
        "[All] Aggregate sells Top 10",
        format_aggregate("SELL", "all", 10),
        "",
        "[KR] New entries",
        format_new_entries(kr_view_etfs, 10),
        "",
        "[KR] Aggregate buys Top 10",
        format_aggregate("BUY", "kr", 10),
        "",
        "[KR] Aggregate sells Top 10",
        format_aggregate("SELL", "kr", 10),
    ]
    return "\n".join(lines)


def send_telegram(config: dict[str, Any], message: str, output_path: Path) -> None:
    telegram = config.get("telegram", {})
    token = os.getenv(telegram.get("bot_token_env", "TELEGRAM_BOT_USA_TOKEN"), telegram.get("bot_token", ""))
    chat_id = os.getenv(telegram.get("chat_id_env", "TELEGRAM_CHAT_USA_ID"), str(telegram.get("chat_id", "")))
    if not token or not chat_id:
        raise ValueError("Telegram token/chat id is missing")
    api = f"https://api.telegram.org/bot{token}"
    post_form(f"{api}/sendMessage", {"chat_id": chat_id, "text": message})
    if telegram.get("send_document"):
        post_multipart(f"{api}/sendDocument", {"chat_id": chat_id}, "document", output_path)


def post_form(url: str, fields: dict[str, str]) -> None:
    data = urllib.parse.urlencode(fields).encode()
    request = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(request, timeout=30):
        pass


def post_multipart(url: str, fields: dict[str, str], file_field: str, file_path: Path) -> None:
    boundary = "----CodexBoundary" + hashlib.sha1(str(datetime.now()).encode()).hexdigest()
    chunks: list[bytes] = []
    for key, value in fields.items():
        chunks.extend(
            [
                f"--{boundary}\r\n".encode(),
                f'Content-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode(),
            ]
        )
    content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    chunks.extend(
        [
            f"--{boundary}\r\n".encode(),
            f'Content-Disposition: form-data; name="{file_field}"; filename="{file_path.name}"\r\n'.encode(),
            f"Content-Type: {content_type}\r\n\r\n".encode(),
            file_path.read_bytes(),
            b"\r\n",
            f"--{boundary}--\r\n".encode(),
        ]
    )
    body = b"".join(chunks)
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60):
        pass


def build_html(brand: str, updated: str, view_panels: dict[str, dict[str, Any]]) -> str:
    tab_buttons = "\n".join(
        f'<button class="view-tab{" is-active" if key == "active" else ""}" type="button" data-view-target="{key}">{esc(panel["label"])}</button>'
        for key, panel in view_panels.items()
    )
    tab_panels = "\n".join(
        f"""
    <section class="view-panel{' is-active' if key == 'active' else ''}" data-view-panel="{key}">
      <section class="top-strip">
        <div class="ticker-box"><b>Universe</b><strong>{panel["stats"]["total"]}</strong></div>
        <div class="ticker-box"><b>Exact</b><strong>{panel["stats"]["exact"]}</strong></div>
        <div class="ticker-box"><b>Proxy</b><strong>{panel["stats"]["proxy"]}</strong></div>
        <div class="ticker-box"><b>Collectors Live</b><strong>{panel["stats"]["live"]}</strong></div>
        <div class="ticker-box"><b>Errors</b><strong>{panel["stats"]["errors"]}</strong></div>
        <div class="ticker-box"><b>Pending</b><strong>{panel["stats"]["pending"]}</strong></div>
      </section>
      {panel["event_summary"]}
      {panel["flow_summary"]}
      {panel["sections"]}
    </section>
    """
        for key, panel in view_panels.items()
    )
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{esc(brand)}</title>
  <style>
    :root {{
      --bg: #050608;
      --bg2: #0c0d10;
      --panel: #111318;
      --panel2: #151922;
      --line: #2a2e36;
      --text: #e6e8eb;
      --muted: #9299a6;
      --accent: #ff9f1a;
      --green: #3bd47f;
      --red: #ff5e57;
      --yellow: #ffd166;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--text);
      background: linear-gradient(180deg, #050608 0%, #0a0b0d 100%);
      font-family: Consolas, Arial, "Malgun Gothic", sans-serif;
      letter-spacing: 0;
    }}
    main {{
      width: min(100% - 24px, 1480px);
      margin: 0 auto;
      padding: 18px 0 36px;
    }}
    header {{
      display: flex;
      justify-content: space-between;
      gap: 14px;
      align-items: flex-start;
      padding: 12px 0 16px;
      border-bottom: 1px solid var(--accent);
    }}
    h1, h2, h3, h4, p {{ margin: 0; }}
    h1 {{ font-size: clamp(26px, 3.6vw, 42px); color: var(--accent); text-transform: uppercase; }}
    h2 {{ font-size: 18px; color: var(--accent); text-transform: uppercase; }}
    h3 {{ font-size: 17px; }}
    h4 {{ margin: 14px 0 8px; font-size: 13px; color: var(--accent); text-transform: uppercase; }}
    a {{ color: var(--yellow); text-decoration: none; }}
    .muted, header p, .group-head p, .meta {{ color: var(--muted); }}
    .brand-kicker {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 10px;
      color: var(--accent);
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0;
    }}
    .brand-kicker::before {{
      content: "";
      width: 10px;
      height: 10px;
      background: var(--yellow);
      display: inline-block;
    }}
    .hero-title {{
      font-size: clamp(22px, 3.2vw, 40px);
      line-height: 1.12;
      color: #f3f5f7;
      font-weight: 800;
      margin-bottom: 10px;
      word-break: keep-all;
    }}
    .hero-subtitle {{
      color: var(--muted);
      font-size: 13px;
    }}
    .top-strip {{
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 10px;
      margin-top: 14px;
    }}
    .event-strip {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-top: 10px;
    }}
    .event-box {{
      min-height: 92px;
      padding: 12px;
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 6px;
    }}
    .event-box h3 {{
      margin-bottom: 8px;
      color: var(--accent);
      font-size: 12px;
      text-transform: uppercase;
    }}
    .event-count {{
      display: inline-flex;
      margin-bottom: 10px;
      padding: 3px 7px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #0d1016;
      color: var(--text);
      font-size: 11px;
      font-weight: 700;
    }}
    .event-items {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }}
    .event-chip {{
      display: inline-flex;
      max-width: 100%;
      padding: 4px 7px;
      border-radius: 6px;
      border: 1px solid var(--line);
      background: #0d1016;
      color: var(--muted);
      font-size: 11px;
      line-height: 1.3;
    }}
    .flow-strip {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-top: 10px;
    }}
    .flow-table table {{
      font-size: 11px;
    }}
    .view-switch {{
      display: inline-flex;
      gap: 8px;
      margin-top: 14px;
      padding: 4px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--panel);
    }}
    .view-tab {{
      min-width: 92px;
      padding: 8px 12px;
      border: 1px solid transparent;
      border-radius: 4px;
      background: transparent;
      color: var(--muted);
      font: inherit;
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      cursor: pointer;
    }}
    .view-tab.is-active {{
      color: var(--accent);
      border-color: #4c4c22;
      background: #12130a;
    }}
    .view-panel {{
      display: none;
    }}
    .view-panel.is-active {{
      display: block;
    }}
    .ticker-box {{
      min-height: 74px;
      padding: 12px;
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 6px;
    }}
    .ticker-box b {{
      display: block;
      font-size: 11px;
      color: var(--muted);
      margin-bottom: 8px;
      text-transform: uppercase;
    }}
    .ticker-box strong {{
      font-size: 24px;
      color: var(--text);
    }}
    .group-band {{
      margin-top: 18px;
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--bg2);
    }}
    .group-head {{
      display: flex;
      justify-content: space-between;
      gap: 14px;
      align-items: flex-end;
      margin-bottom: 10px;
    }}
    .terminal-table {{
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--panel);
    }}
    .kpi-grid, .change-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }}
    .kpi {{
      min-height: 70px;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--panel2);
    }}
    .kpi b {{ display: block; color: var(--muted); font-size: 11px; margin-bottom: 8px; text-transform: uppercase; }}
    .kpi strong {{ font-size: 20px; color: var(--text); }}
    .detail-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-top: 12px;
    }}
    .detail-card {{
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--panel);
    }}
    .detail-card.warning {{ border-color: #6d332b; }}
    .detail-card.pending {{ border-color: #5f5122; }}
    .detail-head {{
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: flex-start;
      margin-bottom: 8px;
    }}
    .meta-strip {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 10px 0 12px;
      color: var(--muted);
      font-size: 12px;
    }}
    .meta-strip span {{
      padding: 4px 6px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #0d1016;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }}
    th, td {{
      padding: 8px 7px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }}
    th {{
      color: var(--accent);
      font-size: 11px;
      text-transform: uppercase;
      background: #0d1016;
      position: sticky;
      top: 0;
    }}
    .mini-table {{
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #0d1016;
    }}
    .mini-table h3 {{
      margin: 0;
      padding: 10px;
      border-bottom: 1px solid var(--line);
      font-size: 12px;
      color: var(--accent);
      text-transform: uppercase;
    }}
    .avg-note {{
      padding: 8px 10px;
      border-bottom: 1px solid var(--line);
      color: var(--muted);
      font-size: 11px;
    }}
    .status {{
      display: inline-flex;
      padding: 4px 7px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 700;
      border: 1px solid var(--line);
      text-transform: uppercase;
    }}
    .status.ok {{ color: var(--green); border-color: #1d5b38; }}
    .status.error {{ color: var(--red); border-color: #6d332b; }}
    .status.pending {{ color: var(--yellow); border-color: #5f5122; }}
    .status.stale {{ color: #ffb366; border-color: #8b5a20; background: #191109; }}
    .mode-chip {{
      display: inline-flex;
      margin-top: 6px;
      padding: 3px 6px;
      border-radius: 6px;
      border: 1px solid var(--line);
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0;
    }}
    .mode-chip.exact {{ color: var(--accent); border-color: #4c4c22; background: #12130a; }}
    .mode-chip.proxy {{ color: #7dc8ff; border-color: #244a67; background: #0b1420; }}
    .avg-badge {{
      display: inline-flex;
      margin-left: 6px;
      padding: 2px 5px;
      border-radius: 999px;
      border: 1px solid #5f5122;
      color: var(--yellow);
      font-size: 10px;
      font-weight: 700;
      vertical-align: middle;
      white-space: nowrap;
    }}
    .up {{ color: var(--green); font-weight: 800; }}
    .down {{ color: var(--red); font-weight: 800; }}
    @media (max-width: 980px) {{
      header, .group-head, .detail-head {{ flex-direction: column; align-items: flex-start; }}
      .top-strip, .event-strip, .flow-strip, .kpi-grid, .change-grid, .detail-grid {{ grid-template-columns: 1fr; }}
      main {{ width: min(100% - 20px, 720px); }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <div class="brand-kicker">EUGENE SECURITIES | AHN SANG HYUN | ACTIVE ETF MORNITOR</div>
        <h1>{esc(brand)}</h1>
        <p class="hero-subtitle">Bloomberg-style monitor for U.S. ETF holdings changes across Active, All, and KR views</p>
        <div class="view-switch">
          {tab_buttons}
        </div>
      </div>
      <p>Updated {esc(updated)} KST</p>
    </header>
    {tab_panels}
  </main>
  <script>
    const tabs = Array.from(document.querySelectorAll('[data-view-target]'));
    const panels = Array.from(document.querySelectorAll('[data-view-panel]'));
    for (const tab of tabs) {{
      tab.addEventListener('click', () => {{
        const target = tab.getAttribute('data-view-target');
        for (const item of tabs) item.classList.toggle('is-active', item === tab);
        for (const panel of panels) panel.classList.toggle('is-active', panel.getAttribute('data-view-panel') === target);
      }});
    }}
  </script>
</body>
</html>
"""


def kpi(label: str, value: str) -> str:
    return f'<div class="kpi"><b>{esc(label)}</b><strong>{esc(value)}</strong></div>'


def change_table(title: str, rows: list[dict[str, Any]], average_delta: float | None = None) -> str:
    body = "\n".join(
        f"""
        <tr>
          <td>{esc(row["holding_ticker"] or "-")}</td>
          <td>{esc(row["holding_name"])}{above_average_badge(row.get("delta"), average_delta)}</td>
          <td class="{tone(row["delta"])}">{esc(fmt_delta(row["delta"]))}</td>
        </tr>
        """
        for row in rows
    )
    if not body:
        body = '<tr><td colspan="3" class="muted">비교 데이터 없음</td></tr>'
    average_note = f'<div class="avg-note">Group avg {esc(fmt_delta(average_delta))}</div>' if average_delta is not None else ""
    return f"""
    <div class="mini-table">
      <h3>{esc(title)}</h3>{average_note}
      <table><thead><tr><th>Ticker</th><th>Name</th><th>Delta</th></tr></thead><tbody>{body}</tbody></table>
    </div>
    """


def holdings_slice_sections(rows: list[sqlite3.Row]) -> list[tuple[str, list[sqlite3.Row]]]:
    if not rows:
        return []
    count = len(rows)
    sections: list[tuple[str, list[sqlite3.Row]]] = []
    labels = [
        "Top 25% - Top 5",
        "25-50% - Top 5",
        "50-75% - Top 5",
        "Bottom 25% - Top 5",
    ]
    boundaries = [0, count // 4, count // 2, (count * 3) // 4, count]
    for index, label in enumerate(labels):
        start = boundaries[index]
        end = boundaries[index + 1]
        bucket = rows[start:end]
        sections.append((label, bucket[:5]))
    return sections


def holdings_table(rows: list[sqlite3.Row]) -> str:
    sections = holdings_slice_sections(rows)
    if not sections:
        return '<p class="muted">?? ??? ??</p>'

    tables = []
    for title, section_rows in sections:
        body = "\n".join(
            f"""
            <tr>
              <td>{esc(row["holding_ticker"] or "-")}</td>
              <td>{esc(row["holding_name"])}</td>
              <td>{esc(fmt_pct(row["weight_pct"]))}</td>
              <td>{esc(fmt_number(row["shares"]))}</td>
              <td>{esc(fmt_money(row["market_value"]))}</td>
            </tr>
            """
            for row in section_rows
        )
        tables.append(
            f"""
            <div class="mini-table holdings-slice">
              <h3>{esc(title)}</h3>
              <table>
                <thead><tr><th>Ticker</th><th>Name</th><th>Weight</th><th>Shares</th><th>Market value</th></tr></thead>
                <tbody>{body}</tbody>
              </table>
            </div>
            """
        )
    return f'<div class="change-grid">{"".join(tables)}</div>'


def group_etfs(etfs: list[dict[str, Any]]) -> list[tuple[str, list[dict[str, Any]]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for etf in sorted(etfs, key=lambda item: (item.get("group", ""), int(item.get("rank", 999)), item["ticker"])):
        grouped.setdefault(etf.get("group", "Other"), []).append(etf)
    return list(grouped.items())


def etfs_for_view(etfs: list[dict[str, Any]], view: str) -> list[dict[str, Any]]:
    if view == "kr":
        return list(etfs)
    if view == "all":
        return list(etfs)
    if view == "active":
        return [etf for etf in etfs if str(etf.get("universe_mode", "active")).lower() != "all_only"]
    return list(etfs)


def collector_mode(etf: dict[str, Any]) -> str:
    mode = str(etf.get("data_mode") or "").strip().lower()
    if mode in {"exact", "proxy"}:
        return mode
    return "proxy" if etf.get("source_type") == "trowe_embedded" else "exact"


def mode_chip(mode: str) -> str:
    label = "EXACT" if mode == "exact" else "PROXY"
    return f'<span class="mode-chip {esc(mode)}">{label}</span>'


def render_overview_row(conn: sqlite3.Connection, etf: dict[str, Any], freshest_as_of_date: str | None) -> str:
    ticker = etf["ticker"].upper()
    mode = collector_mode(etf)
    current = latest_snapshot(conn, ticker, ok_only=False)
    status_label = "PENDING"
    status_class = "pending"
    as_of = "-"
    rows = "-"
    top_move = "-"
    if etf.get("enabled", True):
        if current and current["status"] == "OK":
            status_label = "LIVE"
            status_class = "ok"
            as_of = str(current["as_of_date"])
            rows = str(int(current["row_count"]))
            if is_source_date_stale(as_of, freshest_as_of_date):
                status_label = "SOURCE DATE STALE"
                status_class = "stale"
            previous = previous_snapshot(conn, ticker, int(current["id"]), str(current["as_of_date"]))
            if previous:
                changes = holding_changes(conn, int(current["id"]), int(previous["id"]))
                first = next((change for change in changes if change["delta"] is not None), None)
                if first:
                    top_move = f'{first["holding_ticker"] or first["holding_name"]} {fmt_delta(first["delta"])}'
            else:
                top_move = "First snapshot"
        elif current and current["status"] != "OK":
            status_label = "ERROR"
            status_class = "error"
            as_of = str(current["as_of_date"])
        else:
            status_label = "QUEUED"
    return f"""
    <tr>
      <td>{int(etf.get("rank", 0))}</td>
      <td>{esc(ticker)}</td>
      <td>{mode_chip(mode)}</td>
      <td>{esc(etf.get("issuer", ""))}</td>
      <td>{esc(etf.get("strategy", ""))}</td>
      <td>{esc(etf.get("feature", ""))}</td>
      <td><span class="status {status_class}">{esc(status_label)}</span></td>
      <td>{esc(as_of)}</td>
      <td>{esc(rows)}</td>
      <td class="{tone(parse_overview_delta(top_move))}">{esc(top_move)}</td>
    </tr>
    """


def dashboard_stats(conn: sqlite3.Connection, etfs: list[dict[str, Any]]) -> dict[str, int]:
    total = len(etfs)
    live = 0
    errors = 0
    pending = 0
    exact = 0
    proxy = 0
    for etf in etfs:
        if collector_mode(etf) == "proxy":
            proxy += 1
        else:
            exact += 1
        ticker = etf["ticker"].upper()
        current = latest_snapshot(conn, ticker, ok_only=False)
        if not etf.get("enabled", True):
            pending += 1
        elif current and current["status"] == "OK":
            live += 1
        elif current and current["status"] != "OK":
            errors += 1
        else:
            pending += 1
    return {"total": total, "live": live, "errors": errors, "pending": pending, "exact": exact, "proxy": proxy}


def latest_membership_events_for_view(conn: sqlite3.Connection, etfs: list[dict[str, Any]], event_type: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for etf in etfs:
        ticker = etf["ticker"].upper()
        current = latest_snapshot(conn, ticker, ok_only=True)
        if not current:
            continue
        events = conn.execute(
            """
            SELECT ticker, holding_ticker, holding_name, event_type, delta_pct, as_of_date
            FROM us_etf_membership_events
            WHERE current_snapshot_id = ? AND event_type = ?
            ORDER BY ABS(COALESCE(delta_pct, 0)) DESC, holding_name ASC
            """,
            (int(current["id"]), event_type),
        ).fetchall()
        rows.extend(dict(row) for row in events)
    rows.sort(key=lambda item: (abs(item.get("delta_pct") or 0), item["ticker"], item["holding_name"]), reverse=True)
    return rows


def event_summary_box(title: str, rows: list[dict[str, Any]], event_type: str) -> str:
    chips = "".join(
        f'<span class="event-chip">{esc(row["ticker"])}: {esc(row["holding_ticker"] or row["holding_name"])}</span>'
        for row in rows[:16]
    ) or '<span class="event-chip">없음</span>'
    return f"""
    <div class="event-box">
      <h3>{esc(title)}</h3>
      <div class="event-count">{len(rows)}건</div>
      <div class="event-items">{chips}</div>
    </div>
    """


def render_view_event_summary(conn: sqlite3.Connection, etfs: list[dict[str, Any]]) -> str:
    new_rows = latest_membership_events_for_view(conn, etfs, "NEW")
    removed_rows = latest_membership_events_for_view(conn, etfs, "REMOVED")
    return f"""
    <section class="event-strip">
      {event_summary_box("신규 편입 종목", new_rows, "NEW")}
      {event_summary_box("편출 종목", removed_rows, "REMOVED")}
    </section>
    """


def flow_table(title: str, rows: list[sqlite3.Row]) -> str:
    body = "\n".join(
        f"""
        <tr>
          <td>{esc(row["holding_ticker"] or "-")}</td>
          <td>{esc(row["holding_name"])}</td>
          <td class="{tone(row["total_delta_pct"])}">{esc(fmt_delta(row["total_delta_pct"]))}</td>
          <td>{int(row["contributor_count"])}</td>
        </tr>
        """
        for row in rows
    ) or '<tr><td colspan="4" class="muted">데이터 없음</td></tr>'
    return f"""
    <div class="mini-table flow-table">
      <h3>{esc(title)}</h3>
      <table>
        <thead><tr><th>Ticker</th><th>Name</th><th>Sum Delta</th><th>ETFs</th></tr></thead>
        <tbody>{body}</tbody>
      </table>
    </div>
    """


def render_view_flow_summary(conn: sqlite3.Connection, run_id: int, view_name: str) -> str:
    if not run_id:
        return ""
    buy_rows = aggregate_flow_rows(conn, run_id, view_name, "BUY")
    sell_rows = aggregate_flow_rows(conn, run_id, view_name, "SELL")
    return f"""
    <section class="flow-strip">
      {flow_table("합산 매수 Top 20", buy_rows)}
      {flow_table("합산 매도 Top 20", sell_rows)}
    </section>
    """


def parse_overview_delta(text: str) -> float | None:
    match = re.search(r"([+-][0-9]+(?:\.[0-9]+)?)%p", text)
    return float(match.group(1)) if match else None


def normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9%]+", "", value.lower().replace("$", ""))


def first_value(lookup: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = lookup.get(normalize_header(key))
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def parse_number(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"-", "--", "N/A", "n/a"}:
        return None
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1].strip()
    cleaned = text.replace(",", "").replace("$", "")
    cleaned = re.sub(r"[^0-9eE.+\-]", "", cleaned)
    if cleaned in {"", "-", ".", "-.", "+", "+."}:
        return None
    try:
        number = float(cleaned)
        return -number if negative else number
    except ValueError:
        return None


def parse_as_of_date(text: str) -> str | None:
    patterns = [
        r"as of[:\s]+([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
        r"as of[:\s]+([A-Za-z]+ [0-9]{1,2}, [0-9]{4})",
        r"as of[:\s]+([0-9]{4}-[0-9]{2}-[0-9]{2})",
        r"as of[^0-9A-Za-z]+([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
        r"as of[^0-9A-Za-z]+([A-Za-z]+ [0-9]{1,2}, [0-9]{4})",
        r"as of[^0-9A-Za-z]+([0-9]{4}-[0-9]{2}-[0-9]{2})",
        r"(?:asOfDate|etfHoldingsAsOfDate)[\"']?\s*[:=]\s*[\"']([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
        r"(?:asOfDate|etfHoldingsAsOfDate)[\"']?\s*[:=]\s*[\"']([0-9]{4}-[0-9]{2}-[0-9]{2})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        raw = match.group(1)
        for fmt in ("%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass
    return None


def infer_as_of_from_rows(rows: list[dict[str, Any]]) -> str | None:
    for row in rows[:10]:
        lookup = {normalize_header(key): value for key, value in row.items()}
        value = first_value(lookup, "date", "asofdate", "holdingsasofdate")
        if not value:
            continue
        for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass
    return None


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def is_total_row(name: str) -> bool:
    return bool(re.search(r"^(total|cash and|other assets|net assets)", name.strip(), flags=re.IGNORECASE))


def empty_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text or None


def make_holding_key(ticker: str | None, cusip: str | None, isin: str | None, name: str) -> str:
    raw = ticker or cusip or isin or name
    return re.sub(r"[^A-Z0-9]+", "", raw.upper())[:80]


def fmt_delta(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.2f}%p"


def average_delta(rows: list[dict[str, Any]]) -> float | None:
    values = [float(row["delta"]) for row in rows if row.get("delta") is not None]
    if not values:
        return None
    return sum(values) / len(values)


def above_average_badge(delta: float | None, average: float | None) -> str:
    if delta is None or average is None:
        return ""
    is_above = delta >= average if average >= 0 else delta <= average
    if not is_above:
        return ""
    return ' <span class="avg-badge">Above Avg</span>'


def fmt_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}%"


def fmt_number(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:,.0f}"


def fmt_money(value: float | None) -> str:
    if value is None:
        return "-"
    return f"${value:,.0f}"


def fmt_checked_at(value: str | None) -> str:
    if not value:
        return "-"
    try:
        dt = datetime.fromisoformat(str(value))
        return dt.astimezone(KST).strftime("%Y-%m-%d %H:%M KST")
    except ValueError:
        return str(value)


def tone(value: float | None) -> str:
    if value is None:
        return ""
    return "up" if value > 0 else "down" if value < 0 else ""


def esc(value: Any) -> str:
    return html.escape(str(value), quote=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Track daily US ETF holdings weight changes")
    parser.add_argument("command", choices=["init", "update", "render"])
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config = load_config(args.config)
    conn = connect(config["database_path"])
    init_db(conn)
    if args.command == "init":
        print(f"initialized: {config['database_path']}")
        return 0
    if args.command == "render":
        path = render_dashboard(conn, config)
        print(f"rendered: {path}")
        return 0
    result = update_all(config)
    print(result["summary"])
    return 0 if result["status"] == "OK" else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (urllib.error.URLError, TimeoutError) as exc:
        print(f"Network error: {exc}", file=sys.stderr)
        raise SystemExit(2)
