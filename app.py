"""
Finans Takip — Borsa MCP ile güçlendirilmiş portföy takip uygulaması.
Borsa MCP Remote: https://borsamcp.fastmcp.app/mcp  (MCP Streamable HTTP)
Python 3.9+ uyumlu.
"""
from __future__ import annotations

import json
import asyncio
import uuid
import calendar
import hmac
import hashlib
import os
import secrets as _secrets
from contextlib import asynccontextmanager
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, List, Any, Dict

import httpx
from fastapi import FastAPI, HTTPException, Query, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

# ─────────────────────────────────────────────────────────────────────
# MCP Streamable HTTP Client  (no 3rd-party MCP library needed)
# ─────────────────────────────────────────────────────────────────────

MCP_URL = "https://borsamcp.fastmcp.app/mcp"

# ─── Auth ────────────────────────────────────────────────────────────
# SITE_PASSWORD env var ile koruma. Boş bırakılırsa (geliştirme), auth devre dışı.
SITE_PASSWORD = os.environ.get("SITE_PASSWORD", "")
_SECRET_KEY = os.environ.get("SECRET_KEY") or _secrets.token_hex(32)

ALLOWED_EXCHANGES = {"btcturk", "coinbase", "binance", "kucoin"}


def _session_token() -> str:
    return hmac.new(_SECRET_KEY.encode(), SITE_PASSWORD.encode(), hashlib.sha256).hexdigest()


def _is_authenticated(request: Request) -> bool:
    if not SITE_PASSWORD:
        return True
    session = request.cookies.get("session")
    return bool(session and hmac.compare_digest(session, _session_token()))


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path in ("/login", "/logout") or path.startswith("/static"):
            return await call_next(request)
        if not _is_authenticated(request):
            if path.startswith("/api/"):
                return JSONResponse({"detail": "Giriş gerekli"}, status_code=401)
            return RedirectResponse(url="/login", status_code=302)
        return await call_next(request)


_LOGIN_HTML = """<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Giriş — Finans Takip</title>
<style>
*,*::before,*::after{box-sizing:border-box}
body{font-family:system-ui,sans-serif;background:#080d14;color:#e2e8f0;
  display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}
.card{background:#0d1829;border:1px solid #1e2d45;border-radius:14px;padding:2rem;width:100%;max-width:360px}
h1{font-size:1.25rem;font-weight:600;margin:0 0 1.5rem;color:#f1f5f9}
input{width:100%;padding:.75rem 1rem;background:#0a1628;border:1px solid #1e2d45;
  border-radius:8px;color:#e2e8f0;font-size:1rem;margin-bottom:1rem;outline:none}
input:focus{border-color:#3b82f6}
button{width:100%;padding:.75rem;background:#3b82f6;color:#fff;border:none;
  border-radius:8px;font-size:1rem;font-weight:600;cursor:pointer}
button:hover{background:#2563eb}
.err{color:#f87171;font-size:.875rem;margin-top:.75rem}
</style>
</head>
<body>
<div class="card">
  <h1>📈 Finans Takip</h1>
  <form method="post" action="/login">
    <input type="password" name="password" placeholder="Şifre" autofocus autocomplete="current-password">
    <button type="submit">Giriş Yap</button>
    {error}
  </form>
</div>
</body>
</html>"""


class MCPSession:
    """Thin MCP Streamable-HTTP client.

    Handles:  initialize → notifications/initialized → tools/call
    Supports both inline-JSON and text/event-stream server responses.
    """

    def __init__(self, url: str) -> None:
        self.url = url
        self.session_id: Optional[str] = None
        self._http: Optional[httpx.AsyncClient] = None
        self._ready = False

    # ── lifecycle ──

    async def start(self) -> None:
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10, read=90, write=30, pool=10),
            follow_redirects=True,
        )
        try:
            await self._initialize()
            self._ready = True
        except Exception as exc:
            self._ready = False
            raise RuntimeError(f"MCP init hatası: {exc}") from exc

    async def stop(self) -> None:
        if self._http:
            try:
                if self.session_id:
                    await self._http.delete(
                        self.url, headers={"mcp-session-id": self.session_id}
                    )
            except Exception:
                pass
            await self._http.aclose()
            self._http = None
        self._ready = False

    # ── internal ──

    async def _initialize(self) -> None:
        await self._post(
            {
                "jsonrpc": "2.0",
                "id": "init-1",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "finans-takip", "version": "1.0.0"},
                },
            }
        )
        # Notify server that client is ready (no response expected)
        await self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})

    async def _post(self, payload: dict) -> dict:
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self.session_id:
            headers["mcp-session-id"] = self.session_id

        assert self._http is not None
        resp = await self._http.post(self.url, json=payload, headers=headers)
        resp.raise_for_status()

        new_sid = resp.headers.get("mcp-session-id")
        if new_sid:
            self.session_id = new_sid

        if not resp.content:
            return {}

        ct = resp.headers.get("content-type", "")
        if "text/event-stream" in ct:
            return _parse_sse(resp.text)
        try:
            return resp.json()
        except Exception:
            return {}

    # ── public ──

    async def call_tool(self, name: str, args: dict) -> dict:
        if not self._ready:
            raise RuntimeError("MCP oturumu hazır değil")

        resp = await self._post(
            {
                "jsonrpc": "2.0",
                "id": str(uuid.uuid4()),
                "method": "tools/call",
                "params": {"name": name, "arguments": args},
            }
        )

        if "error" in resp:
            msg = resp["error"].get("message", "Bilinmeyen MCP hatası")
            raise ValueError(f"MCP[{name}]: {msg}")

        result = resp.get("result", {})
        content = result.get("content", [])
        if content and isinstance(content[0], dict):
            text: str = content[0].get("text", "")
            if text:
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    return {"raw": text}
        return result or resp


def _parse_sse(text: str) -> dict:
    """Return the first JSON-RPC payload found in an SSE stream."""
    for line in text.splitlines():
        if line.startswith("data: "):
            try:
                return json.loads(line[6:])
            except json.JSONDecodeError:
                continue
    return {}


# ─────────────────────────────────────────────────────────────────────
# App state
# ─────────────────────────────────────────────────────────────────────

mcp: Optional[MCPSession] = None
DATA_DIR = Path(__file__).parent / "data"
WATCHLIST_FILE = DATA_DIR / "watchlist.json"
STATIC_DIR = Path(__file__).parent / "static"

CRYPTO_UNIVERSE = [
    {"symbol": "BTC", "name": "Bitcoin"},
    {"symbol": "ETH", "name": "Ethereum"},
    {"symbol": "SOL", "name": "Solana"},
    {"symbol": "BNB", "name": "BNB"},
    {"symbol": "XRP", "name": "XRP"},
    {"symbol": "DOGE", "name": "Dogecoin"},
    {"symbol": "ADA", "name": "Cardano"},
    {"symbol": "AVAX", "name": "Avalanche"},
    {"symbol": "LINK", "name": "Chainlink"},
    {"symbol": "DOT", "name": "Polkadot"},
    {"symbol": "TRX", "name": "TRON"},
    {"symbol": "MATIC", "name": "Polygon"},
    {"symbol": "LTC", "name": "Litecoin"},
    {"symbol": "BCH", "name": "Bitcoin Cash"},
    {"symbol": "UNI", "name": "Uniswap"},
    {"symbol": "ATOM", "name": "Cosmos"},
    {"symbol": "ETC", "name": "Ethereum Classic"},
    {"symbol": "FIL", "name": "Filecoin"},
    {"symbol": "APT", "name": "Aptos"},
    {"symbol": "ARB", "name": "Arbitrum"},
    {"symbol": "OP", "name": "Optimism"},
    {"symbol": "NEAR", "name": "NEAR Protocol"},
    {"symbol": "INJ", "name": "Injective"},
    {"symbol": "ICP", "name": "Internet Computer"},
    {"symbol": "AAVE", "name": "Aave"},
    {"symbol": "MKR", "name": "Maker"},
    {"symbol": "SUI", "name": "Sui"},
    {"symbol": "SEI", "name": "Sei"},
    {"symbol": "RNDR", "name": "Render"},
    {"symbol": "FET", "name": "Fetch.ai"},
]

# ─── In-memory price cache ───────────────────────────────────────────
import time as _time
_prices_cache: Dict[str, Any] = {}   # {symbol_market: {price, change_pct, ...}}
_prices_cache_ts: float = 0.0        # epoch seconds of last full refresh
_CACHE_TTL = 70                       # seconds — just over frontend's 60s interval


@asynccontextmanager
async def lifespan(app: FastAPI):
    global mcp
    mcp = MCPSession(MCP_URL)
    try:
        await mcp.start()
    except Exception as e:
        print(f"[UYARI] MCP bağlantısı kurulamadı: {e}")
        print("        Uygulama yine de çalışacak ama MCP araçları devre dışı.")
    yield
    if mcp:
        await mcp.stop()


app = FastAPI(
    title="Finans Takip",
    version="1.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
app.add_middleware(AuthMiddleware)


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

async def _ensure_mcp() -> MCPSession:
    """Return a ready MCPSession, reconnecting automatically if needed."""
    global mcp
    if mcp is None:
        mcp = MCPSession(MCP_URL)
    if not mcp._ready:
        try:
            await mcp.start()
        except Exception as exc:
            raise HTTPException(503, f"Borsa MCP bağlantısı kurulamadı: {exc}")
    return mcp


async def call_mcp(tool: str, args: dict) -> dict:
    client = await _ensure_mcp()
    try:
        return await client.call_tool(tool, args)
    except ValueError as exc:
        raise HTTPException(502, str(exc))
    except Exception as exc:
        # Session may have expired — reinitialize and retry once
        try:
            await client._initialize()
            return await client.call_tool(tool, args)
        except Exception:
            client._ready = False  # mark for full reconnect next call
            raise HTTPException(502, f"Borsa MCP hatası: {exc}")


async def call_mcp_safe(tool: str, args: dict, timeout: float = 10.0) -> dict:
    """call_mcp with a per-call timeout; returns {} on timeout/error."""
    try:
        return await asyncio.wait_for(call_mcp(tool, args), timeout=timeout)
    except (asyncio.TimeoutError, Exception):
        return {}


def load_watchlist() -> List[dict]:
    DATA_DIR.mkdir(exist_ok=True)
    if not WATCHLIST_FILE.exists():
        WATCHLIST_FILE.write_text("[]", encoding="utf-8")
    return json.loads(WATCHLIST_FILE.read_text(encoding="utf-8"))


def save_watchlist(data: List[dict]) -> None:
    WATCHLIST_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


# ─────────────────────────────────────────────────────────────────────
# Watchlist endpoints
# ─────────────────────────────────────────────────────────────────────


class AddAssetRequest(BaseModel):
    symbol: str
    market: str          # bist | us | fund | crypto_tr | crypto_global
    name: Optional[str] = None
    exchange: Optional[str] = "btcturk"


@app.get("/api/watchlist")
async def get_watchlist():
    return load_watchlist()


@app.post("/api/watchlist")
async def add_asset(req: AddAssetRequest):
    global _prices_cache, _prices_cache_ts
    watchlist = load_watchlist()
    sym = req.symbol.upper().strip()
    exchange = req.exchange or ("coinbase" if req.market == "crypto_global" else "btcturk")
    if req.market == "crypto_global":
        exchange = "coinbase"
    for item in watchlist:
        if item["symbol"] == sym and item["market"] == req.market:
            raise HTTPException(400, f"{sym} zaten listede")
    entry = {
        "symbol": sym,
        "market": req.market,
        "name": req.name or sym,
        "exchange": exchange,
    }
    watchlist.append(entry)
    save_watchlist(watchlist)
    _prices_cache = {}
    _prices_cache_ts = 0.0
    return {"success": True, "entry": entry}


@app.delete("/api/watchlist/{symbol}")
async def remove_asset(symbol: str, market: str = Query(...)):
    global _prices_cache, _prices_cache_ts
    sym = symbol.upper().strip()
    watchlist = load_watchlist()
    new = [w for w in watchlist if not (w["symbol"].upper() == sym and w["market"] == market)]
    if len(new) == len(watchlist):
        raise HTTPException(404, "Varlık bulunamadı")
    save_watchlist(new)
    _prices_cache = {}
    _prices_cache_ts = 0.0
    return {"success": True}


# ─────────────────────────────────────────────────────────────────────
# Portfolio  (alış miktarı & maliyet takibi)
# ─────────────────────────────────────────────────────────────────────

PORTFOLIO_FILE = DATA_DIR / "portfolio.json"


def load_portfolio() -> List[dict]:
    DATA_DIR.mkdir(exist_ok=True)
    if not PORTFOLIO_FILE.exists():
        PORTFOLIO_FILE.write_text("[]", encoding="utf-8")
    return json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8"))


def save_portfolio(data: List[dict]) -> None:
    PORTFOLIO_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


class PortfolioPosition(BaseModel):
    symbol: str
    market: str
    name: Optional[str] = None
    quantity: float            # lot / adet
    avg_price: float           # ortalama alış fiyatı
    purchase_date: Optional[str] = None  # YYYY-MM-DD


@app.get("/api/portfolio")
async def get_portfolio():
    return load_portfolio()


@app.post("/api/portfolio")
async def add_position(req: PortfolioPosition):
    portfolio = load_portfolio()
    sym = req.symbol.upper().strip()
    # Aynı sembol + market varsa üzerine güncelle (ortalama maliyet)
    for pos in portfolio:
        if pos["symbol"] == sym and pos["market"] == req.market:
            old_qty   = pos["quantity"]
            old_price = pos["avg_price"]
            new_qty   = old_qty + req.quantity
            pos["avg_price"]      = (old_qty * old_price + req.quantity * req.avg_price) / new_qty
            pos["quantity"]       = new_qty
            pos["purchase_date"]  = req.purchase_date or pos.get("purchase_date")
            pos["name"]           = req.name or pos.get("name", sym)
            save_portfolio(portfolio)
            return {"success": True, "position": pos}
    entry = {
        "id": str(uuid.uuid4()),
        "symbol": sym,
        "market": req.market,
        "name": req.name or sym,
        "quantity": req.quantity,
        "avg_price": req.avg_price,
        "purchase_date": req.purchase_date,
    }
    portfolio.append(entry)
    save_portfolio(portfolio)
    return {"success": True, "position": entry}


@app.delete("/api/portfolio/{position_id}")
async def delete_position(position_id: str):
    portfolio = load_portfolio()
    new = [p for p in portfolio if p.get("id") != position_id]
    if len(new) == len(portfolio):
        raise HTTPException(404, "Pozisyon bulunamadı")
    save_portfolio(new)
    return {"success": True}


@app.put("/api/portfolio/{position_id}")
async def update_position(position_id: str, req: PortfolioPosition):
    portfolio = load_portfolio()
    sym = req.symbol.upper().strip()
    for pos in portfolio:
        if (
            pos.get("id") != position_id
            and pos.get("symbol") == sym
            and pos.get("market") == req.market
        ):
            raise HTTPException(400, f"{sym} için bu piyasada başka bir pozisyon var")
    for pos in portfolio:
        if pos.get("id") == position_id:
            pos["symbol"]        = sym
            pos["market"]        = req.market
            pos["quantity"]      = req.quantity
            pos["avg_price"]     = req.avg_price
            pos["purchase_date"] = req.purchase_date or pos.get("purchase_date")
            pos["name"]          = req.name or pos.get("name", sym)
            save_portfolio(portfolio)
            return {"success": True, "position": pos}
    raise HTTPException(404, "Pozisyon bulunamadı")


# ─────────────────────────────────────────────────────────────────────
# Prices  (batch)
# ─────────────────────────────────────────────────────────────────────


def _fund_payload_has_price(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    fund = payload.get("fund") if isinstance(payload.get("fund"), dict) else {}
    if _safe_float(fund.get("price")) is not None:
        return True
    recent = payload.get("recent_prices")
    return isinstance(recent, list) and any(_safe_float(x.get("price")) is not None for x in recent if isinstance(x, dict))


async def _get_fund_data_robust(symbol: str, include_portfolio: bool = False) -> dict:
    sym = symbol.upper().strip()
    attempts = []
    if include_portfolio:
        attempts.append({"symbol": sym, "include_performance": True, "include_portfolio": True})
    attempts.append({"symbol": sym, "include_performance": True})
    attempts.append({"symbol": sym, "include_performance": False})

    last: dict = {}
    for args in attempts:
        data = await call_mcp_safe("get_fund_data", args, timeout=25)
        if isinstance(data, dict) and data:
            last = data
            if _fund_payload_has_price(data):
                return data
    return last


async def _fetch_all_prices(watchlist: list) -> list:
    """Fetch prices for every watchlist item. May be slow (~25s on cold MCP start)."""
    bist  = [w for w in watchlist if w["market"] == "bist"]
    us    = [w for w in watchlist if w["market"] == "us"]
    funds = [w for w in watchlist if w["market"] == "fund"]
    ctr   = [w for w in watchlist if w["market"] == "crypto_tr"]
    cgl   = [w for w in watchlist if w["market"] == "crypto_global"]

    tasks: Dict[str, Any] = {}

    if bist:
        syms = [w["symbol"] for w in bist][:10]
        tasks["bist"] = call_mcp_safe("get_quick_info", {"symbol": syms, "market": "bist"}, timeout=30)
    if us:
        syms = [w["symbol"] for w in us][:10]
        tasks["us"] = call_mcp_safe("get_quick_info", {"symbol": syms, "market": "us"}, timeout=30)

    for w in bist + us:
        tasks[f"hist_{w['symbol']}"] = call_mcp_safe(
            "get_historical_data",
            {"symbol": w["symbol"], "market": w["market"], "period": "5d"},
            timeout=30,
        )
        tasks[f"profile_{w['symbol']}"] = call_mcp_safe(
            "get_profile",
            {"symbol": w["symbol"], "market": w["market"]},
            timeout=15,
        )

    for w in funds:
        tasks[f"fund_{w['symbol']}"] = call_mcp_safe(
            "get_fund_data", {"symbol": w["symbol"], "include_performance": True}, timeout=30
        )

    for w in ctr:
        pair = _crypto_pair(w["symbol"], "crypto_tr")
        tasks[f"ctr_{w['symbol']}"] = call_mcp_safe(
            "get_crypto_market",
            {"symbol": pair, "exchange": w.get("exchange", "btcturk"), "data_type": "ticker"},
            timeout=15,
        )
        tasks[f"chist_{w['symbol']}"] = call_mcp_safe(
            "get_historical_data",
            {"symbol": pair, "market": "crypto_tr", "period": "5d"},
            timeout=15,
        )
    for w in cgl:
        pair = _crypto_pair(w["symbol"], "crypto_global")
        tasks[f"cgl_{w['symbol']}"] = call_mcp_safe(
            "get_crypto_market",
            {"symbol": pair, "exchange": "coinbase", "data_type": "ticker"},
            timeout=15,
        )
        tasks[f"chist_{w['symbol']}"] = call_mcp_safe(
            "get_historical_data",
            {"symbol": pair, "market": "crypto_global", "period": "5d"},
            timeout=15,
        )

    keys = list(tasks.keys())
    raw = await asyncio.gather(*tasks.values(), return_exceptions=True)
    results = {k: (v if not isinstance(v, Exception) else {}) for k, v in zip(keys, raw)}
    for w in funds:
        key = f"fund_{w['symbol']}"
        if not _fund_payload_has_price(results.get(key)):
            results[key] = await _get_fund_data_robust(w["symbol"])
    return [{**w, **_extract_price(w, results)} for w in watchlist]


@app.get("/api/prices")
async def get_prices():
    global _prices_cache, _prices_cache_ts
    watchlist = load_watchlist()
    if not watchlist:
        return []

    now = _time.time()
    # Return cached data immediately if fresh enough
    wl_keys = {f"{w['symbol']}_{w['market']}" for w in watchlist}
    cache_keys = set(_prices_cache.keys())
    cache_covers = wl_keys.issubset(cache_keys)

    if cache_covers:
        cached = [{**w, **_prices_cache.get(f"{w['symbol']}_{w['market']}", {})} for w in watchlist]
        # Kick off background refresh if cache is stale
        if (now - _prices_cache_ts) >= _CACHE_TTL:
            asyncio.create_task(_refresh_prices_cache(watchlist))
        return cached

    # No cache — fetch now (blocking, but only happens once per server start)
    fresh = await _fetch_all_prices(watchlist)
    _prices_cache = {f"{r['symbol']}_{r['market']}": {k: v for k, v in r.items() if k not in ('symbol','market','exchange')} for r in fresh}
    _prices_cache_ts = now
    return fresh


async def _refresh_prices_cache(watchlist: list) -> None:
    """Background task to refresh price cache without blocking callers."""
    global _prices_cache, _prices_cache_ts
    try:
        fresh = await _fetch_all_prices(watchlist)
        _prices_cache = {f"{r['symbol']}_{r['market']}": {k: v for k, v in r.items() if k not in ('symbol','market','exchange')} for r in fresh}
        _prices_cache_ts = _time.time()
    except Exception:
        pass


def _change_from_hist(hist: dict) -> Optional[float]:
    """Compute daily change% from last 2 data points of historical response."""
    pts = hist.get("data", [])
    if len(pts) >= 2:
        prev = _safe_float(pts[-2].get("close"))
        curr = _safe_float(pts[-1].get("close"))
        if prev and curr and prev != 0:
            return (curr - prev) / prev * 100
    return None


def _latest_hist_bar(hist: dict) -> dict:
    """Return latest usable historical bar from an MCP history response."""
    pts = hist.get("data", []) if isinstance(hist, dict) else []
    clean = [p for p in pts if isinstance(p, dict) and _safe_float(p.get("close")) is not None]
    if not clean:
        return {}
    clean.sort(key=lambda p: str(p.get("date", "")))
    return clean[-1]


def _quote_from_history(symbol: str, market: str, hist: dict) -> dict:
    latest = _latest_hist_bar(hist)
    if not latest:
        return {}
    price = _safe_float(latest.get("close"))
    change = _change_from_hist(hist)
    return {
        "symbol": symbol.upper().strip(),
        "market": market,
        "current_price": price,
        "price": price,
        "change_percent": change,
        "change_pct": change,
        "volume": latest.get("volume"),
        "price_time": str(latest.get("date", ""))[:10],
        "source": "history_fallback",
    }


def _safe_float(val: Any) -> Optional[float]:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _crypto_pair(symbol: str, market: str) -> str:
    sym = symbol.upper().strip()
    if market == "crypto_global":
        if "-" in sym:
            return sym
        if sym.endswith("USD"):
            return f"{sym[:-3]}-USD"
        return f"{sym}-USD"
    if market == "crypto_tr":
        if sym.endswith("TRY"):
            return sym
        return f"{sym}TRY"
    return sym


def _crypto_base_symbol(symbol: str) -> str:
    sym = symbol.upper().strip()
    if sym.endswith("-USD"):
        return sym[:-4]
    if sym.endswith("TRY"):
        return sym[:-3]
    if sym.endswith("USD"):
        return sym[:-3]
    return sym


def _extract_ticker(raw: dict) -> dict:
    if not isinstance(raw, dict):
        return {}
    ticker = raw.get("ticker")
    if isinstance(ticker, dict):
        return ticker
    tickers = raw.get("tickers")
    if isinstance(tickers, list) and tickers and isinstance(tickers[0], dict):
        return tickers[0]
    return {}


def _latest_change_from_history(history: dict) -> Optional[float]:
    bars = history.get("data", []) if isinstance(history, dict) else []
    clean = [b for b in bars if isinstance(b, dict) and _safe_float(b.get("close")) is not None]
    if len(clean) < 2:
        return None
    clean.sort(key=lambda b: str(b.get("date", "")))
    current = _safe_float(clean[-1].get("close"))
    previous = None
    if len(clean) >= 25 and "T" in str(clean[-1].get("date", "")):
        previous = _safe_float(clean[-25].get("close"))
    if previous is None:
        previous = _safe_float(clean[-2].get("close"))
    if current is None or previous in (None, 0):
        return None
    return (current - previous) / previous * 100


def _find_in_data(data: dict, symbol: str) -> dict:
    """Find symbol entry in borsa-mcp response.

    Actual shapes:
      Single: {"data": {symbol fields…}}
      Batch:  {"data": [{symbol fields…}, …]}
      Fund:   {"fund": {fund fields…}}
      Crypto: {"ticker": {ticker fields…}}
    """
    sym_upper = symbol.upper()

    # data list (batch)
    container = data.get("data")
    if isinstance(container, list):
        for item in container:
            if isinstance(item, dict) and item.get("symbol", "").upper() == sym_upper:
                return item
        return {}
    # data dict (single symbol)
    if isinstance(container, dict) and container.get("symbol", "").upper() == sym_upper:
        return container
    # Legacy / fallback shapes
    for k in ("symbols", "results", "stocks", "securities"):
        c = data.get(k)
        if isinstance(c, list):
            for item in c:
                if isinstance(item, dict) and item.get("symbol", "").upper() == sym_upper:
                    return item
        if isinstance(c, dict):
            for key in (symbol, sym_upper):
                if isinstance(c.get(key), dict):
                    return c[key]
    return {}


def _extract_price(w: dict, results: dict) -> dict:
    sym, market = w["symbol"], w["market"]
    base: Dict[str, Any] = {"price": None, "change_pct": None, "currency": "TRY"}
    try:
        if market in ("bist", "us"):
            raw = results.get("bist" if market == "bist" else "us", {})
            d = _find_in_data(raw, sym)
            price = _safe_float(
                d.get("current_price") or d.get("currentPrice") or
                d.get("regularMarketPrice") or d.get("price") or d.get("son")
            )
            hist = results.get(f"hist_{sym}", {})
            if price is None:
                price = _safe_float(_latest_hist_bar(hist).get("close"))
            # change_percent not in quick_info — compute from 5-day history
            chg = _safe_float(
                d.get("change_percent") or d.get("regularMarketChangePercent") or
                d.get("changePercent") or d.get("change_pct") or d.get("gunlukDegisim")
            ) or _change_from_hist(hist)
            currency = d.get("currency", "TRY" if market == "bist" else "USD")
            profile_raw = results.get(f"profile_{sym}", {})
            profile = profile_raw.get("profile", {}) if isinstance(profile_raw, dict) else {}
            raw_name = d.get("longName") or d.get("shortName") or d.get("name") or profile.get("name") or w["name"]
            full_name = raw_name
            if str(full_name or "").upper() == str(sym).upper():
                full_name = profile.get("description") or profile.get("sector") or raw_name
            # Son fiyat saati: tarihsel verinin son barının tarihi + fetch saati
            import datetime as _dt
            hist_bars = results.get(f"hist_{sym}", {}).get("data", [])
            last_bar_date = hist_bars[-1].get("date", "") if hist_bars else ""
            fetch_time = _dt.datetime.now().strftime("%H:%M")
            price_time = f"{last_bar_date[:10]} {fetch_time}" if last_bar_date else fetch_time
            return {
                "price": price,
                "change_pct": chg,
                "currency": currency,
                "name": raw_name,
                "full_name": full_name,
                "sector": profile.get("sector") or profile.get("industry"),
                "description": profile.get("description"),
                "price_time": price_time,
            }
        if market == "fund":
            raw = results.get(f"fund_{sym}", {})
            # Fund response: {"fund": {...}} with fields: price, daily_return, name
            fd: Dict[str, Any] = {}
            for shape_key in ("fund", "data"):
                if isinstance(raw.get(shape_key), dict):
                    fd = raw[shape_key]
                    break
            if not fd:
                fl = raw.get("funds", [])
                if fl and isinstance(fl[0], dict):
                    fd = fl[0]
            if not fd:
                fd = raw
            price = _safe_float(fd.get("price") if fd.get("price") else None) \
                    or _safe_float(fd.get("fiyat")) \
                    or _safe_float(fd.get("birim_pay_degeri"))
            used_fallback = False
            # Bugün fiyat henüz güncellenmemişse (0.0) recent_prices'tan son geçerli fiyatı al
            if not price:
                recent = raw.get("recent_prices", [])
                for rp in recent:
                    rp_val = _safe_float(rp.get("price"))
                    if rp_val:
                        price = rp_val
                        used_fallback = True
                        break
            daily = _safe_float(fd.get("daily_return") or fd.get("gunluk_getiri"))
            # Fallback fiyat kullandıysak veya daily_return -100 ise değişim gösterme
            change_pct = None if (used_fallback or daily == -100) else daily
            # Fon fiyat tarihi: recent_prices[0].date veya fallback fiyatın tarihi
            recent_list = raw.get("recent_prices", [])
            price_date = None
            if recent_list:
                price_date = recent_list[0].get("date")  # "2026-06-19"
            return {
                "price": price,
                "change_pct": change_pct,
                "currency": "TRY",
                "name": fd.get("name") or fd.get("fon_adi") or w["name"],
                "full_name": fd.get("name") or fd.get("fon_adi") or w["name"],
                "fund_type": fd.get("category") or fd.get("fon_turu"),
                "category": fd.get("category") or fd.get("fon_turu"),
                "price_date": price_date,
            }
        if market in ("crypto_tr", "crypto_global"):
            key = f"ctr_{sym}" if market == "crypto_tr" else f"cgl_{sym}"
            raw = results.get(key, {})
            # Crypto response: {"ticker": {price, bid, ask, high_24h, low_24h, volume_24h}}
            # or {"tickers": [...]}
            t = _extract_ticker(raw)
            return {
                "price": _safe_float(
                    t.get("price") or t.get("last") or t.get("lastPrice")
                ),
                "change_pct": _safe_float(
                    t.get("daily_return") or t.get("dailyPercent") or
                    t.get("daily") or t.get("percentChange24h")
                ) or _latest_change_from_history(results.get(f"chist_{sym}", {})),
                "currency": "TRY" if market == "crypto_tr" else "USD",
                "name": w.get("name") or _crypto_base_symbol(sym),
            }
    except Exception:
        pass
    return base


# ─────────────────────────────────────────────────────────────────────
# Asset detail
# ─────────────────────────────────────────────────────────────────────


@app.get("/api/asset/quick")
async def asset_quick(symbol: str, market: str):
    if len(symbol) > 20 or len(market) > 20:
        raise HTTPException(400, "Parametre çok uzun")
    if market in ("crypto_tr", "crypto_global"):
        exchange = "btcturk" if market == "crypto_tr" else "coinbase"
        return await call_mcp(
            "get_crypto_market",
            {"symbol": _crypto_pair(symbol, market), "exchange": exchange, "data_type": "ticker"},
        )
    sym = symbol.upper().strip()
    quick = await call_mcp_safe("get_quick_info", {"symbol": sym, "market": market}, timeout=12)
    if isinstance(quick, dict):
        d = _find_in_data(quick, sym)
        price = _safe_float(
            d.get("current_price") or d.get("currentPrice") or
            d.get("regularMarketPrice") or d.get("price") or d.get("son")
        )
        if price is not None:
            return quick

    hist = await call_mcp_safe(
        "get_historical_data",
        {"symbol": sym, "market": market, "period": "5d"},
        timeout=20,
    )
    fallback = _quote_from_history(sym, market, hist)
    if fallback:
        return {"data": fallback, "fallback": True}
    if quick:
        return quick
    raise HTTPException(404, f"{sym} için fiyat verisi bulunamadı")


@app.get("/api/asset/technical")
async def asset_technical(symbol: str, market: str, timeframe: str = "1d"):
    sym = _crypto_pair(symbol, market) if market in ("crypto_tr", "crypto_global") else symbol
    return await call_mcp("get_technical_analysis", {"symbol": sym, "market": market, "timeframe": timeframe})


@app.get("/api/asset/multi-analysis")
async def multi_analysis(symbol: str, market: str):
    """Multi-source: analyst + daily/weekly technical + fundamentals in parallel."""
    if market in ("fund", "crypto_tr", "crypto_global"):
        return {"symbol": symbol, "market": market}
    tasks: Dict[str, Any] = {
        "analyst":    call_mcp("get_analyst_data",      {"symbol": symbol, "market": market}),
        "tech_daily": call_mcp("get_technical_analysis", {"symbol": symbol, "market": market, "timeframe": "1d"}),
        "tech_weekly":call_mcp("get_technical_analysis", {"symbol": symbol, "market": market, "timeframe": "1W"}),
        "quick":      call_mcp("get_quick_info",         {"symbol": symbol, "market": market}),
    }
    keys = list(tasks.keys())
    raw = await asyncio.gather(*tasks.values(), return_exceptions=True)
    results = {k: (v if not isinstance(v, Exception) else {}) for k, v in zip(keys, raw)}
    return {"symbol": symbol, "market": market, **results}


@app.get("/api/asset/analyst")
async def asset_analyst(symbol: str, market: str):
    return await call_mcp("get_analyst_data", {"symbol": symbol, "market": market})


@app.get("/api/asset/news")
async def asset_news(symbol: str, market: str = "bist", limit: int = Query(10, ge=1, le=50)):
    if market != "bist":
        return {"symbol": symbol.upper().strip(), "market": market, "news": []}
    return await call_mcp("get_news", {"symbol": symbol.upper().strip(), "limit": limit})


@app.get("/api/asset/history")
async def asset_history(symbol: str, market: str, period: str = "3mo"):
    sym = _crypto_pair(symbol, market) if market in ("crypto_tr", "crypto_global") else symbol
    return await call_mcp("get_historical_data", {"symbol": sym, "market": market, "period": period})


# ─────────────────────────────────────────────────────────────────────
# TradingView tarzı AL/SAT sinyal özeti
# ─────────────────────────────────────────────────────────────────────

def _ema(values: list, period: int) -> Optional[float]:
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e

def _rma(values: list, period: int) -> Optional[float]:
    """Wilder's smoothed MA (used in RSI, ADX, ATR)."""
    if len(values) < period:
        return None
    alpha = 1.0 / period
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * alpha + e * (1 - alpha)
    return e

def _wma(values: list, period: int) -> Optional[float]:
    if len(values) < period:
        return None
    weights = list(range(1, period + 1))
    total_w = sum(weights)
    return sum(values[-period + i] * weights[i] for i in range(period)) / total_w

def _sma(values: list, period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period

def _adx(closes: list, highs: list, lows: list, period: int = 14) -> dict:
    """ADX, +DI, -DI hesapla. Wilder smoothing kullanır."""
    n = min(len(closes), len(highs), len(lows))
    if n < period * 2 + 1:
        return {"adx": None, "plus_di": None, "minus_di": None}
    tr_vals, plus_dm, minus_dm = [], [], []
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        tr_vals.append(max(h - l, abs(h - pc), abs(l - pc)))
        up   = highs[i]  - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(up   if up > down and up > 0   else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
    atr  = _rma(tr_vals,   period)
    pdm  = _rma(plus_dm,   period)
    mdm  = _rma(minus_dm,  period)
    if atr is None or atr == 0:
        return {"adx": None, "plus_di": None, "minus_di": None}
    pdi = 100 * pdm / atr
    mdi = 100 * mdm / atr
    # DX zaman serisi için son period bar
    dx_series = []
    # Yeniden hesapla: her bar için kümülatif RMA
    atr_arr = [sum(tr_vals[:period]) / period]
    pdm_arr = [sum(plus_dm[:period]) / period]
    mdm_arr = [sum(minus_dm[:period]) / period]
    alpha = 1.0 / period
    for i in range(period, len(tr_vals)):
        atr_arr.append(tr_vals[i] * alpha + atr_arr[-1] * (1 - alpha))
        pdm_arr.append(plus_dm[i] * alpha + pdm_arr[-1] * (1 - alpha))
        mdm_arr.append(minus_dm[i] * alpha + mdm_arr[-1] * (1 - alpha))
        a = atr_arr[-1]
        if a == 0:
            dx_series.append(0.0)
            continue
        p = 100 * pdm_arr[-1] / a
        m = 100 * mdm_arr[-1] / a
        dx_series.append(100 * abs(p - m) / (p + m) if (p + m) else 0.0)
    adx_val = _rma(dx_series, period) if len(dx_series) >= period else None
    return {"adx": round(adx_val, 2) if adx_val else None,
            "plus_di": round(pdi, 2), "minus_di": round(mdi, 2)}

def _cmo(closes: list, period: int = 9) -> Optional[float]:
    """Chande Momentum Oscillator."""
    if len(closes) < period + 1:
        return None
    diffs = [closes[i] - closes[i - 1] for i in range(len(closes) - period, len(closes))]
    up   = sum(d for d in diffs if d > 0)
    down = sum(-d for d in diffs if d < 0)
    return 100 * (up - down) / (up + down) if (up + down) else 0.0

def _tv_signal(price: float, ma: Optional[float]) -> str:
    if ma is None or price is None:
        return "NÖTR"
    diff = (price - ma) / ma * 100
    if diff > 0.15:
        return "AL"
    if diff < -0.15:
        return "SAT"
    return "NÖTR"

def _compute_tv_signals(cp: float, tech: dict, bars: list) -> dict:
    closes = [b["close"] for b in bars if b.get("close") is not None]
    highs  = [b["high"]  for b in bars if b.get("high")  is not None]
    lows   = [b["low"]   for b in bars if b.get("low")   is not None]
    vols   = [b.get("volume", 0) or 0 for b in bars]
    n = len(closes)

    ma_data  = tech.get("moving_averages", {}) or {}
    ind_data = tech.get("indicators", {})      or {}

    rsi  = _safe_float(ind_data.get("rsi_14"))
    macd = _safe_float(ind_data.get("macd"))
    macd_sig = _safe_float(ind_data.get("macd_signal"))
    sma20 = _safe_float(ma_data.get("sma_20"))
    ema20 = _safe_float(ma_data.get("ema_20"))
    ema50 = _safe_float(ma_data.get("ema_50"))

    # ── Hesaplanan indikatörler ──────────────────────────────────────

    # Stochastic %K (14, 3, 3)
    stoch_k: Optional[float] = None
    if n >= 14:
        k_vals = []
        for i in range(3):
            idx = n - 3 + i
            if idx < 14: continue
            h14 = max(highs[idx-14:idx])
            l14 = min(lows[idx-14:idx])
            k_vals.append((closes[idx] - l14) / (h14 - l14) * 100 if h14 != l14 else 50)
        stoch_k = sum(k_vals) / len(k_vals) if k_vals else None

    # Williams %R (14)
    wr: Optional[float] = None
    if n >= 14:
        h14 = max(highs[-14:])
        l14 = min(lows[-14:])
        wr = (h14 - cp) / (h14 - l14) * -100 if h14 != l14 else 0

    # CCI (20)
    cci: Optional[float] = None
    if n >= 20:
        tp = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(-20, 0)]
        sma_tp = sum(tp) / 20
        md = sum(abs(t - sma_tp) for t in tp) / 20
        cci = (tp[-1] - sma_tp) / (0.015 * md) if md else 0

    # Awesome Oscillator
    ao: Optional[float] = None
    if n >= 34:
        def mid(i: int) -> float: return (highs[i] + lows[i]) / 2
        ao5  = sum(mid(i) for i in range(-5, 0)) / 5
        ao34 = sum(mid(i) for i in range(-34, 0)) / 34
        ao   = ao5 - ao34

    # Momentum (10)
    mom: Optional[float] = None
    if n >= 11:
        mom = closes[-1] - closes[-11]

    # Stochastic RSI Fast (3,3,14,14) — basitleştirilmiş
    stoch_rsi: Optional[float] = None
    if rsi is not None:
        stoch_rsi = rsi  # Proxy: stoch rsi ≈ rsi normalized

    # Bull Bear Power = Close - EMA(13)
    bbp: Optional[float] = None
    e13 = _ema(closes, 13)
    if e13 is not None:
        bbp = cp - e13

    # Ultimate Oscillator (7, 14, 28)
    uo: Optional[float] = None
    if n >= 29:
        def bp(i: int) -> float:
            pc = closes[i - 1] if i > 0 else closes[i]
            return closes[i] - min(lows[i], pc)
        def tr_val(i: int) -> float:
            pc = closes[i - 1] if i > 0 else closes[i]
            return max(highs[i], pc) - min(lows[i], pc)
        def avg_range(start: int) -> float:
            b_sum = sum(bp(i) for i in range(start, 0))
            t_sum = sum(tr_val(i) for i in range(start, 0))
            return b_sum / t_sum if t_sum else 0
        avg7  = avg_range(-7)
        avg14 = avg_range(-14)
        avg28 = avg_range(-28)
        uo = 100 * (4 * avg7 + 2 * avg14 + avg28) / 7

    # ── ADX(14) — Pine Script'ten ────────────────────────────────────
    adx_data = _adx(closes, highs, lows, 14)
    adx_val  = adx_data["adx"]
    plus_di  = adx_data["plus_di"]
    minus_di = adx_data["minus_di"]
    # ADX sinyali: >25 güçlü trend; yön: +DI > -DI → AL, -DI > +DI → SAT
    adx_signal = "NÖTR"
    if adx_val and plus_di and minus_di:
        if adx_val > 25:
            adx_signal = "AL" if plus_di > minus_di else "SAT"

    # ── Chande Momentum Oscillator (CMO)(9) — Pine Script'ten ────────
    cmo = _cmo(closes, 9)
    # Pine Script: >=50 → Aşırı Satım, <-50 → Aşırı Alım
    cmo_signal = "NÖTR"
    if cmo is not None:
        if cmo < -50:   cmo_signal = "AL"   # aşırı satım bölgesi
        elif cmo > 50:  cmo_signal = "SAT"  # aşırı alım bölgesi

    # ── EMA Cross sinyalleri — Pine Script EMA9/30 ve EMA5/22 ────────
    ema5  = _ema(closes, 5)
    ema9  = _ema(closes, 9)
    ema22 = _ema(closes, 22)
    ema30 = _ema(closes, 30)

    ema_cross_9_30: Optional[str] = None
    if ema9 and ema30:
        ema_cross_9_30 = "AL" if ema9 > ema30 else "SAT"
    ema_cross_5_22: Optional[str] = None
    if ema5 and ema22:
        ema_cross_5_22 = "AL" if ema5 > ema22 else "SAT"

    # ── WMA değerleri — Pine Script WMA9/15/22/30 ────────────────────
    wma9  = _wma(closes, 9)
    wma15 = _wma(closes, 15)
    wma22 = _wma(closes, 22)
    wma30 = _wma(closes, 30) if n >= 30 else None

    wma_cross_9_15: Optional[str] = None
    if wma9 and wma15:
        wma_cross_9_15 = "AL" if wma9 > wma15 else "SAT"
    wma_cross_10_20: Optional[str] = None
    wma10 = _wma(closes, 10)
    wma20 = _wma(closes, 20)
    if wma10 and wma20:
        wma_cross_10_20 = "AL" if wma10 > wma20 else "SAT"

    # ── EFSUN / MOST — EMA(10) ± %0.5 bant ──────────────────────────
    efsun_pct = 0.5
    ema10_efsun = _ema(closes, 10)
    efsun_signal: Optional[str] = None
    if ema10_efsun:
        upper = ema10_efsun * (1 + efsun_pct / 100)
        lower = ema10_efsun * (1 - efsun_pct / 100)
        efsun_signal = "AL" if cp > upper else ("SAT" if cp < lower else "NÖTR")

    # ── Hacim Değişimi (10/20/30 bar) ────────────────────────────────
    vol_chg_10: Optional[float] = None
    vol_chg_20: Optional[float] = None
    vol_chg_30: Optional[float] = None
    if n >= 11 and vols[-11] and vols[-11] > 0:
        vol_chg_10 = (vols[-1] - vols[-11]) / vols[-11] * 100
    if n >= 21 and vols[-21] and vols[-21] > 0:
        vol_chg_20 = (vols[-1] - vols[-21]) / vols[-21] * 100
    if n >= 31 and vols[-31] and vols[-31] > 0:
        vol_chg_30 = (vols[-1] - vols[-31]) / vols[-31] * 100

    # ── Moving Averages ────────────────────────────────────────────
    sma10  = _sma(closes, 10)
    ema10  = _ema(closes, 10)
    sma30  = _sma(closes, 30) if n >= 30 else None
    sma50  = _sma(closes, 50) if n >= 50 else None
    ema_50_calc = ema50 or (_ema(closes, 50) if n >= 50 else None)
    sma100 = _sma(closes, 100) if n >= 100 else None
    sma200 = _sma(closes, 200) if n >= 200 else None
    ema100 = _ema(closes, 100) if n >= 100 else None
    ema200 = _ema(closes, 200) if n >= 200 else None

    # Ichimoku Base Line (Kijun-sen, 26 periyot)
    ichi_base: Optional[float] = None
    if n >= 26:
        ichi_base = (max(highs[-26:]) + min(lows[-26:])) / 2

    # VWMA (20)
    vwma: Optional[float] = None
    if n >= 20 and sum(vols[-20:]) > 0:
        pv = sum(closes[i] * vols[n - 20 + i] for i in range(20))
        vv = sum(vols[-20:])
        vwma = pv / vv if vv else None

    # Hull MA (9)
    hull: Optional[float] = None
    if n >= 9:
        wma4h = _wma(closes, 4)
        wma9h = _wma(closes, 9)
        if wma4h is not None and wma9h is not None:
            hull = 2 * wma4h - wma9h

    # ── Sinyal üretici yardımcılar ───────────────────────────────────
    def osc_sig(name: str, val: Optional[float], low_al: float, high_sat: float,
                invert: bool = False) -> dict:
        if val is None:
            return {"name": name, "value": None, "signal": "NÖTR"}
        if not invert:
            sig = "AL" if val < low_al else ("SAT" if val > high_sat else "NÖTR")
        else:
            sig = "AL" if val > high_sat else ("SAT" if val < low_al else "NÖTR")
        return {"name": name, "value": round(val, 2), "signal": sig}

    def cross_sig(name: str, sig: Optional[str], val_a: Optional[float],
                  val_b: Optional[float]) -> dict:
        label = f"{round(val_a, 2)} / {round(val_b, 2)}" if val_a and val_b else None
        return {"name": name, "value": label, "signal": sig or "NÖTR"}

    oscillators = [
        osc_sig("RSI(14)",               rsi,      30,   70),
        {"name": "MACD(12,26)",
         "value": round(macd - macd_sig, 4) if macd is not None and macd_sig is not None else None,
         "signal": ("AL" if (macd or 0) > (macd_sig or 0) else "SAT")
                    if macd is not None and macd_sig is not None else "NÖTR"},
        osc_sig("Stochastic %K(14,3,3)", stoch_k,  20,   80),
        osc_sig("Williams %R(14)",        wr,       -80, -20),
        osc_sig("CCI(20)",                cci,     -100, 100),
        osc_sig("Awesome Osc.",           ao,         0,   0, invert=True) if ao is not None
            else {"name": "Awesome Osc.", "value": None, "signal": "NÖTR"},
        {"name": "Momentum(10)",
         "value": round(mom, 2) if mom is not None else None,
         "signal": ("AL" if (mom or 0) > 0 else "SAT") if mom is not None else "NÖTR"},
        {"name": "Bull Bear Power",
         "value": round(bbp, 2) if bbp is not None else None,
         "signal": ("AL" if (bbp or 0) > 0 else "SAT") if bbp is not None else "NÖTR"},
        osc_sig("Ultimate Osc.(7,14,28)", uo,       30,   70),
        # ── Pine Script'ten gelen yeni osilatörler ──
        {"name": "ADX(14)",
         "value": f"{adx_val} (+DI:{plus_di} / -DI:{minus_di})"
                  if adx_val else None,
         "signal": adx_signal},
        {"name": "Chande MO(9)",
         "value": round(cmo, 2) if cmo is not None else None,
         "signal": cmo_signal},
        {"name": "EFSUN/MOST",
         "value": round(ema10_efsun, 2) if ema10_efsun else None,
         "signal": efsun_signal or "NÖTR"},
    ]

    moving_averages = [
        # EMA/SMA standart
        {"name": "EMA(10)",  "value": round(ema10, 2)  if ema10 else None,  "signal": _tv_signal(cp, ema10)},
        {"name": "SMA(10)",  "value": round(sma10, 2)  if sma10 else None,  "signal": _tv_signal(cp, sma10)},
        {"name": "EMA(20)",  "value": round(ema20, 2)  if ema20 else None,  "signal": _tv_signal(cp, ema20)},
        {"name": "SMA(20)",  "value": round(sma20, 2)  if sma20 else None,  "signal": _tv_signal(cp, sma20)},
        {"name": "EMA(30)",  "value": round(ema30, 2)  if ema30 else None,  "signal": _tv_signal(cp, ema30)},
        {"name": "SMA(30)",  "value": round(sma30, 2)  if sma30 else None,  "signal": _tv_signal(cp, sma30)},
        {"name": "EMA(50)",  "value": round(ema_50_calc, 2) if ema_50_calc else None, "signal": _tv_signal(cp, ema_50_calc)},
        {"name": "SMA(50)",  "value": round(sma50, 2)  if sma50 else None,  "signal": _tv_signal(cp, sma50)},
        {"name": "EMA(100)", "value": round(ema100, 2) if ema100 else None, "signal": _tv_signal(cp, ema100)},
        {"name": "SMA(100)", "value": round(sma100, 2) if sma100 else None, "signal": _tv_signal(cp, sma100)},
        {"name": "EMA(200)", "value": round(ema200, 2) if ema200 else None, "signal": _tv_signal(cp, ema200)},
        {"name": "SMA(200)", "value": round(sma200, 2) if sma200 else None, "signal": _tv_signal(cp, sma200)},
        {"name": "Ichimoku Base(26)", "value": round(ichi_base, 2) if ichi_base else None, "signal": _tv_signal(cp, ichi_base)},
        {"name": "VWMA(20)", "value": round(vwma, 2) if vwma else None, "signal": _tv_signal(cp, vwma)},
        {"name": "Hull MA(9)", "value": round(hull, 2) if hull else None, "signal": _tv_signal(cp, hull)},
        # ── Pine Script WMA ve EMA çapraz sinyalleri ──
        {"name": "WMA(9)",  "value": round(wma9, 2)  if wma9  else None, "signal": _tv_signal(cp, wma9)},
        {"name": "WMA(15)", "value": round(wma15, 2) if wma15 else None, "signal": _tv_signal(cp, wma15)},
        {"name": "WMA(22)", "value": round(wma22, 2) if wma22 else None, "signal": _tv_signal(cp, wma22)},
        {"name": "WMA(30)", "value": round(wma30, 2) if wma30 else None, "signal": _tv_signal(cp, wma30)},
        cross_sig("EMA Cross EMA9/EMA30", ema_cross_9_30, ema9,  ema30),
        cross_sig("EMA Cross EMA5/EMA22", ema_cross_5_22, ema5,  ema22),
        cross_sig("WMA Cross WMA9/WMA15", wma_cross_9_15, wma9,  wma15),
        cross_sig("WMA Cross WMA10/WMA20",wma_cross_10_20, wma10, wma20),
    ]

    # Hacim değişimi — ayrı bölüm olarak ekle
    volume_changes = []
    for label, val in [("Son 10 Bar Hacim Δ", vol_chg_10),
                       ("Son 20 Bar Hacim Δ", vol_chg_20),
                       ("Son 30 Bar Hacim Δ", vol_chg_30)]:
        if val is not None:
            volume_changes.append({
                "name": label,
                "value": round(val, 1),
                "signal": "AL" if val > 0 else "SAT",
            })

    # Veri olmayan satırları çıkar
    osc_active = [x for x in oscillators if x["value"] is not None]
    ma_active  = [x for x in moving_averages if x["value"] is not None]
    all_active = osc_active + ma_active

    buy     = sum(1 for x in all_active if x["signal"] == "AL")
    sell    = sum(1 for x in all_active if x["signal"] == "SAT")
    neutral = sum(1 for x in all_active if x["signal"] == "NÖTR")
    total   = len(all_active)

    if total == 0:
        summary_sig = "NÖTR"
    else:
        buy_r = buy / total
        sell_r = sell / total
        if buy_r >= 0.65:   summary_sig = "GÜÇLÜ AL"
        elif buy_r >= 0.40: summary_sig = "AL"
        elif sell_r >= 0.65: summary_sig = "GÜÇLÜ SAT"
        elif sell_r >= 0.40: summary_sig = "SAT"
        else:                summary_sig = "NÖTR"

    osc_buy  = sum(1 for x in osc_active if x["signal"] == "AL")
    osc_sell = sum(1 for x in osc_active if x["signal"] == "SAT")
    ma_buy   = sum(1 for x in ma_active  if x["signal"] == "AL")
    ma_sell  = sum(1 for x in ma_active  if x["signal"] == "SAT")

    return {
        "current_price": cp,
        "summary": {
            "signal": summary_sig, "buy": buy, "sell": sell, "neutral": neutral,
            "osc_buy": osc_buy, "osc_sell": osc_sell,
            "ma_buy": ma_buy, "ma_sell": ma_sell,
        },
        "oscillators":     osc_active,
        "moving_averages": ma_active,
        "volume_changes":  volume_changes,
    }


@app.get("/api/asset/tv-signals")
async def tv_signals(symbol: str, market: str):
    mkt = market
    sym = _crypto_pair(symbol, mkt) if mkt in ("crypto_tr", "crypto_global") else symbol
    tech, hist_raw = await asyncio.gather(
        call_mcp("get_technical_analysis", {"symbol": sym, "market": mkt, "timeframe": "1d"}),
        call_mcp("get_historical_data",    {"symbol": sym, "market": mkt, "period": "1mo"}),
    )
    cp = _safe_float(tech.get("current_price")) if isinstance(tech, dict) else None
    bars = hist_raw.get("data", []) if isinstance(hist_raw, dict) else (hist_raw if isinstance(hist_raw, list) else [])
    if not cp or not bars:
        return {"error": "Veri yetersiz"}
    return _compute_tv_signals(cp, tech if isinstance(tech, dict) else {}, bars)


@app.get("/api/fund")
async def fund_detail(symbol: str):
    data = await _get_fund_data_robust(symbol, include_portfolio=True)
    if data:
        return data
    raise HTTPException(404, f"{symbol.upper().strip()} fon verisi bulunamadı")


def _extract_bars(payload: Any) -> List[dict]:
    if isinstance(payload, dict):
        for key in ("data", "historical_data", "prices", "history", "items", "results"):
            if isinstance(payload.get(key), list):
                return payload[key]
    if isinstance(payload, list):
        return payload
    return []


def _return_from_bars(payload: Any) -> Optional[float]:
    bars = _extract_bars(payload)
    clean = []
    for b in bars:
        if not isinstance(b, dict):
            continue
        val = _safe_float(b.get("close") or b.get("price") or b.get("value"))
        if val is not None and val > 0:
            clean.append({"date": str(b.get("date", "")), "value": val})
    if len(clean) < 2:
        return None
    clean.sort(key=lambda x: x["date"])
    start = clean[0]["value"]
    end = clean[-1]["value"]
    if not start:
        return None
    return (end - start) / start * 100


def _return_from_fx(payload: Any) -> Optional[float]:
    ret = _return_from_bars(payload)
    if ret is not None:
        return ret
    if isinstance(payload, dict):
        for key in ("change_pct", "return_percent", "period_return", "change_percent"):
            val = _safe_float(payload.get(key))
            if val is not None:
                return val
    return None


def _add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    return date(y, m, 1)


def _shift_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    last = calendar.monthrange(y, m)[1]
    return date(y, m, min(d.day, last))


@app.get("/api/what-if")
async def what_if(symbol: str, market: str, amount: float, months: int = Query(1, ge=1, le=12)):
    sym = symbol.upper().strip()
    mkt = market.strip()
    today = date.today()
    start = _shift_months(today, -months)

    if amount <= 0:
        raise HTTPException(400, "Tutar 0'dan büyük olmalı")
    if mkt not in ("bist", "us", "fund"):
        raise HTTPException(400, "Piyasa bist, us veya fund olmalı")

    if mkt == "fund":
        latest = await call_mcp(
            "get_fund_data",
            {"symbol": sym, "include_performance": True},
        )
        latest_prices = latest.get("recent_prices", []) if isinstance(latest, dict) else []
        latest_date = None
        if latest_prices and isinstance(latest_prices[0], dict):
            latest_date = latest_prices[0].get("date")
        try:
            end_dt = date.fromisoformat(str(latest_date)) if latest_date else today
        except ValueError:
            end_dt = today
        start = _shift_months(end_dt, -months)
        data = await call_mcp(
            "get_fund_data",
            {
                "symbol": sym,
                "include_performance": True,
                "start_date": start.isoformat(),
                "end_date": end_dt.isoformat(),
            },
        )
        fund = data.get("fund", {}) if isinstance(data, dict) else {}
        cr = data.get("custom_return", {}) if isinstance(data, dict) else {}
        start_price = _safe_float(cr.get("start_price"))
        end_price = _safe_float(cr.get("end_price")) or _safe_float(fund.get("price"))
        return_pct = _safe_float(cr.get("return_percent"))
        name = fund.get("name") or sym
        category = fund.get("category")
        currency = "TRY"
        start_date = cr.get("start_date") or start.isoformat()
        end_date = cr.get("end_date") or today.isoformat()
    else:
        hist = await call_mcp(
            "get_historical_data",
            {"symbol": sym, "market": mkt, "start_date": start.isoformat(), "end_date": today.isoformat()},
        )
        bars = _extract_bars(hist)
        clean = []
        for b in bars:
            if not isinstance(b, dict):
                continue
            val = _safe_float(b.get("close") or b.get("price") or b.get("value"))
            if val and val > 0:
                clean.append({"date": str(b.get("date", ""))[:10], "price": val})
        clean.sort(key=lambda x: x["date"])
        if len(clean) < 2:
            raise HTTPException(404, "Bu dönem için yeterli fiyat verisi bulunamadı")
        start_price = clean[0]["price"]
        end_price = clean[-1]["price"]
        return_pct = (end_price - start_price) / start_price * 100
        quick = await call_mcp_safe("get_quick_info", {"symbol": sym, "market": mkt}, timeout=10)
        qd = _find_in_data(quick, sym) if isinstance(quick, dict) else {}
        name = qd.get("longName") or qd.get("name") or qd.get("shortName") or sym
        category = qd.get("sector") or qd.get("industry")
        currency = qd.get("currency", "TRY" if mkt == "bist" else "USD")
        start_date = clean[0]["date"]
        end_date = clean[-1]["date"]

    if not start_price or not end_price:
        raise HTTPException(404, "Başlangıç veya bitiş fiyatı bulunamadı")

    units = amount / start_price
    current_value = units * end_price
    profit = current_value - amount
    return {
        "symbol": sym,
        "market": mkt,
        "name": name,
        "category": category,
        "months": months,
        "amount": amount,
        "currency": currency,
        "start_date": start_date,
        "end_date": end_date,
        "start_price": start_price,
        "end_price": end_price,
        "units": units,
        "current_value": current_value,
        "profit": profit,
        "return_percent": return_pct,
    }


async def _fund_daily_changes(sym: str, start: date, end: date) -> List[dict]:
    pairs = []
    cur = start + timedelta(days=1)
    while cur <= end:
        pairs.append((cur - timedelta(days=1), cur))
        cur += timedelta(days=1)

    async def fetch_pair(s: date, e: date) -> Optional[dict]:
        raw = await call_mcp_safe(
            "get_fund_data",
            {
                "symbol": sym,
                "include_performance": True,
                "start_date": s.isoformat(),
                "end_date": e.isoformat(),
            },
            timeout=8,
        )
        cr = raw.get("custom_return", {}) if isinstance(raw, dict) else {}
        val = _safe_float(cr.get("return_percent"))
        start_price = _safe_float(cr.get("start_price"))
        end_price = _safe_float(cr.get("end_price"))
        start_date = cr.get("start_date")
        end_date = cr.get("end_date")
        if val is None or not start_date or not end_date or start_date == end_date:
            return None
        if start_price is not None and end_price is not None and start_price == end_price:
            return None
        return {
            "date": end_date,
            "start_date": start_date,
            "change_pct": val,
            "start_price": start_price,
            "end_price": end_price,
        }

    raw_changes = await asyncio.gather(*[fetch_pair(s, e) for s, e in pairs])
    by_date: Dict[str, dict] = {}
    for item in raw_changes:
        if item:
            by_date[item["date"]] = item
    return sorted(by_date.values(), key=lambda x: str(x.get("date", "")))


@app.get("/api/fund/performance")
async def fund_performance(symbol: str, days: int = Query(60, ge=7, le=365)):
    sym = symbol.upper().strip()
    today = date.today()
    start = today - timedelta(days=days)
    data = await call_mcp(
        "get_fund_data",
        {
            "symbol": sym,
            "include_performance": True,
            "start_date": start.isoformat(),
            "end_date": today.isoformat(),
        },
    )
    prices = data.get("recent_prices", []) if isinstance(data, dict) else []
    prices = sorted(
        [
            {"date": p.get("date"), "price": _safe_float(p.get("price"))}
            for p in prices if isinstance(p, dict) and _safe_float(p.get("price")) is not None
        ],
        key=lambda x: str(x.get("date", "")),
    )
    changes = await _fund_daily_changes(sym, start, today)
    positives = sum(1 for x in changes if x["change_pct"] > 0)
    negatives = sum(1 for x in changes if x["change_pct"] < 0)
    flats = sum(1 for x in changes if x["change_pct"] == 0)
    best = max(changes, key=lambda x: x["change_pct"], default=None)
    worst = min(changes, key=lambda x: x["change_pct"], default=None)
    custom = data.get("custom_return", {}) if isinstance(data, dict) else {}
    month_start = date(today.year, today.month, 1)
    month_ranges = []
    for i in range(12, 0, -1):
        start_m = _add_months(month_start, -i)
        end_m = _add_months(start_m, 1) - timedelta(days=1)
        if end_m > today:
            end_m = today
        month_ranges.append((start_m, end_m))
    monthly_raw = await asyncio.gather(*[
        call_mcp_safe(
            "get_fund_data",
            {
                "symbol": sym,
                "include_performance": True,
                "start_date": s.isoformat(),
                "end_date": e.isoformat(),
            },
            timeout=10,
        )
        for s, e in month_ranges
    ])
    monthly_returns = []
    for (s, e), raw in zip(month_ranges, monthly_raw):
        cr = raw.get("custom_return", {}) if isinstance(raw, dict) else {}
        val = _safe_float(cr.get("return_percent"))
        if val is not None:
            monthly_returns.append({
                "month": s.strftime("%Y-%m"),
                "start_date": cr.get("start_date") or s.isoformat(),
                "end_date": cr.get("end_date") or e.isoformat(),
                "return_percent": val,
            })
    best_month = max(monthly_returns, key=lambda x: x["return_percent"], default=None)
    worst_month = min(monthly_returns, key=lambda x: x["return_percent"], default=None)
    return {
        "symbol": sym,
        "fund": data.get("fund", {}) if isinstance(data, dict) else {},
        "period_days": custom.get("days") or days,
        "requested_days": days,
        "return_percent": _safe_float(custom.get("return_percent")),
        "start_date": custom.get("start_date"),
        "end_date": custom.get("end_date"),
        "start_price": _safe_float(custom.get("start_price")),
        "end_price": _safe_float(custom.get("end_price")),
        "daily_sample_size": len(changes),
        "positive_days": positives,
        "negative_days": negatives,
        "flat_days": flats,
        "best_day": best,
        "worst_day": worst,
        "best_month": best_month,
        "worst_month": worst_month,
        "monthly_returns": monthly_returns,
        "recent_prices": prices,
    }


@app.get("/api/fund/compare")
async def fund_compare(symbol: str, days: int = Query(60, ge=7, le=365)):
    sym = symbol.upper().strip()
    today = date.today()
    end_s = today.isoformat()

    def fund_return_task(period_days: int):
        start_s = (today - timedelta(days=period_days)).isoformat()
        return call_mcp_safe(
            "get_fund_data",
            {
                "symbol": sym,
                "include_performance": True,
                "start_date": start_s,
                "end_date": end_s,
            },
            timeout=20,
        )

    month_start = date(today.year, today.month, 1)
    month_ranges = []
    for i in range(12, 0, -1):
        start_m = _add_months(month_start, -i)
        end_m = _add_months(start_m, 1) - timedelta(days=1)
        if end_m > today:
            end_m = today
        month_ranges.append((start_m, end_m))

    ret60_task = fund_return_task(60)
    ret90_task = fund_return_task(90)
    monthly_tasks = [
        call_mcp_safe(
            "get_fund_data",
            {
                "symbol": sym,
                "include_performance": True,
                "start_date": s.isoformat(),
                "end_date": e.isoformat(),
            },
            timeout=10,
        )
        for s, e in month_ranges
    ]
    ret60, ret90, *monthly_raw = await asyncio.gather(ret60_task, ret90_task, *monthly_tasks)

    def custom_payload(raw: dict) -> dict:
        cr = raw.get("custom_return", {}) if isinstance(raw, dict) else {}
        return {
            "return_percent": _safe_float(cr.get("return_percent")),
            "start_date": cr.get("start_date"),
            "end_date": cr.get("end_date"),
            "days": cr.get("days"),
            "start_price": _safe_float(cr.get("start_price")),
            "end_price": _safe_float(cr.get("end_price")),
        }

    monthly_returns = []
    for (s, e), raw in zip(month_ranges, monthly_raw):
        cr = raw.get("custom_return", {}) if isinstance(raw, dict) else {}
        val = _safe_float(cr.get("return_percent"))
        if val is not None:
            monthly_returns.append({
                "month": s.strftime("%Y-%m"),
                "start_date": cr.get("start_date") or s.isoformat(),
                "end_date": cr.get("end_date") or e.isoformat(),
                "return_percent": val,
            })

    best_month = max(monthly_returns, key=lambda x: x["return_percent"], default=None)
    worst_month = min(monthly_returns, key=lambda x: x["return_percent"], default=None)
    return {
        "symbol": sym,
        "return_60d": custom_payload(ret60),
        "return_90d": custom_payload(ret90),
        "best_month": best_month,
        "worst_month": worst_month,
        "monthly_returns": monthly_returns,
    }


@app.get("/api/crypto")
async def crypto_detail(symbol: str, exchange: str = "btcturk"):
    if exchange not in ALLOWED_EXCHANGES:
        raise HTTPException(400, "Geçersiz exchange")
    if len(symbol) > 20:
        raise HTTPException(400, "Parametre çok uzun")
    market = "crypto_tr" if exchange == "btcturk" else "crypto_global"
    pair = _crypto_pair(symbol, market)
    return await call_mcp("get_crypto_market", {"symbol": pair, "exchange": exchange, "data_type": "ticker"})


async def _crypto_snapshot(item: dict, market: str) -> dict:
    base = item["symbol"].upper()
    pair = _crypto_pair(base, market)
    exchange = "btcturk" if market == "crypto_tr" else "coinbase"
    ticker, history = await asyncio.gather(
        call_mcp_safe(
            "get_crypto_market",
            {"symbol": pair, "exchange": exchange, "data_type": "ticker"},
            timeout=12,
        ),
        call_mcp_safe(
            "get_historical_data",
            {"symbol": pair, "market": market, "period": "5d"},
            timeout=12,
        ),
    )
    t = _extract_ticker(ticker)
    price = _safe_float(t.get("price") or t.get("last") or t.get("lastPrice"))
    if price is None:
        bars = history.get("data", []) if isinstance(history, dict) else []
        if bars:
            bars = sorted(bars, key=lambda b: str(b.get("date", "")))
            price = _safe_float(bars[-1].get("close"))
    change_pct = _safe_float(
        t.get("daily_return") or t.get("dailyPercent") or
        t.get("daily") or t.get("percentChange24h")
    ) or _latest_change_from_history(history)
    return {
        "symbol": base,
        "pair": pair,
        "name": item.get("name", base),
        "market": market,
        "exchange": exchange,
        "price": price,
        "change_pct": change_pct,
        "currency": "TRY" if market == "crypto_tr" else "USD",
        "volume_24h": _safe_float(t.get("volume_24h") or t.get("volume24h") or t.get("volume")),
        "high_24h": _safe_float(t.get("high_24h") or t.get("high24h")),
        "low_24h": _safe_float(t.get("low_24h") or t.get("low24h")),
    }


@app.get("/api/crypto/list")
async def crypto_list(market: str = "crypto_global", limit: int = 30):
    mkt = market if market in ("crypto_tr", "crypto_global") else "crypto_global"
    max_items = max(1, min(limit, len(CRYPTO_UNIVERSE)))
    universe = CRYPTO_UNIVERSE[:max_items]
    rows = []
    for i in range(0, len(universe), 8):
        batch = universe[i:i + 8]
        rows.extend(await asyncio.gather(*[_crypto_snapshot(item, mkt) for item in batch]))
    rows = sorted(
        rows,
        key=lambda x: (x.get("change_pct") is None, -(x.get("change_pct") or -999999)),
    )
    return {"market": mkt, "count": len(rows), "items": rows}


@app.get("/api/crypto/analysis")
async def crypto_analysis(
    symbol: str,
    market: str = "crypto_global",
    exchange: Optional[str] = None,
    timeframe: str = "1d",
    period: str = "3mo",
):
    """Ticker + technical + history bundle for the crypto dashboard."""
    sym = symbol.upper().strip()
    mkt = market if market in ("crypto_tr", "crypto_global") else "crypto_global"
    pair = _crypto_pair(sym, mkt)
    exc = exchange or ("btcturk" if mkt == "crypto_tr" else "coinbase")
    if mkt == "crypto_global":
        exc = "coinbase"

    ticker_task = call_mcp_safe(
        "get_crypto_market",
        {"symbol": pair, "exchange": exc, "data_type": "ticker"},
        timeout=15,
    )
    tech_task = call_mcp_safe(
        "get_technical_analysis",
        {"symbol": pair, "market": mkt, "timeframe": timeframe},
        timeout=20,
    )
    history_task = call_mcp_safe(
        "get_historical_data",
        {"symbol": pair, "market": mkt, "period": period},
        timeout=20,
    )
    ticker, tech, history = await asyncio.gather(ticker_task, tech_task, history_task)

    tv_signals = {}
    cp = _safe_float(tech.get("current_price")) if isinstance(tech, dict) else None
    bars = history.get("data", []) if isinstance(history, dict) else []
    if cp and bars:
        try:
            tv_signals = _compute_tv_signals(cp, tech, bars)
        except Exception:
            tv_signals = {"error": "Teknik sinyal hesaplanamadı"}

    return {
        "symbol": sym,
        "pair": pair,
        "market": mkt,
        "exchange": exc,
        "ticker": ticker,
        "technical": tech,
        "history": history,
        "tv_signals": tv_signals,
    }


# ─────────────────────────────────────────────────────────────────────
# Search
# ─────────────────────────────────────────────────────────────────────


@app.get("/api/search")
async def search(q: str, market: str = "bist"):
    query = q.upper().strip()
    result = await call_mcp("search_symbol", {"query": query, "market": market, "limit": 15})
    if _extract_list(result):
        return result

    # BIST users often type the common 3-letter root, e.g. THY instead of THYAO.
    if market == "bist" and len(query) == 3:
        fallback = await call_mcp("search_symbol", {"query": f"{query}AO", "market": market, "limit": 15})
        if _extract_list(fallback):
            return fallback

    # TEFAS search may miss exact fund codes; verify the code directly.
    if market == "fund" and len(query) >= 2:
        fund = await call_mcp_safe("get_fund_data", {"symbol": query, "include_performance": False}, timeout=10)
        fd = fund.get("fund") if isinstance(fund, dict) else None
        if isinstance(fd, dict) and (fd.get("code") or fd.get("symbol")):
            return {
                "matches": [{
                    "symbol": (fd.get("code") or query).upper(),
                    "name": fd.get("name") or fd.get("fon_adi") or query,
                    "market": "fund",
                    "asset_type": "fund",
                }],
                "total_count": 1,
            }
    return result


# ─────────────────────────────────────────────────────────────────────
# Screener
# ─────────────────────────────────────────────────────────────────────


def _sort_screener(items: list, preset: Optional[str]) -> list:
    """Preset'e göre sonuçları ilgili metriğe göre sırala."""
    p = (preset or "").lower()
    if p in ("top_gainers", "momentum", "growth_stocks", "big_gainers", "bullish"):
        return sorted(items, key=lambda x: x.get("change_percent") or x.get("change") or 0, reverse=True)
    if p in ("top_losers", "big_losers", "bearish"):
        return sorted(items, key=lambda x: x.get("change_percent") or x.get("change") or 0)
    if p in ("most_active", "high_volume"):
        return sorted(items, key=lambda x: x.get("volume") or 0, reverse=True)
    if p in ("value_stocks", "undervalued", "low_pe"):
        return sorted(items, key=lambda x: x.get("pe_ratio") or 9999)
    if p in ("dividend_stocks", "high_dividend_yield"):
        return sorted(items, key=lambda x: (
            x.get("dividend_yield") or x.get("dividendYield") or
            (x.get("additional_data") or {}).get("dividend_yield") or 0
        ), reverse=True)
    if p in ("blue_chip",):
        return sorted(items, key=lambda x: x.get("market_cap") or 0, reverse=True)
    return items


def _extract_list(data: Any) -> list:
    if isinstance(data, list):
        return data
    for k in ("results", "securities", "stocks", "data", "items", "matches", "tickers"):
        if isinstance(data.get(k), list):
            return data[k]
    return []


# BIST screener preset → scan_stocks preset + index eşleştirmesi
_BIST_SCREENER_MAP: Dict[str, Dict[str, str]] = {
    "top_gainers":       {"scan_preset": "big_gainers",              "index": "XU100"},
    "top_losers":        {"scan_preset": "big_losers",               "index": "XU100"},
    "most_active":       {"scan_preset": "high_volume",              "index": "XU100"},
    "growth_stocks":     {"scan_preset": "bullish_momentum",         "index": "XU100"},
    "momentum":          {"scan_preset": "momentum_breakout",        "index": "XU100"},
    "undervalued":       {"scan_preset": "oversold",                 "index": "XU100"},
    "value_stocks":      {"scan_preset": "oversold_high_volume",     "index": "XU100"},
    "low_pe":            {"scan_preset": "oversold",                 "index": "XU100"},
    "blue_chip":         {"scan_preset": "high_volume",              "index": "XU030"},
    "dividend_stocks":   {"scan_preset": "big_gainers",              "index": "XU100"},
    "high_dividend_yield": {"scan_preset": "big_gainers",            "index": "XU100"},
    "bb_oversold_buy":   {"scan_preset": "bb_oversold_buy",          "index": "XU100"},
    "supertrend_bullish":{"scan_preset": "supertrend_bullish",       "index": "XU100"},
}

# scan_stocks sonuçlarını screener formatına çevir
def _scan_to_screener(item: dict) -> dict:
    return {
        "symbol":         item.get("symbol", ""),
        "name":           item.get("name", ""),
        "market":         "bist",
        "price":          item.get("close"),
        "change_percent": item.get("change"),
        "volume":         item.get("volume"),
        "pe_ratio":       item.get("pe_ratio"),
        "rsi":            item.get("rsi"),
        "additional_indicators": item.get("additional_indicators", {}),
    }


_BIST_SCREENER_FALLBACKS: Dict[str, List[Dict[str, str]]] = {
    "undervalued": [
        {"scan_preset": "oversold_high_volume", "index": "XU100"},
        {"scan_preset": "high_volume", "index": "XU100"},
        {"scan_preset": "high_volume", "index": "XU030"},
    ],
    "low_pe": [
        {"scan_preset": "high_volume", "index": "XU100"},
        {"scan_preset": "high_volume", "index": "XU030"},
        {"scan_preset": "bullish_momentum", "index": "XU100"},
    ],
    "value_stocks": [
        {"scan_preset": "high_volume", "index": "XU100"},
        {"scan_preset": "high_volume", "index": "XU030"},
    ],
}


async def _get_bist_screener_items(preset: str) -> list:
    """BIST taramada boş dönen presetler için çalışan listelerden yedek üret."""
    cfg = _BIST_SCREENER_MAP[preset]
    scan_plan = [cfg] + _BIST_SCREENER_FALLBACKS.get(preset, [])

    seen: Dict[str, dict] = {}
    for scan_cfg in scan_plan:
        raw = await call_mcp_safe(
            "scan_stocks",
            {"preset": scan_cfg["scan_preset"], "index": scan_cfg["index"]},
            timeout=20,
        )
        raw_items = _extract_list(raw) if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
        for raw_item in raw_items:
            if not isinstance(raw_item, dict):
                continue
            item = _scan_to_screener(raw_item)
            symbol = (item.get("symbol") or "").upper()
            if symbol and symbol not in seen:
                seen[symbol] = item

        if seen and scan_cfg is cfg:
            break

    items = list(seen.values())
    if preset == "low_pe":
        with_pe = [item for item in items if (item.get("pe_ratio") or 0) > 0]
        if with_pe:
            items = with_pe
    return _sort_screener(items, preset)


@app.get("/api/screener")
async def screener(market: str = "us", preset: Optional[str] = None, limit: int = 30):
    # BIST için scan_stocks kullan — screen_securities preseti desteklemiyor
    if market == "bist" and preset and preset in _BIST_SCREENER_MAP:
        items = await _get_bist_screener_items(preset)
        return items[:limit]

    # US veya preset'siz BIST
    args: Dict[str, Any] = {"market": market, "limit": limit}
    if preset:
        args["preset"] = preset
    raw = await call_mcp("screen_securities", args)
    items = _extract_list(raw) if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
    if items and preset:
        items = _sort_screener(items, preset)
    if isinstance(raw, list):
        return items
    for k in ("results", "securities", "stocks", "data", "items", "matches", "tickers"):
        if k in raw:
            raw[k] = items
            return raw
    return items


@app.get("/api/screener/analyst-favorites")
async def analyst_favorites(market: str = "us"):
    """Gerçek analist güçlü al/al oranına göre sıralanmış hisseler."""
    if market == "us":
        # Birden fazla preset'ten aday al
        raw1, raw2 = await asyncio.gather(
            call_mcp("screen_securities", {"market": "us", "preset": "blue_chip", "limit": 30}),
            call_mcp("screen_securities", {"market": "us", "preset": "growth_stocks", "limit": 20}),
        )
        items1 = _extract_list(raw1) if isinstance(raw1, dict) else (raw1 if isinstance(raw1, list) else [])
        items2 = _extract_list(raw2) if isinstance(raw2, dict) else (raw2 if isinstance(raw2, list) else [])
        # Sembol bazında birleştir, tekrarları at
        seen: Dict[str, Any] = {}
        for it in items1 + items2:
            sym = (it.get("symbol") or "").upper()
            if sym and sym not in seen:
                seen[sym] = it
        candidates = list(seen.values())[:40]

        # Paralel analist verisi çek
        async def _fetch_analyst(item: dict) -> dict:
            sym = (item.get("symbol") or "").upper()
            try:
                d = await call_mcp("get_analyst_data", {"symbol": sym, "market": "us"})
                summ = d.get("summary", {}) if isinstance(d, dict) else {}
                sb   = summ.get("strong_buy", 0) or 0
                b    = summ.get("buy", 0) or 0
                h    = summ.get("hold", 0) or 0
                s    = summ.get("sell", 0) or 0
                ss   = summ.get("strong_sell", 0) or 0
                total = sb + b + h + s + ss
                score = (sb * 2 + b) / max(total, 1)  # ağırlıklı puan
                item["_analyst_score"]  = score
                item["_total_analysts"] = total
                item["_strong_buy"]     = sb
                item["_buy"]            = b
                item["_hold"]           = h
                item["_sell"]           = s + ss
                item["_consensus"]      = summ.get("consensus", "")
                item["_mean_target"]    = summ.get("mean_target")
                item["recommendation"]  = summ.get("consensus", "")
            except Exception:
                item["_analyst_score"] = 0
            return item

        enriched = await asyncio.gather(*[_fetch_analyst(it) for it in candidates])
        # En az 5 analist görüşü olanları filtrele, puana göre sırala
        filtered = [x for x in enriched if (x.get("_total_analysts") or 0) >= 5]
        filtered.sort(key=lambda x: x.get("_analyst_score", 0), reverse=True)
        return filtered[:25]

    # BIST için: scan_stocks ile gerçek filtrelenmiş sonuçları birleştir
    raw1, raw2, raw3 = await asyncio.gather(
        call_mcp("scan_stocks", {"preset": "bullish_momentum", "index": "XU100"}),
        call_mcp("scan_stocks", {"preset": "high_volume",      "index": "XU030"}),
        call_mcp("scan_stocks", {"preset": "big_gainers",      "index": "XU100"}),
    )
    def _to_list(r):
        if isinstance(r, list): return r
        if isinstance(r, dict): return _extract_list(r)
        return []
    items1 = [_scan_to_screener(x) for x in _to_list(raw1)]
    items2 = [_scan_to_screener(x) for x in _to_list(raw2)]
    items3 = [_scan_to_screener(x) for x in _to_list(raw3)]
    seen2: Dict[str, Any] = {}
    for it in items1 + items2 + items3:
        sym = (it.get("symbol") or "").upper()
        if sym and sym not in seen2:
            seen2[sym] = it
    bist_items = list(seen2.values())
    # Değişim oranına göre büyükten küçüğe
    bist_items.sort(key=lambda x: x.get("change_percent") or x.get("change") or 0, reverse=True)
    return bist_items[:25]


@app.get("/api/screener/help")
async def screener_help():
    return await call_mcp("get_screener_help", {})


# ─────────────────────────────────────────────────────────────────────
# Technical scanner
# ─────────────────────────────────────────────────────────────────────


def _sort_scan(items: list, preset: str) -> list:
    p = preset.lower()
    if "oversold" in p:
        return sorted(items, key=lambda x: x.get("rsi") or 100)  # en düşük RSI önce
    if "overbought" in p:
        return sorted(items, key=lambda x: x.get("rsi") or 0, reverse=True)
    if "big_gainers" in p or "bullish" in p or "positive" in p or "breakout" in p:
        return sorted(items, key=lambda x: x.get("change") or 0, reverse=True)
    if "big_losers" in p or "bearish" in p or "negative" in p:
        return sorted(items, key=lambda x: x.get("change") or 0)
    if "volume" in p:
        return sorted(items, key=lambda x: x.get("volume") or 0, reverse=True)
    return sorted(items, key=lambda x: x.get("change") or 0, reverse=True)


@app.get("/api/scan")
async def scan(preset: str = "oversold", index: str = "XU100"):
    raw = await call_mcp("scan_stocks", {"preset": preset, "index": index})
    items = _extract_list(raw) if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
    items = _sort_scan(items, preset)
    if isinstance(raw, list):
        return items
    for k in ("results", "securities", "stocks", "data", "items", "matches", "tickers"):
        if k in raw:
            raw[k] = items
            return raw
    return items


# ─────────────────────────────────────────────────────────────────────
# Index
# ─────────────────────────────────────────────────────────────────────


@app.get("/api/index")
async def index_data(symbol: str = "XU100"):
    return await call_mcp("get_index_data", {"symbol": symbol})


# ─────────────────────────────────────────────────────────────────────
# MCP health check
# ─────────────────────────────────────────────────────────────────────


@app.get("/api/status")
async def status():
    ready = mcp is not None and mcp._ready
    return {"mcp_ready": ready}


# ─────────────────────────────────────────────────────────────────────
# Frontend
# ─────────────────────────────────────────────────────────────────────

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if _is_authenticated(request):
        return RedirectResponse("/", status_code=302)
    return HTMLResponse(_LOGIN_HTML.replace("{error}", ""))


@app.post("/login")
async def do_login(password: str = Form(...)):
    if SITE_PASSWORD and hmac.compare_digest(password, SITE_PASSWORD):
        resp = RedirectResponse("/", status_code=302)
        resp.set_cookie(
            "session", _session_token(),
            httponly=True, samesite="lax", max_age=86400 * 30,
        )
        return resp
    err = '<p class="err">Yanlış şifre</p>'
    return HTMLResponse(_LOGIN_HTML.replace("{error}", err), status_code=401)


@app.post("/logout")
async def logout():
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("session")
    return resp


@app.get("/", response_class=HTMLResponse)
async def root():
    return (STATIC_DIR / "index.html").read_text(encoding="utf-8")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000)
