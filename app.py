"""
ValueLens Backend v4 — Python Flask
Data Sources (layered):
  1. Indian Stock Market API → CMP, PE, MCap, EPS, sector (free, INR)
  2. yfinance → Income statements, balance sheets (free, INR)
  3. EODHD → Future upgrade path for financials
  4. Local hardcoded → Fallback when all APIs fail

All numbers in INR. Zero conversion needed.
"""

import os
import time
import math
import logging
import traceback
from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor

# ═══════ CONFIG ═══════
app = Flask(__name__)
CORS(app)
PORT = int(os.environ.get("PORT", 10000))

# Indian Stock Market API (free, no key)
ISMA_BASE = "https://military-jobye-haiqstudios-14f59639.koyeb.app"

# EODHD (optional, for future upgrade)
EODHD_KEY = os.environ.get("EODHD_API_KEY", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("valuelens")

# ═══════ CACHE ═══════
cache = {}
TTL = {"quote": 300, "financials": 86400, "search": 86400}  # seconds


def cached(key, ttl_type="quote"):
    entry = cache.get(key)
    if not entry:
        return None
    if time.time() - entry["t"] > TTL.get(ttl_type, 300):
        del cache[key]
        return None
    return entry["d"]


def set_cache(key, data):
    cache[key] = {"d": data, "t": time.time()}


# ═══════ LAYER 1: Indian Stock Market API ═══════
def fetch_isma(symbol):
    """Fetch real-time price data from Indian Stock Market API. Returns INR natively."""
    try:
        # Remove .NS/.BO suffix if present — ISMA uses plain symbols
        clean = symbol.replace(".NS", "").replace(".BO", "")
        url = f"{ISMA_BASE}/stock?symbol={clean}&res=num"
        log.info(f"[ISMA] Fetching {clean}")
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            log.warning(f"[ISMA] {resp.status_code} for {clean}")
            return None
        data = resp.json()
        if data.get("status") != "success":
            return None
        d = data.get("data", {})
        return {
            "cmp": d.get("last_price", 0),
            "mcap_raw": d.get("market_cap", 0),  # Full INR value
            "pe": d.get("pe_ratio", 0),
            "eps": d.get("earnings_per_share", 0),
            "sector": d.get("sector", ""),
            "industry": d.get("industry", ""),
            "name": d.get("company_name", ""),
            "change": d.get("change", 0),
            "changePct": d.get("percent_change", 0),
            "yearHigh": d.get("year_high", 0),
            "yearLow": d.get("year_low", 0),
            "volume": d.get("volume", 0),
            "bookValue": d.get("book_value", 0),
            "dividendYield": d.get("dividend_yield", 0),
        }
    except Exception as e:
        log.error(f"[ISMA ERROR] {symbol}: {e}")
        return None


def search_isma(query):
    """Search stocks via Indian Stock Market API."""
    try:
        url = f"{ISMA_BASE}/search?query={query}"
        log.info(f"[ISMA SEARCH] {query}")
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return []
        data = resp.json()
        if data.get("status") != "success":
            return []
        results = data.get("results", [])
        return [
            {
                "sym": r.get("symbol", ""),
                "name": r.get("company_name", ""),
                "sec": "NSE",
            }
            for r in results[:15]
        ]
    except Exception as e:
        log.error(f"[ISMA SEARCH ERROR] {e}")
        return []


# ═══════ LAYER 2: yfinance (Financial Statements) ═══════
def fetch_yfinance_financials(symbol):
    """Fetch income statements from yfinance. Returns Revenue & PAT in INR (Crores)."""
    try:
        ticker = symbol if "." in symbol else symbol + ".NS"
        log.info(f"[YFINANCE] Fetching financials for {ticker}")
        t = yf.Ticker(ticker)

        # Get annual income statement
        inc = t.financials  # columns = dates, rows = line items
        if inc is None or inc.empty:
            log.warning(f"[YFINANCE] No financials for {ticker}")
            return None

        years = []
        for col in inc.columns:
            year = str(col.year) if hasattr(col, "year") else str(col)[:4]
            rev = 0
            pat = 0

            # Revenue: try multiple possible row names
            for key in ["Total Revenue", "Operating Revenue", "Revenue"]:
                if key in inc.index and not math.isnan(inc.at[key, col]):
                    rev = float(inc.at[key, col])
                    break

            # PAT: try multiple possible row names
            for key in ["Net Income", "Net Income Common Stockholders",
                        "Net Income From Continuing Operations"]:
                if key in inc.index and not math.isnan(inc.at[key, col]):
                    pat = float(inc.at[key, col])
                    break

            years.append({
                "year": year,
                "rev": round(rev / 1e7, 2),  # INR → Crores
                "pat": round(pat / 1e7, 2),  # INR → Crores
            })

        # Also try to get shares outstanding
        info = t.info or {}
        shares = info.get("sharesOutstanding", 0)

        return {"years": years, "shares": shares}
    except Exception as e:
        log.error(f"[YFINANCE ERROR] {symbol}: {e}")
        log.error(traceback.format_exc())
        return None


# ═══════ LAYER 3: EODHD (Future upgrade) ═══════
def fetch_eodhd_financials(symbol):
    """Fetch from EODHD if API key is available."""
    if not EODHD_KEY:
        return None
    try:
        ticker = symbol.replace(".NS", "").replace(".BO", "") + ".NSE"
        url = f"https://eodhd.com/api/fundamentals/{ticker}"
        params = {"api_token": EODHD_KEY, "fmt": "json", "filter": "Financials"}
        log.info(f"[EODHD] Fetching {ticker}")
        resp = requests.get(url, params=params, timeout=20)
        if resp.status_code != 200:
            log.warning(f"[EODHD] {resp.status_code} for {ticker}")
            return None
        data = resp.json()

        # Parse income statements
        income = data.get("Financials", {}).get("Income_Statement", {}).get("yearly", {})
        if not income:
            return None

        years = []
        for date_key, stmt in sorted(income.items(), reverse=True)[:10]:
            year = date_key[:4]
            rev = float(stmt.get("totalRevenue", 0) or 0)
            pat = float(stmt.get("netIncome", 0) or 0)
            years.append({
                "year": year,
                "rev": round(rev / 1e7, 2),
                "pat": round(pat / 1e7, 2),
            })
        return {"years": years, "shares": 0}
    except Exception as e:
        log.error(f"[EODHD ERROR] {symbol}: {e}")
        return None


# ═══════ HELPERS ═══════
def calc_cagr(arr, field, n):
    """Calculate CAGR over n years from array of yearly data."""
    if len(arr) < n + 1:
        return None
    a = arr[0].get(field, 0)  # most recent
    b = arr[n].get(field, 0)  # n years ago
    if not b or b <= 0 or not a or a <= 0:
        return None
    return round((math.pow(a / b, 1 / n) - 1) * 100, 1)


# ═══════ ROUTES ═══════

@app.route("/")
def health():
    return jsonify({
        "status": "ok",
        "service": "ValueLens API v4",
        "sources": {
            "realtime": "Indian Stock Market API (INR)",
            "financials": "EODHD" if EODHD_KEY else "Yahoo Finance (INR)",
            "fallback": "Local hardcoded data",
        },
        "cache_entries": len(cache),
        "eodhd_configured": bool(EODHD_KEY),
    })


@app.route("/api/search")
def search():
    q = request.args.get("q", "").strip()
    if not q or len(q) < 2:
        return jsonify([])

    ck = f"search:{q.lower()}"
    c = cached(ck, "search")
    if c:
        return jsonify(c)

    results = search_isma(q)

    # If ISMA search returns nothing, try yfinance search as backup
    if not results:
        try:
            log.info(f"[YFINANCE SEARCH] Trying yfinance for {q}")
            # yfinance doesn't have great search, but we can try ticker directly
            t = yf.Ticker(q + ".NS")
            info = t.info or {}
            if info.get("regularMarketPrice"):
                results = [{
                    "sym": q.upper(),
                    "name": info.get("longName", info.get("shortName", q.upper())),
                    "sec": info.get("sector", "NSE"),
                }]
        except:
            pass

    set_cache(ck, results)
    return jsonify(results)


@app.route("/api/fullstock/<symbol>")
def fullstock(symbol):
    sym = symbol.upper().replace(".NS", "").replace(".BO", "")
    ck = f"full:{sym}"
    c = cached(ck, "quote")
    if c:
        return jsonify(c)

    # ── LAYER 1: Real-time data from Indian Stock Market API ──
    isma = fetch_isma(sym)

    # ── LAYER 2: Financial statements (EODHD first, then yfinance) ──
    fin = fetch_eodhd_financials(sym)
    fin_source = "eodhd" if fin else None

    if not fin:
        fin = fetch_yfinance_financials(sym)
        fin_source = "yfinance" if fin else None

    # ── MERGE ──
    cmp = isma["cmp"] if isma else 0
    mcap_raw = isma["mcap_raw"] if isma else 0
    mcap_cr = mcap_raw / 1e7 if mcap_raw > 1e6 else mcap_raw  # Handle if already in Cr
    pe = isma["pe"] if isma else 0
    eps = isma["eps"] if isma else 0

    # Shares: derive from mcap and CMP
    shr_cr = mcap_cr / cmp if cmp > 0 and mcap_cr > 0 else 0

    # Financial data
    years = fin["years"] if fin else []
    latest_rev = years[0]["rev"] if years else 0
    latest_pat = years[0]["pat"] if years else 0

    result = {
        "sym": sym,
        "name": isma["name"] if isma else sym,
        "sec": isma["sector"] if isma else "Unknown",
        "industry": isma["industry"] if isma else "",
        "cmp": cmp,
        "shr": round(shr_cr, 2),
        "mcapCr": round(mcap_cr, 0),
        "pe": pe,
        "eps": eps,
        "pat": latest_pat,
        "rev": latest_rev,
        "r3": calc_cagr(years, "rev", 3),
        "r5": calc_cagr(years, "rev", 5),
        "p3": calc_cagr(years, "pat", 3),
        "p5": calc_cagr(years, "pat", 5),
        "dayChange": isma["change"] if isma else 0,
        "dayChangePct": isma["changePct"] if isma else 0,
        "yearHigh": isma["yearHigh"] if isma else 0,
        "yearLow": isma["yearLow"] if isma else 0,
        "bookValue": isma["bookValue"] if isma else 0,
        "dividendYield": isma["dividendYield"] if isma else 0,
        "_source": {
            "realtime": "isma" if isma else "none",
            "financials": fin_source or "none",
            "years_available": len(years),
        },
    }

    log.info(
        f"[RESULT] {sym}: CMP={cmp} MCap={round(mcap_cr)}Cr "
        f"PE={pe} PAT={latest_pat}Cr Rev={latest_rev}Cr "
        f"({len(years)} yrs from {fin_source or 'none'})"
    )

    set_cache(ck, result)
    return jsonify(result)


@app.route("/api/batch-quotes", methods=["POST"])
def batch_quotes():
    symbols = request.json.get("symbols", []) if request.json else []
    if not symbols:
        return jsonify([])

    ck = f"batch:{'_'.join(sorted(symbols[:20]))}"
    c = cached(ck, "quote")
    if c:
        return jsonify(c)

    results = []
    # Use Indian Stock Market API batch endpoint
    try:
        syms_str = ",".join(s.replace(".NS", "").replace(".BO", "") for s in symbols[:20])
        url = f"{ISMA_BASE}/stock/list?symbols={syms_str}&res=num"
        log.info(f"[BATCH] Fetching {len(symbols)} stocks")
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            stocks = data.get("stocks", [])
            for s in stocks:
                results.append({
                    "sym": s.get("symbol", ""),
                    "name": s.get("company_name", ""),
                    "cmp": s.get("last_price", 0),
                    "pe": s.get("pe_ratio", 0),
                    "mcapCr": round(s.get("market_cap", 0) / 1e7, 0),
                    "dayChangePct": s.get("percent_change", 0),
                })
    except Exception as e:
        log.error(f"[BATCH ERROR] {e}")

    set_cache(ck, results)
    return jsonify(results)


@app.route("/api/test")
def test():
    """Debug endpoint — tests all data sources."""
    result = {"status": "ok", "sources": {}}

    # Test Indian Stock Market API
    try:
        isma = fetch_isma("TCS")
        result["sources"]["isma"] = {
            "working": bool(isma and isma["cmp"] > 0),
            "tcs_cmp": isma["cmp"] if isma else 0,
            "tcs_mcap_cr": round(isma["mcap_raw"] / 1e7) if isma else 0,
            "tcs_pe": isma["pe"] if isma else 0,
        }
    except Exception as e:
        result["sources"]["isma"] = {"working": False, "error": str(e)}

    # Test yfinance
    try:
        t = yf.Ticker("TCS.NS")
        inc = t.financials
        has_data = inc is not None and not inc.empty
        result["sources"]["yfinance"] = {
            "working": has_data,
            "years": len(inc.columns) if has_data else 0,
        }
    except Exception as e:
        result["sources"]["yfinance"] = {"working": False, "error": str(e)}

    # Test EODHD if configured
    if EODHD_KEY:
        try:
            url = f"https://eodhd.com/api/eod/TCS.NSE?api_token={EODHD_KEY}&fmt=json&limit=1"
            resp = requests.get(url, timeout=10)
            result["sources"]["eodhd"] = {
                "working": resp.status_code == 200,
                "key_prefix": EODHD_KEY[:5] + "...",
            }
        except Exception as e:
            result["sources"]["eodhd"] = {"working": False, "error": str(e)}
    else:
        result["sources"]["eodhd"] = {"configured": False, "note": "Set EODHD_API_KEY env var to enable"}

    return jsonify(result)


# ═══════ START ═══════
if __name__ == "__main__":
    log.info(f"\n ValueLens API v4 | Port {PORT}")
    log.info(f"  Realtime: Indian Stock Market API (free, INR)")
    log.info(f"  Financials: {'EODHD' if EODHD_KEY else 'Yahoo Finance'} (INR)")
    log.info(f"  EODHD key: {'YES' if EODHD_KEY else 'NOT SET'}\n")
    app.run(host="0.0.0.0", port=PORT)
