import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install",
    "requests", "beautifulsoup4", "pdfplumber", "gspread", "google-auth", "-q"])

import requests, re, io, time, urllib3, socket
from datetime import datetime, timedelta
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import gspread
from google.auth import default
from google.oauth2.service_account import Credentials as SACredentials
import os, json
import email_digest
import pdfplumber

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ════════════════════════════════════════════════════════════
# CONFIG
# First run:  WEEKS_TO_SCRAPE = 12  (3 month backfill)
# Weekly run: WEEKS_TO_SCRAPE = 2
# ════════════════════════════════════════════════════════════
SHEET_ID        = "172bpv-b2_nK5ENE1XPk5rWeokvnr1sjHvLBfVzHWh6c"
WEEKS_TO_SCRAPE = 12

# ── Verified Idox portals only ────────────────────────────
# Each URL is tested at startup — dead ones are skipped automatically.
# All use the standard Idox /search.do?action=advanced endpoint.
COUNCILS = {
    # ── GREATER MANCHESTER ─────────────────────────────────
    # HTTP 500 is intermittent load — preflight will retry
    "Manchester":    "https://pa.manchester.gov.uk/online-applications",
    "Salford":       "https://publicaccess.salford.gov.uk/online-applications",
    "Tameside":      "https://publicaccess.tameside.gov.uk/online-applications",
    "Trafford":      "https://publicaccess.trafford.gov.uk/online-applications",
    "Oldham":        "https://planning.oldham.gov.uk/online-applications",
    "Bolton":        "https://www.planningpa.bolton.gov.uk/online-applications-17",
    "Wigan":         "https://planning.wigan.gov.uk/online-applications",
    "Bury":          "https://planning.bury.gov.uk/online-applications",

    # ── MERSEYSIDE & CHESHIRE ──────────────────────────────
    "Wirral":        "https://planning.wirral.gov.uk/online-applications",
    "Knowsley":      "https://publicaccess.knowsley.gov.uk/online-applications",
    "Warrington":    "https://planning.warrington.gov.uk/online-applications",
    "Cheshire East": "https://pa.cheshireeast.gov.uk/online-applications",
    "Blackburn":     "https://idox.blackburn.gov.uk/online-applications",
    "St Helens":     "https://pa.sthelens.gov.uk/online-applications",

    # ── WEST YORKSHIRE ─────────────────────────────────────
    "Leeds":         "https://publicaccess.leeds.gov.uk/online-applications",
    "Bradford":      "https://planning.bradford.gov.uk/online-applications",
    "Calderdale":    "https://publicaccess.calderdale.gov.uk/online-applications",
    "Wakefield":     "https://planning.wakefield.gov.uk/online-applications",
    "Kirklees":      "https://www.kirklees.gov.uk/beta/planning-and-building-control/planning-applications/search-planning-applications",

    # ── SOUTH YORKSHIRE ────────────────────────────────────
    "Sheffield":     "https://planningapps.sheffield.gov.uk/online-applications",
    "Rotherham":     "https://planningonline.rotherham.gov.uk/online-applications",
    "Doncaster":     "https://planning.doncaster.gov.uk/online-applications",

    # ── EAST MIDLANDS ──────────────────────────────────────
    "Leicester":     "https://planning.leicester.gov.uk/online-applications",
    "Nottingham":    "https://publicaccess.nottinghamcity.gov.uk/online-applications",
    "Peterborough":  "https://planning.peterborough.gov.uk/online-applications",
    "Derby":         "https://eplanning.derby.gov.uk/online-applications",
    "Lincoln":       "https://planning.lincoln.gov.uk/online-applications",
    "Northampton":   "https://publicaccess.northampton.gov.uk/online-applications",

    # ── WEST MIDLANDS ──────────────────────────────────────
    "Solihull":      "https://publicaccess.solihull.gov.uk/online-applications",
    "Wolverhampton": "https://planningonline.wolverhampton.gov.uk/online-applications",
    "Walsall":       "https://planning.walsall.gov.uk/online-applications",

    # ── SOUTH WEST ─────────────────────────────────────────
    "Bristol":       "https://planningonline.bristol.gov.uk/online-applications",
    "Plymouth":      "https://planning.plymouth.gov.uk/online-applications",
    "Exeter":        "https://publicaccess.exeter.gov.uk/online-applications",
    "Cornwall":      "https://planning.cornwall.gov.uk/online-applications",
    "Swindon":       "https://pa.swindon.gov.uk/online-applications",
    "Bath":          "https://www.bathnes.gov.uk/services/planning-and-building-control/planning-applications/search-planning-applications",
    "Gloucester":    "https://publicaccess.gloucester.gov.uk/online-applications",
    "Cheltenham":    "https://publicaccess.cheltenham.gov.uk/online-applications",
    "Taunton":       "https://www.somersetwestandtaunton.gov.uk/planning/search-planning-applications",

    # ── SOUTH EAST ─────────────────────────────────────────
    "Portsmouth":    "https://publicaccess.portsmouth.gov.uk/online-applications",
    "Southampton":   "https://planningpublicaccess.southampton.gov.uk/online-applications",
    "Reading":       "https://planning.reading.gov.uk/online-applications",
    "Oxford":        "https://public.oxford.gov.uk/online-applications",
    "Milton Keynes": "https://pa.milton-keynes.gov.uk/online-applications",
    "Medway":        "https://planning.medway.gov.uk/online-applications",
    "Canterbury":    "https://publicaccess.canterbury.gov.uk/online-applications",
    "Maidstone":     "https://www.maidstone.gov.uk/planning-portal/search-for-planning-applications",
    "Guildford":     "https://planningpublicaccess.guildford.gov.uk/online-applications",
    "Thanet":        "https://planning.thanet.gov.uk/online-applications",
    "Winchester":    "https://planningapps.winchester.gov.uk/online-applications",
    "Eastbourne":    "https://planning.eastbourne.gov.uk/online-applications",

    # ── EAST OF ENGLAND ────────────────────────────────────
    "Norfolk":       "https://idoxpa.north-norfolk.gov.uk/online-applications",
    "Norwich":       "https://planning.norwich.gov.uk/online-applications",
    "Ipswich":       "https://publicaccess.ipswich.gov.uk/online-applications",
    "Cambridge":     "https://applications.greatercambridgeplanning.org/online-applications",
    "Luton":         "https://planning.luton.gov.uk/online-applications",
    "Chelmsford":    "https://publicaccess.chelmsford.gov.uk/online-applications",
    "Colchester":    "https://planningpa.colchester.gov.uk/online-applications",
    "Southend":      "https://publicaccess.southend-on-sea.gov.uk/online-applications",
    "Tendring":      "https://idox.tendringdc.gov.uk/online-applications",

    # ── LONDON ─────────────────────────────────────────────
    "Westminster":   "https://idoxpa.westminster.gov.uk/online-applications",
    "Camden":        "https://camdocs.camden.gov.uk/online-applications",
    "Southwark":     "https://planning.southwark.gov.uk/online-applications",
    "Ealing":        "https://pam.ealing.gov.uk/online-applications",
    "Islington":     "https://www.islington.gov.uk/planning/planning-applications/search-planning-applications",
    "Lewisham":      "https://planning.lewisham.gov.uk/online-applications",
    "Lambeth":       "https://planning.lambeth.gov.uk/online-applications",
    "Croydon":       "https://publicaccess.croydon.gov.uk/online-applications",
    "Barnet":        "https://publicaccess.barnet.gov.uk/online-applications",
    "Enfield":       "https://planningandbuildingcontrol.enfield.gov.uk/online-applications",
    "Brent":         "https://pa.brent.gov.uk/online-applications",
    "Tower Hamlets": "https://development.towerhamlets.gov.uk/online-applications",
    "Greenwich":     "https://planning.royalgreenwich.gov.uk/online-applications",
    "Hackney":       "https://planningapps.hackney.gov.uk/online-applications",
    "Newham":        "https://pa.newham.gov.uk/online-applications",
    "Haringey":      "https://www.planningservices.haringey.gov.uk/online-applications",
    "Wandsworth":    "https://planning.wandsworth.gov.uk/online-applications",
    "Waltham Forest":"https://planning.walthamforest.gov.uk/online-applications",
    "Hounslow":      "https://planningpa.hounslow.gov.uk/online-applications",
    "Sutton":        "https://planningregister.sutton.gov.uk/online-applications",
    "Kingston":      "https://publicaccess.kingston.gov.uk/online-applications",
    "Merton":        "https://planning.merton.gov.uk/online-applications",
    "Richmond":      "https://www2.richmond.gov.uk/online-applications",
    "Bromley":       "https://searchapps.bromley.gov.uk/online-applications",
    "Hillingdon":    "https://pa.hillingdon.gov.uk/online-applications",
    "Harrow":        "https://planningsearch.harrow.gov.uk/online-applications",
    "Redbridge":     "https://publicaccess.redbridge.gov.uk/online-applications",
    "Havering":      "https://development.havering.gov.uk/online-applications",
    "Bexley":        "https://pa.bexley.gov.uk/online-applications",
    "Kensington":    "https://www.rbkc.gov.uk/planning/searches/default.aspx",

    # ── NORTH EAST ─────────────────────────────────────────
    "Newcastle":     "https://publicaccess.newcastle.gov.uk/online-applications",
    "Gateshead":     "https://publicaccess.gateshead.gov.uk/online-applications",
    "Sunderland":    "https://publicaccess.sunderland.gov.uk/online-applications",
    "North Tyneside":"https://idoxpublicaccess.northtyneside.gov.uk/online-applications",
    "South Tyneside":"https://www.southtyneside.gov.uk/article/37030/Search-for-planning-applications",
    "Durham":        "https://publicaccess.durham.gov.uk/online-applications",
    "Middlesbrough": "https://planning.middlesbrough.gov.uk/online-applications",
    "Sunderland":    "https://publicaccess.sunderland.gov.uk/online-applications",

    # ── NORTH WEST ─────────────────────────────────────────
    "Preston":       "https://www.preston.gov.uk/planning-portal/online-applications",
    "Lancaster":     "https://planning.lancaster.gov.uk/online-applications",
    "Blackpool":     "https://idox.blackpool.gov.uk/online-applications",
    "Carlisle":      "https://planning.carlisle.gov.uk/online-applications",
    "Cumbria":       "https://planning.cumbria.gov.uk/online-applications",
}

RETAIL_KEYWORDS = ["Retail", "Class E", "shop", "supermarket", "convenience", "comparison"]

PDF_TRIGGERS = [
    "sequential test", "sequential approach", "sequential",
    "retail impact", "impact assessment", "town centre impact",
    "primary shopping", "primary retail",
    "out-of-centre", "out of centre",
    "main town centre", "nppf",
]

HEADERS_HTTP = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# ════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════
def log(msg, i=0):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {'  '*i}{msg}", flush=True)

# ════════════════════════════════════════════════════════════
# SESSION
# ════════════════════════════════════════════════════════════
def new_session():
    s = requests.Session()
    s.headers.update(HEADERS_HTTP)
    s.verify = False
    return s

def _is_dns_error(e):
    """True if the error is a DNS resolution failure — pointless to retry."""
    msg = str(e)
    return any(x in msg for x in [
        "NameResolutionError", "Name or service not known",
        "nodename nor servname", "getaddrinfo failed",
        "[Errno -2]", "[Errno 11001]",
    ])

def safe_get(sess, url, timeout=25, retries=2):
    for attempt in range(retries):
        try:
            r = sess.get(url, timeout=timeout, allow_redirects=True)
            return r
        except requests.exceptions.ConnectionError as e:
            if _is_dns_error(e):
                # DNS won't fix itself on retry — fail immediately
                log(f"  ❌ DNS failure (dead URL): {url[:70]}", 2)
                return None
            if attempt < retries - 1:
                time.sleep(4)
            else:
                log(f"  ❌ GET failed: {e}", 2)
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                log(f"  ⏱️  Timeout, retry {attempt+2}...", 2)
                time.sleep(5)
            else:
                log(f"  ❌ Timeout after {retries} attempts: {url[:60]}", 2)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(4)
            else:
                log(f"  ❌ GET failed: {e}", 2)
    return None

# ════════════════════════════════════════════════════════════
# PRE-FLIGHT: test every council URL before scraping
# ════════════════════════════════════════════════════════════
def _check_one_url(name, base_url, timeout=35):
    """
    Test a single council URL.  Returns ("ok"|"dead"|"retry", reason).
    Tries up to 3 times for transient 5xx errors.
    """
    test_url = f"{base_url}/search.do?action=advanced&searchType=Application"
    sess = new_session()
    last_status = None

    for attempt in range(3):
        try:
            r = sess.get(test_url, timeout=timeout, allow_redirects=True, verify=False)
            last_status = r.status_code

            if r.status_code == 200:
                has_form = bool(BeautifulSoup(r.text, "html.parser").find("form"))
                if has_form:
                    return "ok", None
                # 200 but not Idox — no point retrying
                return "dead", "200 but no Idox form"

            if r.status_code in (500, 502, 503, 504):
                # Server-side error — retry after brief pause
                if attempt < 2:
                    log(f"  ⏳ {name:22s} HTTP {r.status_code} — retry {attempt+2}/3 in 15s…", 0)
                    time.sleep(15)
                    continue
                return "dead", f"HTTP {r.status_code} (3 attempts)"

            if r.status_code == 403:
                return "dead", "HTTP 403 (bot-blocked)"
            if r.status_code == 406:
                return "dead", "HTTP 406 (Not Acceptable)"
            if r.status_code == 404:
                return "dead", "HTTP 404 (wrong URL path)"

            return "dead", f"HTTP {r.status_code}"

        except requests.exceptions.ConnectionError as e:
            reason = "DNS failure — URL doesn't exist" if _is_dns_error(e) else "Connection error"
            if "DNS" in reason or attempt == 2:
                return "dead", reason
            # Connection error (timeout/reset) — retry
            log(f"  ⏳ {name:22s} {reason} — retry {attempt+2}/3 in 10s…", 0)
            time.sleep(10)
            continue

        except requests.exceptions.Timeout:
            if attempt < 2:
                log(f"  ⏳ {name:22s} Timeout — retry {attempt+2}/3 with longer timeout…", 0)
                timeout = 55   # increase timeout on retry
                time.sleep(5)
                continue
            return "dead", "Timeout (55s, 3 attempts)"

        except Exception as e:
            return "dead", f"{type(e).__name__}: {str(e)[:40]}"

    return "dead", f"HTTP {last_status} after 3 attempts"


def preflight_check(councils):
    """
    Tests every council URL.  Returns (live_dict, dead_dict).
    Retries transient 500/502/503/timeout errors up to 3 times.
    """
    log("\n🔍 PRE-FLIGHT URL CHECK")
    log("=" * 60)
    live = {}
    dead = {}

    for name, base_url in councils.items():
        status, reason = _check_one_url(name, base_url)
        if status == "ok":
            live[name] = base_url
            log(f"  ✅ {name:22s} OK")
        else:
            dead[name] = reason
            emoji = "❌" if "DNS" in reason or "404" in reason else "⚠️ "
            log(f"  {emoji} {name:22s} {reason}")

    log(f"\n  ✅ {len(live)} live   ❌ {len(dead)} unreachable")
    if dead:
        log(f"  Skipping: {', '.join(dead.keys())}")
    log("=" * 60)
    return live, dead

# ════════════════════════════════════════════════════════════
# GOOGLE SHEETS — with retry + in-memory dedup cache
# ════════════════════════════════════════════════════════════
SHEET_HEADERS = [
    "Council", "Reference", "Address", "Description", "App Type",
    "Applicant", "Agent", "Date Received", "Date Decided", "Decision",
    "Trigger Words", "Score", "Keyword", "Portal Link", "Decision Doc URL",
    "Date Found", "Mark's Comments",
    # ── Sales Intelligence (added v16) ──
    "Est. Project Value", "Developer", "Architect",
    "Impact Probability", "CH Number", "Registered Address", "Contact Link",
]

_ws           = None   # cached worksheet
_existing_refs = set() # in-memory dedup — loaded once at startup

def sheets_retry(fn, retries=5, base_delay=10):
    """Exponential backoff for transient Google API errors (500/503/quota)."""
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            msg = str(e)
            transient = any(code in msg for code in [
                "500", "503", "quota", "rate", "UNAVAILABLE",
                "internal", "temporarily", "overloaded",
            ])
            if transient and attempt < retries - 1:
                delay = base_delay * (2 ** attempt)  # 10s, 20s, 40s, 80s, 160s
                log(f"  ⚠️  Sheets API error (attempt {attempt+1}/{retries}): {msg[:55]}")
                log(f"  ⏳ Waiting {delay}s...")
                time.sleep(delay)
            else:
                raise

def _make_gspread_client():
    """
    Returns an authorised gspread client.
    - GitHub Actions / automated: reads GCP_SERVICE_ACCOUNT_JSON env var.
    - Google Colab interactive:   uses google.colab.auth + default().
    """
    sa_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "").strip()
    if sa_json:
        info  = json.loads(sa_json)
        creds = SACredentials.from_service_account_info(info, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ])
        log("✅ Auth via service account (automated mode)")
        return gspread.authorize(creds)
    else:
        creds, _ = default()
        log("✅ Auth via Colab default credentials")
        return gspread.authorize(creds)


def get_sheet():
    global _ws
    if _ws:
        return _ws
    try:
        def _connect():
            gc_client = _make_gspread_client()
            ws = gc_client.open_by_key(SHEET_ID).worksheet("Leads")
            existing = ws.row_values(1)
            if existing != SHEET_HEADERS:
                ws.update(values=[SHEET_HEADERS], range_name="A1")
                log("✅ Headers written")
            else:
                log("✅ Sheets connected")
            return ws
        _ws = sheets_retry(_connect)
        return _ws
    except Exception as e:
        log(f"❌ Sheets connect failed after retries: {e}")
        return None

def load_existing_refs():
    """
    Load all existing reference numbers from column B into memory.
    Called once at startup — avoids per-lead API calls for dedup.
    """
    global _existing_refs
    ws = get_sheet()
    if not ws:
        return
    try:
        refs = sheets_retry(lambda: ws.col_values(2))
        _existing_refs = set(refs[1:])  # skip header row
        log(f"✅ Loaded {len(_existing_refs)} existing refs (dedup cache)")
    except Exception as e:
        log(f"⚠️  Could not load existing refs: {e} — duplicate check may miss some")

def write_lead(lead):
    ws = get_sheet()
    if not ws:
        return False

    # Fast in-memory dedup check
    if lead["ref"] in _existing_refs:
        log(f"  ⏭️  Duplicate: {lead['ref']}")
        return False

    row_data = [
        lead["council"], lead["ref"], lead["addr"], lead["desc"],
        lead["app_type"], lead["applicant"], lead["agent"],
        lead["date_rec"], lead["date_dec"], "Refused",
        lead["triggers"], lead["score"], lead["keyword"],
        lead["url"], lead["doc_url"],
        datetime.now().strftime("%Y-%m-%d %H:%M"), "",
        # Sales intelligence columns
        lead.get("est_value",""),
        lead.get("developer",""),
        lead.get("architect",""),
        str(lead.get("impact_prob","")) + "%" if lead.get("impact_prob") else "",
        lead.get("ch_number",""),
        lead.get("reg_address",""),
        lead.get("contact_link",""),
    ]

    try:
        sheets_retry(lambda: ws.append_row(row_data))
        _existing_refs.add(lead["ref"])  # update in-memory cache
        log(f"  💾 SAVED: {lead['ref']} | {lead['triggers'][:50]}")
        return True
    except Exception as e:
        log(f"  ❌ Sheets write failed after retries: {e}")
        return False

# ════════════════════════════════════════════════════════════
# SCORING
# ════════════════════════════════════════════════════════════
def score_lead(desc, triggers):
    s  = 50
    d  = desc.lower()
    tw = " ".join(triggers).lower()
    for w, p in [
        ("supermarket", 20), ("food store", 20), ("retail park", 15),
        ("out of centre", 15), ("out-of-centre", 15), ("class e", 10), ("major", 10),
    ]:
        if w in d: s += p
    sqm = re.findall(r'(\d[\d,]*)\s*(?:sq\.?\s*m|sqm|square metre)', d)
    if sqm:
        try:
            n = int(sqm[0].replace(",", ""))
            s += 30 if n >= 2500 else 20 if n >= 1000 else 10 if n >= 500 else 0
        except Exception:
            pass
    for w, p in [("sequential test", 15), ("retail impact", 15), ("impact assessment", 10)]:
        if w in tw: s += p
    return min(s, 100)

# ════════════════════════════════════════════════════════════
# SALES INTELLIGENCE ENRICHMENT
# ════════════════════════════════════════════════════════════

# Build rate per sqm by use type (conservative UK estimates, £/sqm)
_BUILD_RATES = {
    "supermarket":    1800,
    "food store":     1800,
    "retail park":    1200,
    "retail":         1100,
    "class e":        1000,
    "mixed use":      1400,
    "restaurant":     1600,
    "convenience":    1100,
    "comparison":     1000,
    "shop":           1000,
}
_LONDON_BOROUGHS = {
    "westminster","camden","southwark","ealing","islington","hackney",
    "lewisham","lambeth","newham","croydon","barnet","enfield","brent",
    "tower hamlets","greenwich","waltham forest","wandsworth","haringey",
}

def estimate_project_value(desc, council, triggers):
    """
    Estimate construction value from:
    1. Floor area (sqm) × build rate per use type
    2. If no sqm found, use keyword-based banding
    Returns a string like "£2.1m–£3.4m" or "£500k–£1m"
    """
    d   = desc.lower()
    loc = council.lower()
    london_premium = 1.35 if any(b in loc for b in _LONDON_BOROUGHS) else 1.0

    # Detect build rate
    rate = 1000  # default
    for kw, r in _BUILD_RATES.items():
        if kw in d:
            rate = r
            break

    rate = int(rate * london_premium)

    # Try to find sqm
    sqm_match = re.findall(
        r'(\d[\d,]*)\s*(?:sq\.?\s*m(?:etres?)?|sqm|m2|square\s+metre)', d
    )
    if sqm_match:
        try:
            sqm = int(sqm_match[0].replace(",",""))
            lo  = sqm * rate
            hi  = sqm * int(rate * 1.3)
            return _fmt_value(lo), _fmt_value(hi)
        except Exception:
            pass

    # No sqm — band by keywords
    if any(w in d for w in ["major","superstore","supermarket","retail park","district centre"]):
        lo, hi = 3_000_000, 15_000_000
    elif any(w in d for w in ["food store","convenience","large format"]):
        lo, hi = 1_000_000, 5_000_000
    elif any(w in d for w in ["retail","class e","shop","commercial"]):
        lo, hi = 250_000, 1_500_000
    else:
        lo, hi = 150_000, 750_000

    lo = int(lo * london_premium)
    hi = int(hi * london_premium)
    return _fmt_value(lo), _fmt_value(hi)

def _fmt_value(n):
    if n >= 1_000_000:
        return f"£{n/1_000_000:.1f}m"
    return f"£{n//1000}k"

def impact_probability(desc, triggers, score):
    """
    0–100 probability that this project needs a formal retail impact study.
    Based on NPPF threshold indicators and trigger word strength.
    """
    d  = desc.lower()
    tw = " ".join(triggers).lower() if triggers else ""
    p  = 40  # base

    # Size indicators (main NPPF trigger: >2500 sqm needs full RIA)
    sqm_m = re.findall(r'(\d[\d,]*)\s*(?:sq\.?\s*m|sqm|m2)', d)
    if sqm_m:
        try:
            sqm = int(sqm_m[0].replace(",",""))
            if sqm >= 2500: p += 40
            elif sqm >= 1000: p += 25
            elif sqm >= 500:  p += 10
        except Exception:
            pass

    # Use type
    for kw, pts in [("supermarket",25),("food store",25),("retail park",20),
                    ("out of centre",20),("out-of-centre",20),
                    ("major",10),("district centre",10)]:
        if kw in d: p += pts

    # Trigger words confirm retail policy engagement
    if "sequential test"   in tw: p += 15
    if "retail impact"     in tw: p += 15
    if "impact assessment" in tw: p += 10
    if "main town centre"  in tw: p += 5
    if "primary shopping"  in tw: p += 5

    # High score = more complex = more likely to need study
    p += (score - 50) // 5

    return min(p, 98)  # never show 100% — leaves room for nuance

_CH_CACHE = {}  # avoid re-querying same company name

def lookup_companies_house(name):
    """
    Free Companies House API — no key required.
    Returns dict with: ch_number, reg_address, contact_link
    """
    if not name or len(name) < 4:
        return {}
    key = name.strip().lower()
    if key in _CH_CACHE:
        return _CH_CACHE[key]

    # Strip common suffixes to improve match quality
    clean = re.sub(
        r'(ltd|limited|plc|llp|llc|group|holdings|properties|developments?|'
        r'architects?|associates?|consulting|consultants?|design)',
        "", name, flags=re.I
    ).strip(" .,")
    if len(clean) < 3:
        clean = name

    try:
        url  = f"https://api.company-information.service.gov.uk/search/companies?q={requests.utils.quote(clean)}&items_per_page=3"
        resp = requests.get(url, timeout=8,
                            headers={"User-Agent":"MAPlanning/1.0"})
        if resp.status_code != 200:
            _CH_CACHE[key] = {}
            return {}

        items = resp.json().get("items", [])
        if not items:
            _CH_CACHE[key] = {}
            return {}

        # Pick best match: prefer active companies, then closest name
        best = None
        for item in items:
            status = item.get("company_status","").lower()
            if status in ("active",""):
                best = item; break
        if not best:
            best = items[0]

        ch_num  = best.get("company_number","")
        addr_obj= best.get("registered_office_address",{})
        addr    = ", ".join(filter(None,[
            addr_obj.get("address_line_1",""),
            addr_obj.get("locality",""),
            addr_obj.get("postal_code",""),
        ]))
        ch_link = f"https://find-and-update.company-information.service.gov.uk/company/{ch_num}"

        result = {
            "ch_number":    ch_num,
            "reg_address":  addr,
            "contact_link": ch_link,
        }
        _CH_CACHE[key] = result
        time.sleep(0.3)   # respect CH rate limit
        return result

    except Exception as e:
        log(f"  ⚠️  Companies House lookup failed for '{name[:30]}': {e}", 2)
        _CH_CACHE[key] = {}
        return {}


def enrich_lead(lead):
    """
    Adds sales intelligence fields to a qualified lead dict.
    Called after PDF scan confirms the lead is real.
    """
    desc     = lead.get("desc","")
    triggers = lead.get("triggers","").split(", ")
    council  = lead.get("council","")
    score    = lead.get("score", 50)

    log(f"  🔬 Enriching…", 2)

    # 1. Project value estimate
    lo, hi = estimate_project_value(desc, council, triggers)
    lead["est_value"] = f"{lo} – {hi}"
    log(f"  💰 Est. value: {lead['est_value']}", 2)

    # 2. Impact probability
    prob = impact_probability(desc, triggers, score)
    lead["impact_prob"] = prob
    log(f"  📊 Impact probability: {prob}%", 2)

    # 3. Companies House lookup for applicant (developer)
    applicant = lead.get("applicant","")
    ch_app    = lookup_companies_house(applicant) if applicant else {}
    lead["developer"]    = applicant  # keep original name
    lead["ch_number"]    = ch_app.get("ch_number","")
    lead["reg_address"]  = ch_app.get("reg_address","")
    lead["contact_link"] = ch_app.get("contact_link","")
    if ch_app:
        log(f"  🏢 CH: {lead['ch_number']} | {lead['reg_address'][:50]}", 2)

    # 4. Architect — treat agent as architect for planning purposes
    #    (planning agent is almost always an architect or planning consultant)
    lead["architect"] = lead.get("agent","")

    return lead


# ════════════════════════════════════════════════════════════
# FORM DISCOVERY
# Reads ALL fields from the Idox search page HTML so hidden
# CSRF tokens are automatically included in the POST body.
# ════════════════════════════════════════════════════════════
def read_form(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form")
    if not form:
        return None

    action = form.get("action", "")
    if action.startswith("http"):
        form_action = action
    elif action.startswith("/"):
        p = urlparse(base_url)
        form_action = f"{p.scheme}://{p.netloc}{action}"
    else:
        form_action = f"{base_url}/{action.lstrip('/')}"

    fields = {}
    for el in form.find_all(["input", "select", "textarea"]):
        name = el.get("name")
        if not name:
            continue
        tag = el.name.lower()
        if tag == "input":
            t = el.get("type", "text").lower()
            if t == "submit":
                continue
            if t in ("checkbox", "radio") and not el.get("checked"):
                continue
            fields[name] = el.get("value", "")
        elif tag == "select":
            first = el.find("option")
            fields[name] = first.get("value", "") if first else ""
        elif tag == "textarea":
            fields[name] = el.get_text(strip=True)

    # Find description / keyword field
    desc_field = None
    for el in form.find_all("input"):
        nm = el.get("name", "").lower()
        ei = el.get("id",   "").lower()
        if "description" in nm or "description" in ei or "keyword" in nm:
            desc_field = el.get("name")
            break

    # Find decision dropdown — 3-pass matching to avoid picking wrong option
    decision_field = None
    refused_value  = None
    for sel in form.find_all("select"):
        nm = sel.get("name", "").lower()
        ei = sel.get("id",   "").lower()
        if "decision" not in nm and "decision" not in ei:
            continue
        if "appeal" in nm or "appeal" in ei:
            continue
        opts = [(opt.get_text(strip=True), opt.get("value","")) for opt in sel.find_all("option")]
        # Pass 1: exact label "Refused"
        exact = None
        for label, val in opts:
            if label.strip().lower() == "refused":
                exact = (sel.get("name"), val); break
        # Pass 2: label contains "refus" but not "split"/"part"
        partial = None
        if not exact:
            for label, val in opts:
                lt = label.strip().lower()
                if "refus" in lt and "split" not in lt and "part" not in lt:
                    partial = (sel.get("name"), val); break
        # Pass 3: known Idox refused value codes
        coded = None
        if not exact and not partial:
            for label, val in opts:
                if val.upper() in {"REF","REFUSED","R","RFD"}:
                    coded = (sel.get("name"), val); break
        chosen = exact or partial or coded
        if chosen:
            decision_field, refused_value = chosen
        if decision_field:
            break

    # Find decision date start / end fields
    date_start = None
    date_end   = None
    for el in form.find_all("input"):
        nm = (el.get("name", "") + el.get("id", "")).lower()
        if not date_start and any(h in nm for h in [
            "decisionstart", "decidedstart", "applicationdecisionstart"
        ]):
            date_start = el.get("name")
        if not date_end and any(h in nm for h in [
            "decisionend", "decidedend", "applicationdecisionend"
        ]):
            date_end = el.get("name")

    return {
        "form_action": form_action,
        "fields":      fields,
        "desc":        desc_field,
        "decision":    decision_field,
        "refused":     refused_value,
        "date_start":  date_start,
        "date_end":    date_end,
    }

# ════════════════════════════════════════════════════════════
# SEARCH ONE KEYWORD
# ════════════════════════════════════════════════════════════
def _do_post(sess, base_url, keyword, date_from, date_to, with_refused=True):
    """
    One attempt at the Idox search form POST.
    Returns (items_list, form_info_dict) or ([], None) on failure.
    with_refused=False skips the decision filter entirely — used as fallback
    when the refused-filtered search returns 0 results.
    """
    search_url = f"{base_url}/search.do?action=advanced&searchType=Application"

    r = safe_get(sess, search_url, timeout=25)
    if not r or r.status_code != 200:
        log(f"  ❌ Search page HTTP {r.status_code if r else 'no response'}", 1)
        return [], None

    form = read_form(r.text, base_url)
    if not form:
        log(f"  ❌ No form on search page", 1)
        return [], None

    post = dict(form["fields"])
    post["searchType"] = "Application"
    post[form["desc"] or "searchCriteria.description"] = keyword

    if with_refused:
        if form["decision"] and form["refused"]:
            post[form["decision"]] = form["refused"]
        else:
            post["searchCriteria.caseDecision"] = "REF"
    # else: leave decision field at its default (blank / any) so ALL decisions come back

    post[form["date_start"] or "date(applicationDecisionStart)"] = date_from
    post[form["date_end"]   or "date(applicationDecisionEnd)"]   = date_to

    try:
        pr = sess.post(form["form_action"], data=post,
                       headers={"Referer": search_url}, timeout=30, allow_redirects=True)
        log(f"  POST → HTTP {pr.status_code}", 1)
    except Exception as e:
        log(f"  ❌ POST failed: {e}", 1)
        return [], None

    time.sleep(2)  # give server time to store session

    # Some portals redirect the POST straight to results — check first
    if pr.url and "Results" in pr.url and pr.status_code == 200:
        items = collect_pages(sess, base_url, pr, keyword)
        if items:
            return items, form

    # Standard: GET the results page — try two common URL variants
    result_urls = [
        f"{base_url}/advancedSearchResults.do?action=firstPage",
        f"{base_url}/searchResults.do?action=firstPage",
    ]
    for rurl in result_urls:
        rr = safe_get(sess, rurl)
        if not rr:
            continue
        # Check if we got results or bounced back to search form
        soup_title = ""
        try:
            from bs4 import BeautifulSoup as _BS
            soup_title = _BS(rr.text, "html.parser").title.get_text(strip=True) if _BS(rr.text,"html.parser").title else ""
        except Exception:
            pass
        if "Results" in soup_title or "result" in rr.url.lower():
            items = collect_pages(sess, base_url, rr, keyword)
            if items:
                return items, form
        elif "Applications Search" not in soup_title:
            # Unknown page — still try to parse
            items = collect_pages(sess, base_url, rr, keyword)
            if items:
                return items, form

    # Fallback: just try the first URL and return whatever we get
    rr = safe_get(sess, result_urls[0])
    if not rr:
        return [], form
    items = collect_pages(sess, base_url, rr, keyword)
    return items, form


def search_one_keyword(sess, base_url, keyword, date_from, date_to):
    log(f"  🔎 '{keyword}'  {date_from} → {date_to}", 1)

    # ── Attempt 1: keyword + refused decision filter + date range ────────────
    items, form = _do_post(sess, base_url, keyword, date_from, date_to, with_refused=True)

    if form:
        log(
            f"  desc='{form['desc']}' decision='{form['decision']}' "
            f"refused='{form['refused']}' "
            f"start='{form['date_start']}' end='{form['date_end']}'", 1
        )

    if items:
        return items

    # ── Attempt 2: 0 results with refused filter — retry WITHOUT it ──────────
    # Reason: some portals use non-standard refused values (e.g. "RAW"),
    # or the refused+keyword combo genuinely has 0 results but keyword alone does.
    # The PDF scanner already filters for refusal trigger words, so this is safe.
    if form is not None:
        log(f"  ⚠️  0 results with decision filter — retrying without it", 1)
        time.sleep(2)
        # Need a fresh session cookie (JSESSIONID) for new search
        items2, _ = _do_post(sess, base_url, keyword, date_from, date_to, with_refused=False)
        if items2:
            log(f"  ✅ Got {len(items2)} results without decision filter — PDF scanner will qualify", 1)
        return items2

    return []


def collect_pages(sess, base_url, first_resp, keyword):
    all_items = []
    page_num  = 1
    resp      = first_resp

    while True:
        soup  = BeautifulSoup(resp.text, "html.parser")
        title = soup.title.get_text().strip() if soup.title else ""
        items = parse_results(soup)

        if not items:
            if page_num == 1:
                log(f"  ⚠️  0 results — title='{title}'", 1)
                # Print a snippet to help diagnose silent failures
                snippet = soup.get_text(separator=" ", strip=True)[:250]
                log(f"  Page text: {snippet}", 1)
            else:
                log(f"  ✅ {len(all_items)} total", 1)
            break

        log(f"  📄 Page {page_num}: {len(items)} results", 1)
        for r in items:
            log(f"    • {r['ref'][:38]} — {r['desc'][:55]}", 1)
        all_items.extend(items)

        has_next = bool(
            soup.find("a", string=re.compile(r"Next", re.I)) or
            soup.find("a", href=re.compile(r"searchCriteria\.page="))
        )
        if not has_next:
            break

        page_num += 1
        next_url = f"{base_url}/pagedSearchResults.do?action=page&searchCriteria.page={page_num}"
        resp = safe_get(sess, next_url)
        if not resp:
            break
        time.sleep(1)

    log(f"  → {len(all_items)} for '{keyword}'", 1)
    return all_items

# ════════════════════════════════════════════════════════════
# PARSE RESULT CARDS
# ════════════════════════════════════════════════════════════
def extract_ref(text):
    for pat in [
        r'Ref\.?\s*[Nn]o[.:\s]+([A-Z0-9][A-Z0-9/\-]{3,30})',
        r'Reference[:\s]+([A-Z0-9][A-Z0-9/\-]{3,30})',
        r'\b([A-Z]{1,3}\d{4}/\d{4,})\b',
        r'\b(\d{5,}/[A-Z0-9]{2,}/\d{4})\b',
        r'\b([A-Z]{2}/\d{4}/\d{4,}/[A-Z0-9]+)\b',
        r'\b(\d{2}/\d{4,}/[A-Z]+)\b',
    ]:
        m = re.search(pat, text)
        if m:
            c = m.group(1).strip().rstrip(".")
            if 4 < len(c) < 35:
                return c
    return ""

def parse_results(soup):
    items = []
    rows = (
        soup.select("li.searchresult")            or
        soup.select("div.searchresult")           or
        soup.select("li[class*='searchresult']")  or
        soup.select("div[class*='searchresult']")
    )
    for card in rows:
        a = (
            card.select_one("a[href*='keyVal']") or
            card.select_one("a[href*='applicationDetails']") or
            card.select_one("a")
        )
        if not a:
            continue
        href    = a.get("href", "")
        key_val = href.split("keyVal=")[-1].split("&")[0] if "keyVal=" in href else ""
        if not key_val:
            continue
        card_text = card.get_text(separator=" ", strip=True)
        desc      = a.get_text(strip=True)[:250]
        ref       = extract_ref(card_text) or key_val
        addr_el   = card.select_one(".address") or card.select_one(".addressCol")
        addr      = addr_el.get_text(strip=True) if addr_el else ""
        if not addr:
            m = re.search(r'([A-Z][^\|]{8,80}[A-Z]{1,2}\d{1,2}\s?\d[A-Z]{2})', card_text)
            addr = m.group(1).strip() if m else ""
        items.append({"ref": ref, "keyVal": key_val, "desc": desc, "addr": addr[:150]})
    return items

# ════════════════════════════════════════════════════════════
# APPLICATION DETAILS (summary + details tabs)
# ════════════════════════════════════════════════════════════
def get_details(sess, base_url, key_val):
    d = {}
    r = safe_get(sess, f"{base_url}/applicationDetails.do?activeTab=summary&keyVal={key_val}")
    if r and r.status_code == 200:
        soup = BeautifulSoup(r.text, "html.parser")
        for row in soup.select("tr"):
            th = row.find("th")
            td = row.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True).lower().strip()
            value = td.get_text(strip=True)
            if   label == "proposal":              d["proposal"]  = value
            elif label == "address":               d["address"]   = value
            elif label == "decision":              d["decision"]  = value
            elif label == "decision issued date":  d["date_dec"]  = value
            elif label == "application validated": d["date_rec"]  = value
            elif label == "date received":         d.setdefault("date_rec", value)
        log(f"  Decision='{d.get('decision','?')}' Decided='{d.get('date_dec','?')}'", 2)
    time.sleep(0.5)
    r2 = safe_get(sess, f"{base_url}/applicationDetails.do?activeTab=details&keyVal={key_val}")
    if r2 and r2.status_code == 200:
        soup2 = BeautifulSoup(r2.text, "html.parser")
        for row in soup2.select("tr"):
            th = row.find("th")
            td = row.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True).lower().strip()
            value = td.get_text(strip=True)
            if "applicant name"    in label and not d.get("applicant"): d["applicant"] = value
            if "agent name"        in label and not d.get("agent"):     d["agent"]     = value
            if label == "agent"             and not d.get("agent"):     d["agent"]     = value
            if "application type"  in label and not d.get("app_type"): d["app_type"]  = value
    return d

# ════════════════════════════════════════════════════════════
# FIND DECISION DOCUMENT
# Scores each document row and picks the best match.
# ════════════════════════════════════════════════════════════
def find_decision_doc(sess, base_url, key_val):
    log(f"  📂 Documents...", 2)
    p    = urlparse(base_url)
    root = f"{p.scheme}://{p.netloc}"

    r = safe_get(sess, f"{base_url}/applicationDetails.do?activeTab=documents&keyVal={key_val}")
    if not r or r.status_code != 200:
        log(f"  ❌ Documents tab failed", 2)
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    rows = [row for row in soup.find_all("tr") if len(row.find_all("td")) >= 2]
    log(f"  {len(rows)} doc rows", 2)

    def abs_url(href):
        if not href or href in ("#", "javascript:void(0)"):
            return None
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            return f"{root}{href}"
        return f"{base_url}/{href.lstrip('/')}"

    best_url = None
    best_s   = 0

    for row in rows:
        cells = row.find_all("td")
        texts = [c.get_text(strip=True).lower() for c in cells]
        s = 0
        for t in texts:
            if t == "decision":             s = max(s, 100)
            if t == "refusal":              s = max(s, 100)
            if "decision notice"  in t:     s = max(s, 95)
            if "refusal notice"   in t:     s = max(s, 95)
            if "decision letter"  in t:     s = max(s, 95)
            if "refusal letter"   in t:     s = max(s, 95)
            if "decision"         in t and s < 40: s = max(s, 40)
            if "officer report"   in t:     s = max(s, 25)
        if s == 0:
            continue
        log(f"  Row score={s}: {'|'.join(texts[:4])[:65]}", 2)
        link = None
        for cell in reversed(cells):
            for a in cell.find_all("a", href=True):
                u = abs_url(a["href"])
                if u:
                    link = u
                    break
            if link:
                break
        if link and s > best_s:
            best_s   = s
            best_url = link
            log(f"  → ...{best_url[-65:]}", 2)

    if best_url:
        return best_url

    # Fallback 1 — any PDF in /files/ path
    for a in soup.find_all("a", href=True):
        u = abs_url(a["href"])
        if u and "/files/" in u and ".pdf" in u.lower():
            log(f"  Fallback PDF: ...{u[-55:]}", 2)
            return u

    # Fallback 2 — known Idox document download patterns
    for a in soup.find_all("a", href=True):
        u = abs_url(a["href"])
        if u and any(k in u for k in ["downloadDocument", "viewDoc", "fileDetails"]):
            log(f"  Fallback link: ...{u[-55:]}", 2)
            return u

    log(f"  ❌ No decision doc found", 2)
    return None

# ════════════════════════════════════════════════════════════
# PDF SCANNER
# ════════════════════════════════════════════════════════════
def scan_pdf(sess, pdf_url):
    log(f"  📥 ...{pdf_url[-55:]}", 2)
    try:
        r = sess.get(
            pdf_url,
            headers={"Accept": "application/pdf,*/*", "Referer": pdf_url},
            timeout=45,
            allow_redirects=True,
        )
        ct   = r.headers.get("Content-Type", "").lower()
        size = len(r.content)
        log(f"  HTTP {r.status_code} | {size:,}b | {ct[:30]}", 2)
        if r.status_code != 200:
            return []
        if "html" in ct:
            log(f"  ⚠️  Got HTML not PDF", 2)
            return []
        if size < 500:
            log(f"  ⚠️  Too small to be a PDF", 2)
            return []
        text = ""
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            log(f"  {len(pdf.pages)} pages", 2)
            for pg in pdf.pages:
                t = pg.extract_text()
                if t:
                    text += t.lower() + " "
        if not text.strip():
            log(f"  ⚠️  No extractable text (scanned image PDF?)", 2)
            return []
        log(f"  {len(text):,} chars extracted", 2)
        found = [w for w in PDF_TRIGGERS if w in text]
        for w in found:
            log(f"  🎯 '{w}'", 2)
        if not found:
            log(f"  ❌ No trigger words found", 2)
        return found
    except Exception as e:
        log(f"  ⚠️  PDF error: {e}", 2)
        return []

# ════════════════════════════════════════════════════════════
# PROCESS ONE APPLICATION
# ════════════════════════════════════════════════════════════
def process_app(sess, base_url, council, item):
    kv  = item["keyVal"]
    ref = item["ref"]
    log(f"")
    log(f"  ──────────────────────────────────────────────")
    log(f"  📋 {ref}")
    log(f"  {item['desc'][:90]}")

    det     = get_details(sess, base_url, kv)
    doc_url = find_decision_doc(sess, base_url, kv)
    if not doc_url:
        log(f"  ⚠️  No decision doc — skip")
        return None

    triggers = scan_pdf(sess, doc_url)
    if not triggers:
        log(f"  ❌ No trigger words — not a retail impact refusal")
        return None

    log(f"  🏆 QUALIFIED — Triggers: {triggers}")
    desc = det.get("proposal", item["desc"])
    sc   = score_lead(desc, triggers)
    log(f"  Score: {sc}/100")

    lead = {
        "council":   council,
        "ref":       ref,
        "addr":      det.get("address",   item["addr"]),
        "desc":      desc,
        "app_type":  det.get("app_type",  ""),
        "applicant": det.get("applicant", ""),
        "agent":     det.get("agent",     ""),
        "date_rec":  det.get("date_rec",  ""),
        "date_dec":  det.get("date_dec",  ""),
        "triggers":  ", ".join(triggers),
        "score":     sc,
        "keyword":   item["keyword"],
        "url":       f"{base_url}/applicationDetails.do?activeTab=summary&keyVal={kv}",
        "doc_url":   doc_url,
    }
    # Sales intelligence enrichment
    enrich_lead(lead)
    write_lead(lead)
    return lead

# ════════════════════════════════════════════════════════════
# SCRAPE ONE COUNCIL
# ════════════════════════════════════════════════════════════
def scrape_council(council, base_url, date_from, date_to):
    log(f"\n{'='*60}")
    log(f"🏛️  {council.upper()}  |  {date_from} → {date_to}")
    log(f"{'='*60}")

    sess      = new_session()
    all_items = []
    qualified = []

    for kw in RETAIL_KEYWORDS:
        try:
            items = search_one_keyword(sess, base_url, kw, date_from, date_to)
            new   = [i for i in items
                     if i["keyVal"] not in {x["keyVal"] for x in all_items}]
            for i in new:
                i["keyword"] = kw
            all_items.extend(new)
            time.sleep(2)
        except Exception as e:
            log(f"  ❌ Keyword '{kw}': {e}")

    log(f"\n  {len(all_items)} unique applications to scan")

    if not all_items:
        return []

    for idx, item in enumerate(all_items):
        log(f"\n  [{idx+1}/{len(all_items)}]")
        try:
            lead = process_app(sess, base_url, council, item)
            if lead:
                qualified.append(lead)
        except Exception as e:
            log(f"  ❌ {item.get('ref','?')}: {e}")
        time.sleep(1)

    log(f"\n✅ {council}: {len(qualified)} qualified leads")
    return qualified

# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
def run():
    today     = datetime.now()
    date_to   = today.strftime("%d/%m/%Y")
    date_from = (today - timedelta(weeks=WEEKS_TO_SCRAPE)).strftime("%d/%m/%Y")

    print("=" * 60)
    print(f"🏗️  MAPlanning Retail Lead Engine v17")
    print(f"📅  {today.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📆  {date_from} → {date_to}  ({WEEKS_TO_SCRAPE} weeks)")
    print(f"🏛️  {len(COUNCILS)} councils configured")
    print(f"🔎  {', '.join(RETAIL_KEYWORDS)}")
    print("=" * 60)

    # ── Step 1: connect to Sheets & load existing refs ──────
    if not get_sheet():
        print("❌ Sheets connection failed — stopping"); return
    load_existing_refs()

    # ── Step 2: pre-flight — only scrape working URLs ────────
    live_councils, dead_councils = preflight_check(COUNCILS)
    if not live_councils:
        print("❌ No reachable councils — check network"); return

    # ── Step 3: scrape every live council ───────────────────
    grand   = []
    summary = {}
    failed  = []

    for idx, (name, url) in enumerate(live_councils.items()):
        log(f"\n{'━'*60}")
        log(f"Council {idx+1}/{len(live_councils)}: {name}")
        log(f"{'━'*60}")
        leads = []
        for attempt in range(2):
            try:
                if attempt > 0:
                    log(f"🔄 Retry attempt 2...")
                    time.sleep(15)
                leads = scrape_council(name, url, date_from, date_to)
                break
            except Exception as e:
                log(f"❌ {name} attempt {attempt+1}: {e}")
                if attempt == 1:
                    failed.append(name)
        summary[name] = len(leads)
        grand.extend(leads)
        remaining = len(live_councils) - idx - 1
        if remaining > 0:
            log(f"\n⏸️  10s pause | {remaining} remaining | {len(grand)} leads so far")
            time.sleep(10)

    grand.sort(key=lambda x: x["score"], reverse=True)

    # ── Final report ─────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"📊 FINAL RESULTS")
    print(f"{'='*60}")

    print(f"\n  Live councils scraped ({len(live_councils)}):")
    for c, n in summary.items():
        mark = "❌ FAILED" if c in failed else f"🏆 {n} leads" if n else "  0"
        print(f"    {c:22s}: {mark}")

    if dead_councils:
        print(f"\n  Skipped ({len(dead_councils)} unreachable):")
        for c, reason in dead_councils.items():
            print(f"    {c:22s}: {reason}")

    print(f"\n  {'─'*36}")
    print(f"  {'TOTAL QUALIFIED':22s}: {len(grand)} leads")
    print(f"{'='*60}")

    if grand:
        print(f"\n🏆 TOP LEADS:")
        for lead in grand[:10]:
            print(f"\n  [{lead['score']}pts] {lead['council']} | {lead['ref']}")
            print(f"  {lead['addr']}")
            print(f"  {lead['desc'][:100]}")
            print(f"  Triggers: {lead['triggers']}")
            print(f"  {lead['url']}")

    # Email digest — only in automated (GitHub Actions) mode
    if os.environ.get("GMAIL_APP_PASSWORD"):
        log("\n📧 Sending email digest…")
        email_digest.send_digest(grand, summary, failed, date_from, date_to, log_fn=log)
    else:
        log("ℹ️  Email skipped (Colab mode — set GMAIL_APP_PASSWORD secret for automated emails)")

# ── Authenticate Google ──────────────────────────────────────
# In GitHub Actions: GCP_SERVICE_ACCOUNT_JSON env var is set — no action needed here.
# In Colab: trigger interactive auth so default() works.
if not os.environ.get("GCP_SERVICE_ACCOUNT_JSON"):
    try:
        from google.colab import auth
        auth.authenticate_user()
        print("✅ Google Colab auth done")
    except Exception:
        pass  # already authenticated or running locally

run()
