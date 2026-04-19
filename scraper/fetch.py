#!/usr/bin/env python3
"""
Monroe County, NY — Motivated Seller Lead Scraper
Pulls: Lis Pendens, Foreclosure Notices, Tax Deeds, Judgments, Liens, Probate, NOC
Sources: searchiqs.com/nymonr  |  iapps courts  |  monroecounty.gov parcel data
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import traceback
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ── optional dbfread ──────────────────────────────────────────────────────────
try:
    from dbfread import DBF
    HAS_DBFREAD = True
except ImportError:
    HAS_DBFREAD = False
    logging.warning("dbfread not installed – parcel lookup disabled")

# ── Playwright (async) ────────────────────────────────────────────────────────
try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False
    logging.warning("playwright not installed – clerk portal scraping disabled")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
TODAY = date.today()
CUTOFF = TODAY - timedelta(days=LOOKBACK_DAYS)

CLERK_URL        = "https://searchiqs.com/nymonr/"
COURT_FCAS_URL   = "https://iapps.courts.state.ny.us/webcivil/FCASMain"
COURT_NYSCEF_URL = "https://iapps.courts.state.ny.us/nyscef/HomePage"
PARCEL_PAGE_URL  = "https://www.monroecounty.gov/etc/rp"

# Document type → category mapping
DOC_TYPE_MAP: dict[str, tuple[str, str]] = {
    # Lis pendens
    "LP":      ("LP",      "Lis Pendens"),
    "LIS PENDENS": ("LP", "Lis Pendens"),
    "RELLP":   ("RELLP",  "Release Lis Pendens"),
    # Foreclosure
    "NOFC":    ("NOFC",   "Notice of Foreclosure"),
    "FORECLOSURE": ("NOFC", "Notice of Foreclosure"),
    # Tax deed
    "TAXDEED": ("TAXDEED","Tax Deed"),
    "TAX DEED":("TAXDEED","Tax Deed"),
    # Judgments
    "JUD":     ("JUD",    "Judgment"),
    "JUDGMENT":("JUD",    "Judgment"),
    "CCJ":     ("CCJ",    "Certified Court Judgment"),
    "DRJUD":   ("DRJUD",  "Domestic Relations Judgment"),
    # Liens
    "LNCORPTX":("LNCORPTX","Corporate Tax Lien"),
    "LNIRS":   ("LNIRS",  "IRS Lien"),
    "LNFED":   ("LNFED",  "Federal Lien"),
    "LN":      ("LN",     "Lien"),
    "LNMECH":  ("LNMECH", "Mechanic's Lien"),
    "LNHOA":   ("LNHOA",  "HOA Lien"),
    "MECHANIC":("LNMECH", "Mechanic's Lien"),
    "HOA":     ("LNHOA",  "HOA Lien"),
    "MEDLN":   ("MEDLN",  "Medicaid Lien"),
    "MEDICAID":("MEDLN",  "Medicaid Lien"),
    # Probate
    "PRO":     ("PRO",    "Probate"),
    "PROBATE": ("PRO",    "Probate"),
    # Notice of commencement
    "NOC":     ("NOC",    "Notice of Commencement"),
}

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# RETRY HELPER
# ─────────────────────────────────────────────────────────────────────────────
def retry_get(url: str, session: requests.Session, attempts: int = 3,
              timeout: int = 30, **kwargs) -> requests.Response | None:
    for i in range(attempts):
        try:
            r = session.get(url, timeout=timeout, **kwargs)
            r.raise_for_status()
            return r
        except Exception as exc:
            log.warning("GET %s attempt %d/%d failed: %s", url, i + 1, attempts, exc)
            if i < attempts - 1:
                time.sleep(2 ** i)
    return None


def retry_post(url: str, session: requests.Session, attempts: int = 3,
               timeout: int = 30, **kwargs) -> requests.Response | None:
    for i in range(attempts):
        try:
            r = session.post(url, timeout=timeout, **kwargs)
            r.raise_for_status()
            return r
        except Exception as exc:
            log.warning("POST %s attempt %d/%d failed: %s", url, i + 1, attempts, exc)
            if i < attempts - 1:
                time.sleep(2 ** i)
    return None

# ─────────────────────────────────────────────────────────────────────────────
# PARCEL / PROPERTY APPRAISER
# ─────────────────────────────────────────────────────────────────────────────
class ParcelLookup:
    """Download bulk parcel DBF from Monroe County and build owner→address index."""

    def __init__(self):
        self.by_owner: dict[str, dict] = {}  # normalized_name → parcel dict
        self.loaded = False

    # ------------------------------------------------------------------
    def load(self):
        if not HAS_DBFREAD:
            log.warning("ParcelLookup: dbfread unavailable, skipping")
            return
        try:
            self._download_and_index()
        except Exception:
            log.error("ParcelLookup: failed to load parcel data\n%s", traceback.format_exc())

    # ------------------------------------------------------------------
    def _download_and_index(self):
        """Try several known Monroe County bulk-parcel endpoints."""
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0 LeadScraper/1.0"})

        dbf_bytes = None

        # Strategy 1: direct known ZIP endpoints
        candidates = [
            "https://www.monroecounty.gov/etc/rp/tax_parcel_download.php",
            "https://www.monroecounty.gov/etc/rp/parcels.zip",
            "https://gis.monroecounty.gov/opendata/parcels.zip",
        ]
        for url in candidates:
            r = retry_get(url, session, attempts=2, timeout=60,
                          allow_redirects=True)
            if r and r.status_code == 200 and len(r.content) > 1000:
                dbf_bytes = self._extract_dbf(r.content)
                if dbf_bytes:
                    log.info("Parcel ZIP fetched from %s", url)
                    break

        # Strategy 2: scrape download form on the RP page
        if not dbf_bytes:
            dbf_bytes = self._scrape_download_form(session)

        if not dbf_bytes:
            log.warning("ParcelLookup: no parcel DBF obtained – address enrichment skipped")
            return

        # Parse DBF
        count = 0
        try:
            dbf = DBF(None, filedata=io.BytesIO(dbf_bytes), encoding="latin-1",
                      ignore_missing_memofile=True)
            for rec in dbf:
                try:
                    self._index_record(dict(rec))
                    count += 1
                except Exception:
                    pass
            log.info("ParcelLookup: indexed %d parcel records", count)
            self.loaded = True
        except Exception as exc:
            log.error("ParcelLookup: DBF parse error: %s", exc)

    # ------------------------------------------------------------------
    def _scrape_download_form(self, session: requests.Session) -> bytes | None:
        """Handle __doPostBack form on the RP page."""
        r = retry_get(PARCEL_PAGE_URL, session, attempts=2, timeout=30)
        if not r:
            return None
        soup = BeautifulSoup(r.text, "lxml")
        # Look for download link/button that mentions parcel or DBF
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(k in href.lower() for k in ["parcel", "dbf", "shp", "zip"]):
                full = urljoin(PARCEL_PAGE_URL, href)
                rr = retry_get(full, session, timeout=60)
                if rr and len(rr.content) > 1000:
                    return self._extract_dbf(rr.content)
        # __doPostBack
        vsfield = soup.find("input", {"id": "__VIEWSTATE"})
        evval = soup.find("input", {"id": "__EVENTVALIDATION"})
        target = None
        for inp in soup.find_all("input", {"type": "submit"}):
            if any(k in (inp.get("value", "") + inp.get("id", "")).lower()
                   for k in ["parcel", "download", "dbf", "export"]):
                target = inp.get("name") or inp.get("id")
                break
        if target and vsfield:
            payload = {
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": vsfield.get("value", ""),
                "__EVENTVALIDATION": evval.get("value", "") if evval else "",
                target: "Download",
            }
            rr = retry_post(PARCEL_PAGE_URL, session, data=payload, timeout=60)
            if rr and len(rr.content) > 1000:
                return self._extract_dbf(rr.content)
        return None

    # ------------------------------------------------------------------
    @staticmethod
    def _extract_dbf(content: bytes) -> bytes | None:
        """Return raw DBF bytes from either a ZIP archive or bare DBF."""
        if content[:4] == b"PK\x03\x04":  # ZIP magic
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(".dbf"):
                            return zf.read(name)
            except Exception:
                pass
        elif content[0] in (0x03, 0x83, 0xF5):  # DBF magic bytes
            return content
        return None

    # ------------------------------------------------------------------
    def _index_record(self, rec: dict):
        # Normalize key names (different county exports use different names)
        def g(*keys):
            for k in keys:
                v = rec.get(k) or rec.get(k.upper()) or rec.get(k.lower())
                if v and str(v).strip():
                    return str(v).strip()
            return ""

        owner     = g("OWN1", "OWNER", "OWNER1", "OWN_NAME")
        site_addr = g("SITEADDR", "SITE_ADDR", "PROP_ADDR", "ADDRESS")
        site_city = g("SITE_CITY", "CITY", "PROP_CITY")
        site_zip  = g("SITE_ZIP",  "ZIP",  "PROP_ZIP")
        mail_addr = g("MAILADR1", "ADDR_1", "MAIL_ADDR", "MAIL_ADDRESS")
        mail_city = g("MAILCITY", "CITY2", "MAIL_CITY")
        mail_state= g("STATE", "MAIL_STATE", "ST")
        mail_zip  = g("MAILZIP", "ZIP2", "MAIL_ZIP")

        if not owner:
            return

        parcel = {
            "prop_address": site_addr,
            "prop_city":    site_city or "Rochester",
            "prop_state":   "NY",
            "prop_zip":     site_zip,
            "mail_address": mail_addr or site_addr,
            "mail_city":    mail_city or site_city or "Rochester",
            "mail_state":   mail_state or "NY",
            "mail_zip":     mail_zip or site_zip,
        }

        # Build all name variants
        for variant in self._name_variants(owner):
            self.by_owner.setdefault(variant, parcel)

    # ------------------------------------------------------------------
    @staticmethod
    def _name_variants(raw: str) -> list[str]:
        """Produce 'FIRST LAST', 'LAST FIRST', 'LAST, FIRST' variants."""
        raw = raw.upper().strip()
        variants = {raw}
        # strip suffixes
        cleaned = re.sub(r"\b(JR|SR|II|III|IV|ESTATE|TRUST|LLC|INC|CORP)\b\.?",
                         "", raw).strip(" ,")
        variants.add(cleaned)
        # comma swap:  "SMITH, JOHN" → "JOHN SMITH"
        if "," in cleaned:
            parts = [p.strip() for p in cleaned.split(",", 1)]
            if len(parts) == 2:
                variants.add(f"{parts[1]} {parts[0]}")
                variants.add(f"{parts[0]} {parts[1]}")
        else:
            words = cleaned.split()
            if len(words) >= 2:
                variants.add(f"{words[-1]}, {' '.join(words[:-1])}")
                variants.add(f"{words[-1]} {' '.join(words[:-1])}")
        return [v for v in variants if v]

    # ------------------------------------------------------------------
    def lookup(self, owner: str) -> dict:
        if not owner:
            return {}
        for variant in self._name_variants(owner.upper()):
            result = self.by_owner.get(variant)
            if result:
                return result
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# SCORING
# ─────────────────────────────────────────────────────────────────────────────
def compute_flags_and_score(rec: dict) -> tuple[list[str], int]:
    flags: list[str] = []
    cat = rec.get("cat", "")
    owner = rec.get("owner", "").upper()
    amount = rec.get("amount") or 0
    filed_str = rec.get("filed", "")
    has_address = bool(rec.get("prop_address"))

    # Assign flags
    if cat in ("LP", "RELLP"):
        flags.append("Lis pendens")
    if cat == "NOFC":
        flags.append("Pre-foreclosure")
    if cat in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien")
    if cat in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"):
        flags.append("Tax lien")
    if cat == "LNMECH":
        flags.append("Mechanic lien")
    if cat == "PRO":
        flags.append("Probate / estate")
    if any(k in owner for k in ("LLC", "INC", "CORP", "LTD", "LP ", "L.P.", "L.L.C")):
        flags.append("LLC / corp owner")
    # New this week
    try:
        fd = datetime.strptime(filed_str[:10], "%Y-%m-%d").date()
        if (TODAY - fd).days <= 7:
            flags.append("New this week")
    except Exception:
        pass

    # Score
    score = 30
    score += 10 * len(flags)
    # LP + FC combo bonus
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    if amount:
        if amount > 100_000:
            score += 15
        elif amount > 50_000:
            score += 10
    if "New this week" in flags:
        score += 5
    if has_address:
        score += 5

    return flags, min(score, 100)


# ─────────────────────────────────────────────────────────────────────────────
# CLERK PORTAL — searchiqs.com/nymonr  (Playwright)
# ─────────────────────────────────────────────────────────────────────────────
async def scrape_clerk_portal() -> list[dict]:
    if not HAS_PLAYWRIGHT:
        log.warning("Playwright unavailable – skipping clerk portal")
        return []

    records: list[dict] = []
    date_from = CUTOFF.strftime("%m/%d/%Y")
    date_to   = TODAY.strftime("%m/%d/%Y")

    # Document types to query (IQS code → our internal code)
    doc_types_to_query = [
        ("LP",),
        ("NOFC",),
        ("TAXDEED",),
        ("JUD", "CCJ", "DRJUD"),
        ("LNCORPTX", "LNIRS", "LNFED", "LN", "LNMECH", "LNHOA", "MEDLN"),
        ("PRO",),
        ("NOC",),
        ("RELLP",),
    ]

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx     = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120"
        )
        page    = await ctx.new_page()

        try:
            log.info("Navigating to IQS clerk portal …")
            await page.goto(CLERK_URL, wait_until="networkidle", timeout=60_000)
            await asyncio.sleep(2)

            # Take screenshot of page structure for debugging
            # Try to find the search form
            # IQS portals typically have Date From / Date To + Doc Type filters

            # Fill date range
            for sel in ['input[name*="DateFrom"]', 'input[id*="DateFrom"]',
                        'input[placeholder*="From"]', '#txtDateFrom']:
                try:
                    await page.fill(sel, date_from, timeout=3000)
                    break
                except Exception:
                    pass

            for sel in ['input[name*="DateTo"]', 'input[id*="DateTo"]',
                        'input[placeholder*="To"]', '#txtDateTo']:
                try:
                    await page.fill(sel, date_to, timeout=3000)
                    break
                except Exception:
                    pass

            # Try doc-type search for each group
            for group in doc_types_to_query:
                try:
                    batch = await _iqs_search_group(page, group, date_from, date_to)
                    records.extend(batch)
                    log.info("IQS %s → %d records", group, len(batch))
                except Exception:
                    log.warning("IQS group %s failed:\n%s", group, traceback.format_exc())

        except Exception:
            log.error("Clerk portal scrape failed:\n%s", traceback.format_exc())
        finally:
            await browser.close()

    log.info("Clerk portal total: %d records", len(records))
    return records


async def _iqs_search_group(page, group: tuple, date_from: str, date_to: str) -> list[dict]:
    """Search IQS for one doc-type group and return parsed records."""
    records: list[dict] = []

    # IQS: navigate to search, select doc type, set dates, submit
    await page.goto(CLERK_URL, wait_until="networkidle", timeout=60_000)
    await asyncio.sleep(1)

    # Try to select doc type from dropdown
    for code in group:
        for sel in ['select[name*="DocType"]', 'select[id*="DocType"]',
                    '#ddlDocType', 'select']:
            try:
                options = await page.eval_on_selector(
                    sel,
                    "el => Array.from(el.options).map(o => ({v: o.value, t: o.text.toUpperCase()}))"
                )
                # Find matching option
                matched = None
                for opt in options:
                    if code in opt["t"] or code in opt["v"].upper():
                        matched = opt["v"]
                        break
                if matched:
                    await page.select_option(sel, matched)
                    break
            except Exception:
                pass

    # Set date range again (navigation reset it)
    for sel in ['input[name*="DateFrom"]', 'input[id*="DateFrom"]', '#txtDateFrom']:
        try:
            await page.fill(sel, date_from, timeout=3000)
            break
        except Exception:
            pass
    for sel in ['input[name*="DateTo"]', 'input[id*="DateTo"]', '#txtDateTo']:
        try:
            await page.fill(sel, date_to, timeout=3000)
            break
        except Exception:
            pass

    # Submit search
    for sel in ['input[type="submit"]', 'button[type="submit"]',
                '#btnSearch', 'button:text("Search")']:
        try:
            await page.click(sel, timeout=5000)
            await page.wait_for_load_state("networkidle", timeout=30_000)
            break
        except Exception:
            pass

    await asyncio.sleep(2)

    # Parse results table — IQS returns an HTML results table
    records.extend(await _parse_iqs_results_page(page, group[0]))

    # Handle pagination
    page_num = 2
    while True:
        try:
            next_btn = await page.query_selector('a:text("Next"), a:text(">"), #btnNext, .pager a[href*="Next"]')
            if not next_btn:
                break
            await next_btn.click()
            await page.wait_for_load_state("networkidle", timeout=20_000)
            await asyncio.sleep(1)
            new_recs = await _parse_iqs_results_page(page, group[0])
            if not new_recs:
                break
            records.extend(new_recs)
            page_num += 1
            if page_num > 20:  # safety cap
                break
        except Exception:
            break

    return records


async def _parse_iqs_results_page(page, default_code: str) -> list[dict]:
    """Parse the IQS HTML results table into record dicts."""
    records: list[dict] = []
    try:
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
        # IQS results table has class 'resultsTable' or similar
        tables = soup.find_all("table")
        if not tables:
            return records

        results_table = None
        for t in tables:
            headers = [th.get_text(strip=True).upper() for th in
                       t.find_all(["th", "td"])[:10]]
            if any(h in headers for h in ["DOC", "DOCUMENT", "GRANTOR", "GRANTEE", "DATE"]):
                results_table = t
                break
        if not results_table:
            return records

        # Map header → column index
        header_row = results_table.find("tr")
        if not header_row:
            return records
        headers = [th.get_text(strip=True).upper()
                   for th in header_row.find_all(["th", "td"])]

        def col(row_cells, *names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h and i < len(row_cells):
                        return row_cells[i].get_text(strip=True)
            return ""

        current_url = page.url

        for row in results_table.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            try:
                doc_num  = col(cells, "DOC #", "BOOK", "INSTRUMENT", "DOC NUM")
                doc_type = col(cells, "DOC TYPE", "TYPE", "DOCUMENT TYPE") or default_code
                filed    = col(cells, "DATE", "FILED", "RECORD DATE", "REC DATE")
                grantor  = col(cells, "GRANTOR", "OWNER", "SELLER")
                grantee  = col(cells, "GRANTEE", "BUYER")
                legal    = col(cells, "LEGAL", "DESCRIPTION", "PREMISES")
                amount   = col(cells, "AMOUNT", "CONSIDERATION", "VALUE")

                # Try to extract doc-detail link
                clerk_url = current_url
                link = row.find("a", href=True)
                if link:
                    clerk_url = urljoin(CLERK_URL, link["href"])

                # Parse date
                filed_iso = _parse_date(filed)
                if filed_iso and datetime.strptime(filed_iso, "%Y-%m-%d").date() < CUTOFF:
                    continue  # outside lookback window

                # Parse amount
                amt_val = _parse_amount(amount)

                # Normalize doc type
                cat, cat_label = _classify_doc_type(doc_type or default_code)

                records.append({
                    "doc_num":   doc_num,
                    "doc_type":  doc_type or default_code,
                    "filed":     filed_iso or filed,
                    "cat":       cat,
                    "cat_label": cat_label,
                    "owner":     grantor,
                    "grantee":   grantee,
                    "amount":    amt_val,
                    "legal":     legal,
                    "clerk_url": clerk_url,
                    "source":    "Monroe County Clerk (IQS)",
                })
            except Exception:
                pass

    except Exception:
        log.warning("_parse_iqs_results_page error:\n%s", traceback.format_exc())
    return records


# ─────────────────────────────────────────────────────────────────────────────
# COURT RECORDS — iapps.courts.state.ny.us
# ─────────────────────────────────────────────────────────────────────────────
def scrape_court_records() -> list[dict]:
    """Scrape NY FCAS (foreclosure) and eCourts for Monroe County."""
    records: list[dict] = []
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })

    # FCAS — Foreclosure Case Search
    try:
        records.extend(_scrape_fcas(session))
    except Exception:
        log.error("FCAS scrape failed:\n%s", traceback.format_exc())

    # NYSCEF — eLaw filing search for Monroe County LP / Probate
    try:
        records.extend(_scrape_nyscef(session))
    except Exception:
        log.error("NYSCEF scrape failed:\n%s", traceback.format_exc())

    log.info("Court records total: %d", len(records))
    return records


def _scrape_fcas(session: requests.Session) -> list[dict]:
    """NY FCAS – Foreclosure Action Status System for Monroe County."""
    records: list[dict] = []
    date_from = CUTOFF.strftime("%m/%d/%Y")
    date_to   = TODAY.strftime("%m/%d/%Y")

    # Load FCAS search page
    r = retry_get(COURT_FCAS_URL, session, timeout=30)
    if not r:
        return records

    soup = BeautifulSoup(r.text, "lxml")

    # Extract ASP.NET hidden fields
    def get_hidden(name):
        el = soup.find("input", {"name": name})
        return el["value"] if el and el.get("value") else ""

    payload = {
        "__VIEWSTATE":       get_hidden("__VIEWSTATE"),
        "__EVENTVALIDATION": get_hidden("__EVENTVALIDATION"),
        "__EVENTTARGET":     "",
        "__EVENTARGUMENT":   "",
        "county":            "28",           # Monroe County code
        "dtFrom":            date_from,
        "dtTo":              date_to,
        "action":            "Search",
    }
    # Add any other visible form fields
    for inp in soup.find_all("input"):
        n = inp.get("name", "")
        v = inp.get("value", "")
        t = inp.get("type", "text").lower()
        if n and t not in ("submit", "button", "image") and n not in payload:
            payload[n] = v

    rr = retry_post(COURT_FCAS_URL, session, data=payload, timeout=30)
    if not rr:
        return records

    soup2 = BeautifulSoup(rr.text, "lxml")
    for row in soup2.find_all("tr")[1:]:
        cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        if len(cells) < 3:
            continue
        try:
            # FCAS columns typically: Index#, Plaintiff, Defendant, County, Date, Status
            index_num = cells[0] if cells else ""
            plaintiff  = cells[1] if len(cells) > 1 else ""
            defendant  = cells[2] if len(cells) > 2 else ""
            filed      = cells[4] if len(cells) > 4 else cells[-1]

            filed_iso = _parse_date(filed)
            if filed_iso and datetime.strptime(filed_iso, "%Y-%m-%d").date() < CUTOFF:
                continue

            link_tag = row.find("a", href=True)
            url = urljoin(COURT_FCAS_URL, link_tag["href"]) if link_tag else COURT_FCAS_URL

            records.append({
                "doc_num":   index_num,
                "doc_type":  "NOFC",
                "filed":     filed_iso or filed,
                "cat":       "NOFC",
                "cat_label": "Notice of Foreclosure",
                "owner":     defendant,
                "grantee":   plaintiff,
                "amount":    None,
                "legal":     "",
                "clerk_url": url,
                "source":    "NY Courts FCAS",
            })
        except Exception:
            pass

    log.info("FCAS: %d records", len(records))
    return records


def _scrape_nyscef(session: requests.Session) -> list[dict]:
    """NYSCEF – NY State Courts Electronic Filing for LP & Probate."""
    records: list[dict] = []
    # NYSCEF search for Monroe County (county code 29 in NYSCEF)
    search_url = "https://iapps.courts.state.ny.us/nyscef/CaseSearch"

    doc_types = [
        ("Foreclosure", "NOFC"),
        ("Lis Pendens", "LP"),
        ("Probate", "PRO"),
    ]
    date_from = CUTOFF.strftime("%m/%d/%Y")
    date_to   = TODAY.strftime("%m/%d/%Y")

    r = retry_get(COURT_NYSCEF_URL, session, timeout=30)
    if not r:
        return records

    for label, cat in doc_types:
        try:
            soup = BeautifulSoup(r.text, "lxml")
            payload = {
                "__VIEWSTATE":       (soup.find("input", {"name": "__VIEWSTATE"}) or {}).get("value", ""),
                "__EVENTVALIDATION": (soup.find("input", {"name": "__EVENTVALIDATION"}) or {}).get("value", ""),
                "county":            "Monroe",
                "caseType":          label,
                "filedDateFrom":     date_from,
                "filedDateTo":       date_to,
                "Submit":            "Search",
            }
            rr = retry_post(search_url, session, data=payload, timeout=30)
            if not rr:
                continue

            soup2 = BeautifulSoup(rr.text, "lxml")
            for row in soup2.find_all("tr")[1:50]:   # cap at 50
                cells = [c.get_text(strip=True) for c in row.find_all("td")]
                if len(cells) < 3:
                    continue
                link = row.find("a", href=True)
                doc_url = urljoin(search_url, link["href"]) if link else search_url
                filed_iso = _parse_date(cells[-1]) or _parse_date(cells[0])
                if filed_iso and datetime.strptime(filed_iso, "%Y-%m-%d").date() < CUTOFF:
                    continue
                cat_code, cat_label_str = _classify_doc_type(cat)
                records.append({
                    "doc_num":   cells[0],
                    "doc_type":  cat,
                    "filed":     filed_iso or "",
                    "cat":       cat_code,
                    "cat_label": cat_label_str,
                    "owner":     cells[2] if len(cells) > 2 else "",
                    "grantee":   cells[1] if len(cells) > 1 else "",
                    "amount":    None,
                    "legal":     "",
                    "clerk_url": doc_url,
                    "source":    "NY Courts NYSCEF",
                })
        except Exception:
            log.warning("NYSCEF %s failed: %s", label, traceback.format_exc())

    log.info("NYSCEF: %d records", len(records))
    return records


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _parse_date(raw: str) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y", "%Y%m%d",
                "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(raw[:len(fmt)], fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    # try extracting date with regex
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", raw)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(1)), int(m.group(2))).strftime("%Y-%m-%d")
        except Exception:
            pass
    return None


def _parse_amount(raw: str) -> float | None:
    if not raw:
        return None
    cleaned = re.sub(r"[^\d.]", "", raw)
    try:
        v = float(cleaned)
        return v if v > 0 else None
    except Exception:
        return None


def _classify_doc_type(raw: str) -> tuple[str, str]:
    key = raw.upper().strip()
    if key in DOC_TYPE_MAP:
        return DOC_TYPE_MAP[key]
    # partial match
    for k, v in DOC_TYPE_MAP.items():
        if k in key or key in k:
            return v
    return (key, raw.title())


def _dedup(records: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for r in records:
        key = f"{r.get('doc_num','')}|{r.get('filed','')}|{r.get('owner','')}"
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# ENRICH WITH PARCEL DATA
# ─────────────────────────────────────────────────────────────────────────────
def enrich_records(records: list[dict], parcel: ParcelLookup) -> list[dict]:
    enriched: list[dict] = []
    for rec in records:
        try:
            p = parcel.lookup(rec.get("owner", ""))
            rec.setdefault("prop_address", p.get("prop_address", ""))
            rec.setdefault("prop_city",    p.get("prop_city", "Rochester"))
            rec.setdefault("prop_state",   p.get("prop_state", "NY"))
            rec.setdefault("prop_zip",     p.get("prop_zip", ""))
            rec.setdefault("mail_address", p.get("mail_address", ""))
            rec.setdefault("mail_city",    p.get("mail_city", ""))
            rec.setdefault("mail_state",   p.get("mail_state", "NY"))
            rec.setdefault("mail_zip",     p.get("mail_zip", ""))

            flags, score = compute_flags_and_score(rec)
            rec["flags"] = flags
            rec["score"] = score
            enriched.append(rec)
        except Exception:
            rec.setdefault("flags", [])
            rec.setdefault("score", 30)
            enriched.append(rec)
    # Sort by score desc
    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)
    return enriched


# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT
# ─────────────────────────────────────────────────────────────────────────────
RECORD_KEYS = [
    "doc_num", "doc_type", "filed", "cat", "cat_label",
    "owner", "grantee", "amount", "legal",
    "prop_address", "prop_city", "prop_state", "prop_zip",
    "mail_address", "mail_city", "mail_state", "mail_zip",
    "clerk_url", "flags", "score", "source",
]


def normalize_record(rec: dict) -> dict:
    out = {}
    for k in RECORD_KEYS:
        out[k] = rec.get(k, "")
    return out


def save_json(records: list[dict]):
    payload: dict[str, Any] = {
        "fetched_at":    datetime.utcnow().isoformat() + "Z",
        "source":        "Monroe County, NY – Public Records",
        "date_range":    {"from": CUTOFF.isoformat(), "to": TODAY.isoformat()},
        "total":         len(records),
        "with_address":  sum(1 for r in records if r.get("prop_address")),
        "records":       [normalize_record(r) for r in records],
    }
    for path in ["dashboard/records.json", "data/records.json"]:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        log.info("Saved %s (%d records)", path, len(records))


def save_ghl_csv(records: list[dict]):
    Path("data").mkdir(parents=True, exist_ok=True)
    path = "data/ghl_export.csv"
    fieldnames = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed",
        "Document Number", "Amount/Debt Owed",
        "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]

    def split_name(full: str) -> tuple[str, str]:
        parts = full.strip().split()
        if not parts:
            return "", ""
        if len(parts) == 1:
            return "", parts[0]
        # Handle "LAST, FIRST" format
        if parts[0].endswith(","):
            return " ".join(parts[1:]), parts[0].rstrip(",")
        return parts[0], " ".join(parts[1:])

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            first, last = split_name(rec.get("owner", ""))
            flags = rec.get("flags", [])
            writer.writerow({
                "First Name":            first,
                "Last Name":             last,
                "Mailing Address":       rec.get("mail_address", ""),
                "Mailing City":          rec.get("mail_city", ""),
                "Mailing State":         rec.get("mail_state", "NY"),
                "Mailing Zip":           rec.get("mail_zip", ""),
                "Property Address":      rec.get("prop_address", ""),
                "Property City":         rec.get("prop_city", ""),
                "Property State":        rec.get("prop_state", "NY"),
                "Property Zip":          rec.get("prop_zip", ""),
                "Lead Type":             rec.get("cat_label", rec.get("cat", "")),
                "Document Type":         rec.get("doc_type", ""),
                "Date Filed":            rec.get("filed", ""),
                "Document Number":       rec.get("doc_num", ""),
                "Amount/Debt Owed":      rec.get("amount") or "",
                "Seller Score":          rec.get("score", 30),
                "Motivated Seller Flags": "; ".join(flags) if isinstance(flags, list) else flags,
                "Source":                rec.get("source", ""),
                "Public Records URL":    rec.get("clerk_url", ""),
            })
    log.info("GHL CSV saved: %s (%d rows)", path, len(records))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
async def main():
    log.info("=" * 60)
    log.info("Monroe County NY Lead Scraper  |  %s → %s", CUTOFF, TODAY)
    log.info("=" * 60)

    # 1. Load parcel data
    parcel = ParcelLookup()
    log.info("Loading parcel data …")
    parcel.load()
    log.info("Parcel lookup ready: %d owners indexed", len(parcel.by_owner))

    # 2. Scrape clerk portal
    log.info("Scraping clerk portal (IQS) …")
    clerk_records = await scrape_clerk_portal()

    # 3. Scrape court records
    log.info("Scraping court records …")
    court_records = scrape_court_records()

    # 4. Merge + dedup
    all_records = clerk_records + court_records
    all_records = _dedup(all_records)
    log.info("Combined unique records: %d", len(all_records))

    # 5. Enrich + score
    log.info("Enriching with parcel data and scoring …")
    all_records = enrich_records(all_records, parcel)

    # 6. Save outputs
    save_json(all_records)
    save_ghl_csv(all_records)

    log.info("=" * 60)
    log.info("Done. %d motivated seller leads saved.", len(all_records))
    log.info("  With address: %d", sum(1 for r in all_records if r.get("prop_address")))
    log.info("  High score (≥70): %d", sum(1 for r in all_records if r.get("score", 0) >= 70))
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
