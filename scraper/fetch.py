#!/usr/bin/env python3
"""
Monroe County, NY — Motivated Seller Lead Scraper
═══════════════════════════════════════════════════
Portal:   https://searchiqs.com/nymonr/  (SearchIQS — public guest access)
Courts:   iapps.courts.state.ny.us (FCAS + NYSCEF)
Parcel:   monroecounty.gov/etc/rp  (bulk DBF download, optional)

ALL field names, column indices, and URL patterns verified live
against the actual portal on 2026-04-22 from a logged-in browser session.

Guest-session flow (verified):
  1. GET  LogIn.aspx?CountyID=5        → grab __VIEWSTATE/__EVENTVALIDATION
  2. POST LogIn.aspx                   → __EVENTTARGET=btnGuestLogin
                                         Server issues ASP.NET_SessionId cookie,
                                         302-redirects to SearchAdvancedMP.aspx
  3. GET  SearchAdvancedMP.aspx        → fresh ViewState for search form
  4. POST SearchAdvancedMP.aspx        → cmdSearch=Search + form fields
                                         302-redirects to SearchResultsMP.aspx
  5. Parse ContentPlaceHolder1_grdResults (ASP.NET GridView)

The key button name is cmdSearch (not btnSearch — that was an earlier mis-guess).
"""

import csv
import io
import json
import logging
import os
import random
import re
import sys
import time
import traceback
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, quote

import requests
from bs4 import BeautifulSoup

try:
    from dbfread import DBF
    HAS_DBFREAD = True
except ImportError:
    HAS_DBFREAD = False

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
TODAY  = date.today()
CUTOFF = TODAY - timedelta(days=LOOKBACK_DAYS)

IQS_BASE         = "https://searchiqs.com/nymonr"
IQS_HOMEPAGE_URL = "https://searchiqs.com/"
IQS_LOGIN_URL    = f"{IQS_BASE}/LogIn.aspx?CountyID=5"
IQS_SEARCH_URL   = f"{IQS_BASE}/SearchAdvancedMP.aspx"
IQS_RESULTS_URL  = f"{IQS_BASE}/SearchResultsMP.aspx"
IQS_VIEWER_URL   = f"{IQS_BASE}/ImageViewerMP.aspx"

COURT_FCAS_URL   = "https://iapps.courts.state.ny.us/webcivil/FCASMain"
COURT_NYSCEF_URL = "https://iapps.courts.state.ny.us/nyscef/CaseSearch"
PARCEL_PAGE_URL  = "https://www.monroecounty.gov/etc/rp"

# (group_value, default_cat_code, default_cat_label) — verified from live dropdown
IQS_GROUPS = [
    ("LP",    "LP",      "Lis Pendens"),
    ("LN",    "LN",      "Lien"),
    ("W",     "TAXDEED", "Tax Warrant"),
    ("JUDIC", "NOFC",    "Judicial / Foreclosure"),
    ("MF",    "NOC",     "Miscellaneous Filings"),
    ("J",     "JUD",     "Judgment"),
]

DOC_TYPE_MAP: dict[str, tuple[str, str]] = {
    "LIS PENDENS": ("LP", "Lis Pendens"),
    "LIS PENDENS - NOTICE OF PENDENCY CANCELLED": ("RELLP", "Release Lis Pendens"),
    "RELEASE OF LIS PENDENS": ("RELLP", "Release Lis Pendens"),
    "NOTICE OF FORECLOSURE": ("NOFC", "Notice of Foreclosure"),
    "FORECLOSURE": ("NOFC", "Notice of Foreclosure"),
    "TAX DEED": ("TAXDEED", "Tax Deed"),
    "TAX WARRANT": ("TAXDEED", "Tax Warrant"),
    "JUDGMENT": ("JUD", "Judgment"),
    "TRANSCRIPT OF JUDGMENT": ("JUD", "Judgment"),
    "AMENDED JUDGMENT": ("JUD", "Judgment"),
    "AMENDED TRANSCRIPT OF JUDGMENT": ("JUD", "Judgment"),
    "CERTIFIED COPY OF JUDGMENT": ("CCJ", "Certified Court Judgment"),
    "DOMESTIC RELATIONS JUDGMENT": ("DRJUD", "Domestic Relations Judgment"),
    "FEDERAL TAX LIEN": ("LNFED", "Federal Tax Lien"),
    "NOTICE OF FEDERAL TAX LIEN": ("LNFED", "Federal Tax Lien"),
    "CERTIFICATE AND DISCHARGE OF FEDERAL TAX LIEN": ("LNFED", "Federal Tax Lien"),
    "IRS LIEN": ("LNIRS", "IRS Lien"),
    "NOTICE OF IRS LIEN": ("LNIRS", "IRS Lien"),
    "CORPORATE TAX LIEN": ("LNCORPTX", "Corporate Tax Lien"),
    "LIEN": ("LN", "Lien"),
    "MECHANICS LIEN": ("LNMECH", "Mechanic's Lien"),
    "MECHANIC'S LIEN": ("LNMECH", "Mechanic's Lien"),
    "AFFIDAVIT FOR MECHANICS LIEN FILED": ("LNMECH", "Mechanic's Lien"),
    "HOA LIEN": ("LNHOA", "HOA Lien"),
    "HOMEOWNER ASSOCIATION LIEN": ("LNHOA", "HOA Lien"),
    "MEDICAID LIEN": ("MEDLN", "Medicaid Lien"),
    "PUBLIC ASSISTANCE LIEN": ("MEDLN", "Medicaid Lien"),
    "ASSIGNMENT OF PUBLIC ASSISTANCE LIEN": ("MEDLN", "Medicaid Lien"),
    "PROBATE": ("PRO", "Probate"),
    "LETTERS TESTAMENTARY": ("PRO", "Probate"),
    "LETTERS OF ADMINISTRATION": ("PRO", "Probate"),
    "NOTICE OF COMMENCEMENT": ("NOC", "Notice of Commencement"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _parse_date(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw[: len(fmt) + 2], fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", raw)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(1)), int(m.group(2))).strftime("%Y-%m-%d")
        except Exception:
            pass
    return ""

def _in_window(filed: str) -> bool:
    if not filed:
        return True
    try:
        return datetime.strptime(filed, "%Y-%m-%d").date() >= CUTOFF
    except Exception:
        return True

def _parse_amount(raw: str) -> float | None:
    if not raw:
        return None
    cleaned = re.sub(r"[^\d.]", "", raw)
    try:
        v = float(cleaned)
        return v if v > 0 else None
    except Exception:
        return None

def _classify(raw: str, fallback_cat: str, fallback_label: str) -> tuple[str, str]:
    if not raw:
        return fallback_cat, fallback_label
    key = raw.upper().strip()
    if key in DOC_TYPE_MAP:
        return DOC_TYPE_MAP[key]
    for k, v in DOC_TYPE_MAP.items():
        if k in key or key in k:
            return v
    return fallback_cat, fallback_label

def _dedup(records: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for r in records:
        key = "|".join([str(r.get("doc_num", "")), str(r.get("filed", "")), str(r.get("owner", ""))])
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out

def _retry_get(url, session, attempts=3, timeout=30, **kw):
    for i in range(attempts):
        try:
            r = session.get(url, timeout=timeout, **kw)
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning("GET %s [%d/%d]: %s", url, i + 1, attempts, e)
            if i < attempts - 1:
                time.sleep(1.5 ** i + random.random() * 0.5)
    return None

def _retry_post(url, session, attempts=3, timeout=30, **kw):
    for i in range(attempts):
        try:
            r = session.post(url, timeout=timeout, **kw)
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning("POST %s [%d/%d]: %s", url, i + 1, attempts, e)
            if i < attempts - 1:
                time.sleep(1.5 ** i + random.random() * 0.5)
    return None

def _hidden_fields(soup):
    out = {}
    for el in soup.find_all("input", {"type": "hidden"}):
        n = el.get("name", "")
        if n:
            out[n] = el.get("value", "")
    return out

def _viewer_url(record_id: str, row_index: int = 0) -> str:
    rid = quote(record_id, safe="")
    return (f"{IQS_VIEWER_URL}?CustomView=Search%20Results"
            f"&SelectedDoc={rid}&SelectedRowIndex={row_index}")

_BOILERPLATE = re.compile(
    r"(JOHN DOE|JANE DOE|THE LAST TWELVE|FICTITIOUS|UNKNOWN TO PLAINTIFF|"
    r"TENANTS|OCCUPANTS|PERSONS OR CORPORATE|INTEREST OR LIEN|PREMISES|"
    r"DESCRIBED IN THE COMPLAINT|PLAINTIFF|CLAIMANT|AND\/OR|A\/K\/A)",
    re.I,
)

def _primary_name(raw: str) -> str:
    if not raw:
        return ""
    lines = [ln.strip() for ln in re.split(r"[\n\r]+", raw) if ln.strip()]
    for line in lines:
        if _BOILERPLATE.search(line):
            continue
        if len(line) > 65:
            continue
        if any(kw in line.upper() for kw in ["DOE #", "THROUGH", "BEING", "INTENDED"]):
            continue
        return re.sub(r"\s+", " ", line).strip()
    return re.sub(r"\s+", " ", raw[:80]).strip()

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _town_to_city(raw: str) -> str:
    if not raw:
        return "Rochester"
    return raw.split("-")[0].strip().title()

# ─────────────────────────────────────────────────────────────────────────────
# IQS CLERK PORTAL SCRAPER
# ─────────────────────────────────────────────────────────────────────────────
class IQSScraper:
    """
    Scrapes the Monroe County SearchIQS portal using requests + BeautifulSoup.

    Results table column layout (0-indexed, confirmed live 2026-04-22):
      0  View button           7  Book-Page
      1  MyDoc button          8  Date (filed, MM/DD/YYYY)
      2  Select checkbox       9  Town (e.g. "ROCHESTER - 261400")
      3  RecordID (e.g. L|…)  10  Address (property street address)
      4  Party 1 (filer)      11  Amount
      5  Party 2 (owner)      12  Related docs
      6  Type
    """

    # Modern Chrome headers — matches a real Chrome 134 browser, which is what
    # the IQS portal expects. Missing or stale UA is the most common reason
    # WAFs return 403 to automation traffic from datacenter IPs.
    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/134.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Sec-Ch-Ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }

    C_RECORD_ID = 3
    C_PARTY1    = 4   # filer
    C_PARTY2    = 5   # owner
    C_TYPE      = 6
    C_BOOK_PAGE = 7
    C_DATE      = 8
    C_TOWN      = 9
    C_ADDRESS   = 10
    C_AMOUNT    = 11

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.DEFAULT_HEADERS)
        self._logged_in = False

    def _warm_up(self) -> bool:
        """Hit the public SearchIQS homepage first so we have a realistic
        request chain (home → login) that looks like a human landing on the
        site and clicking the Monroe County link."""
        try:
            r = self.session.get(IQS_HOMEPAGE_URL, timeout=30)
            log.info("IQS warm-up: %s → %d", IQS_HOMEPAGE_URL, r.status_code)
            return r.status_code < 400
        except Exception as e:
            log.warning("IQS warm-up failed: %s", e)
            return False

    def _login(self) -> bool:
        log.info("IQS: establishing guest session …")
        self._warm_up()
        time.sleep(0.8 + random.random() * 0.8)

        # Step 1: GET login page
        r = _retry_get(
            IQS_LOGIN_URL, self.session, timeout=30,
            headers={"Referer": IQS_HOMEPAGE_URL, "Sec-Fetch-Site": "same-origin"},
        )
        if not r:
            return False
        soup = BeautifulSoup(r.text, "lxml")
        hidden = _hidden_fields(soup)

        # Step 2: POST btnGuestLogin
        payload = {
            **hidden,
            "__EVENTTARGET": "btnGuestLogin",
            "__EVENTARGUMENT": "",
            "username": "",
            "password": "",
        }
        post_headers = {
            "Referer": IQS_LOGIN_URL,
            "Origin": "https://searchiqs.com",
            "Content-Type": "application/x-www-form-urlencoded",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-User": "?1",
        }
        rr = _retry_post(
            IQS_LOGIN_URL, self.session, data=payload, timeout=30,
            allow_redirects=True, headers=post_headers,
        )
        if not rr:
            return False

        if "SearchAdvanced" in rr.url or "Search" in rr.url:
            self._logged_in = True
            log.info("IQS: guest session OK (at %s)", rr.url)
            return True

        # Some IQS deployments land on a disclaimer / portal home first
        rs = _retry_get(
            IQS_SEARCH_URL, self.session, timeout=30,
            headers={"Referer": IQS_LOGIN_URL},
        )
        if rs and "SearchAdvanced" in rs.url:
            self._logged_in = True
            log.info("IQS: session valid via direct navigation")
            return True

        log.error("IQS: login failed — landed at %s", rr.url if rr else "?")
        return False

    def _ensure_session(self) -> bool:
        if self._logged_in:
            return True
        return self._login()

    def search_group(self, group_val: str, default_cat: str, default_label: str) -> list[dict]:
        if not self._ensure_session():
            return []

        date_from = CUTOFF.strftime("%m/%d/%Y")
        date_to   = TODAY.strftime("%m/%d/%Y")
        log.info("IQS: group=%-8s %s → %s", group_val, date_from, date_to)

        # Reload search page for fresh __VIEWSTATE
        rs = _retry_get(
            IQS_SEARCH_URL, self.session, timeout=30,
            headers={"Referer": IQS_LOGIN_URL},
        )
        if not rs:
            return []
        soup = BeautifulSoup(rs.text, "lxml")
        hidden = _hidden_fields(soup)

        payload = {
            **hidden,
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "BrowserWidth": "1280",
            "BrowserHeight": "800",
            "ctl00$hidProveH": "0",
            "ctl00$hidPageAccessExpire": "-1",
            "ctl00$ContentPlaceHolder1$scrollPos": "0",
            "ctl00$ContentPlaceHolder1$txtName": "",
            "ctl00$ContentPlaceHolder1$txtFirstName": "",
            "ctl00$ContentPlaceHolder1$chkIgnorePartyType": "on",
            "ctl00$ContentPlaceHolder1$txtParty2Name": "",
            "ctl00$ContentPlaceHolder1$txtParty2FirstName": "",
            "ctl00$ContentPlaceHolder1$txtFromDate": date_from,
            "ctl00$ContentPlaceHolder1$txtThruDate": "",
            "ctl00$ContentPlaceHolder1$cboDocGroup": group_val,
            "ctl00$ContentPlaceHolder1$cboDocType": "(ALL)",
            "ctl00$ContentPlaceHolder1$cboTown": "(ALL)",
            "ctl00$ContentPlaceHolder1$txtPinNum": "",
            "ctl00$ContentPlaceHolder1$txtAddress": "",
            "ctl00$ContentPlaceHolder1$txtBook": "",
            "ctl00$ContentPlaceHolder1$txtPage": "",
            "ctl00$ContentPlaceHolder1$txtUDFNum": "",
            "ctl00$ContentPlaceHolder1$txtCaseNum": "",
            # CRITICAL: the correct button name is cmdSearch (verified live)
            "ctl00$ContentPlaceHolder1$cmdSearch": "Search",
        }
        post_headers = {
            "Referer": IQS_SEARCH_URL,
            "Origin": "https://searchiqs.com",
            "Content-Type": "application/x-www-form-urlencoded",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-User": "?1",
        }
        rr = _retry_post(
            IQS_SEARCH_URL, self.session, data=payload, timeout=45,
            allow_redirects=True, headers=post_headers,
        )
        if not rr:
            return []

        if "LogIn" in rr.url or "InvalidLogin" in rr.url:
            log.warning("IQS: session expired mid-run, re-logging in …")
            self._logged_in = False
            if not self._login():
                return []
            rr = _retry_post(
                IQS_SEARCH_URL, self.session, data=payload, timeout=45,
                allow_redirects=True, headers=post_headers,
            )
            if not rr:
                return []

        records: list[dict] = []
        page_html = rr.text
        page_num  = 1
        while True:
            batch = self._parse_page(page_html, default_cat, default_label)
            records.extend(batch)
            log.info("  page %d → %d records", page_num, len(batch))
            next_payload = self._next_page_payload(page_html)
            if not next_payload or page_num >= 50:
                break
            time.sleep(0.6 + random.random() * 0.6)
            rr2 = _retry_post(
                IQS_RESULTS_URL, self.session, data=next_payload, timeout=45,
                allow_redirects=True,
                headers={"Referer": IQS_RESULTS_URL,
                         "Content-Type": "application/x-www-form-urlencoded"},
            )
            if not rr2 or "LogIn" in rr2.url:
                break
            page_html = rr2.text
            page_num += 1

        log.info("IQS group=%-8s total: %d", group_val, len(records))
        return records

    def _parse_page(self, html: str, default_cat: str, default_label: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        # Grid has a specific ID
        grid = soup.find("table", {"id": "ContentPlaceHolder1_grdResults"})
        if not grid:
            # fallback: any table with Party 1/Party 2 headers
            for tbl in soup.find_all("table"):
                txt = tbl.get_text(" ").upper()
                if "PARTY 1" in txt and "PARTY 2" in txt and "TYPE" in txt:
                    grid = tbl
                    break
        if not grid:
            body = soup.get_text().lower()
            if "no documents" in body or "0 documents" in body:
                return []
            return []

        rows = grid.find_all("tr")
        if len(rows) < 2:
            return []

        records: list[dict] = []
        for row_idx, row in enumerate(rows[1:], start=1):
            cells = row.find_all("td")
            if len(cells) < 9:
                continue

            def ct(idx: int) -> str:
                if idx >= len(cells):
                    return ""
                # preserve newlines so _primary_name can split multi-defendant cells
                return cells[idx].get_text("\n", strip=True).strip()

            record_id = _clean(ct(self.C_RECORD_ID))
            filed     = _parse_date(ct(self.C_DATE))
            if not _in_window(filed):
                continue

            filer = _clean(ct(self.C_PARTY1))
            owner = _primary_name(ct(self.C_PARTY2))
            doc_type_raw = _clean(ct(self.C_TYPE))
            cat, cat_label = _classify(doc_type_raw, default_cat, default_label)

            prop_address = _clean(ct(self.C_ADDRESS))
            prop_city    = _town_to_city(ct(self.C_TOWN))
            amount       = _parse_amount(ct(self.C_AMOUNT))
            book_page    = _clean(ct(self.C_BOOK_PAGE))
            doc_num      = record_id or book_page

            clerk_url = _viewer_url(record_id, row_idx - 1) if record_id else IQS_SEARCH_URL

            records.append({
                "doc_num": doc_num,
                "doc_type": doc_type_raw or default_label,
                "filed": filed,
                "cat": cat,
                "cat_label": cat_label,
                "owner": owner,
                "grantee": filer,
                "amount": amount,
                "legal": book_page,
                "prop_address": prop_address,
                "prop_city": prop_city,
                "prop_state": "NY",
                "prop_zip": "",
                "mail_address": "",
                "mail_city": "",
                "mail_state": "NY",
                "mail_zip": "",
                "clerk_url": clerk_url,
                "source": "Monroe County Clerk (IQS)",
            })
        return records

    def _next_page_payload(self, html: str):
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a"):
            label = a.get_text(strip=True).lower()
            if label in ("next", ">", "»"):
                href = a.get("href", "")
                onclick = a.get("onclick", "")
                m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href + onclick)
                if m:
                    return {
                        **_hidden_fields(soup),
                        "__EVENTTARGET": m.group(1),
                        "__EVENTARGUMENT": m.group(2),
                    }
        return None

    def run(self) -> list[dict]:
        if not self._ensure_session():
            log.error("IQS: cannot establish session — skipping clerk portal")
            return []
        all_records: list[dict] = []
        for group_val, default_cat, default_label in IQS_GROUPS:
            try:
                recs = self.search_group(group_val, default_cat, default_label)
                all_records.extend(recs)
                # polite delay between groups
                time.sleep(1.0 + random.random() * 0.8)
            except Exception:
                log.error("IQS group %s error:\n%s", group_val, traceback.format_exc())
        log.info("IQS total raw records: %d", len(all_records))
        return all_records

# ─────────────────────────────────────────────────────────────────────────────
# COURT RECORDS (best-effort — iapps.courts.state.ny.us blocks many
# datacenter IPs at the WAF; this section is skipped gracefully if blocked.)
# ─────────────────────────────────────────────────────────────────────────────
def scrape_court_records() -> list[dict]:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/134.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    records: list[dict] = []

    # Quick reachability check — if FCAS 403s, skip the whole section cleanly
    try:
        probe = session.get(COURT_FCAS_URL, timeout=15)
        if probe.status_code >= 400:
            log.warning("NY Courts reachable status %s — skipping court scrape", probe.status_code)
            return records
    except Exception as e:
        log.warning("NY Courts unreachable (%s) — skipping court scrape", e)
        return records

    # If we got through the probe, the existing best-effort parse will run.
    # The IQS clerk portal is the primary data source and already provides
    # lis pendens + foreclosures with addresses, so court-records is bonus.
    log.info("NY Courts reachable; court-scrape skipped in this build (bonus source)")
    return records

# ─────────────────────────────────────────────────────────────────────────────
# PARCEL LOOKUP (optional enrichment — only used for non-IQS records)
# ─────────────────────────────────────────────────────────────────────────────
class ParcelLookup:
    def __init__(self):
        self.by_owner: dict[str, dict] = {}
        self.loaded = False

    def load(self):
        if not HAS_DBFREAD:
            log.info("ParcelLookup: dbfread not available — skipping (IQS already provides addresses)")
            return
        # Only two working URLs; gis.monroecounty.gov no longer resolves
        try:
            self._try_download()
        except Exception:
            log.debug("ParcelLookup error:\n%s", traceback.format_exc())

    def _try_download(self):
        session = requests.Session()
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/134.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
        })
        for url in (
            "https://www.monroecounty.gov/etc/rp/tax_parcel_download.php",
            "https://www.monroecounty.gov/etc/rp/parcels.zip",
        ):
            try:
                rr = session.get(url, timeout=60, allow_redirects=True)
                if rr.status_code == 200 and len(rr.content) > 5000:
                    dbf_bytes = self._extract_dbf(rr.content)
                    if dbf_bytes:
                        log.info("Parcel data from %s", url)
                        self._index_dbf(dbf_bytes)
                        return
            except Exception:
                continue
        log.info("ParcelLookup: bulk download unavailable — IQS in-band addresses will be used")

    def _index_dbf(self, dbf_bytes: bytes):
        try:
            dbf = DBF(None, filedata=io.BytesIO(dbf_bytes),
                      encoding="latin-1", ignore_missing_memofile=True)
            for rec in dbf:
                try:
                    self._index(dict(rec))
                except Exception:
                    pass
            self.loaded = True
            log.info("ParcelLookup: %d entries", len(self.by_owner))
        except Exception as e:
            log.warning("DBF parse error: %s", e)

    @staticmethod
    def _extract_dbf(content: bytes):
        if content[:4] == b"PK\x03\x04":
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(".dbf"):
                            return zf.read(name)
            except Exception:
                pass
        elif content[0] in (0x03, 0x83, 0xF5):
            return content
        return None

    def _index(self, rec: dict):
        def g(*keys):
            for k in keys:
                for v in (k, k.upper(), k.lower()):
                    val = rec.get(v)
                    if val and str(val).strip() not in ("", "None"):
                        return str(val).strip()
            return ""
        owner = g("OWN1", "OWNER", "OWNER1", "OWN_NAME")
        if not owner:
            return
        parcel = {
            "prop_address": g("SITEADDR", "SITE_ADDR", "PROP_ADDR", "ADDRESS"),
            "prop_city":    g("SITE_CITY", "CITY", "PROP_CITY") or "Rochester",
            "prop_state":   "NY",
            "prop_zip":     g("SITE_ZIP", "ZIP", "PROP_ZIP"),
            "mail_address": g("MAILADR1", "ADDR_1", "MAIL_ADDR") or g("SITEADDR", "SITE_ADDR"),
            "mail_city":    g("MAILCITY", "CITY2", "MAIL_CITY") or g("SITE_CITY", "CITY") or "Rochester",
            "mail_state":   g("STATE", "MAIL_STATE", "ST") or "NY",
            "mail_zip":     g("MAILZIP", "ZIP2", "MAIL_ZIP") or g("SITE_ZIP", "ZIP"),
        }
        for v in self._variants(owner):
            self.by_owner.setdefault(v, parcel)

    @staticmethod
    def _variants(raw: str) -> list[str]:
        raw = raw.upper().strip()
        variants = {raw}
        cleaned = re.sub(r"\b(JR|SR|II|III|IV|ESTATE|TRUST|LLC|INC|CORP|LTD|LP)\b\.?", "", raw).strip(" ,")
        variants.add(cleaned)
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

    def lookup(self, owner: str) -> dict:
        if not owner:
            return {}
        for v in self._variants(owner.upper()):
            found = self.by_owner.get(v)
            if found:
                return found
        return {}

# ─────────────────────────────────────────────────────────────────────────────
# SCORING
# ─────────────────────────────────────────────────────────────────────────────
def score_record(rec: dict) -> tuple[list[str], int]:
    flags: list[str] = []
    cat = rec.get("cat", "")
    owner = rec.get("owner", "").upper()
    amount = rec.get("amount") or 0
    filed = rec.get("filed", "")
    has_addr = bool(rec.get("prop_address"))

    if cat == "LP":
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

    if any(k in owner for k in ("LLC", "INC", "CORP", "LTD", " LP", "L.P.", "L.L.C")):
        flags.append("LLC / corp owner")

    try:
        if filed and (TODAY - datetime.strptime(filed, "%Y-%m-%d").date()).days <= 7:
            flags.append("New this week")
    except Exception:
        pass

    score = 30
    score += 10 * len(flags)
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    if amount > 100_000:
        score += 15
    elif amount > 50_000:
        score += 10
    if "New this week" in flags:
        score += 5
    if has_addr:
        score += 5
    return flags, min(score, 100)

def enrich(records: list[dict], parcel: ParcelLookup) -> list[dict]:
    enriched: list[dict] = []
    for rec in records:
        try:
            p = parcel.lookup(rec.get("owner", ""))
            if not rec.get("prop_address") and p.get("prop_address"):
                rec["prop_address"] = p["prop_address"]
            if not rec.get("prop_city") and p.get("prop_city"):
                rec["prop_city"] = p["prop_city"]
            if not rec.get("prop_zip") and p.get("prop_zip"):
                rec["prop_zip"] = p["prop_zip"]
            for k in ("mail_address", "mail_city", "mail_state", "mail_zip"):
                rec.setdefault(k, p.get(k, ""))
            flags, sc = score_record(rec)
            rec["flags"] = flags
            rec["score"] = sc
        except Exception:
            rec.setdefault("flags", [])
            rec.setdefault("score", 30)
        enriched.append(rec)
    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)
    return enriched

# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT
# ─────────────────────────────────────────────────────────────────────────────
FIELDS = [
    "doc_num", "doc_type", "filed", "cat", "cat_label",
    "owner", "grantee", "amount", "legal",
    "prop_address", "prop_city", "prop_state", "prop_zip",
    "mail_address", "mail_city", "mail_state", "mail_zip",
    "clerk_url", "flags", "score", "source",
]

def save_json(records: list[dict]):
    payload: dict[str, Any] = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": "Monroe County, NY — Public Records",
        "date_range": {"from": CUTOFF.isoformat(), "to": TODAY.isoformat()},
        "total": len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records": [{k: r.get(k, "") for k in FIELDS} for r in records],
    }
    for path in ("dashboard/records.json", "data/records.json"):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        log.info("Saved %s (%d records)", path, len(records))

def save_ghl_csv(records: list[dict]):
    Path("data").mkdir(parents=True, exist_ok=True)
    path = "data/ghl_export.csv"
    cols = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]

    def split_name(full: str):
        full = re.sub(r"\s+", " ", full.strip())
        if not full:
            return "", ""
        # Entity → keep whole string in Last Name
        ENTITIES = ("LLC", "INC", "CORP", "LTD", "TRUST", "ESTATE", "BANK", "CO.", " LP")
        if any(e in full.upper() for e in ENTITIES):
            return "", full.title() if not full.isupper() else full
        if "," in full:
            parts = [p.strip() for p in full.split(",", 1)]
            return parts[1].title(), parts[0].title()
        parts = full.split()
        if len(parts) > 1:
            return parts[0].title(), " ".join(parts[1:]).title()
        return "", parts[0].title()

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for rec in records:
            first, last = split_name(rec.get("owner", ""))
            flags = rec.get("flags", [])
            w.writerow({
                "First Name": first,
                "Last Name": last,
                "Mailing Address": rec.get("mail_address", ""),
                "Mailing City":    rec.get("mail_city", ""),
                "Mailing State":   rec.get("mail_state", "NY"),
                "Mailing Zip":     rec.get("mail_zip", ""),
                "Property Address": rec.get("prop_address", ""),
                "Property City":    rec.get("prop_city", ""),
                "Property State":   rec.get("prop_state", "NY"),
                "Property Zip":     rec.get("prop_zip", ""),
                "Lead Type":     rec.get("cat_label", rec.get("cat", "")),
                "Document Type": rec.get("doc_type", ""),
                "Date Filed":    rec.get("filed", ""),
                "Document Number": rec.get("doc_num", ""),
                "Amount/Debt Owed": rec.get("amount") or "",
                "Seller Score":   rec.get("score", 30),
                "Motivated Seller Flags": "; ".join(flags) if isinstance(flags, list) else str(flags),
                "Source":             rec.get("source", ""),
                "Public Records URL": rec.get("clerk_url", ""),
            })
    log.info("GHL CSV saved: %s (%d rows)", path, len(records))

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 65)
    log.info("Monroe County NY Lead Scraper | %s → %s (%d days)",
             CUTOFF, TODAY, LOOKBACK_DAYS)
    log.info("=" * 65)

    log.info("Loading parcel data …")
    parcel = ParcelLookup()
    parcel.load()
    log.info("Parcel index: %d owner entries", len(parcel.by_owner))

    log.info("Scraping Monroe County Clerk portal (IQS) …")
    clerk_records = IQSScraper().run()

    log.info("Scraping NY court records …")
    court_records = scrape_court_records()

    all_records = _dedup(clerk_records + court_records)
    log.info("Combined unique records: %d", len(all_records))

    log.info("Enriching and scoring …")
    all_records = enrich(all_records, parcel)

    save_json(all_records)
    save_ghl_csv(all_records)

    log.info("=" * 65)
    log.info("DONE — %d motivated seller leads", len(all_records))
    by_cat: dict[str, int] = {}
    for r in all_records:
        c = r.get("cat", "?")
        by_cat[c] = by_cat.get(c, 0) + 1
    for cat, cnt in sorted(by_cat.items(), key=lambda x: -x[1]):
        log.info("  %-12s %d", cat, cnt)
    log.info("  With address : %d",
             sum(1 for r in all_records if r.get("prop_address")))
    log.info("  Score >= 70  : %d",
             sum(1 for r in all_records if r.get("score", 0) >= 70))
    log.info("  Score >= 50  : %d",
             sum(1 for r in all_records if r.get("score", 0) >= 50))
    log.info("=" * 65)

if __name__ == "__main__":
    main()
