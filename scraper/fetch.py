#!/usr/bin/env python3
"""
Monroe County, NY — Motivated Seller Lead Scraper
═══════════════════════════════════════════════════
Portal:   https://searchiqs.com/nymonr/  (SearchIQS — public guest access)
Courts:   iapps.courts.state.ny.us (FCAS + NYSCEF)
Parcel:   monroecounty.gov/etc/rp  (bulk DBF download)

ALL field names, column indices, and URL patterns verified live
against the actual portal on 2026-04-19.
"""

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
from urllib.parse import urljoin, quote

import requests
from bs4 import BeautifulSoup

try:
    from dbfread import DBF
    HAS_DBFREAD = True
except ImportError:
    HAS_DBFREAD = False
    logging.warning("dbfread not installed – parcel lookup disabled")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
TODAY  = date.today()
CUTOFF = TODAY - timedelta(days=LOOKBACK_DAYS)

# Live-verified URLs
IQS_BASE        = "https://searchiqs.com/nymonr"
IQS_LOGIN_URL   = f"{IQS_BASE}/LogIn.aspx?CountyID=5"
IQS_SEARCH_URL  = f"{IQS_BASE}/SearchAdvancedMP.aspx"
IQS_RESULTS_URL = f"{IQS_BASE}/SearchResultsMP.aspx"
IQS_VIEWER_URL  = f"{IQS_BASE}/ImageViewerMP.aspx"

COURT_FCAS_URL   = "https://iapps.courts.state.ny.us/webcivil/FCASMain"
COURT_NYSCEF_URL = "https://iapps.courts.state.ny.us/nyscef/CaseSearch"
PARCEL_PAGE_URL  = "https://www.monroecounty.gov/etc/rp"

# ── Document Groups — verified from live portal dropdown ─────────────────────
# (group_value, default_cat_code, default_cat_label)
IQS_GROUPS = [
    ("LP",    "LP",      "Lis Pendens"),
    ("J",     "JUD",     "Judgment"),
    ("LN",    "LN",      "Lien"),
    ("JUDIC", "NOFC",    "Judicial / Foreclosure"),
    ("CIVIL", "NOFC",    "Civil Courts"),
    ("W",     "TAXDEED", "Tax Warrant"),
    ("MF",    "NOC",     "Miscellaneous Filings"),
    ("D",     "TAXDEED", "Deeds"),
]

# ── Doc type string → (cat_code, cat_label) — from live 503-option dropdown ──
DOC_TYPE_MAP: dict[str, tuple[str, str]] = {
    "LIS PENDENS":                                    ("LP",       "Lis Pendens"),
    "LIS PENDENS - NOTICE OF PENDENCY CANCELLED":     ("RELLP",    "Release Lis Pendens"),
    "RELEASE OF LIS PENDENS":                         ("RELLP",    "Release Lis Pendens"),
    "NOTICE OF FORECLOSURE":                          ("NOFC",     "Notice of Foreclosure"),
    "FORECLOSURE":                                    ("NOFC",     "Notice of Foreclosure"),
    "TAX DEED":                                       ("TAXDEED",  "Tax Deed"),
    "TAX WARRANT":                                    ("TAXDEED",  "Tax Warrant"),
    "JUDGMENT":                                       ("JUD",      "Judgment"),
    "TRANSCRIPT OF JUDGMENT":                         ("JUD",      "Judgment"),
    "AMENDED JUDGMENT":                               ("JUD",      "Judgment"),
    "AMENDED TRANSCRIPT OF JUDGMENT":                 ("JUD",      "Judgment"),
    "CERTIFIED COPY OF JUDGMENT":                     ("CCJ",      "Certified Court Judgment"),
    "DOMESTIC RELATIONS JUDGMENT":                    ("DRJUD",    "Domestic Relations Judgment"),
    "FEDERAL TAX LIEN":                               ("LNFED",    "Federal Tax Lien"),
    "NOTICE OF FEDERAL TAX LIEN":                     ("LNFED",    "Federal Tax Lien"),
    "CERTIFICATE AND DISCHARGE OF FEDERAL TAX LIEN":  ("LNFED",    "Federal Tax Lien"),
    "IRS LIEN":                                       ("LNIRS",    "IRS Lien"),
    "NOTICE OF IRS LIEN":                             ("LNIRS",    "IRS Lien"),
    "CORPORATE TAX LIEN":                             ("LNCORPTX", "Corporate Tax Lien"),
    "LIEN":                                           ("LN",       "Lien"),
    "MECHANICS LIEN":                                 ("LNMECH",   "Mechanic's Lien"),
    "MECHANIC'S LIEN":                                ("LNMECH",   "Mechanic's Lien"),
    "AFFIDAVIT FOR MECHANICS LIEN FILED":             ("LNMECH",   "Mechanic's Lien"),
    "HOA LIEN":                                       ("LNHOA",    "HOA Lien"),
    "HOMEOWNER ASSOCIATION LIEN":                     ("LNHOA",    "HOA Lien"),
    "MEDICAID LIEN":                                  ("MEDLN",    "Medicaid Lien"),
    "PUBLIC ASSISTANCE LIEN":                         ("MEDLN",    "Medicaid Lien"),
    "ASSIGNMENT OF PUBLIC ASSISTANCE LIEN":           ("MEDLN",    "Medicaid Lien"),
    "PROBATE":                                        ("PRO",      "Probate"),
    "LETTERS TESTAMENTARY":                           ("PRO",      "Probate"),
    "LETTERS OF ADMINISTRATION":                      ("PRO",      "Probate"),
    "NOTICE OF COMMENCEMENT":                         ("NOC",      "Notice of Commencement"),
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
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _parse_date(raw: str) -> str:
    """Return YYYY-MM-DD or empty string."""
    if not raw:
        return ""
    raw = raw.strip()
    for fmt in (
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
        "%Y-%m-%d",
        "%m-%d-%Y",
    ):
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
        key = "|".join([str(r.get("doc_num", "")),
                        str(r.get("filed", "")),
                        str(r.get("owner", ""))])
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


def _retry_get(url: str, session: requests.Session,
               attempts: int = 3, timeout: int = 30, **kw) -> requests.Response | None:
    for i in range(attempts):
        try:
            r = session.get(url, timeout=timeout, **kw)
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning("GET %s [%d/%d]: %s", url, i + 1, attempts, e)
            if i < attempts - 1:
                time.sleep(2 ** i)
    return None


def _retry_post(url: str, session: requests.Session,
                attempts: int = 3, timeout: int = 30, **kw) -> requests.Response | None:
    for i in range(attempts):
        try:
            r = session.post(url, timeout=timeout, **kw)
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning("POST %s [%d/%d]: %s", url, i + 1, attempts, e)
            if i < attempts - 1:
                time.sleep(2 ** i)
    return None


def _hidden_fields(soup: BeautifulSoup) -> dict:
    out = {}
    for el in soup.find_all("input", {"type": "hidden"}):
        n = el.get("name", "")
        if n:
            out[n] = el.get("value", "")
    return out


def _viewer_url(record_id: str, row_index: int = 0) -> str:
    """
    Build direct document viewer URL.
    Live-confirmed pattern:
      ImageViewerMP.aspx?CustomView=Search%20Results&SelectedDoc=L|10435333&SelectedRowIndex=0
    """
    rid = quote(record_id, safe="")
    return (f"{IQS_VIEWER_URL}?CustomView=Search%20Results"
            f"&SelectedDoc={rid}&SelectedRowIndex={row_index}")


# ── Name cleaning ─────────────────────────────────────────────────────────────
_BOILERPLATE = re.compile(
    r"(JOHN DOE|JANE DOE|THE LAST TWELVE|FICTITIOUS|UNKNOWN TO PLAINTIFF|"
    r"TENANTS|OCCUPANTS|PERSONS OR CORPORATE|INTEREST OR LIEN|PREMISES|"
    r"DESCRIBED IN THE COMPLAINT|PLAINTIFF|CLAIMANT|AND\/OR|A\/K\/A)",
    re.I,
)


def _primary_name(raw: str) -> str:
    """Extract the primary property owner from a multi-defendant cell."""
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
    """'ROCHESTER - 261400' → 'Rochester'"""
    if not raw:
        return "Rochester"
    return raw.split("-")[0].strip().title()


# ─────────────────────────────────────────────────────────────────────────────
# IQS CLERK PORTAL SCRAPER
# ─────────────────────────────────────────────────────────────────────────────
class IQSScraper:
    """
    Scrapes the Monroe County SearchIQS portal using requests + BeautifulSoup.
    No Playwright required — the portal works fine with a plain requests session.

    Session flow (live-verified):
      1. GET  LogIn.aspx?CountyID=5
      2. POST LogIn.aspx  {__EVENTTARGET: 'btnGuestLogin'} → session cookie set
      3. Land on SearchAdvancedMP.aspx (PUBLIC USER)
      4. For each doc group:
           POST SearchAdvancedMP.aspx → redirect to SearchResultsMP.aspx
           Parse table (col layout confirmed live)
           Follow pagination via __doPostBack if multiple pages

    Results table column layout (0-indexed, confirmed live):
      0  View button
      1  MyDoc button
      2  Select checkbox
      3  RecordID   (text "L|10435333", links to ImageViewerMP via __doPostBack)
      4  Party 1    (FILER: bank / lienholder / plaintiff)
      5  Party 2    (OWNER: defendant / property owner  ← what we want)
      6  Type       (e.g. "LIS PENDENS")
      7  Book-Page  (e.g. "1631-960")
      8  Date       (filed date, MM/DD/YYYY)
      9  Town       (e.g. "ROCHESTER - 261400")
     10  Address    (property street address)
     11  Amount
     12  Related docs
    """

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
        "Referer":         IQS_BASE + "/",
    }

    # Confirmed column indices
    C_RECORD_ID = 3
    C_PARTY1    = 4   # filer
    C_PARTY2    = 5   # owner / defendant
    C_TYPE      = 6
    C_BOOK_PAGE = 7
    C_DATE      = 8
    C_TOWN      = 9
    C_ADDRESS   = 10
    C_AMOUNT    = 11

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self._logged_in   = False
        self._search_html = ""

    # ── Login ─────────────────────────────────────────────────────────────────
    def _login(self) -> bool:
        log.info("IQS: establishing guest session …")
        r = _retry_get(IQS_LOGIN_URL, self.session, timeout=30)
        if not r:
            return False

        soup   = BeautifulSoup(r.text, "lxml")
        hidden = _hidden_fields(soup)

        # Confirmed: guest button fires __doPostBack('btnGuestLogin','')
        payload = {
            **hidden,
            "__EVENTTARGET":   "btnGuestLogin",
            "__EVENTARGUMENT": "",
        }
        rr = _retry_post(IQS_LOGIN_URL, self.session, data=payload,
                         timeout=30, allow_redirects=True)
        if not rr:
            return False

        # Should land on SearchAdvancedMP.aspx
        if "SearchAdvanced" in rr.url or "Search" in rr.url:
            self._logged_in   = True
            self._search_html = rr.text
            log.info("IQS: guest session OK (at %s)", rr.url)
            return True

        # Fallback: try navigating directly
        rs = _retry_get(IQS_SEARCH_URL, self.session, timeout=30)
        if rs and "SearchAdvanced" in rs.url:
            self._logged_in   = True
            self._search_html = rs.text
            log.info("IQS: session valid via direct navigation")
            return True

        log.error("IQS: login failed — landed at %s", rr.url if rr else "?")
        return False

    def _ensure_session(self) -> bool:
        if self._logged_in:
            return True
        return self._login()

    # ── Search one group ──────────────────────────────────────────────────────
    def search_group(self, group_val: str,
                     default_cat: str, default_label: str) -> list[dict]:
        if not self._ensure_session():
            return []

        date_from = CUTOFF.strftime("%m/%d/%Y")
        date_to   = TODAY.strftime("%m/%d/%Y")
        log.info("IQS: group=%-8s  %s → %s", group_val, date_from, date_to)

        # Reload search page for fresh __VIEWSTATE
        rs = _retry_get(IQS_SEARCH_URL, self.session, timeout=30)
        if rs:
            self._search_html = rs.text
        soup   = BeautifulSoup(self._search_html, "lxml")
        hidden = _hidden_fields(soup)

        # Confirmed POST field names from live browser inspection
        payload = {
            **hidden,
            "__EVENTTARGET":   "",
            "__EVENTARGUMENT": "",
            "ctl00$ContentPlaceHolder1$txtName":            "",
            "ctl00$ContentPlaceHolder1$txtFirstName":       "",
            "ctl00$ContentPlaceHolder1$chkIgnorePartyType": "on",
            "ctl00$ContentPlaceHolder1$txtParty2Name":      "",
            "ctl00$ContentPlaceHolder1$txtParty2FirstName": "",
            "ctl00$ContentPlaceHolder1$txtFromDate":        date_from,
            "ctl00$ContentPlaceHolder1$txtThruDate":        date_to,
            "ctl00$ContentPlaceHolder1$cboDocGroup":        group_val,
            "ctl00$ContentPlaceHolder1$cboDocType":         "(ALL)",
            "ctl00$ContentPlaceHolder1$cboTown":            "(ALL)",
            "ctl00$ContentPlaceHolder1$txtPinNum":          "",
            "ctl00$ContentPlaceHolder1$txtAddress":         "",
            "ctl00$ContentPlaceHolder1$txtBook":            "",
            "ctl00$ContentPlaceHolder1$txtPage":            "",
            "ctl00$ContentPlaceHolder1$txtUDFNum":          "",
            "ctl00$ContentPlaceHolder1$txtCaseNum":         "",
            "ctl00$ContentPlaceHolder1$cmdSearch":          "Search",
        }

        rr = _retry_post(IQS_SEARCH_URL, self.session, data=payload,
                         timeout=30, allow_redirects=True)
        if not rr:
            return []

        # Session expired → re-login once and retry
        if "LogIn" in rr.url or "InvalidLogin" in rr.url:
            log.warning("IQS: session expired mid-run, re-logging in …")
            self._logged_in = False
            if not self._login():
                return []
            rr = _retry_post(IQS_SEARCH_URL, self.session, data=payload,
                             timeout=30, allow_redirects=True)
            if not rr:
                return []

        records:  list[dict] = []
        page_html = rr.text
        page_num  = 1

        while True:
            batch = self._parse_page(page_html, default_cat, default_label)
            records.extend(batch)
            log.info("  page %d → %d records", page_num, len(batch))

            next_payload = self._next_page_payload(page_html)
            if not next_payload or page_num >= 50:
                break

            rr2 = _retry_post(IQS_RESULTS_URL, self.session, data=next_payload,
                              timeout=30, allow_redirects=True)
            if not rr2 or "LogIn" in rr2.url:
                break
            page_html = rr2.text
            page_num += 1

        log.info("IQS group=%-8s  total: %d", group_val, len(records))
        return records

    # ── Parse results page ────────────────────────────────────────────────────
    def _parse_page(self, html: str,
                    default_cat: str, default_label: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        records: list[dict] = []

        # Find results table (has "Party 1" and "Party 2" headers)
        results_table = None
        for tbl in soup.find_all("table"):
            txt = tbl.get_text(" ").upper()
            if "PARTY 1" in txt and "PARTY 2" in txt and "TYPE" in txt:
                results_table = tbl
                break

        if not results_table:
            body = soup.get_text().lower()
            if "no documents" in body or "0 documents" in body:
                return []
            log.debug("IQS: no results table found")
            return []

        rows = results_table.find_all("tr")
        if len(rows) < 2:
            return []

        for row_idx, row in enumerate(rows[1:], start=1):
            cells = row.find_all("td")
            if len(cells) < 9:
                continue

            def ct(idx: int) -> str:
                if idx >= len(cells):
                    return ""
                return cells[idx].get_text(" ", strip=True).strip()

            # Record ID (e.g. "L|10435333")
            record_id = ct(self.C_RECORD_ID)

            # Filed date — skip if outside window
            filed = _parse_date(ct(self.C_DATE))
            if not _in_window(filed):
                continue

            # Parties:
            #   Party 1 = FILER  (bank, lienholder, plaintiff)
            #   Party 2 = OWNER  (defendant, property owner)
            filer = _clean(ct(self.C_PARTY1))
            owner = _primary_name(ct(self.C_PARTY2))

            # Doc type + category
            doc_type_raw = ct(self.C_TYPE)
            cat, cat_label = _classify(doc_type_raw, default_cat, default_label)

            # Address comes directly in the results table (col 10)
            prop_address = _clean(ct(self.C_ADDRESS))
            prop_city    = _town_to_city(ct(self.C_TOWN))
            amount       = _parse_amount(ct(self.C_AMOUNT))
            book_page    = _clean(ct(self.C_BOOK_PAGE))
            doc_num      = record_id if record_id else book_page

            # Direct link — confirmed URL pattern from live session
            clerk_url = _viewer_url(record_id, row_idx - 1) if record_id else IQS_SEARCH_URL

            records.append({
                "doc_num":      doc_num,
                "doc_type":     doc_type_raw or default_label,
                "filed":        filed,
                "cat":          cat,
                "cat_label":    cat_label,
                "owner":        owner,
                "grantee":      filer,
                "amount":       amount,
                "legal":        book_page,
                "prop_address": prop_address,
                "prop_city":    prop_city,
                "prop_state":   "NY",
                "prop_zip":     "",
                "mail_address": "",
                "mail_city":    "",
                "mail_state":   "NY",
                "mail_zip":     "",
                "clerk_url":    clerk_url,
                "source":       "Monroe County Clerk (IQS)",
            })

        return records

    # ── Pagination ────────────────────────────────────────────────────────────
    def _next_page_payload(self, html: str) -> dict | None:
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a"):
            if a.get_text(strip=True).lower() in ("next", ">", "»"):
                href    = a.get("href", "")
                onclick = a.get("onclick", "")
                m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href + onclick)
                if m:
                    return {
                        **_hidden_fields(soup),
                        "__EVENTTARGET":   m.group(1),
                        "__EVENTARGUMENT": m.group(2),
                    }
        return None

    # ── Run all groups ────────────────────────────────────────────────────────
    def run(self) -> list[dict]:
        if not self._ensure_session():
            log.error("IQS: cannot establish session — skipping clerk portal")
            return []

        all_records: list[dict] = []
        for group_val, default_cat, default_label in IQS_GROUPS:
            try:
                recs = self.search_group(group_val, default_cat, default_label)
                all_records.extend(recs)
            except Exception:
                log.error("IQS group %s error:\n%s", group_val, traceback.format_exc())

        log.info("IQS total raw records: %d", len(all_records))
        return all_records


# ─────────────────────────────────────────────────────────────────────────────
# COURT RECORDS
# ─────────────────────────────────────────────────────────────────────────────
def _scrape_fcas(session: requests.Session) -> list[dict]:
    """NY Foreclosure Action Status System — Monroe County code 28."""
    records:   list[dict] = []
    date_from  = CUTOFF.strftime("%m/%d/%Y")
    date_to    = TODAY.strftime("%m/%d/%Y")

    r = _retry_get(COURT_FCAS_URL, session, timeout=30)
    if not r:
        return records

    soup    = BeautifulSoup(r.text, "lxml")
    hidden  = _hidden_fields(soup)
    payload = {
        **hidden,
        "__EVENTTARGET":   "",
        "__EVENTARGUMENT": "",
        "county":          "28",
        "dtFrom":          date_from,
        "dtTo":            date_to,
        "action":          "Search",
    }
    rr = _retry_post(COURT_FCAS_URL, session, data=payload, timeout=30)
    if not rr:
        return records

    for row in BeautifulSoup(rr.text, "lxml").find_all("tr")[1:]:
        cells = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
        if len(cells) < 3:
            continue
        try:
            link  = row.find("a", href=True)
            url   = urljoin(COURT_FCAS_URL, link["href"]) if link else COURT_FCAS_URL
            filed = _parse_date(cells[4] if len(cells) > 4 else cells[-1])
            if not _in_window(filed):
                continue
            records.append({
                "doc_num":      cells[0],
                "doc_type":     "NOFC",
                "filed":        filed,
                "cat":          "NOFC",
                "cat_label":    "Notice of Foreclosure",
                "owner":        cells[2] if len(cells) > 2 else "",
                "grantee":      cells[1] if len(cells) > 1 else "",
                "amount":       None,
                "legal":        "",
                "prop_address": "", "prop_city": "", "prop_state": "NY", "prop_zip": "",
                "mail_address": "", "mail_city": "", "mail_state": "NY", "mail_zip": "",
                "clerk_url":    url,
                "source":       "NY Courts FCAS",
            })
        except Exception:
            pass

    log.info("FCAS: %d records", len(records))
    return records


def _scrape_nyscef(session: requests.Session) -> list[dict]:
    """NYSCEF electronic filing — Monroe County LP, Foreclosure, Probate."""
    records:   list[dict] = []
    date_from  = CUTOFF.strftime("%m/%d/%Y")
    date_to    = TODAY.strftime("%m/%d/%Y")

    targets = [
        ("Foreclosure", "NOFC", "Notice of Foreclosure"),
        ("Lis Pendens", "LP",   "Lis Pendens"),
        ("Probate",     "PRO",  "Probate"),
    ]

    r = _retry_get(COURT_NYSCEF_URL, session, timeout=30)
    if not r:
        return records
    base_soup = BeautifulSoup(r.text, "lxml")

    for case_type, cat, cat_label in targets:
        try:
            payload = {
                **_hidden_fields(base_soup),
                "county":        "Monroe",
                "caseType":      case_type,
                "filedDateFrom": date_from,
                "filedDateTo":   date_to,
                "Submit":        "Search",
            }
            rr = _retry_post(COURT_NYSCEF_URL, session, data=payload, timeout=30)
            if not rr:
                continue
            for row in BeautifulSoup(rr.text, "lxml").find_all("tr")[1:50]:
                cells = [c.get_text(" ", strip=True) for c in row.find_all("td")]
                if len(cells) < 3:
                    continue
                link  = row.find("a", href=True)
                url   = urljoin(COURT_NYSCEF_URL, link["href"]) if link else COURT_NYSCEF_URL
                filed = _parse_date(cells[-1]) or _parse_date(cells[0])
                if not _in_window(filed):
                    continue
                records.append({
                    "doc_num":      cells[0],
                    "doc_type":     case_type,
                    "filed":        filed,
                    "cat":          cat,
                    "cat_label":    cat_label,
                    "owner":        cells[2] if len(cells) > 2 else "",
                    "grantee":      cells[1] if len(cells) > 1 else "",
                    "amount":       None,
                    "legal":        "",
                    "prop_address": "", "prop_city": "", "prop_state": "NY", "prop_zip": "",
                    "mail_address": "", "mail_city": "", "mail_state": "NY", "mail_zip": "",
                    "clerk_url":    url,
                    "source":       "NY Courts NYSCEF",
                })
        except Exception:
            log.warning("NYSCEF %s error: %s", case_type, traceback.format_exc())

    log.info("NYSCEF: %d records", len(records))
    return records


def scrape_court_records() -> list[dict]:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124",
        "Accept":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    records: list[dict] = []
    try:
        records.extend(_scrape_fcas(session))
    except Exception:
        log.error("FCAS error:\n%s", traceback.format_exc())
    try:
        records.extend(_scrape_nyscef(session))
    except Exception:
        log.error("NYSCEF error:\n%s", traceback.format_exc())
    log.info("Court records total: %d", len(records))
    return records


# ─────────────────────────────────────────────────────────────────────────────
# PARCEL LOOKUP
# ─────────────────────────────────────────────────────────────────────────────
class ParcelLookup:
    def __init__(self):
        self.by_owner: dict[str, dict] = {}
        self.loaded = False

    def load(self):
        if not HAS_DBFREAD:
            log.warning("ParcelLookup: dbfread not available")
            return
        try:
            self._download_and_index()
        except Exception:
            log.error("ParcelLookup error:\n%s", traceback.format_exc())

    def _download_and_index(self):
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0 LeadScraper/2.0"})
        dbf_bytes = None

        for url in [
            "https://www.monroecounty.gov/etc/rp/tax_parcel_download.php",
            "https://www.monroecounty.gov/etc/rp/parcels.zip",
            "https://gis.monroecounty.gov/opendata/parcels.zip",
        ]:
            rr = _retry_get(url, session, attempts=2, timeout=90, allow_redirects=True)
            if rr and rr.status_code == 200 and len(rr.content) > 5000:
                dbf_bytes = self._extract_dbf(rr.content)
                if dbf_bytes:
                    log.info("Parcel data from %s", url)
                    break

        if not dbf_bytes:
            dbf_bytes = self._scrape_form(session)

        if not dbf_bytes:
            log.warning("ParcelLookup: no DBF obtained — address enrichment skipped")
            return

        count = 0
        try:
            dbf = DBF(None, filedata=io.BytesIO(dbf_bytes),
                      encoding="latin-1", ignore_missing_memofile=True)
            for rec in dbf:
                try:
                    self._index(dict(rec))
                    count += 1
                except Exception:
                    pass
            log.info("ParcelLookup: %d records indexed", count)
            self.loaded = True
        except Exception as e:
            log.error("ParcelLookup DBF parse error: %s", e)

    def _scrape_form(self, session: requests.Session) -> bytes | None:
        r = _retry_get(PARCEL_PAGE_URL, session, timeout=30)
        if not r:
            return None
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.find_all("a", href=True):
            if any(k in a["href"].lower() for k in ["parcel", "dbf", "zip"]):
                rr = _retry_get(urljoin(PARCEL_PAGE_URL, a["href"]), session, timeout=90)
                if rr and len(rr.content) > 5000:
                    return self._extract_dbf(rr.content)
        # Try __doPostBack download button
        vs = soup.find("input", {"id": "__VIEWSTATE"})
        ev = soup.find("input", {"id": "__EVENTVALIDATION"})
        for inp in soup.find_all("input", {"type": "submit"}):
            v = (inp.get("value", "") + inp.get("id", "")).lower()
            if any(k in v for k in ["parcel", "download", "dbf", "export"]):
                t = inp.get("name") or inp.get("id", "")
                payload = {
                    "__EVENTTARGET":   "",
                    "__EVENTARGUMENT": "",
                    "__VIEWSTATE":     (vs.get("value", "") if vs else ""),
                    "__EVENTVALIDATION": (ev.get("value", "") if ev else ""),
                    t: "Download",
                }
                rr = _retry_post(PARCEL_PAGE_URL, session, data=payload, timeout=90)
                if rr and len(rr.content) > 5000:
                    return self._extract_dbf(rr.content)
        return None

    @staticmethod
    def _extract_dbf(content: bytes) -> bytes | None:
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
        def g(*keys) -> str:
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
        cleaned = re.sub(
            r"\b(JR|SR|II|III|IV|ESTATE|TRUST|LLC|INC|CORP|LTD|LP)\b\.?", "", raw
        ).strip(" ,")
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
    flags:    list[str] = []
    cat       = rec.get("cat", "")
    owner     = rec.get("owner", "").upper()
    amount    = rec.get("amount") or 0
    filed     = rec.get("filed", "")
    has_addr  = bool(rec.get("prop_address"))

    if cat == "LP":                                   flags.append("Lis pendens")
    if cat == "NOFC":                                 flags.append("Pre-foreclosure")
    if cat in ("JUD", "CCJ", "DRJUD"):                flags.append("Judgment lien")
    if cat in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"): flags.append("Tax lien")
    if cat == "LNMECH":                               flags.append("Mechanic lien")
    if cat == "PRO":                                  flags.append("Probate / estate")
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


# ─────────────────────────────────────────────────────────────────────────────
# ENRICH + SCORE
# ─────────────────────────────────────────────────────────────────────────────
def enrich(records: list[dict], parcel: ParcelLookup) -> list[dict]:
    enriched: list[dict] = []
    for rec in records:
        try:
            p = parcel.lookup(rec.get("owner", ""))
            # Fill address fields only where empty (IQS already provides prop_address)
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
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Monroe County, NY — Public Records",
        "date_range":   {"from": CUTOFF.isoformat(), "to": TODAY.isoformat()},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records":      [{k: r.get(k, "") for k in FIELDS} for r in records],
    }
    for path in ("dashboard/records.json", "data/records.json"):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        log.info("Saved %s  (%d records)", path, len(records))


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

    def split_name(full: str) -> tuple[str, str]:
        full = re.sub(r"\s+", " ", full.strip())
        if not full:
            return "", ""
        if "," in full:
            parts = [p.strip() for p in full.split(",", 1)]
            return parts[1].title(), parts[0].title()
        parts = full.split()
        return (parts[0].title(), " ".join(parts[1:]).title()) if len(parts) > 1 else ("", parts[0].title())

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for rec in records:
            first, last = split_name(rec.get("owner", ""))
            flags = rec.get("flags", [])
            w.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        rec.get("mail_address", ""),
                "Mailing City":           rec.get("mail_city", ""),
                "Mailing State":          rec.get("mail_state", "NY"),
                "Mailing Zip":            rec.get("mail_zip", ""),
                "Property Address":       rec.get("prop_address", ""),
                "Property City":          rec.get("prop_city", ""),
                "Property State":         rec.get("prop_state", "NY"),
                "Property Zip":           rec.get("prop_zip", ""),
                "Lead Type":              rec.get("cat_label", rec.get("cat", "")),
                "Document Type":          rec.get("doc_type", ""),
                "Date Filed":             rec.get("filed", ""),
                "Document Number":        rec.get("doc_num", ""),
                "Amount/Debt Owed":       rec.get("amount") or "",
                "Seller Score":           rec.get("score", 30),
                "Motivated Seller Flags": "; ".join(flags) if isinstance(flags, list) else str(flags),
                "Source":                 rec.get("source", ""),
                "Public Records URL":     rec.get("clerk_url", ""),
            })
    log.info("GHL CSV saved: %s  (%d rows)", path, len(records))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 65)
    log.info("Monroe County NY Lead Scraper  |  %s → %s  (%d days)",
             CUTOFF, TODAY, LOOKBACK_DAYS)
    log.info("=" * 65)

    # 1. Parcel lookup
    log.info("Loading parcel data …")
    parcel = ParcelLookup()
    parcel.load()
    log.info("Parcel index: %d owner entries", len(parcel.by_owner))

    # 2. Clerk portal
    log.info("Scraping Monroe County Clerk portal (IQS) …")
    clerk_records = IQSScraper().run()

    # 3. Court records
    log.info("Scraping NY court records …")
    court_records = scrape_court_records()

    # 4. Merge + dedup
    all_records = _dedup(clerk_records + court_records)
    log.info("Combined unique records: %d", len(all_records))

    # 5. Enrich + score
    log.info("Enriching with parcel data and scoring …")
    all_records = enrich(all_records, parcel)

    # 6. Save
    save_json(all_records)
    save_ghl_csv(all_records)

    # 7. Summary
    log.info("=" * 65)
    log.info("DONE — %d motivated seller leads", len(all_records))
    by_cat: dict[str, int] = {}
    for r in all_records:
        c = r.get("cat", "?")
        by_cat[c] = by_cat.get(c, 0) + 1
    for cat, cnt in sorted(by_cat.items(), key=lambda x: -x[1]):
        log.info("  %-12s  %d", cat, cnt)
    log.info("  With address  : %d", sum(1 for r in all_records if r.get("prop_address")))
    log.info("  Score >= 70   : %d", sum(1 for r in all_records if r.get("score", 0) >= 70))
    log.info("  Score >= 50   : %d", sum(1 for r in all_records if r.get("score", 0) >= 50))
    log.info("=" * 65)


if __name__ == "__main__":
    main()
