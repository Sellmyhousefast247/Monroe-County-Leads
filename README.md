# Monroe County NY — Motivated Seller Lead Scraper

Automated daily scraper for off-market motivated seller leads in Monroe County, New York. Pulls public records from the county clerk portal, NY court systems, and enriches leads with parcel/owner data.

---

## 🗂 File Structure

```
.github/
  workflows/
    scrape.yml          # Daily GitHub Actions workflow (7 AM CST)
scraper/
  fetch.py              # Main scraper (Playwright + requests/BS4)
  requirements.txt      # Python dependencies
dashboard/
  index.html            # Dark-mode lead dashboard (GitHub Pages)
  records.json          # Latest records for dashboard display
data/
  records.json          # Mirror of records (for downstream use)
  ghl_export.csv        # GHL-ready CSV export (generated on each run)
README.md
```

---

## 📋 Lead Types Collected

| Code | Description |
|------|-------------|
| LP | Lis Pendens |
| NOFC | Notice of Foreclosure |
| TAXDEED | Tax Deed |
| JUD / CCJ / DRJUD | Judgment / Certified Judgment / Domestic Judgment |
| LNCORPTX / LNIRS / LNFED | Corporate Tax / IRS / Federal Lien |
| LN / LNMECH / LNHOA | Lien / Mechanic's Lien / HOA Lien |
| MEDLN | Medicaid Lien |
| PRO | Probate Documents |
| NOC | Notice of Commencement |
| RELLP | Release of Lis Pendens |

---

## 🏆 Seller Score (0–100)

| Rule | Points |
|------|--------|
| Base score | +30 |
| Per motivated flag | +10 each |
| LP + Foreclosure combo | +20 |
| Amount > $100,000 | +15 |
| Amount > $50,000 | +10 |
| Filed within last 7 days | +5 |
| Has property address | +5 |

**Flags assigned:** Lis pendens · Pre-foreclosure · Judgment lien · Tax lien · Mechanic lien · Probate / estate · LLC / corp owner · New this week

---

## 🚀 One-Time GitHub Setup

1. Push this repo to GitHub
2. Go to **Settings → Pages → Source → GitHub Actions**
3. That's it — `GITHUB_TOKEN` is built-in, no secrets required

The workflow runs automatically every day at **7 AM CST** and also supports manual runs via **Actions → Run workflow**.

---

## 🔧 Data Sources

| Source | URL |
|--------|-----|
| Monroe County Clerk (IQS) | https://searchiqs.com/nymonr/ |
| Monroe County Clerk (alt) | https://www.monroecounty.gov/clerk-records |
| NY Courts FCAS (Foreclosure) | https://iapps.courts.state.ny.us/webcivil/FCASMain |
| NY Courts NYSCEF | https://iapps.courts.state.ny.us/nyscef/HomePage |
| Parcel / Tax Data | https://www.monroecounty.gov/etc/rp |

---

## 📤 GHL Export

Each run generates `data/ghl_export.csv` with columns:

`First Name · Last Name · Mailing Address · Mailing City · Mailing State · Mailing Zip · Property Address · Property City · Property State · Property Zip · Lead Type · Document Type · Date Filed · Document Number · Amount/Debt Owed · Seller Score · Motivated Seller Flags · Source · Public Records URL`

You can also export directly from the dashboard using the **Export GHL CSV** button (respects active filters).

---

## ⚙️ Local Development

```bash
pip install -r scraper/requirements.txt
python -m playwright install --with-deps chromium
python scraper/fetch.py
```

Output files written to `dashboard/records.json` and `data/records.json`.

Set `LOOKBACK_DAYS` environment variable to change the lookback window (default: 7).

```bash
LOOKBACK_DAYS=14 python scraper/fetch.py
```
