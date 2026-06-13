# Portfolio OS — Master Guide

> Single source of truth for the whole system. If you are an AI or developer with **zero prior context**, read this top-to-bottom and you can operate, fix, build, and deploy everything. Last updated: 2026-06-13.

---

## 0. TL;DR / Vision

**Portfolio OS** is Ouriel's personal investment-tracking system. It tracks holdings across **three brokers** — **Bit2C** (crypto, ILS), **Excellence / אקסלנס** (stocks+ETFs), **Horizon / הורייזון** — and presents them in several synchronized front-ends.

**The whole design rests on one idea:** a single **Google Sheet** is the source of truth. Everything (every app, the Telegram bot, and Claude) reads and writes that sheet through one **Google Apps Script Web App**. Change anything in one place → it shows up everywhere.

**The end vision:** fully synced apps + sheet + Claude (me) + a **Telegram bot that can eventually do everything**, all kept in one canonical home (`C:\Users\ourie\Desktop\Portfolio OS`).

---

## 1. Architecture at a glance

```
                         ┌─────────────────────────────┐
                         │   Google Sheet (תמונת מצב)   │  ← SINGLE SOURCE OF TRUTH
                         │   + sheets: דשבורד, דף הבית,  │
                         │   הפקדות ידניות, היסטוריית שווי│
                         └──────────────┬──────────────┘
                                        │  (all access goes through)
                         ┌──────────────┴──────────────┐
                         │  Apps Script Web App         │  gas/קוד.js  (mirror: Code.gs)
                         │  doPost actions: read_snapshot│  deployed as a PINNED web app
                         │  add/edit/delete, fix,        │  + hosts the Telegram bot (cloud)
                         │  read/save_manual_deposits,   │  + read/save_sim_prefs (device sync)
                         │  read/save_sim_prefs, tg_*    │
                         └──────────────┬──────────────┘
        ┌───────────────┬──────────────┼───────────────┬────────────────────┐
        │               │              │               │                    │
   Streamlit        Next.js        Telegram bot     Claude bridge        EXE apps
   app.py           nextjs-        (in Apps Script, portfolio_cli.py     PortfolioOS /
   • desktop:8501   portfolio      cloud polling)   + clasp (deploy)     PortfolioOS Aurora
   • phone: Streamlit • Vercel                                          (Edge --app window
     Community Cloud  • APK (Capacitor)                                  onto :8501 / :8502)
        │               │
       APK            APK
   (Capacitor →    (Capacitor →
   Streamlit Cloud) Vercel)
```

**Sync rule:** the sheet is truth. Writes go through the Apps Script API and appear in every app immediately (apps read live). Simulator *settings* sync via a shared key-value store (`read_sim_prefs`/`save_sim_prefs`).

---

## 2. Canonical home & folder layout

Everything lives under **`C:\Users\ourie\Desktop\Portfolio OS\`**:

| Path | What it is |
|---|---|
| `my-portfolio-os\` | **The git repo** (all source). Remote: `github.com/ouriel9/my-portfolio-os` (PUBLIC). This is where you edit code. |
| `PortfolioOS\` | Desktop EXE app **v1** (PyInstaller onedir). Opens a chromeless Edge/Chrome window onto the always-on server. |
| `PortfolioOS Aurora\` | Desktop EXE app **v2** ("Aurora" dark theme, `PP_DESIGN_V2=1`, port 8502). |
| `Portfolio OS APK\` | Built Android APKs (`PortfolioOS-NextJS.apk`, `PortfolioOS-Streamlit.apk`). |
| `Portfolio OS Videos\` | Full UI walkthrough screen recordings. |
| `*.lnk`, `*.bat` | Desktop shortcuts + launch scripts. |
| `MASTER_GUIDE.md` | **This file** (also committed inside `my-portfolio-os\`). |

The repo `my-portfolio-os\` also has its own `.venv` (Python env) so the servers run from here. `node_modules` is **not** kept (regenerate with `npm install` only when building Next.js / APKs).

> The desktop apps `PortfolioOS\` / `PortfolioOS Aurora\` are also referenced by Desktop shortcuts — they were intentionally left on the Desktop too (copies live in this folder).

---

## 3. The Google Sheet + Apps Script (the brain)

- **Spreadsheet:** `https://docs.google.com/spreadsheets/d/1ccQTLsllpPLSXpmMP5h2Uu7M-5udcUC2FYUDXZC-LhE/edit`
- **Apps Script source:** `my-portfolio-os/gas/קוד.js` (Hebrew "קוד" = Code). Repo mirror: `my-portfolio-os/Code.gs`.
- **Script ID:** `1mgo2SfdU5H9zyAg49oxsuIOF4zkuzoZi-Ul0HWpyp-YETJYIs0bZkzop`
- **Pinned web-app deployment ID (the URL the apps call):** `AKfycbyDKgJszq8NWNgG7OQVPLflfN2rufBhAT5-fzmjy8iEVFMmNLZlK_CeI4MFvx1dijZF`
- **Web app URL:** `https://script.google.com/macros/s/<deployment-id>/exec`

**Sheets (tabs):**
- `תמונת מצב` — the raw transactions/positions grid (THE data; header row 1, data row 2+). Every app reads it via `read_snapshot`.
- `דשבורד` — rich dashboard (KPIs, per-platform, holdings, allocation, charts) built by `buildDashboardV2()`.
- `דף הבית` — older home view (`buildDashboard()`).
- `הפקדות ידניות` — manual deposits (title row 1, headers row 2, **data row 3+**; readers are title-aware).
- `היסטוריית שווי` — daily portfolio-value log (for weekly/monthly returns), written by `logPortfolioValueDaily` trigger.
- per-platform sheets (אקסלנס / Bit2C / הורייזון), `תגובות לטופס 1` = audit log.

**doPost actions** (all POST JSON, most guarded by `token`): `read_snapshot`, `add`/`edit`/`delete` (trade), `read_manual_deposits`/`save_manual_deposits`, `fix` (normalize + rebuild), `dump_all` (inspect every sheet), `read_sim_prefs`/`save_sim_prefs` (cross-device simulator settings), and Telegram: `tg_set_config`, `tg_answer`, `tg_send_test`, `tg_setup_polling`, `tg_poll_now`, `log_value`.

**Trade_ID** is a canonical SHA1 over 10 fields, derived server-side. Rows with an empty stored Trade_ID can't be edited/deleted via the API — use `correctionRules_()` / `removalRules_()` in Code.gs instead.

### Editing & deploying Code.gs (autonomous, via clasp)
```bash
cd "C:\Users\ourie\Desktop\Portfolio OS\my-portfolio-os\gas"
# edit קוד.js
node --check "קוד.js"
npx @google/clasp push -f
# MUST redeploy the PINNED deployment (push alone does NOT update the live web app):
npx @google/clasp deploy -i AKfycbyDKgJszq8NWNgG7OQVPLflfN2rufBhAT5-fzmjy8iEVFMmNLZlK_CeI4MFvx1dijZF -d "message"
# sync the repo mirror + commit:
cp "קוד.js" ../Code.gs
```
clasp OAuth creds: `C:\Users\ourie\.clasprc.json` (NEVER commit). `gas/.clasp.json` holds the scriptId (gitignored).

---

## 4. The apps

### 4a. Streamlit — `app.py` (PRIMARY app)
- **Run locally:** `.venv\Scripts\python.exe -m streamlit run app.py --server.port 8501`
- **Always-on servers** auto-start at logon via Windows Startup bats: `PortfolioOS_StreamlitServer.bat` (8501, v1) and `..._V2.bat` (8502, Aurora). They `set APP_ROOT=...Desktop\Portfolio OS\my-portfolio-os` and run `python -m streamlit`.
- **Phone:** hosted on **Streamlit Community Cloud → `https://djt7ecnaumycu5zame4xnc.streamlit.app`** (must be set **public** in the Streamlit dashboard). On Cloud it reads config from `st.secrets` (`web_app_url`, `api_token`); locally from `app_local_config.json`.
- **Pages:** Dashboard (Overview / Allocation / Reports&Analytics / Transactions), Trade Management (Add/Edit/Delete), Risk & FIFO, Simulator, Data Quality. Sidebar: Language (he/en), Appearance (system/light/dark), Demo mode.
- **Simulator** includes a **Financial-Independence calculator** (`sim_required_monthly_for_fi`): enter desired monthly retirement income → it computes the required nest egg (SWR-based, inflation-adjusted), credits projected pension + education funds, and solves the monthly saving needed.
- **Data freshness:** `load_snapshot_data()` auto-pulls the live sheet when the local store is >30 min stale and there are no pending offline edits (fixes a past staleness bug).

### 4b. Next.js — `nextjs-portfolio/` (hosted on Vercel)
- **Live:** `https://nextjs-portfolio-ouriel-s-projects1.vercel.app` (Vercel deployment-protection turned OFF so it's public).
- Uses **server-side API routes** (`/api/snapshot`, `/api/trades`, `/api/connection-status`) that hold the token via Vercel **env vars** `APPS_SCRIPT_URL`, `APPS_SCRIPT_TOKEN`, `WORKSHEET_NAME`, `NEXT_PUBLIC_*`.
- **Deploy:** `cd nextjs-portfolio && npx vercel --prod` (logged in as `ourieldahan1`). Project: `ouriel-s-projects1/nextjs-portfolio`.

### 4c. APKs (Capacitor WebView shells)
Both are thin full-screen shells that load the hosted web app (so they're 100% identical and work with the PC off):
- `nextjs-portfolio/android` → `server.url` = the Vercel URL (appId `com.ouriel.portfolio_os`).
- `apk_streamlit/` → `server.url` = the Streamlit Cloud URL (appId `com.ouriel.portfolio_streamlit`).
- **Build needs Temurin JDK 21** (Capacitor 8): JDK at `C:\Users\ourie\jdk21\jdk-21.0.11+10`, pinned via `org.gradle.java.home` in each `android/gradle.properties`. Build: `Set-Location <proj>\android; & .\gradlew.bat assembleDebug`. Output APKs copied to `Desktop\Portfolio OS APK\`.

### 4d. Desktop EXE (`PortfolioOS`, `PortfolioOS Aurora`)
- `launcher.py` opens a chromeless Edge/Chrome `--app` window onto the always-on server (health-checks :8501/:8502, else cold-starts a bundled server). Built with PyInstaller (`PortfolioOS.spec`). Each EXE folder has its own `app_local_config.json` so it's self-sufficient.

---

## 5. The Telegram bot (cloud, 24/7 — works with PC off)
Lives **inside Apps Script** (`Code.gs`), not on the PC:
- `doPost` detects Telegram updates; `answerPortfolioQuestion_` computes answers live from the sheet (Hebrew intent matching: summary, total/period return, P&L, cash, deposits, commissions, per-platform, allocation, winners/losers, per-ticker, FX, help).
- Uses **cloud polling** (`pollTelegramTick` time trigger, every 1 min) — NOT a webhook (Apps Script `/exec` returns a 302 Telegram won't follow).
- Token + owner chat_id in **Script Properties** (`TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`); only the owner gets answers.
- **One-time setup:** run `authorizeBot()` once from the Apps Script editor (grants UrlFetchApp + trigger scopes). Done.

---

## 6. Integrations (data IN)
| Source | Status | How |
|---|---|---|
| **Bit2C** | auto | `bit2c_sync.py` (API key) — pulls trades into the sheet |
| **Horizon** | semi | `horizon_import.py` parses the CSV you download |
| **Excellence** | manual | PDF statements; add trades via screenshot→Claude or the app |
| **Pension** | manual | no public API |

**Claude bridge:** `portfolio_cli.py` (`snapshot` / `add --json` / `edit --json` / `delete --id`). The `add-trade` skill (`.claude/skills/add-trade/`) parses a buy/sell screenshot → trade JSON → writes to the sheet → all apps update.

---

## 7. Common tasks (cheat-sheet)
- **Change sheet logic:** edit `gas/קוד.js` → `node --check` → `clasp push -f` → `clasp deploy -i <id>` → `cp to Code.gs` → commit.
- **Change Streamlit:** edit `app.py` → push to GitHub (Streamlit Cloud auto-redeploys the phone) → restart the local :8501 server (or it hot-reloads if started with `--server.runOnSave=true`).
- **Change Next.js:** edit → `cd nextjs-portfolio && npx vercel --prod`.
- **Rebuild an APK:** ensure JDK 21 → `cd <proj>\android` → `gradlew assembleDebug` → copy APK to `Desktop\Portfolio OS APK\`.
- **Always** `git push` after changes — the phone (Streamlit Cloud) and any cloud surface redeploy from GitHub.

---

## 8. Secrets (the repo is PUBLIC — NEVER commit these)
Gitignored, live only on disk: `app_local_config.json` (web_app_url + **api_token**), `telegram_config.json`, `bit2c_api.json`, `excellence_config.json` / `horizon_config.json`, `.streamlit/secrets.toml`, `DATA/` (broker statements). clasp creds: `C:\Users\ourie\.clasprc.json`. Cloud secrets live in the platform: Streamlit Cloud → Settings → Secrets; Vercel → env vars; Telegram token → Apps Script Script Properties.

---

## 9. Current state (2026-06-13, verified)
Open value ≈ **₪201k**, unrealized **≈ −30%**, **34 open lots / 2 closed** (VT +13.7%, SCHD +1.0% realized), deposits **₪315k**, cash **≈ ₪27.6k**, total account **≈ ₪229k**. USD/ILS ≈ 2.92. Holdings by value: BTC, VOO, IBIT, ETHA, QQQ, SOL, ETH, MSTR, BSOL.

---

## 10. Gotchas / history worth knowing
- The Streamlit data was once served from a **44-day-stale local store**; fixed with the auto-pull. Don't reintroduce local-first-without-refresh.
- USD/ILS once had a hard-coded `3.6` fallback (inflated USD ~20%); now `_usd_ils_rate()` prefers live → session last-good → 3.3.
- Windows venvs: invoke `python.exe -m streamlit` (not `streamlit.exe`) so a relocated venv still works.
- Git Bash mangles `/E`,`/d` flags and the PowerShell tool sometimes returns empty output — prefer `python.exe`/`cp`/explicit paths.
- This system was consolidated onto the Desktop on 2026-06-13 (moved off `C:\Users\ourie\PycharmProjects\my-portfolio-os`, which was deleted). The old PycharmProjects path may still appear in a few docs/log files — ignore it.
