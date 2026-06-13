// =========================================================================
// Portfolio Manager OS - Google Sheets Backend (Final)
// Preserves prior dashboard behavior + adds reconciled transaction fixes.
// =========================================================================

const CRYPTO_ETFS = ["IBIT", "ETHA", "BSOL", "MSTR"];
const PORTFOLIO_SHEET = "תמונת מצב";
const AUDIT_SHEET = "תגובות לטופס 1";
const DEPOSITS_SHEET = "הפקדות ידניות";
const SNAPSHOT_CANONICAL_HEADERS = [
  "מיקום נוכחי", "פלטפורמה", "סוג נכס", "טיקר", "תאריך רכישה", "כמות", "שער קנייה",
  "עלות כוללת", "מטבע", "עמלה", "סטטוס", "שער נוכחי USD", "עלות USD", "עלות ILS",
  "שווי USD", "שווי ILS", "שער קנייה USD", "שער קנייה ILS", "שער נוכחי ILS", "שער מכירה",
  "תאריך מכירה", "תשואה במכירה", "תשואה מקור", "תשואה שקלית", "Trade_ID"
];
const SNAPSHOT_FIELD_ALIASES = {
  location: ["מיקום נוכחי", "Current_Location"],
  platform: ["פלטפורמה", "Platform"],
  type: ["סוג נכס", "Type"],
  ticker: ["טיקר", "Ticker"],
  purchaseDate: ["תאריך רכישה", "Purchase_Date"],
  quantity: ["כמות", "Quantity"],
  buyPrice: ["שער קנייה", "Origin_Buy_Price"],
  costOrigin: ["עלות כוללת", "Cost_Origin"],
  currency: ["מטבע", "Origin_Currency"],
  fee: ["עמלה", "Commission"],
  status: ["סטטוס", "Status"],
  sellDate: ["תאריך מכירה", "Sell_Date"],
  spotUsd: ["שער נוכחי USD"],
  valueUsd: ["שווי USD", "שווי נוכחי USD"],
  costUsd: ["עלות USD"],
  costIls: ["עלות ILS"],
  valueIls: ["שווי ILS"],
  buyUsd: ["שער קנייה USD"],
  buyIls: ["שער קנייה ILS"],
  spotIls: ["שער נוכחי ILS"],
  sellPrice: ["שער מכירה", "Sell_Price_Origin"],
  yieldAtSale: ["תשואה במכירה", "Yield_At_Sale"],
  yieldOrigin: ["תשואה מקור"],
  yieldIls: ["תשואה שקלית"],
  tradeId: ["Trade_ID"]
};

function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu("🚀 אפליקציית השקעות")
    .addItem("רענן נתונים ודף הבית", "RefreshAllData")
    .addItem("🎨 עצב את מסד הנתונים", "formatMainSheet")
    .addSeparator()
    .addItem("🛡️ העברה לארנק קר (ללא כפילויות)", "TransferToColdWallet")
    .addSeparator()
    .addItem("🛠️ רופא המערכת - ניקוי ופיצול עמלות", "SystemDoctor")
    .addItem("🧹 צמצום עמודות כפולות (חד-פעמי)", "RunSnapshotSchemaDedupOnce")
    .addItem("⚙️ התקנה ותיקון נוסחאות (מבנה חדש)", "InstallSystem")
    .addToUi();
}


// --------------------------
// Core utilities
// --------------------------

function cleanText(val) {
  if (val === null || val === undefined) return "";
  return String(val).replace(new RegExp("[\\u200e\\u200f]", "g"), "").trim();
}

function parseNum(val) {
  if (val === null || val === undefined || val === "") return 0;
  if (typeof val === "number") return val;
  return parseFloat(String(val).replace("₪", "").replace("$", "").split(",").join("").split("%").join("").split(" ").join("").trim()) || 0;
}

function buildSnapshotHeaderIndexMap_(headers) {
  const cleanHeaders = (headers || []).map(cleanText);
  const out = {};
  Object.keys(SNAPSHOT_FIELD_ALIASES).forEach(function (key) {
    out[key] = -1;
    const aliases = SNAPSHOT_FIELD_ALIASES[key] || [];
    for (let i = 0; i < aliases.length; i++) {
      const idx = cleanHeaders.indexOf(aliases[i]);
      if (idx >= 0) {
        out[key] = idx;
        break;
      }
    }
  });
  return out;
}

function rowVal_(row, map, key) {
  const idx = map[key];
  if (idx === undefined || idx < 0 || idx >= row.length) return "";
  return row[idx];
}

function normalizeDateOnly_(val) {
  const tz = SpreadsheetApp.getActiveSpreadsheet().getSpreadsheetTimeZone() || Session.getScriptTimeZone();
  if (Object.prototype.toString.call(val) === "[object Date]") {
    return Utilities.formatDate(val, tz, "yyyy-MM-dd");
  }
  const s = cleanText(val);
  if (!s) return "";

  // Try DD/MM/YYYY or DD/MM/YY first (Israeli format) before anything else
  const slashParts = s.split("/");
  if (slashParts.length === 3) {
    const dd = ("0" + slashParts[0]).slice(-2);
    const mm = ("0" + slashParts[1]).slice(-2);
    const yyyy = slashParts[2].length === 2 ? "20" + slashParts[2] : slashParts[2].slice(0, 4);
    return yyyy + "-" + mm + "-" + dd;
  }

  // Already in YYYY-MM-DD or similar ISO format
  const d = new Date(s);
  if (String(d) !== "Invalid Date") {
    return Utilities.formatDate(d, tz, "yyyy-MM-dd");
  }

  return s;
}

function toSheetDateOrText_(val) {
  const normalized = normalizeDateOnly_(val);
  if (!normalized) return "";
  const asDate = new Date(normalized + "T00:00:00");
  return String(asDate) !== "Invalid Date" ? asDate : normalized;
}

function inferCryptoLocationByFields_(platform, type, ticker, purchaseDate, currentLocation) {
  const t = cleanText(type);
  if (t !== "קריפטו") return "";

  const existing = cleanText(currentLocation);
  if (existing && (existing.indexOf("ארנק") >= 0 || existing.indexOf("זירת") >= 0)) return existing;

  const p = cleanText(platform);
  const tick = cleanText(ticker).toUpperCase();
  const d = normalizeDateOnly_(purchaseDate);

  if (p === "Bit2C") {
    if (tick === "BTC" && d && d <= "2025-08-29") return "ארנק קר (Ledger)";
    return "Bit2C (זירת מסחר)";
  }
  if (p === "הורייזון") return "Horizon (זירת מסחר)";
  return p ? p + " (זירת מסחר)" : "זירת מסחר";
}

function almostEqual_(a, b, eps) {
  return Math.abs(parseNum(a) - parseNum(b)) <= (eps || 1e-8);
}

function ensureCoreSheets_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss.getSheetByName(PORTFOLIO_SHEET)) {
    throw new Error("Missing required sheet: " + PORTFOLIO_SHEET);
  }
  if (!ss.getSheetByName(AUDIT_SHEET)) {
    ss.insertSheet(AUDIT_SHEET);
    ss.getSheetByName(AUDIT_SHEET).getRange(1, 1, 1, 5).setValues([["Timestamp", "Action", "Trade_ID", "Status", "Payload"]]);
  }
  if (!ss.getSheetByName(DEPOSITS_SHEET)) {
    ss.insertSheet(DEPOSITS_SHEET);
    ss.getSheetByName(DEPOSITS_SHEET)
      .getRange(1, 1, 1, 4)
      .setValues([["Timestamp", "Mode", "Platform", "Manual_Deposit_ILS"]]);
  }
}

function sanitizeManualDepositsRows_(rows) {
  const out = [];
  const src = Array.isArray(rows) ? rows : [];
  for (let i = 0; i < src.length; i++) {
    const r = src[i] || {};
    const platform = cleanText(r.Platform || r.platform || "");
    if (!platform) continue;
    out.push({
      Platform: platform,
      Manual_Deposit_ILS: parseNum(r.Manual_Deposit_ILS || r.manual_deposit_ils || r.Deposit || 0)
    });
  }
  return out;
}

function readManualDeposits_(mode) {
  ensureCoreSheets_();
  const ws = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(DEPOSITS_SHEET);
  const currentMode = cleanText(mode || "live").toLowerCase() === "demo" ? "demo" : "live";
  const lastRow = ws.getLastRow();
  // Title-aware: a big title row may sit at row 1 (headers then row 2, data row 3+).
  const dataStart = cleanText(ws.getRange(1, 1).getValue()).indexOf("הפקדות ידניות") >= 0 ? 3 : 2;
  if (lastRow < dataStart) return { mode: currentMode, rows: [] };

  const values = ws.getRange(dataStart, 1, lastRow - dataStart + 1, 4).getValues();
  const rows = values
    .filter(function (r) { return cleanText(r[1]).toLowerCase() === currentMode; })
    .map(function (r) {
      return {
        Platform: cleanText(r[2]),
        Manual_Deposit_ILS: parseNum(r[3])
      };
    })
    .filter(function (r) { return r.Platform !== ""; });

  return { mode: currentMode, rows: rows };
}

function writeManualDeposits_(mode, rows) {
  ensureCoreSheets_();
  const ws = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(DEPOSITS_SHEET);
  const currentMode = cleanText(mode || "live").toLowerCase() === "demo" ? "demo" : "live";
  const normalized = sanitizeManualDepositsRows_(rows);

  const lastRow = ws.getLastRow();
  const dataStart = cleanText(ws.getRange(1, 1).getValue()).indexOf("הפקדות ידניות") >= 0 ? 3 : 2;
  if (lastRow >= dataStart) {
    const existingModes = ws.getRange(dataStart, 2, lastRow - dataStart + 1, 1).getValues();
    for (let i = existingModes.length - 1; i >= 0; i--) {
      if (cleanText(existingModes[i][0]).toLowerCase() === currentMode) {
        ws.deleteRow(i + dataStart);
      }
    }
  }

  if (normalized.length > 0) {
    const payload = normalized.map(function (r) {
      return [new Date(), currentMode, r.Platform, r.Manual_Deposit_ILS];
    });
    ws.getRange(ws.getLastRow() + 1, 1, payload.length, 4).setValues(payload);
  }

  return { ok: true, mode: currentMode, rows: normalized, count: normalized.length };
}

function appendAudit_(action, tradeId, status, payload) {
  ensureCoreSheets_();
  SpreadsheetApp.getActiveSpreadsheet().getSheetByName(AUDIT_SHEET)
    .appendRow([new Date(), action, tradeId, status, payload]);
}

function _isBlankCell_(v) {
  return cleanText(v) === "";
}

function _snapshotSemanticGroups_() {
  return [
    ["מיקום נוכחי", "Current_Location"],
    ["פלטפורמה", "Platform"],
    ["סוג נכס", "Type"],
    ["טיקר", "Ticker"],
    ["תאריך רכישה", "Purchase_Date"],
    ["כמות", "Quantity"],
    ["שער קנייה", "Origin_Buy_Price"],
    ["עלות כוללת", "Cost_Origin"],
    ["מטבע", "Origin_Currency"],
    ["עמלה", "Commission"],
    ["סטטוס", "Status"],
    ["שער נוכחי USD", "שער נוכחי USD (כפילות)"],
    ["עלות USD"],
    ["עלות ILS"],
    ["שווי USD", "שווי נוכחי USD"],
    ["שווי ILS"],
    ["שער קנייה USD"],
    ["שער קנייה ILS"],
    ["שער נוכחי ILS"],
    ["שער מכירה", "Sell_Price_Origin"],
    ["תאריך מכירה", "Sell_Date"],
    ["תשואה במכירה", "Yield_At_Sale"],
    ["תשואה מקור"],
    ["תשואה שקלית"],
    ["Trade_ID"],
    ["שער USD/ILS עדכני:", "USD/ILS", "שער דולר שקל"]
  ];
}

function _findFirstHeaderByAliases_(headers, aliases) {
  for (let i = 0; i < aliases.length; i++) {
    const ix = headers.indexOf(cleanText(aliases[i]));
    if (ix >= 0) return ix;
  }
  return -1;
}

function _aliasesForCanonicalHeader_(header) {
  const groups = _snapshotSemanticGroups_();
  for (let i = 0; i < groups.length; i++) {
    if (groups[i].indexOf(header) >= 0) return groups[i];
  }
  return [header];
}

function _mergeColumnInto_(values, keepIx, dropIx) {
  let filled = 0;
  let conflicts = 0;
  for (let r = 1; r < values.length; r++) {
    const keepVal = values[r][keepIx];
    const dropVal = values[r][dropIx];
    if (_isBlankCell_(keepVal) && !_isBlankCell_(dropVal)) {
      values[r][keepIx] = dropVal;
      filled++;
    } else if (!_isBlankCell_(keepVal) && !_isBlankCell_(dropVal) && cleanText(keepVal) !== cleanText(dropVal)) {
      conflicts++;
    }
  }
  return { filled: filled, conflicts: conflicts };
}

function dedupeSnapshotSchemaOnce_() {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    const props = PropertiesService.getScriptProperties();
    const done = cleanText(props.getProperty("SNAPSHOT_SCHEMA_DEDUP_DONE"));
    if (done) {
      return { ok: true, skipped: true, reason: "already_done", done_at: done };
    }

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const ws = ss.getSheetByName(PORTFOLIO_SHEET);
    if (!ws) return { ok: false, error: "Missing snapshot sheet" };

    const lastRow = ws.getLastRow();
    const lastCol = ws.getLastColumn();
    if (lastRow < 1 || lastCol < 1) {
      props.setProperty("SNAPSHOT_SCHEMA_DEDUP_DONE", new Date().toISOString());
      return { ok: true, skipped: true, reason: "empty_sheet" };
    }

    const rng = ws.getRange(1, 1, lastRow, lastCol);
    const values = rng.getValues();
    const headers = values[0].map(cleanText);

    const backupName = PORTFOLIO_SHEET + "_backup_before_schema_dedup_" + Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyyMMdd_HHmmss");
    ws.copyTo(ss).setName(backupName);

    const mergeStats = [];
    const groups = _snapshotSemanticGroups_();
    groups.forEach(function (group) {
      const indexes = [];
      group.forEach(function (alias) {
        const ix = headers.indexOf(cleanText(alias));
        if (ix >= 0 && indexes.indexOf(ix) < 0) indexes.push(ix);
      });
      if (indexes.length <= 1) return;
      const keepIx = indexes[0];
      for (let i = 1; i < indexes.length; i++) {
        const dropIx = indexes[i];
        const merged = _mergeColumnInto_(values, keepIx, dropIx);
        headers[dropIx] = "";
        mergeStats.push({
          kept_header: headers[keepIx],
          kept_col: keepIx + 1,
          removed_col: dropIx + 1,
          filled_from_duplicate: merged.filled,
          conflicts: merged.conflicts
        });
      }
    });

    const canonicalHeaders = SNAPSHOT_CANONICAL_HEADERS.slice();
    const canonicalIdx = canonicalHeaders.map(function (h) {
      return _findFirstHeaderByAliases_(headers, _aliasesForCanonicalHeader_(h));
    });

    const outHeaders = [];
    const outRows = [];
    for (let r = 0; r < values.length; r++) {
      const outRow = [];
      for (let c = 0; c < canonicalHeaders.length; c++) {
        if (r === 0) {
          outRow.push(canonicalHeaders[c]);
          continue;
        }
        const srcIx = canonicalIdx[c];
        outRow.push(srcIx >= 0 ? values[r][srcIx] : "");
      }
      if (r === 0) outHeaders.push(outRow);
      else outRows.push(outRow);
    }

    ws.clear();
    ws.getRange(1, 1, 1, canonicalHeaders.length).setValues(outHeaders);
    if (outRows.length > 0) {
      ws.getRange(2, 1, outRows.length, canonicalHeaders.length).setValues(outRows);
    }
    formatMainSheet();

    const doneAt = new Date().toISOString();
    props.setProperty("SNAPSHOT_SCHEMA_DEDUP_DONE", doneAt);
    const result = {
      ok: true,
      backup_sheet: backupName,
      rows: outRows.length,
      columns_after: canonicalHeaders.length,
      merges: mergeStats,
      done_at: doneAt
    };
    appendAudit_("snapshot_schema_dedup", "SYSTEM", "OK", JSON.stringify(result));
    return result;
  } finally {
    lock.releaseLock();
  }
}

function RunSnapshotSchemaDedupOnce() {
  const result = dedupeSnapshotSchemaOnce_();
  SpreadsheetApp.getUi().alert("צמצום סכמת תמונת מצב", JSON.stringify(result, null, 2), SpreadsheetApp.getUi().ButtonSet.OK);
}

function getColumnMap_(sheet) {
  const headers = sheet.getRange(1, 1, 1, Math.max(25, sheet.getLastColumn())).getValues()[0].map(cleanText);
  const ix = {};
  function findAny_(names) {
    for (let i = 0; i < names.length; i++) {
      const p = headers.indexOf(names[i]);
      if (p >= 0) return p;
    }
    return -1;
  }

  ix.location = findAny_(["מיקום נוכחי"]);
  ix.platform = findAny_(["פלטפורמה", "Platform"]);
  ix.type = findAny_(["סוג נכס", "Type"]);
  ix.ticker = findAny_(["טיקר", "Ticker"]);
  ix.purchaseDate = findAny_(["תאריך רכישה", "Purchase_Date"]);
  ix.quantity = findAny_(["כמות", "Quantity"]);
  ix.buyPrice = findAny_(["שער קנייה", "Origin_Buy_Price"]);
  ix.cost = findAny_(["עלות כוללת", "Cost_Origin"]);
  ix.currency = findAny_(["מטבע", "Origin_Currency"]);
  ix.fee = findAny_(["עמלה", "Commission"]);
  ix.status = findAny_(["סטטוס", "Status"]);
  ix.sellPrice = findAny_(["שער מכירה", "Sell_Price_Origin"]);
  ix.sellDate = findAny_(["תאריך מכירה", "Sell_Date"]);
  ix.tradeId = findAny_(["Trade_ID", "Trade ID", "מזהה עסקה", "trade_id"]);

  if (ix.tradeId < 0) {
    const col = sheet.getLastColumn() + 1;
    sheet.getRange(1, col).setValue("Trade_ID");
    ix.tradeId = col - 1;
  }
  return ix;
}

function copyCalculatedFormulaCells_(ws, sourceRow, targetRow) {
  if (!ws || sourceRow < 2 || targetRow < 2) return;
  const lastCol = ws.getLastColumn();
  if (lastCol < 1) return;
  const headers = ws.getRange(1, 1, 1, lastCol).getValues()[0].map(cleanText);
  const map = buildSnapshotHeaderIndexMap_(headers);
  const keys = [
    "spotUsd", "costUsd", "costIls", "valueUsd", "valueIls",
    "buyUsd", "buyIls", "spotIls", "sellPrice", "yieldAtSale",
    "yieldOrigin", "yieldIls"
  ];
  keys.forEach(function (k) {
    const ix = map[k];
    if (ix === undefined || ix < 0 || ix >= lastCol) return;
    const srcCell = ws.getRange(sourceRow, ix + 1, 1, 1);
    const formula = srcCell.getFormula();
    if (!formula) return;
    srcCell.copyTo(ws.getRange(targetRow, ix + 1, 1, 1), SpreadsheetApp.CopyPasteType.PASTE_FORMULA, false);
  });
}

function findFormulaTemplateRow_(ws, excludeRow) {
  if (!ws) return -1;
  const lastRow = ws.getLastRow();
  if (lastRow < 2) return -1;
  const headers = ws.getRange(1, 1, 1, ws.getLastColumn()).getValues()[0].map(cleanText);
  const map = buildSnapshotHeaderIndexMap_(headers);
  const keys = [
    "spotUsd", "costUsd", "costIls", "valueUsd", "valueIls",
    "buyUsd", "buyIls", "spotIls", "sellPrice", "yieldAtSale",
    "yieldOrigin", "yieldIls"
  ];

  for (let r = lastRow; r >= 2; r--) {
    if (excludeRow && r === excludeRow) continue;
    let hasFormula = false;
    for (let i = 0; i < keys.length; i++) {
      const ix = map[keys[i]];
      if (ix === undefined || ix < 0) continue;
      const f = ws.getRange(r, ix + 1).getFormula();
      if (cleanText(f)) {
        hasFormula = true;
        break;
      }
    }
    if (hasFormula) return r;
  }
  return -1;
}

function findTradeRowLoose_(ws, ix, trade) {
  const last = ws.getLastRow();
  if (last < 2) return -1;
  if (ix.platform < 0 || ix.ticker < 0 || ix.purchaseDate < 0) return -1;

  const rows = ws.getRange(2, 1, last - 1, ws.getLastColumn()).getValues();
  const targetPlatform = cleanText(trade.Platform || "");
  const targetTicker = cleanText(trade.Ticker || "").toUpperCase();
  const targetDate = normalizeDateOnly_(trade.Purchase_Date || "");
  const targetQty = parseNum(trade.Quantity || 0);

  let bestRow = -1;
  let bestQtyDelta = Infinity;
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    if (targetPlatform && cleanText(row[ix.platform]) !== targetPlatform) continue;
    if (targetTicker && cleanText(row[ix.ticker]).toUpperCase() !== targetTicker) continue;
    if (targetDate && normalizeDateOnly_(row[ix.purchaseDate]) !== targetDate) continue;

    if (ix.quantity >= 0) {
      const delta = Math.abs(parseNum(row[ix.quantity]) - targetQty);
      if (delta < bestQtyDelta) {
        bestQtyDelta = delta;
        bestRow = i + 2;
      }
    } else {
      return i + 2;
    }
  }
  return bestRow;
}

function normalizeCurrencyCode_(value) {
  // Must match app.py._normalize_currency_code and core.py._normalize_currency_code
  // byte-for-byte — it feeds the canonical Trade_ID identity tuple.
  const raw = cleanText(value).toUpperCase();
  if (raw === "" || raw === "NAN") return "";
  if (raw === "ILS" || raw === "NIS" || raw === "₪" || raw === "שח" || raw === 'ש"ח') return "ILS";
  if (raw === "USD" || raw === "$") return "USD";
  return raw;
}

function tradeIdFromRow_(row, ix) {
  // CANONICAL 10-field identity built from a raw snapshot row via ix.
  // Previously this was a DIFFERENT 5-field hash than buildTradeIdFromTrade_,
  // so a trade added (10-field id) disagreed with the same row re-hashed on
  // normalize (5-field id) -> duplicate rows. Now identical to tradeIdentityRaw_
  // and to app.py/core.py._to_trade_id.
  const raw = [
    cleanText(row[ix.platform]),
    ix.location >= 0 ? cleanText(row[ix.location]) : "",
    ix.type >= 0 ? cleanText(row[ix.type]) : "",
    cleanText(row[ix.ticker]).toUpperCase(),
    normalizeDateOnly_(row[ix.purchaseDate]),
    parseNum(row[ix.quantity]).toFixed(12),
    ix.buyPrice >= 0 ? parseNum(row[ix.buyPrice]).toFixed(12) : (0).toFixed(12),
    ix.cost >= 0 ? parseNum(row[ix.cost]).toFixed(12) : (0).toFixed(12),
    ix.currency >= 0 ? normalizeCurrencyCode_(row[ix.currency]) : "",
    ix.fee >= 0 ? parseNum(row[ix.fee]).toFixed(12) : (0).toFixed(12)
  ].join("|");
  return hashTradeId_(raw);
}

function hashTradeId_(raw) {
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_1, String(raw || ""), Utilities.Charset.UTF_8);
  return digest.map(function (b) {
    const v = (b + 256) % 256;
    return ("0" + v.toString(16)).slice(-2);
  }).join("").slice(0, 16);
}

function tradeIdentityRaw_(trade) {
  // CANONICAL 10-field identity (object form). Mirrors tradeIdFromRow_ and
  // app.py/core.py._to_trade_id. Currency goes through normalizeCurrencyCode_
  // so ₪/$ symbols map to ILS/USD identically across all clients.
  return [
    cleanText(trade.Platform || ""),
    cleanText(trade.Current_Location || ""),
    cleanText(trade.Type || ""),
    cleanText(trade.Ticker || "").toUpperCase(),
    normalizeDateOnly_(trade.Purchase_Date || ""),
    parseNum(trade.Quantity || 0).toFixed(12),
    parseNum(trade.Origin_Buy_Price || 0).toFixed(12),
    parseNum(trade.Cost_Origin || 0).toFixed(12),
    normalizeCurrencyCode_(trade.Origin_Currency || ""),
    parseNum(trade.Commission || 0).toFixed(12)
  ].join("|");
}

function buildTradeIdFromTrade_(trade) {
  return hashTradeId_(tradeIdentityRaw_(trade));
}

function ensureUniqueTradeIdForAdd_(ws, ix, trade) {
  let base = cleanText(trade.Trade_ID || "");
  if (!base) base = buildTradeIdFromTrade_(trade);
  if (findTradeRowById_(ws, ix, base) < 0) return base;

  for (let i = 1; i <= 50; i++) {
    const candidate = hashTradeId_(base + "|" + Date.now() + "|" + i + "|" + Math.random());
    if (findTradeRowById_(ws, ix, candidate) < 0) return candidate;
  }
  return hashTradeId_(base + "|fallback|" + Date.now() + "|" + Math.random());
}

// --------------------------
// Reconciliation (cross-check from DATA)
// --------------------------

function correctionRules_() {
  // Rules validated against DATA/verified_data.csv + the unified DATA.docx.
  // These are the rows that were rounded or slightly drifted versus raw exports.
  return [
    {
      platform: "הורייזון",
      ticker: "BTC",
      date: "2025-09-07",
      qtyFrom: 0.01777571,
      qtyTo: 0.01777571,
      costTo: 2061.45,
      feeTo: 0,
      note: "Horizon BTC cost correction (prevent inflated 500% return)"
    },
    {
      platform: "הורייזון",
      ticker: "ETH",
      date: "2026-01-29",
      qtyFrom: 0.71601,
      qtyTo: 0.716013,
      costTo: 1967.62,
      feeTo: 97.4030397,
      note: "Raw Horizon CSV precision correction"
    },
    {
      platform: "הורייזון",
      ticker: "SOL",
      date: "2026-01-29",
      qtyFrom: 20.8643,
      qtyTo: 20.864301,
      costTo: 2518.16,
      feeTo: 118.7655,
      note: "Raw Horizon CSV precision correction"
    },
    {
      platform: "Bit2C",
      ticker: "BTC",
      date: "2026-01-30",
      qtyFrom: 0.07657,
      qtyTo: 0.07657038,
      costTo: 20234.68004475,
      feeTo: 249.81086475,
      locationTo: "Bit2C (זירת מסחר)",
      note: "Raw Bit2C export precision correction"
    },
    {
      platform: "Bit2C",
      ticker: "BTC",
      date: "2026-01-30",
      qtyFrom: 0.00005,
      qtyTo: 0.00005797,
      costTo: 15.31929713,
      feeTo: 0.18912713,
      locationTo: "Bit2C (זירת מסחר)",
      note: "Rounded quantity correction from raw Bit2C"
    },
    {
      platform: "Bit2C",
      ticker: "BTC",
      date: "2026-02-04",
      qtyFrom: 0.08681,
      qtyTo: 0.08681743,
      costTo: 20193.64740135,
      feeTo: 225.63850135,
      locationTo: "Bit2C (זירת מסחר)",
      note: "Raw Bit2C export precision correction"
    },
    {
      platform: "Bit2C",
      ticker: "BTC",
      date: "2026-02-04",
      qtyFrom: 0.00014,
      qtyTo: 0.00014344,
      costTo: 33.36400056,
      feeTo: 0.37280056,
      locationTo: "Bit2C (זירת מסחר)",
      note: "Raw Bit2C export precision correction"
    },
    {
      platform: "אקסלנס",
      ticker: "SCHD",
      date: "2025-05-01",
      qtyFrom: 216,
      qtyTo: 216,
      costTo: 5454,
      feeTo: 2,
      sellPriceTo: 26.47,
      note: "SCHD real sale price (formula previously showed live market price)"
    },
    {
      platform: "אקסלנס",
      ticker: "VT",
      date: "2025-05-01",
      qtyFrom: 43,
      qtyTo: 43,
      costTo: 4699.71,
      feeTo: 12,
      sellPriceTo: 132.83,
      note: "VT real cost (22@108.9 + 21@109.71) + $6×2 fee + real sale price $132.83"
    }
  ];
}

function dedupeRows_(rows, ix) {
  const seen = {};
  const out = [];
  let dup = 0;
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const tradeId = cleanText(r[ix.tradeId]);
    const key = tradeId
      ? "TID|" + tradeId
      : [
        cleanText(r[ix.platform]),
        cleanText(r[ix.type]),
        cleanText(r[ix.ticker]).toUpperCase(),
        normalizeDateOnly_(r[ix.purchaseDate]),
        parseNum(r[ix.quantity]).toFixed(8),
        parseNum(r[ix.buyPrice]).toFixed(8),
        parseNum(r[ix.cost]).toFixed(8),
        cleanText(r[ix.status])
      ].join("|");
    if (seen[key]) {
      dup++;
      continue;
    }
    seen[key] = true;
    out.push(r);
  }
  return { rows: out, duplicates: dup };
}

function applyCrossCheckPatches_(rows, ix) {
  let updates = 0;
  const rules = correctionRules_();
  rules.forEach(function (rule) {
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (cleanText(r[ix.platform]) !== cleanText(rule.platform)) continue;
      if (cleanText(r[ix.ticker]).toUpperCase() !== cleanText(rule.ticker).toUpperCase()) continue;
      if (normalizeDateOnly_(r[ix.purchaseDate]) !== normalizeDateOnly_(rule.date)) continue;
      if (!almostEqual_(r[ix.quantity], rule.qtyFrom, 1e-8)) continue;

      r[ix.quantity] = rule.qtyTo;
      r[ix.cost] = rule.costTo;
      r[ix.fee] = rule.feeTo;
      if (rule.sellPriceTo !== undefined && ix.sellPrice >= 0) r[ix.sellPrice] = rule.sellPriceTo;
      if (ix.location >= 0 && rule.locationTo) r[ix.location] = rule.locationTo;
      r[ix.tradeId] = tradeIdFromRow_(r, ix);
      updates++;
    }
  });
  return { rows: rows, updates: updates, adds: 0 };
}

function fixSnapshotSheet_() {
  const ws = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(PORTFOLIO_SHEET);
  const ix = getColumnMap_(ws);
  const lastRow = ws.getLastRow();
  if (lastRow < 2) return { rows: 0, removedDuplicates: 0, updated: 0, added: 0 };

  const width = Math.max(ws.getLastColumn(), ix.tradeId + 1);
  let rows = ws.getRange(2, 1, lastRow - 1, width).getValues();

  // Normalize base fields + ensure Trade_ID.
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    r[ix.platform] = cleanText(r[ix.platform]);
    r[ix.type] = cleanText(r[ix.type]);
    r[ix.ticker] = cleanText(r[ix.ticker]).toUpperCase();
    r[ix.currency] = cleanText(r[ix.currency]);
    r[ix.status] = cleanText(r[ix.status]);
    r[ix.purchaseDate] = normalizeDateOnly_(r[ix.purchaseDate]);
    if (ix.location >= 0) {
      r[ix.location] = inferCryptoLocationByFields_(r[ix.platform], r[ix.type], r[ix.ticker], r[ix.purchaseDate], r[ix.location]);
    }
    r[ix.quantity] = parseNum(r[ix.quantity]);
    r[ix.buyPrice] = parseNum(r[ix.buyPrice]);
    r[ix.cost] = parseNum(r[ix.cost]);
    r[ix.fee] = parseNum(r[ix.fee]);
    if (!cleanText(r[ix.tradeId])) {
      r[ix.tradeId] = tradeIdFromRow_(r, ix);
    }
  }

  // Remove rows flagged for deletion (canceled / never-executed orders).
  let removed = 0;
  const removals = removalRules_();
  rows = rows.filter(function (r) {
    for (let j = 0; j < removals.length; j++) {
      const rule = removals[j];
      if (cleanText(r[ix.platform]) === cleanText(rule.platform) &&
          cleanText(r[ix.ticker]).toUpperCase() === cleanText(rule.ticker).toUpperCase() &&
          normalizeDateOnly_(r[ix.purchaseDate]) === normalizeDateOnly_(rule.date) &&
          almostEqual_(r[ix.quantity], rule.qty, 1e-8)) {
        removed++;
        return false;
      }
    }
    return true;
  });

  const dedup = dedupeRows_(rows, ix);
  const patched = applyCrossCheckPatches_(rows, ix);

  // Write the (possibly fewer) normalized rows; delete any orphaned trailing rows.
  const outRows = patched.rows;
  if (outRows.length > 0) {
    ws.getRange(2, 1, outRows.length, width).setValues(outRows);
  }
  const orphanCount = (lastRow - 1) - outRows.length;
  if (orphanCount > 0) {
    ws.deleteRows(2 + outRows.length, orphanCount);
  }

  // Restore formulas/formatting, then rebuild the per-platform sheets AND the
  // home dashboard so EVERYTHING (snapshot, אקסלנס/Bit2C/הורייזון, דף הבית)
  // stays in sync with the corrected data.
  InstallSystem();
  SpreadsheetApp.flush();
  try { RefreshAllData(); } catch (e) {}

  return {
    rows: patched.rows.length,
    removedDuplicates: dedup.duplicates,
    removed: removed,
    updated: patched.updates,
    added: patched.adds
  };
}

function removalRules_() {
  // Rows to DELETE entirely — orders that never actually executed.
  return [
    {
      platform: "הורייזון",
      ticker: "BTC",
      date: "2025-09-07",
      qty: 0.01777571,
      note: "Canceled limit order @ $110,500 (never filled) — confirmed Canceled on Horizon trade dashboard"
    }
  ];
}


// --------------------------
// Streamlit -> Apps Script endpoint
// --------------------------

function doGet() {
  return ContentService.createTextOutput(JSON.stringify({ ok: true, service: "portfolio-sync" }))
    .setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  try {
    const payload = JSON.parse((e && e.postData && e.postData.contents) || "{}");
    // Telegram webhook updates carry no api token — identify by update_id/message and
    // handle them on a fully separate path so they can never affect the apps' API.
    if (payload && (payload.update_id !== undefined || payload.message || payload.edited_message)) {
      try { handleTelegramUpdate_(payload); } catch (tgErr) { try { appendAudit_("telegram_error", "TG", "ERROR", String(tgErr)); } catch (e2) {} }
      return ContentService.createTextOutput("ok");
    }
    validateToken_(payload.token);

    const rawAction = cleanText(payload.action || payload.Action || "");
    // Accept common variants from different clients/deployments.
    const action = rawAction.toLowerCase().replace(/\s+/g, "_").replace(/-/g, "_");
    ensureCoreSheets_();

    const readAliases = {
      "read_snapshot": true,
      "readsnapshot": true,
      "read": true,
      "snapshot": true,
      "get_snapshot": true,
      "getsnapshot": true
    };

    const readDepositsAliases = {
      "read_manual_deposits": true,
      "readmanualdeposits": true,
      "get_manual_deposits": true,
      "getmanualdeposits": true
    };

    const writeDepositsAliases = {
      "save_manual_deposits": true,
      "savemanualdeposits": true,
      "write_manual_deposits": true,
      "writemanualdeposits": true
    };

    if (action === "fix") {
      const stats = fixSnapshotSheet_();
      appendAudit_("fix", "SYSTEM", "OK", JSON.stringify(stats));
      return jsonResponse_({ ok: true, stats: stats });
    }

    if (action === "dump_all") {
      const ssd = SpreadsheetApp.getActiveSpreadsheet();
      const out = {};
      ssd.getSheets().forEach(function (sh) {
        const lr = sh.getLastRow(), lc = sh.getLastColumn();
        out[sh.getName()] = {
          rows: lr, cols: lc,
          data: (lr > 0 && lc > 0) ? sh.getRange(1, 1, Math.min(lr, 80), Math.min(lc, 32)).getDisplayValues() : []
        };
      });
      return jsonResponse_({ ok: true, sheets: out });
    }

    if (action === "dedupe_snapshot_schema") {
      const result = dedupeSnapshotSchemaOnce_();
      return jsonResponse_(result);
    }

    // ---- Telegram bot setup/diagnostics (token-guarded) ----
    if (action === "tg_set_config") {
      const props = PropertiesService.getScriptProperties();
      if (payload.telegram_token) props.setProperty("TELEGRAM_TOKEN", cleanText(payload.telegram_token));
      if (payload.allowed_chat_id) props.setProperty("TELEGRAM_CHAT_ID", cleanText(String(payload.allowed_chat_id)));
      return jsonResponse_({ ok: true, hasToken: !!props.getProperty("TELEGRAM_TOKEN"), chat: props.getProperty("TELEGRAM_CHAT_ID") || "" });
    }
    if (action === "tg_answer") {
      // Compute an answer without sending — lets us verify the brain over the API.
      return jsonResponse_({ ok: true, answer: answerPortfolioQuestion_(cleanText(payload.text || "סיכום")) });
    }
    if (action === "tg_webhook_info") {
      return jsonResponse_({ ok: true, result: tgApi_("getWebhookInfo", {}) });
    }
    if (action === "tg_send_test") {
      const chat = payload.chat_id || PropertiesService.getScriptProperties().getProperty("TELEGRAM_CHAT_ID");
      return jsonResponse_({ ok: true, result: tgSend_(chat, answerPortfolioQuestion_(cleanText(payload.text || "סיכום"))) });
    }
    if (action === "log_value") {
      logPortfolioValue_();
      return jsonResponse_({ ok: true });
    }

    if (readAliases[action]) {
      const data = readSnapshotRows_();
      return jsonResponse_({ ok: true, data: data });
    }

    if (readDepositsAliases[action]) {
      const mode = cleanText(payload.mode || "live").toLowerCase();
      const data = readManualDeposits_(mode);
      return jsonResponse_({ ok: true, data: data });
    }

    if (writeDepositsAliases[action]) {
      const mode = cleanText(payload.mode || "live").toLowerCase();
      const rows = payload.rows || [];
      const result = writeManualDeposits_(mode, rows);
      appendAudit_("manual_deposits_save", "SYSTEM", "OK", JSON.stringify({ mode: mode, count: result.count }));
      return jsonResponse_(result);
    }

    if (!["add", "edit", "delete"].includes(action)) {
      return jsonResponse_({ ok: false, error: "Unsupported action", action_received: rawAction, action_normalized: action });
    }

    const trade = sanitizeTrade_(payload.trade || {});
    if (!trade.Trade_ID) return jsonResponse_({ ok: false, error: "Trade_ID is required" });

    const result = action === "delete" ? deleteTrade_(trade) : upsertTrade_(action, trade);
    appendAudit_(action, trade.Trade_ID, result.ok ? "OK" : "ERROR", JSON.stringify(result));
    return jsonResponse_(result);
  } catch (err) {
    appendAudit_("error", "SYSTEM", "ERROR", String(err));
    return jsonResponse_({ ok: false, error: String(err) });
  }
}

function readSnapshotRows_() {
  const ws = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(PORTFOLIO_SHEET);
  if (!ws) return { headers: [], rows: [] };
  const lastRow = ws.getLastRow();
  const lastCol = ws.getLastColumn();
  if (lastRow < 1 || lastCol < 1) return { headers: [], rows: [] };

  const values = ws.getRange(1, 1, lastRow, lastCol).getValues();
  const headers = values[0].map(cleanText);
  const map = buildSnapshotHeaderIndexMap_(headers);
  const tz = SpreadsheetApp.getActiveSpreadsheet().getSpreadsheetTimeZone() || Session.getScriptTimeZone();
  const rows = values.slice(1)
    .filter(function(r) { return cleanText(rowVal_(r, map, "ticker")) !== ""; })
    .map(function(row) {
      return row.map(function(cell) {
        if (Object.prototype.toString.call(cell) === "[object Date]") {
          return Utilities.formatDate(cell, tz, "yyyy-MM-dd");
        }
        return cell;
      });
    });
  return { headers: headers, rows: rows };
}

function sanitizeTrade_(trade) {
  const out = {};
  Object.keys(trade).forEach(function (k) { out[k] = trade[k]; });
  out.Current_Location = cleanText(out.Current_Location || out["מיקום נוכחי"] || "");
  out.Platform = cleanText(out.Platform || out["פלטפורמה"] || "");
  out.Type = cleanText(out.Type || out["סוג נכס"] || "קריפטו");
  out.Ticker = cleanText(out.Ticker || out["טיקר"] || "").toUpperCase();
  out.Purchase_Date = normalizeDateOnly_(out.Purchase_Date || out["תאריך רכישה"] || "");
  out.Quantity = parseNum(out.Quantity || out["כמות"]);
  out.Origin_Buy_Price = parseNum(out.Origin_Buy_Price || out["שער קנייה"]);
  out.Cost_Origin = parseNum(out.Cost_Origin || out["עלות כוללת"]);
  out.Origin_Currency = cleanText(out.Origin_Currency || out["מטבע"] || "ILS").toUpperCase();
  out.Commission = parseNum(out.Commission || out["עמלה"]);
  out.Status = cleanText(out.Status || out["סטטוס"] || "פתוח");
  out.Sell_Date = normalizeDateOnly_(out.Sell_Date || out["תאריך מכירה"] || "");
  out.Trade_ID = cleanText(out.Trade_ID || "");

  if (out.Cost_Origin <= 0 && out.Quantity > 0 && out.Origin_Buy_Price > 0) {
    out.Cost_Origin = out.Quantity * out.Origin_Buy_Price;
  }

  if (!out.Trade_ID) {
    out.Trade_ID = buildTradeIdFromTrade_(out);
  }
  return out;
}

function applyCalculatedFormulasForRow_(ws, rowNum) {
  if (!ws || rowNum < 2) return;
  const headers = ws.getRange(1, 1, 1, ws.getLastColumn()).getValues()[0].map(cleanText);
  const map = buildSnapshotHeaderIndexMap_(headers);
  const r = rowNum;
  const hRate = "IFERROR(IFNA(INDEX(GOOGLEFINANCE(\"CURRENCY:USDILS\", \"price\", E" + r + "), 2, 2), IFNA(INDEX(GOOGLEFINANCE(\"CURRENCY:USDILS\", \"price\", E" + r + "-1), 2, 2), $AA$1)), $AA$1)";
  const sRate = "IFERROR(IFNA(INDEX(GOOGLEFINANCE(\"CURRENCY:USDILS\", \"price\", V" + r + "), 2, 2), IFNA(INDEX(GOOGLEFINANCE(\"CURRENCY:USDILS\", \"price\", V" + r + "-1), 2, 2), $AA$1)), $AA$1)";
  const formulas = {
    spotUsd: '=IF(D' + r + '="","", IF(C' + r + '="קריפטו", GOOGLEFINANCE("CURRENCY:"&D' + r + '&"USD"), IF(C' + r + '="שוק ההון", GOOGLEFINANCE(D' + r + ', "price"), 0)))',
    costUsd: '=IF(H' + r + '="","", (H' + r + '+IF(J' + r + '="",0,J' + r + ')) / IF($I' + r + '="USD", 1, ' + hRate + '))',
    costIls: '=IF(H' + r + '="","", (H' + r + '+IF(J' + r + '="",0,J' + r + ')) * IF($I' + r + '="ILS", 1, ' + hRate + '))',
    valueUsd: '=IF(F' + r + '="", 0, F' + r + '*IF(AND(K' + r + '="סגור", U' + r + '<>""), U' + r + ', M' + r + '))',
    valueIls: '=IF(P' + r + '="","", P' + r + ' * IF(K' + r + '="סגור", ' + sRate + ', $AA$1))',
    buyUsd: '=IF(G' + r + '="","", IF($I' + r + '="USD", $G' + r + ', $G' + r + ' / ' + hRate + '))',
    buyIls: '=IF(G' + r + '="","", IF($I' + r + '="ILS", $G' + r + ', $G' + r + ' * ' + hRate + '))',
    spotIls: '=IF(M' + r + '="","", M' + r + ' * $AA$1)',
    // שער מכירה (U) is a STORED value (the real sale price) for closed positions,
    // entered at sell time — NOT auto-computed from the live market price.
    yieldAtSale: '=IF(OR(K' + r + '<>"סגור", G' + r + '="", G' + r + '=0, U' + r + '=""), "", (U' + r + '-G' + r + ')/G' + r + ')',
    yieldOrigin: '=IF(OR($H' + r + '="", $H' + r + '=0), 0, (IF($I' + r + '="USD", $P' + r + ', $Q' + r + ') - ($H' + r + '+IF(J' + r + '="",0,J' + r + '))) / ($H' + r + '+IF(J' + r + '="",0,J' + r + ')))',
    yieldIls: '=IF(OR($O' + r + '="", $O' + r + '=0), 0, ($Q' + r + '-$O' + r + ')/$O' + r + ')'
  };

  Object.keys(formulas).forEach(function (k) {
    const ix = map[k];
    if (ix === undefined || ix < 0) return;
    ws.getRange(r, ix + 1).setFormula(formulas[k]);
  });
}

function rowToMapByHeaders_(headers, row) {
  const out = {};
  for (let i = 0; i < headers.length; i++) {
    out[cleanText(headers[i])] = row[i];
  }
  return out;
}

function tradeFromAuditRow_(headers, row) {
  const asMap = rowToMapByHeaders_(headers, row);

  // If user pasted a JSON payload in Audit sheet, prefer it.
  const payloadRaw = asMap.Payload || asMap.payload || "";
  if (cleanText(payloadRaw)) {
    try {
      const parsed = JSON.parse(String(payloadRaw));
      const tradeObj = parsed && parsed.trade ? parsed.trade : parsed;
      return sanitizeTrade_(tradeObj || {});
    } catch (err) {
      // Fall back to direct column mapping.
    }
  }

  return sanitizeTrade_({
    Current_Location: asMap.Current_Location || asMap["מיקום נוכחי"] || "",
    Platform: asMap.Platform || asMap["פלטפורמה"] || "",
    Type: asMap.Type || asMap["סוג נכס"] || "קריפטו",
    Ticker: asMap.Ticker || asMap["טיקר"] || "",
    Purchase_Date: asMap.Purchase_Date || asMap["תאריך רכישה"] || "",
    Quantity: asMap.Quantity || asMap["כמות"] || 0,
    Origin_Buy_Price: asMap.Origin_Buy_Price || asMap["שער קנייה"] || 0,
    Cost_Origin: asMap.Cost_Origin || asMap["עלות כוללת"] || 0,
    Origin_Currency: asMap.Origin_Currency || asMap["מטבע"] || "ILS",
    Commission: asMap.Commission || asMap["עמלה"] || 0,
    Status: asMap.Status || asMap["סטטוס"] || "פתוח",
    Sell_Date: asMap.Sell_Date || asMap["תאריך מכירה"] || "",
    Trade_ID: asMap.Trade_ID || asMap.trade_id || ""
  });
}

function syncAuditRowToPortfolio_(sheet, rowNum) {
  if (!sheet || rowNum < 2) return;
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0].map(cleanText);
  const row = sheet.getRange(rowNum, 1, 1, headers.length).getValues()[0];
  const map = rowToMapByHeaders_(headers, row);
  const action = cleanText(map.Action || map.action || map["פעולה"] || "add").toLowerCase();

  if (["add", "edit", "delete"].indexOf(action) < 0) return;

  const trade = tradeFromAuditRow_(headers, row);
  if (!trade || !trade.Trade_ID) return;

  const result = action === "delete" ? deleteTrade_(trade) : upsertTrade_(action, trade);
  const statusCell = headers.indexOf("Status") >= 0 ? headers.indexOf("Status") + 1 : -1;
  if (statusCell > 0) {
    sheet.getRange(rowNum, statusCell).setValue(result.ok ? "OK" : "ERROR");
  }
  appendAudit_("audit_sheet_" + action, trade.Trade_ID, result.ok ? "OK" : "ERROR", JSON.stringify(result));
}

function findTradeRowById_(ws, ix, tradeId) {
  const last = ws.getLastRow();
  if (last < 2) return -1;
  const targetId = cleanText(tradeId);
  if (!targetId) return -1;

  if (ix.tradeId >= 0) {
    const values = ws.getRange(2, ix.tradeId + 1, last - 1, 1).getValues();
    for (let i = 0; i < values.length; i++) {
      if (cleanText(values[i][0]) === targetId) return i + 2;
    }
  }

  const headerRow = ws.getRange(1, 1, 1, ws.getLastColumn()).getValues()[0].map(cleanText);
  const fallbackCols = ["Trade_ID", "Trade ID", "מזהה עסקה", "trade_id"]
    .map(function (h) { return headerRow.indexOf(h); })
    .filter(function (idx) { return idx >= 0; });

  for (let c = 0; c < fallbackCols.length; c++) {
    const colIx = fallbackCols[c];
    const values = ws.getRange(2, colIx + 1, last - 1, 1).getValues();
    for (let i = 0; i < values.length; i++) {
      if (cleanText(values[i][0]) === targetId) return i + 2;
    }
  }
  return -1;
}

function findTradeRowByKey_(ws, ix, trade) {
  const last = ws.getLastRow();
  if (last < 2) return -1;
  if (ix.platform < 0 || ix.ticker < 0 || ix.purchaseDate < 0 || ix.quantity < 0 || ix.cost < 0) return -1;
  const width = ws.getLastColumn();
  const rows = ws.getRange(2, 1, last - 1, width).getValues();

  const targetPlatform = cleanText(trade.Platform || "");
  const targetTicker = cleanText(trade.Ticker || "").toUpperCase();
  const targetDate = normalizeDateOnly_(trade.Purchase_Date || "");
  const targetQty = parseNum(trade.Quantity || 0);
  const targetCost = parseNum(trade.Cost_Origin || 0);

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    if (targetPlatform && cleanText(row[ix.platform]) !== targetPlatform) continue;
    if (targetTicker && cleanText(row[ix.ticker]).toUpperCase() !== targetTicker) continue;
    if (targetDate && normalizeDateOnly_(row[ix.purchaseDate]) !== targetDate) continue;
    if (!almostEqual_(row[ix.quantity], targetQty, 1e-8)) continue;
    if (!almostEqual_(row[ix.cost], targetCost, 0.01)) continue;
    return i + 2;
  }
  return -1;
}

function upsertTrade_(action, trade) {
  const ws = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(PORTFOLIO_SHEET);
  const ix = getColumnMap_(ws);
  const width = ws.getLastColumn();
  let rowIndex = findTradeRowById_(ws, ix, trade.Trade_ID);
  if (action === "edit" && rowIndex < 0) {
    rowIndex = findTradeRowByKey_(ws, ix, trade);
  }
  if (action === "edit" && rowIndex < 0) {
    rowIndex = findTradeRowLoose_(ws, ix, trade);
  }

  if (action === "add") {
    // Similar purchases are allowed; only resolve Trade_ID collisions.
    if (rowIndex > 0) {
      trade.Trade_ID = ensureUniqueTradeIdForAdd_(ws, ix, trade);
      rowIndex = -1;
    }
  }
  if (action === "edit" && rowIndex < 0) return { ok: false, error: "Trade not found" };

  if (action === "add") {
    const target = ws.getLastRow() + 1;
    const row = new Array(width).fill("");
    const isClosed = cleanText(trade.Status) === "סגור";
    const sellDateVal = cleanText(trade.Sell_Date);
    if (ix.location >= 0) row[ix.location] = trade.Current_Location;
    row[ix.platform] = trade.Platform;
    row[ix.type] = trade.Type;
    row[ix.ticker] = trade.Ticker;
    row[ix.purchaseDate] = toSheetDateOrText_(trade.Purchase_Date);
    row[ix.quantity] = trade.Quantity;
    row[ix.buyPrice] = trade.Origin_Buy_Price;
    row[ix.cost] = trade.Cost_Origin;
    row[ix.currency] = trade.Origin_Currency;
    row[ix.fee] = trade.Commission;
    row[ix.status] = trade.Status;
    if (ix.sellDate >= 0) row[ix.sellDate] = isClosed ? toSheetDateOrText_(sellDateVal || new Date()) : "עדיין פתוח";
    row[ix.tradeId] = trade.Trade_ID;
    ws.getRange(target, 1, 1, width).setValues([row]);
    if (target >= 2) {
      const templateRow = findFormulaTemplateRow_(ws, target);
      if (templateRow >= 2) {
        copyCalculatedFormulaCells_(ws, templateRow, target);
      } else {
        applyCalculatedFormulasForRow_(ws, target);
      }
      ws.getRange(target - 1, 1, 1, width).copyTo(ws.getRange(target, 1, 1, width), SpreadsheetApp.CopyPasteType.PASTE_FORMAT, false);
    }
    return { ok: true, message: "Added", row: target, trade_id: trade.Trade_ID };
  }

  const rowRange = ws.getRange(rowIndex, 1, 1, width);
  const existing = rowRange.getValues()[0];
  const existingFormulas = rowRange.getFormulas()[0];

  if (ix.location >= 0) existing[ix.location] = trade.Current_Location;
  existing[ix.platform] = trade.Platform;
  existing[ix.type] = trade.Type;
  existing[ix.ticker] = trade.Ticker;
  existing[ix.purchaseDate] = toSheetDateOrText_(trade.Purchase_Date);
  existing[ix.quantity] = trade.Quantity;
  existing[ix.buyPrice] = trade.Origin_Buy_Price;
  existing[ix.cost] = trade.Cost_Origin;
  existing[ix.currency] = trade.Origin_Currency;
  existing[ix.fee] = trade.Commission;
  existing[ix.status] = trade.Status;
  if (ix.sellDate >= 0) {
    const nowClosed = cleanText(trade.Status) === "סגור";
    const incomingSellDate = cleanText(trade.Sell_Date);
    const existingSellDate = cleanText(existing[ix.sellDate]);
    if (nowClosed) {
      existing[ix.sellDate] = toSheetDateOrText_(incomingSellDate || (existingSellDate === "עדיין פתוח" ? "" : existingSellDate) || new Date());
    } else {
      existing[ix.sellDate] = "עדיין פתוח";
    }
  }
  existing[ix.tradeId] = trade.Trade_ID;

  // Preserve formula-based calculated cells during edit so yields stay alive.
  const protectedIx = {};
  [ix.location, ix.platform, ix.type, ix.ticker, ix.purchaseDate, ix.quantity, ix.buyPrice, ix.cost, ix.currency, ix.fee, ix.status, ix.sellDate, ix.tradeId]
    .forEach(function (v) { if (v >= 0) protectedIx[v] = true; });
  for (let c = 0; c < width; c++) {
    if (protectedIx[c]) continue;
    if (existingFormulas[c]) {
      existing[c] = existingFormulas[c];
    }
  }

  rowRange.setValues([existing]);

  // If this row lost formulas previously, restore them from a nearby template row.
  const templateRowForEdit = findFormulaTemplateRow_(ws, rowIndex);
  if (templateRowForEdit >= 2) {
    copyCalculatedFormulaCells_(ws, templateRowForEdit, rowIndex);
  } else {
    applyCalculatedFormulasForRow_(ws, rowIndex);
  }
  return { ok: true, message: "Updated", row: rowIndex, trade_id: trade.Trade_ID };
}

function deleteTrade_(tradeOrId) {
  const ws = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(PORTFOLIO_SHEET);
  const ix = getColumnMap_(ws);
  const asTrade = (typeof tradeOrId === "object" && tradeOrId !== null) ? tradeOrId : { Trade_ID: tradeOrId };
  const tradeId = cleanText(asTrade.Trade_ID || "");
  let rowIndex = findTradeRowById_(ws, ix, tradeId);
  if (rowIndex < 0) {
    rowIndex = findTradeRowByKey_(ws, ix, asTrade);
  }
  if (rowIndex < 0) return { ok: false, error: "Trade not found" };
  ws.deleteRow(rowIndex);
  return { ok: true, message: "Deleted", row: rowIndex };
}

function validateToken_(token) {
  const required = PropertiesService.getScriptProperties().getProperty("API_TOKEN");
  // Fail CLOSED. Previously this only checked the token when API_TOKEN was set,
  // so an unset/cleared property silently authorized every request — granting
  // anyone with the public Web-App URL read/write access to the sheet.
  if (!required) throw new Error("Unauthorized: API_TOKEN script property is not configured");
  if (token !== required) throw new Error("Unauthorized");
}

// ─────────────────────────────────────────────────────────────────────
// MONTHLY EMAIL REPORT — zero-credential summary via MailApp.
// One-time setup: Apps Script editor → Triggers (⏰) → Add Trigger →
//   function: sendMonthlyReport, event source: Time-driven,
//   type: Month timer, day: 1st, hour: e.g. 8-9am.
// Sends a Hebrew summary of the portfolio to the sheet owner's email.
// ─────────────────────────────────────────────────────────────────────
function sendMonthlyReport() {
  const data = readSnapshotRows_();
  const headers = data.headers;
  const rows = data.rows;
  const ixOf = function (names) {
    for (let i = 0; i < headers.length; i++) {
      if (names.indexOf(cleanText(headers[i])) >= 0) return i;
    }
    return -1;
  };
  const ixTicker = ixOf(["טיקר", "Ticker"]);
  const ixStatus = ixOf(["סטטוס", "Status"]);
  const ixCostIls = ixOf(["עלות ILS"]);
  const ixValueIls = ixOf(["שווי ILS"]);
  const closedSet = ["סגור", "closed", "close", "sold", "נמכר"];

  let totalCost = 0, totalValue = 0, nOpen = 0, nClosed = 0;
  const byTicker = {};
  rows.forEach(function (r) {
    const status = cleanText(r[ixStatus] || "").toLowerCase();
    if (closedSet.indexOf(status) >= 0) { nClosed++; return; }
    nOpen++;
    const cost = parseNum(r[ixCostIls]);
    const value = parseNum(r[ixValueIls]);
    totalCost += cost;
    totalValue += value;
    const t = cleanText(r[ixTicker] || "?").toUpperCase();
    byTicker[t] = (byTicker[t] || 0) + value;
  });

  const profit = totalValue - totalCost;
  const retPct = totalCost > 0 ? (profit / totalCost) * 100 : 0;
  const fmt = function (n) { return "₪" + Math.round(n).toLocaleString("en-US"); };
  const top = Object.keys(byTicker)
    .map(function (t) { return [t, byTicker[t]]; })
    .sort(function (a, b) { return b[1] - a[1]; })
    .slice(0, 5)
    .map(function (e) {
      const w = totalValue > 0 ? ((e[1] / totalValue) * 100).toFixed(1) : "0";
      return "  • " + e[0] + ": " + fmt(e[1]) + " (" + w + "%)";
    })
    .join("\n");

  const now = new Date();
  const subject = "📊 Portfolio OS — סיכום חודשי " + Utilities.formatDate(now, "Asia/Jerusalem", "MM/yyyy");
  const body =
    "סיכום התיק נכון ל-" + Utilities.formatDate(now, "Asia/Jerusalem", "dd/MM/yyyy HH:mm") + "\n\n" +
    "שווי כולל:        " + fmt(totalValue) + "\n" +
    "עלות כוללת:       " + fmt(totalCost) + "\n" +
    "רווח/הפסד פתוח:   " + fmt(profit) + " (" + retPct.toFixed(2) + "%)\n" +
    "פוזיציות:          " + nOpen + " פתוחות, " + nClosed + " סגורות\n\n" +
    "5 ההחזקות הגדולות:\n" + top + "\n\n" +
    "— נשלח אוטומטית ע\"י Portfolio OS (Apps Script monthly trigger)";

  MailApp.sendEmail(Session.getEffectiveUser().getEmail(), subject, body);
  appendAudit_("monthly_report", "", "OK", subject);
  return subject;
}

function jsonResponse_(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}

// --------------------------
// Existing dashboard logic (preserved)
// --------------------------

function TransferToColdWallet() {
  const ui = SpreadsheetApp.getUi();
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("תמונת מצב");
  const rowRes = ui.prompt("העברה לארנק קר 🛡️", "הכנס את מספר השורה בגוגל שיטס של הנכס שאתה מעביר ללדג'ר (לדוגמה: 17):", ui.ButtonSet.OK_CANCEL);
  if (rowRes.getSelectedButton() !== ui.Button.OK) return;
  const rowNum = parseInt(rowRes.getResponseText(), 10);
  if (isNaN(rowNum) || rowNum < 2 || rowNum > sheet.getLastRow()) return ui.alert("⚠️ מספר שורה לא תקין.");
  const ticker = sheet.getRange(rowNum, 4).getValue();
  const currentQty = parseNum(sheet.getRange(rowNum, 6).getValue());
  const currentFee = parseNum(sheet.getRange(rowNum, 10).getValue());
  const feeRes = ui.prompt("עמלת משיכה 💸", "הנכס: " + ticker + "\nאם שילמת עמלת משיכה נוספת מתוך היתרה בשקלים/דולרים, הכנס את הסכום כאן.\n(אם העמלה נגבתה מהמטבע עצמו, השאר 0):", ui.ButtonSet.OK_CANCEL);
  if (feeRes.getSelectedButton() !== ui.Button.OK) return;
  const additionalFee = parseNum(feeRes.getResponseText());
  const qtyRes = ui.prompt("כמות סופית נטו 💰", "הכמות המקורית: " + currentQty + "\nאם הפלטפורמה גזרה עמלת משיכה מהמטבע, הכנס כאן את הכמות נטו שהגיעה ללדג'ר:", ui.ButtonSet.OK_CANCEL);
  if (qtyRes.getSelectedButton() !== ui.Button.OK) return;
  let finalQty = parseFloat(qtyRes.getResponseText());
  if (isNaN(finalQty) || qtyRes.getResponseText() === "") finalQty = currentQty;
  sheet.getRange(rowNum, 1).setValue("ארנק קר (Ledger)");
  sheet.getRange(rowNum, 6).setValue(finalQty);
  sheet.getRange(rowNum, 10).setValue(currentFee + additionalFee);
  SpreadsheetApp.flush();
  ui.alert("✅ ההעברה הושלמה בהצלחה! הנכס מוגדר כעת בארנק הקר מבלי ליצור כפילויות.");
}

function SystemDoctor() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("תמונת מצב");
  if (!sheet) return;
  const data = sheet.getDataRange().getValues();
  let rowsDeleted = 0;
  let rowsFixed = 0;
  for (let i = data.length - 1; i >= 1; i--) {
    const r = i + 1;
    const ticker = data[i][3];
    const date = data[i][4];
    const qty = parseNum(data[i][5]);
    const price = parseNum(data[i][6]);
    const costH = parseNum(data[i][7]);
    const statusK = data[i][10];
    let isDup = false;
    for (let j = 1; j < i; j++) {
      if (data[j][3] === ticker && data[j][4] === date && almostEqual_(data[j][5], qty, 1e-7) && almostEqual_(data[j][6], price, 1e-7)) {
        isDup = true;
        break;
      }
    }
    if (isDup) {
      // Keep the row so the ledger remains complete; only count suspected duplicates.
      rowsDeleted++;
      continue;
    }
    let changed = false;
    if (ticker === "VOO" && statusK !== "סגור") {
      sheet.getRange(r, 11).setValue("סגור");
      changed = true;
    }
    const calcCost = qty * price;
    if (calcCost > 0 && costH - calcCost > 0.5) {
      sheet.getRange(r, 10).setValue(costH - calcCost);
      sheet.getRange(r, 8).setValue(calcCost);
      changed = true;
    }
    if (changed) rowsFixed++;
  }
  const stats = fixSnapshotSheet_();
  SpreadsheetApp.getUi().alert("✅ טיפול מערכת הסתיים.\nנמחקו " + rowsDeleted + " כפילויות, תוקנו " + rowsFixed + " שורות, נוספו " + stats.added + " רשומות מהצלבת DATA.");
}

function sortSheetByDate() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("תמונת מצב");
  if (sheet.getLastRow() > 1) {
    sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).sort({ column: 5, ascending: true });
  }
}

function InstallSystem() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const mainSheet = ss.getSheetByName("תמונת מצב");
  if (!mainSheet) return;
  if (mainSheet.getRange("J1").getValue() !== "עמלה") {
    mainSheet.insertColumnsAfter(9, 2);
    mainSheet.getRange("J1:K1").setValues([["עמלה", "סטטוס"]]).setBackground("#4A5568").setFontColor("white").setFontWeight("bold");
    if (mainSheet.getLastRow() > 1) {
      mainSheet.getRange(2, 10, mainSheet.getLastRow() - 1, 1).setValue(0);
      mainSheet.getRange(2, 11, mainSheet.getLastRow() - 1, 1).setValue("פתוח");
    }
  }
  // Keep Sell_Date values in V intact; only clear formula regions around it.
  mainSheet.getRange("L1").setValue("סטטוס מכירה");
  mainSheet.getRange("M:T").clear();  // preserve U (שער מכירה) — it's stored sale-price data now
  mainSheet.getRange("W:AE").clear();
  const metricHeaders = [["שער נוכחי USD", "עלות USD", "עלות ILS", "שווי USD", "שווי ILS", "שער קנייה USD", "שער קנייה ILS", "שער נוכחי ILS", "שער מכירה"]];
  mainSheet.getRange("M1:U1").setValues(metricHeaders).setBackground("#2C5282").setFontColor("white").setFontWeight("bold").setHorizontalAlignment("center");
  mainSheet.getRange("V1").setValue("תאריך מכירה").setBackground("#4A5568").setFontColor("white").setFontWeight("bold").setHorizontalAlignment("center");
  const yieldHeaders = [["תשואה במכירה", "תשואה מקור", "תשואה שקלית"]];
  mainSheet.getRange("W1:Y1").setValues(yieldHeaders).setBackground("#2C5282").setFontColor("white").setFontWeight("bold").setHorizontalAlignment("center");
  mainSheet.getRange("Z1").setValue("שער USD/ILS עדכני:").setFontWeight("bold");
  mainSheet.getRange("AA1").setFormula('=GOOGLEFINANCE("CURRENCY:USDILS")').setBackground("#FFF2CC").setFontWeight("bold").setHorizontalAlignment("center");
  mainSheet.getRange("AA1").setNumberFormat("#,##0.000000");
  const numRows = Math.max(mainSheet.getLastRow(), 2) - 1;
  const pricingFormulas = [];
  const yieldFormulas = [];
  for (let i = 0; i < numRows; i++) {
    const r = i + 2;
    const hRate = "IFERROR(IFNA(INDEX(GOOGLEFINANCE(\"CURRENCY:USDILS\", \"price\", E" + r + "), 2, 2), IFNA(INDEX(GOOGLEFINANCE(\"CURRENCY:USDILS\", \"price\", E" + r + "-1), 2, 2), $AA$1)), $AA$1)";
    const sRate = "IFERROR(IFNA(INDEX(GOOGLEFINANCE(\"CURRENCY:USDILS\", \"price\", V" + r + "), 2, 2), IFNA(INDEX(GOOGLEFINANCE(\"CURRENCY:USDILS\", \"price\", V" + r + "-1), 2, 2), $AA$1)), $AA$1)";
    pricingFormulas.push([
      '=IF(D' + r + '="","", IF(C' + r + '="קריפטו", GOOGLEFINANCE("CURRENCY:"&D' + r + '&"USD"), IF(C' + r + '="שוק ההון", GOOGLEFINANCE(D' + r + ', "price"), 0)))',
      '=IF(H' + r + '="","", (H' + r + '+IF(J' + r + '="",0,J' + r + ')) / IF($I' + r + '="USD", 1, ' + hRate + '))',
      '=IF(H' + r + '="","", (H' + r + '+IF(J' + r + '="",0,J' + r + ')) * IF($I' + r + '="ILS", 1, ' + hRate + '))',
      '=IF(F' + r + '="", 0, F' + r + '*IF(AND(K' + r + '="סגור", U' + r + '<>""), U' + r + ', M' + r + '))',
      '=IF(P' + r + '="","", P' + r + ' * IF(K' + r + '="סגור", ' + sRate + ', $AA$1))',
      '=IF(G' + r + '="","", IF($I' + r + '="USD", $G' + r + ', $G' + r + ' / ' + hRate + '))',
      '=IF(G' + r + '="","", IF($I' + r + '="ILS", $G' + r + ', $G' + r + ' * ' + hRate + '))',
      '=IF(M' + r + '="","", M' + r + ' * $AA$1)'
    ]);
    yieldFormulas.push([
      '=IF(OR(K' + r + '<>"סגור", G' + r + '="", G' + r + '=0, U' + r + '=""), "", (U' + r + '-G' + r + ')/G' + r + ')',
      '=IF(OR($H' + r + '="", $H' + r + '=0), 0, (IF($I' + r + '="USD", $P' + r + ', $Q' + r + ') - ($H' + r + '+IF(J' + r + '="",0,J' + r + '))) / ($H' + r + '+IF(J' + r + '="",0,J' + r + ')))',
      '=IF(OR($O' + r + '="", $O' + r + '=0), 0, ($Q' + r + '-$O' + r + ')/$O' + r + ')'
    ]);
  }
  if (numRows > 0) {
    mainSheet.getRange(2, 13, numRows, 8).setFormulas(pricingFormulas); // M:T (U=שער מכירה is stored data)
    mainSheet.getRange(2, 23, numRows, 3).setFormulas(yieldFormulas);   // W:Y
  }

  // Normalize numeric formats to avoid mixed text/currency exports in CSV.
  if (numRows > 0) {
    mainSheet.getRange(2, 6, numRows, 1).setNumberFormat("#,##0.00000000"); // כמות
    mainSheet.getRange(2, 7, numRows, 1).setNumberFormat("#,##0.00");       // שער קנייה
    mainSheet.getRange(2, 8, numRows, 1).setNumberFormat("#,##0.00");       // עלות כוללת
    mainSheet.getRange(2, 10, numRows, 1).setNumberFormat("#,##0.00");      // עמלה

    mainSheet.getRange(2, 22, numRows, 1).setNumberFormat("dd/MM/yyyy");     // תאריך מכירה (V)
    mainSheet.getRange(2, 13, numRows, 9).setNumberFormat("#,##0.00");       // M:U numeric values

    // Ensure yield columns are always formatted as percentages by header name.
    const headerMap = buildSnapshotHeaderIndexMap_(mainSheet.getRange(1, 1, 1, mainSheet.getLastColumn()).getValues()[0]);
    const yieldCols = [headerMap.yieldAtSale, headerMap.yieldOrigin, headerMap.yieldIls].filter(function (ix) { return ix >= 0; });
    yieldCols.forEach(function (ix) {
      mainSheet.getRange(2, ix + 1, numRows, 1).setNumberFormat("0.00%");
    });

    // Open rows should explicitly show "עדיין פתוח" in Sell_Date.
    if (headerMap.status >= 0 && headerMap.sellDate >= 0) {
      const statusVals = mainSheet.getRange(2, headerMap.status + 1, numRows, 1).getValues();
      const sellVals = mainSheet.getRange(2, headerMap.sellDate + 1, numRows, 1).getValues();
      const outSell = [];
      for (let i = 0; i < numRows; i++) {
        const statusVal = cleanText(statusVals[i][0]);
        const existingVal = sellVals[i][0];
        const existingText = cleanText(existingVal);
        if (statusVal === "סגור") {
          outSell.push([existingText && existingText !== "עדיין פתוח" ? existingVal : new Date()]);
        } else {
          outSell.push(["עדיין פתוח"]);
        }
      }
      mainSheet.getRange(2, headerMap.sellDate + 1, numRows, 1).setValues(outSell);
    }
  }

  sortSheetByDate();
  formatMainSheet();
}

function formatMainSheet() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("תמונת מצב");
  if (!sheet) return;
  const lastRow = Math.max(sheet.getLastRow(), 2);
  const lastCol = Math.max(sheet.getLastColumn(), 1);
  sheet.setRightToLeft(true);
  sheet.setFrozenRows(1);
  const fullRange = sheet.getRange(1, 1, lastRow, lastCol);
  fullRange.setHorizontalAlignment("center").setVerticalAlignment("middle");
  sheet.getBandings().forEach(function (b) { b.remove(); });
  fullRange.applyRowBanding(SpreadsheetApp.BandingTheme.INDIGO, true, false);
  // Uniform header styling (aligns with the platform sheets' look)
  sheet.getRange(1, 1, 1, Math.min(lastCol, 25)).setBackground("#2C5282").setFontColor("white").setFontWeight("bold").setHorizontalAlignment("center").setVerticalAlignment("middle").setWrap(true);
  sheet.setRowHeight(1, 40);
  // Clean + hide the unused 'סטטוס מכירה' column (L/12): had leftover junk dates.
  // (Physically deleting it would shift all metric columns and break the formulas.)
  if (lastRow > 1) sheet.getRange(2, 12, lastRow - 1, 1).clearContent();
  sheet.hideColumns(12);
  sheet.autoResizeColumns(1, 11);

  // Color the return columns (W=תשואה במכירה, X=תשואה מקור, Y=תשואה שקלית) green/red.
  if (lastRow > 1) {
    const yRanges = [sheet.getRange(2, 23, lastRow - 1, 3)]; // W:Y
    const pos = SpreadsheetApp.newConditionalFormatRule().whenNumberGreaterThan(0).setFontColor("#1E7E34").setBold(true).setRanges(yRanges).build();
    const neg = SpreadsheetApp.newConditionalFormatRule().whenNumberLessThan(0).setFontColor("#C53030").setBold(true).setRanges(yRanges).build();
    sheet.setConditionalFormatRules([pos, neg]);
  }
}

function styleDepositsSheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const ws = ss.getSheetByName("הפקדות ידניות");
  if (!ws) return;
  ws.setRightToLeft(true);
  // Big title row at top (idempotent — readManualDeposits_/writeManualDeposits_ are title-aware).
  if (cleanText(ws.getRange(1, 1).getValue()).indexOf("הפקדות ידניות") < 0) {
    ws.insertRowBefore(1);
  }
  ws.getRange(1, 1, 1, 4).merge();
  ws.getRange(1, 1).setValue("💰 הפקדות ידניות — לפי פלטפורמה")
    .setBackground("#1A365D").setFontColor("white").setFontWeight("bold").setFontSize(16)
    .setHorizontalAlignment("center").setVerticalAlignment("middle");
  ws.setRowHeight(1, 46);
  // readManualDeposits_ reads by POSITION (r[1],r[2],r[3]) so Hebrew labels are safe.
  ws.getRange(2, 1, 1, 4).setValues([["תאריך", "מצב", "פלטפורמה", "סכום הפקדה (₪)"]])
    .setBackground("#2C5282").setFontColor("white").setFontWeight("bold").setHorizontalAlignment("center").setVerticalAlignment("middle");
  ws.setRowHeight(2, 30);
  ws.setFrozenRows(2);
  const lr = Math.max(ws.getLastRow(), 2);
  if (lr > 2) {
    ws.getRange(3, 4, lr - 2, 1).setNumberFormat("#,##0");
    ws.getRange(2, 1, lr - 1, 4).setHorizontalAlignment("center").setVerticalAlignment("middle");
    ws.getBandings().forEach(function (b) { b.remove(); });
    ws.getRange(2, 1, lr - 1, 4).applyRowBanding(SpreadsheetApp.BandingTheme.BLUE, true, false);
  }
  ws.autoResizeColumns(1, 4);
}

function RefreshAllData() {
  distributeToPlatformSheets();
  buildDashboard();
  try { styleDepositsSheet_(); } catch (e) {}
  try { buildDashboardV2(); } catch (e) {}
}

function buildDashboardV2() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const main = ss.getSheetByName("תמונת מצב");
  if (!main) return;
  let sh = ss.getSheetByName("דשבורד");
  if (!sh) sh = ss.insertSheet("דשבורד", 0);
  sh.clear();
  try { sh.getCharts().forEach(function (c) { sh.removeChart(c); }); } catch (e) {}
  try { sh.getBandings().forEach(function (b) { b.remove(); }); } catch (e) {}
  try { sh.getRange(1, 1, sh.getMaxRows(), sh.getMaxColumns()).breakApart(); } catch (e) {}
  sh.setRightToLeft(true);
  try { sh.setHiddenGridlines(true); } catch (e) {}

  // ===================== gather data =====================
  const data = main.getDataRange().getValues();
  const map = buildSnapshotHeaderIndexMap_(data[0] || []);
  const all = data.slice(1).filter(function (r) { return cleanText(rowVal_(r, map, "ticker")) !== ""; });
  let rate = 0; try { rate = parseNum(main.getRange("AA1").getValue()); } catch (e) {}
  if (!rate) rate = 1;
  const isClosed = function (r) { return cleanText(rowVal_(r, map, "status")) === "סגור"; };
  const open = all.filter(function (r) { return !isClosed(r); });
  const closed = all.filter(isClosed);

  const cryptoTk = { "IBIT": 1, "ETHA": 1, "BSOL": 1, "MSTR": 1 };
  let totCost = 0, totVal = 0, totFeeIls = 0, usdVal = 0, firstDate = null;
  const byTicker = {}, byPlat = {};
  open.forEach(function (r) {
    const t = cleanText(rowVal_(r, map, "ticker")), p = cleanText(rowVal_(r, map, "platform"));
    const ci = parseNum(rowVal_(r, map, "costIls")), vi = parseNum(rowVal_(r, map, "valueIls"));
    const tp = cleanText(rowVal_(r, map, "type")), cur = cleanText(rowVal_(r, map, "currency"));
    const qty = parseNum(rowVal_(r, map, "quantity")), fee = parseNum(rowVal_(r, map, "fee"));
    totCost += ci; totVal += vi;
    totFeeIls += (cur === "USD" ? fee * rate : fee);
    if (cur === "USD") usdVal += vi;
    if (!byTicker[t]) byTicker[t] = { qty: 0, cost: 0, val: 0, type: tp, cur: cur };
    byTicker[t].qty += qty; byTicker[t].cost += ci; byTicker[t].val += vi;
    if (!byPlat[p]) byPlat[p] = { cost: 0, val: 0, dep: 0 };
    byPlat[p].cost += ci; byPlat[p].val += vi;
    const d = normalizeDateOnly_(rowVal_(r, map, "purchaseDate"));
    if (d && (!firstDate || d < firstDate)) firstDate = d;
  });
  // realized P&L from closed positions
  let realized = 0;
  const closedAgg = {};
  closed.forEach(function (r) {
    const t = cleanText(rowVal_(r, map, "ticker"));
    const ci = parseNum(rowVal_(r, map, "costIls")), vi = parseNum(rowVal_(r, map, "valueIls"));
    const cur = cleanText(rowVal_(r, map, "currency")), fee = parseNum(rowVal_(r, map, "fee"));
    realized += (vi - ci);
    totFeeIls += (cur === "USD" ? fee * rate : fee);
    if (!closedAgg[t]) closedAgg[t] = { cost: 0, val: 0 };
    closedAgg[t].cost += ci; closedAgg[t].val += vi;
  });
  // deposits (total + per platform)
  let deposits = 0; const depByPlat = {};
  try { (readManualDeposits_("live").rows || []).forEach(function (d) { deposits += parseNum(d.Manual_Deposit_ILS); depByPlat[cleanText(d.Platform)] = parseNum(d.Manual_Deposit_ILS); }); } catch (e) {}
  Object.keys(depByPlat).forEach(function (p) { if (byPlat[p]) byPlat[p].dep = depByPlat[p]; });

  const cash = deposits - totCost, totalAccount = totVal + cash, netPL = totVal - totCost, ret = totCost ? netPL / totCost : 0;
  let cryptoVal = 0, equityVal = 0;
  Object.keys(byTicker).forEach(function (t) { if (byTicker[t].type === "קריפטו" || cryptoTk[t]) cryptoVal += byTicker[t].val; else equityVal += byTicker[t].val; });
  const tks = Object.keys(byTicker).sort(function (a, b) { return byTicker[b].val - byTicker[a].val; });
  let top3 = 0; tks.slice(0, 3).forEach(function (t) { top3 += byTicker[t].val; });
  const conc = totVal ? top3 / totVal : 0;
  let days = 0; if (firstDate) { try { days = Math.round((new Date() - new Date(firstDate + "T00:00:00")) / 86400000); } catch (e) {} }
  // ranked returns for winners/losers
  const ranked = tks.filter(function (t) { return byTicker[t].cost > 0; })
    .map(function (t) { return { t: t, y: (byTicker[t].val - byTicker[t].cost) / byTicker[t].cost }; })
    .sort(function (a, b) { return b.y - a.y; });

  const money = function (v) { return "₪" + Math.round(v).toLocaleString("en-US"); };
  const pct = function (v) { return (v * 100).toFixed(2) + "%"; };
  const plBg = function (v) { return v >= 0 ? "#F0FFF4" : "#FFF5F5"; };
  const plFc = function (v) { return v >= 0 ? "#1E7E34" : "#C53030"; };
  const colorPL = function (rng, v) { rng.setFontColor(v >= 0 ? "#1E7E34" : "#C53030").setFontWeight("bold"); };

  // ===================== layout =====================
  for (let c = 1; c <= 14; c++) sh.setColumnWidth(c, c === 1 ? 24 : 112);

  // ---- hero ----
  sh.getRange("B2:M2").merge().setValue("📊  דשבורד — תיק ההשקעות של אוריאל")
    .setFontSize(24).setFontWeight("bold").setFontColor("white").setBackground("#0F2A4A").setHorizontalAlignment("center").setVerticalAlignment("middle");
  sh.setRowHeight(2, 54);
  sh.getRange("B3:M3").merge().setValue("עודכן " + Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "dd/MM/yyyy HH:mm") + "    •    שער דולר/שקל " + rate.toFixed(3) + "    •    " + tks.length + " אחזקות פתוחות · " + Object.keys(byPlat).length + " פלטפורמות · " + days + " ימי השקעה")
    .setFontSize(10).setFontColor("#A0AEC0").setBackground("#0F2A4A").setHorizontalAlignment("center").setVerticalAlignment("middle");
  sh.setRowHeight(3, 22);

  // ---- KPI cards (two rows of 6) ----
  const kpi = function (row, col, icon, label, value, bg, fc) {
    sh.getRange(row, col, 1, 2).merge().setValue(icon + "  " + label).setBackground("#2B6CB0").setFontColor("white").setFontWeight("bold").setFontSize(9).setHorizontalAlignment("center").setVerticalAlignment("middle");
    sh.getRange(row + 1, col, 1, 2).merge().setValue(value).setBackground(bg).setFontColor(fc).setFontWeight("bold").setFontSize(15).setHorizontalAlignment("center").setVerticalAlignment("middle");
    sh.getRange(row, col, 2, 2).setBorder(true, true, true, true, false, false, "#2B6CB0", SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
  };
  const blue = "#EBF4FF", bf = "#1A365D", teal = "#E6FFFA", tf = "#234E52", amber = "#FFFAF0", af = "#9C4221", purp = "#FAF5FF", pf = "#553C9A";
  kpi(5, 2, "💰", "הפקדות בפועל", money(deposits), blue, bf);
  kpi(5, 4, "📊", "שווי תיק", money(totVal), blue, bf);
  kpi(5, 6, "💵", "מזומן פנוי", money(cash), blue, bf);
  kpi(5, 8, "🏦", "שווי חשבון כולל", money(totalAccount), teal, tf);
  kpi(5, 10, "📈", "רווח/הפסד לא ממומש", money(netPL), plBg(netPL), plFc(netPL));
  kpi(5, 12, "🎯", "תשואה כוללת", pct(ret), plBg(ret), plFc(ret));
  sh.setRowHeight(5, 22); sh.setRowHeight(6, 40);
  kpi(8, 2, "✅", "רווח/הפסד ממומש", money(realized), plBg(realized), plFc(realized));
  kpi(8, 4, "💸", "עמלות ששולמו", money(totFeeIls), amber, af);
  kpi(8, 6, "🔢", "עסקאות פתוחות", String(open.length), blue, bf);
  kpi(8, 8, "📅", "ימי השקעה", String(days), blue, bf);
  kpi(8, 10, "⚖️", "ריכוז 3 הגדולות", pct(conc), purp, pf);
  kpi(8, 12, "🌎", "חשיפת מט\"ח (USD)", pct(totVal ? usdVal / totVal : 0), amber, af);
  sh.setRowHeight(8, 22); sh.setRowHeight(9, 40);

  const sec = function (row, col, span, title) {
    sh.getRange(row, col, 1, span).merge().setValue(title).setBackground("#2C5282").setFontColor("white").setFontWeight("bold").setFontSize(12).setHorizontalAlignment("center").setVerticalAlignment("middle");
    sh.setRowHeight(row, 26);
  };

  // ---- per-platform table (B..H = 7 cols) ----
  let r = 11;
  sec(r, 2, 7, "🏦 פילוח לפי פלטפורמה"); r++;
  sh.getRange(r, 2, 1, 7).setValues([["פלטפורמה", "הפקדה (₪)", "עלות (₪)", "שווי (₪)", "רווח/הפסד", "תשואה", "משקל"]]).setBackground("#BEE3F8").setFontWeight("bold").setHorizontalAlignment("center"); r++;
  const platTop = r - 1;
  Object.keys(byPlat).sort(function (a, b) { return byPlat[b].val - byPlat[a].val; }).forEach(function (p, i) {
    const o = byPlat[p], pl = o.val - o.cost, y = o.cost ? pl / o.cost : 0, w = totVal ? o.val / totVal : 0;
    sh.getRange(r, 2, 1, 7).setValues([[p, Math.round(o.dep), Math.round(o.cost), Math.round(o.val), Math.round(pl), y, w]]).setHorizontalAlignment("center");
    sh.getRange(r, 3, 1, 4).setNumberFormat("#,##0"); sh.getRange(r, 7).setNumberFormat("0.00%"); sh.getRange(r, 8).setNumberFormat("0.0%");
    colorPL(sh.getRange(r, 6), pl); colorPL(sh.getRange(r, 7), y);
    if (i % 2 === 1) sh.getRange(r, 2, 1, 7).setBackground("#F7FAFC");
    r++;
  });
  sh.getRange(r, 2, 1, 7).setValues([["סה\"כ", Math.round(deposits), Math.round(totCost), Math.round(totVal), Math.round(netPL), ret, 1]]).setFontWeight("bold").setBackground("#2C5282").setFontColor("white").setHorizontalAlignment("center");
  sh.getRange(r, 3, 1, 4).setNumberFormat("#,##0"); sh.getRange(r, 7).setNumberFormat("0.00%"); sh.getRange(r, 8).setNumberFormat("0.0%");
  r++;
  sh.getRange(platTop, 2, r - platTop, 7).setBorder(true, true, true, true, true, true, "#CBD5E0", SpreadsheetApp.BorderStyle.SOLID);

  // ---- full holdings table (B..K = 10 cols, with weight bar) ----
  r += 1;
  sec(r, 2, 10, "🏆 כל האחזקות הפתוחות"); r++;
  sh.getRange(r, 2, 1, 10).setValues([["נכס", "כמות", "עלות ₪/יח'", "מחיר ₪/יח'", "עלות (₪)", "שווי (₪)", "רווח/הפסד", "תשואה", "משקל", "פילוח"]]).setBackground("#BEE3F8").setFontWeight("bold").setHorizontalAlignment("center"); r++;
  const holdTop = r - 1;
  tks.forEach(function (t, i) {
    const o = byTicker[t], pl = o.val - o.cost, y = o.cost ? pl / o.cost : 0, w = totVal ? o.val / totVal : 0;
    const avg = o.qty ? o.cost / o.qty : 0, cur = o.qty ? o.val / o.qty : 0;
    const bar = "█".repeat(Math.max(0, Math.min(22, Math.round(w * 70))));
    sh.getRange(r, 2, 1, 10).setValues([[t, o.qty, Math.round(avg), Math.round(cur), Math.round(o.cost), Math.round(o.val), Math.round(pl), y, w, bar]]).setHorizontalAlignment("center");
    sh.getRange(r, 3).setNumberFormat("#,##0.####"); sh.getRange(r, 4, 1, 5).setNumberFormat("#,##0"); sh.getRange(r, 9).setNumberFormat("0.00%"); sh.getRange(r, 10).setNumberFormat("0.0%");
    sh.getRange(r, 11).setHorizontalAlignment("right").setFontColor("#2B6CB0").setFontFamily("Consolas");
    colorPL(sh.getRange(r, 8), pl); colorPL(sh.getRange(r, 9), y);
    sh.getRange(r, 2).setFontWeight("bold");
    if (i % 2 === 1) sh.getRange(r, 2, 1, 10).setBackground("#F7FAFC");
    r++;
  });
  sh.getRange(holdTop, 2, r - holdTop, 10).setBorder(true, true, true, true, true, true, "#CBD5E0", SpreadsheetApp.BorderStyle.SOLID);

  // ---- two side-by-side panels: realized (B..F) + allocation/movers (H..M) ----
  const panelTop = r + 1;
  // LEFT: closed / realized positions
  let lr = panelTop;
  sec(lr, 2, 5, "✅ פוזיציות שנסגרו (רווח ממומש)"); lr++;
  sh.getRange(lr, 2, 1, 5).setValues([["נכס", "עלות (₪)", "תמורה (₪)", "רווח/הפסד", "תשואה"]]).setBackground("#C6F6D5").setFontWeight("bold").setHorizontalAlignment("center"); lr++;
  const closedKeys = Object.keys(closedAgg);
  if (closedKeys.length === 0) {
    sh.getRange(lr, 2, 1, 5).merge().setValue("אין פוזיציות סגורות").setHorizontalAlignment("center").setFontColor("#718096"); lr++;
  } else {
    closedKeys.sort(function (a, b) { return (closedAgg[b].val - closedAgg[b].cost) - (closedAgg[a].val - closedAgg[a].cost); }).forEach(function (t, i) {
      const o = closedAgg[t], pl = o.val - o.cost, y = o.cost ? pl / o.cost : 0;
      sh.getRange(lr, 2, 1, 5).setValues([[t, Math.round(o.cost), Math.round(o.val), Math.round(pl), y]]).setHorizontalAlignment("center");
      sh.getRange(lr, 3, 1, 2).setNumberFormat("#,##0"); sh.getRange(lr, 5).setNumberFormat("#,##0"); sh.getRange(lr, 6).setNumberFormat("0.00%");
      colorPL(sh.getRange(lr, 5), pl); colorPL(sh.getRange(lr, 6), y);
      sh.getRange(lr, 2).setFontWeight("bold");
      if (i % 2 === 1) sh.getRange(lr, 2, 1, 5).setBackground("#F7FAFC");
      lr++;
    });
  }
  sh.getRange(panelTop, 2, lr - panelTop, 5).setBorder(true, true, true, true, true, true, "#CBD5E0", SpreadsheetApp.BorderStyle.SOLID);

  // RIGHT: allocation + top movers
  let pr = panelTop;
  sec(pr, 8, 6, "🍩 הקצאת נכסים"); pr++;
  sh.getRange(pr, 8, 1, 6).setValues([["קטגוריה", "", "שווי (₪)", "", "אחוז", ""]]); // spacer headers
  sh.getRange(pr, 8, 1, 2).merge().setValue("קטגוריה"); sh.getRange(pr, 10, 1, 2).merge().setValue("שווי (₪)"); sh.getRange(pr, 12, 1, 2).merge().setValue("אחוז");
  sh.getRange(pr, 8, 1, 6).setBackground("#BEE3F8").setFontWeight("bold").setHorizontalAlignment("center"); pr++;
  const allocRows = [["₿ קריפטו (כולל קרנות סל)", cryptoVal], ["📈 מניות / ETF רחב", equityVal]];
  allocRows.forEach(function (a) {
    sh.getRange(pr, 8, 1, 2).merge().setValue(a[0]).setHorizontalAlignment("center");
    sh.getRange(pr, 10, 1, 2).merge().setValue(Math.round(a[1])).setNumberFormat("#,##0").setHorizontalAlignment("center");
    sh.getRange(pr, 12, 1, 2).merge().setValue(totVal ? a[1] / totVal : 0).setNumberFormat("0.0%").setHorizontalAlignment("center");
    pr++;
  });
  pr++;
  sec(pr, 8, 6, "🥇 מנצחים / 🔻 מפסידים"); pr++;
  const topN = ranked.slice(0, 3), botN = ranked.slice(-3).reverse();
  for (let i = 0; i < 3; i++) {
    const wn = topN[i], ls = botN[i];
    sh.getRange(pr, 8, 1, 3).merge().setValue(wn ? "🟢 " + wn.t + "   " + pct(wn.y) : "").setFontColor("#1E7E34").setFontWeight("bold").setHorizontalAlignment("center");
    sh.getRange(pr, 11, 1, 3).merge().setValue(ls ? "🔴 " + ls.t + "   " + pct(ls.y) : "").setFontColor("#C53030").setFontWeight("bold").setHorizontalAlignment("center");
    pr++;
  }
  sh.getRange(panelTop, 8, pr - panelTop, 6).setBorder(true, true, true, true, false, false, "#CBD5E0", SpreadsheetApp.BorderStyle.SOLID);

  // ===================== charts (2x2 grid) =====================
  const chartTop = Math.max(lr, pr) + 2;
  try {
    // helper data blocks in hidden far-right columns (P=16 onward)
    sh.getRange(2, 16, 1, 2).setValues([["נכס", "שווי"]]);
    tks.forEach(function (t, i) { sh.getRange(3 + i, 16, 1, 2).setValues([[t, Math.round(byTicker[t].val)]]); });
    const dataHold = sh.getRange(2, 16, tks.length + 1, 2);

    sh.getRange(2, 19, 1, 2).setValues([["קטגוריה", "שווי"]]);
    sh.getRange(3, 19, 1, 2).setValues([["קריפטו", Math.round(cryptoVal)]]);
    sh.getRange(4, 19, 1, 2).setValues([["מניות/ETF", Math.round(equityVal)]]);
    const dataAlloc = sh.getRange(2, 19, 3, 2);

    sh.getRange(2, 22, 1, 2).setValues([["נכס", "רווח/הפסד (₪)"]]);
    tks.forEach(function (t, i) { sh.getRange(3 + i, 22, 1, 2).setValues([[t, Math.round(byTicker[t].val - byTicker[t].cost)]]); });
    const dataPL = sh.getRange(2, 22, tks.length + 1, 2);

    const platKeys = Object.keys(byPlat);
    sh.getRange(2, 25, 1, 3).setValues([["פלטפורמה", "עלות", "שווי"]]);
    platKeys.forEach(function (p, i) { sh.getRange(3 + i, 25, 1, 3).setValues([[p, Math.round(byPlat[p].cost), Math.round(byPlat[p].val)]]); });
    const dataPlat = sh.getRange(2, 25, platKeys.length + 1, 3);

    const W = 470, H = 300;
    sh.insertChart(sh.newChart().asPieChart().addRange(dataHold).setPosition(chartTop, 2, 5, 0)
      .setOption("title", "פילוח התיק לפי אחזקה (₪)").setOption("pieHole", 0.45).setOption("width", W).setOption("height", H).setOption("legend", { position: "right" }).build());
    sh.insertChart(sh.newChart().asPieChart().addRange(dataAlloc).setPosition(chartTop, 8, 5, 0)
      .setOption("title", "קריפטו מול מניות").setOption("pieHole", 0.5).setOption("width", W).setOption("height", H)
      .setOption("colors", ["#DD6B20", "#3182CE"]).setOption("legend", { position: "right" }).build());
    sh.insertChart(sh.newChart().asColumnChart().addRange(dataPL).setPosition(chartTop + 16, 2, 5, 0)
      .setOption("title", "רווח/הפסד לפי אחזקה (₪)").setOption("width", W).setOption("height", H).setOption("legend", { position: "none" }).build());
    sh.insertChart(sh.newChart().asColumnChart().addRange(dataPlat).setPosition(chartTop + 16, 8, 5, 0)
      .setOption("title", "עלות מול שווי לפי פלטפורמה (₪)").setOption("width", W).setOption("height", H)
      .setOption("colors", ["#A0AEC0", "#2B6CB0"]).setOption("legend", { position: "top" }).build());
    sh.hideColumns(16, 12);
  } catch (e) {}

  sh.setFrozenRows(3);
}

// ============================================================
//  Telegram bot — runs 24/7 inside Apps Script (works with PC off).
//  Replies are computed live from the sheet (which has GOOGLEFINANCE
//  prices), so it can actually answer instead of deflecting.
// ============================================================
const VALUE_LOG_SHEET = "היסטוריית שווי";

function tgApi_(method, params) {
  const token = cleanText(PropertiesService.getScriptProperties().getProperty("TELEGRAM_TOKEN") || "");
  if (!token) return { ok: false, error: "no token" };
  const res = UrlFetchApp.fetch("https://api.telegram.org/bot" + token + "/" + method, {
    method: "post", contentType: "application/json",
    payload: JSON.stringify(params || {}), muteHttpExceptions: true
  });
  try { return JSON.parse(res.getContentText()); } catch (e) { return { ok: false, error: String(e) }; }
}

function tgSend_(chatId, text) {
  return tgApi_("sendMessage", { chat_id: chatId, text: text, parse_mode: "HTML", disable_web_page_preview: true });
}

function handleTelegramUpdate_(update) {
  const msg = update.message || update.edited_message;
  if (!msg || !msg.chat) return;
  const chatId = String(msg.chat.id);
  // Privacy: only answer the owner (the web app is anonymous-accessible).
  const allowed = cleanText(PropertiesService.getScriptProperties().getProperty("TELEGRAM_CHAT_ID") || "");
  if (allowed && chatId !== allowed) { tgSend_(chatId, "מצטער, זהו בוט פרטי של אוריאל 🔒"); return; }
  const text = cleanText(msg.text || "");
  if (!text) { tgSend_(chatId, tgHelp_()); return; }
  let answer;
  try { answer = answerPortfolioQuestion_(text); }
  catch (e) { answer = "אירעה שגיאה בחישוב: " + String(e).slice(0, 200); }
  tgSend_(chatId, answer);
}

// ---- formatting helpers ----
function tgMoney_(v) { return "₪" + Math.round(v).toLocaleString("en-US"); }
function tgPct_(v) { return (v >= 0 ? "+" : "") + (v * 100).toFixed(2) + "%"; }
function tgDot_(v) { return v >= 0 ? "🟢" : "🔴"; }

function tgHelp_() {
  return "🤖 <b>בוט התיק של אוריאל</b> — שאל אותי בחופשיות. דוגמאות:\n\n" +
    "• <b>סיכום</b> — מצב התיק המלא\n" +
    "• <b>תשואה</b> / תשואה שבועית / חודשית / שנתית\n" +
    "• <b>רווח</b> / הפסד / רווח ממומש\n" +
    "• <b>מזומן</b> / הפקדות / עמלות\n" +
    "• <b>פילוח</b> לפי פלטפורמה / אקסלנס / הורייזון / Bit2C\n" +
    "• <b>הקצאה</b> (קריפטו מול מניות)\n" +
    "• <b>מנצחים</b> / מפסידים\n" +
    "• שם נכס (למשל <b>BTC</b>, VOO, IBIT) — פירוט אחזקה\n" +
    "• <b>שער דולר</b>\n\nאני עובד 24/7 גם כשהמחשב כבוי ☁️";
}

function computePortfolioStats_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const main = ss.getSheetByName("תמונת מצב");
  const data = main.getDataRange().getValues();
  const map = buildSnapshotHeaderIndexMap_(data[0] || []);
  const all = data.slice(1).filter(function (r) { return cleanText(rowVal_(r, map, "ticker")) !== ""; });
  let rate = 0; try { rate = parseNum(main.getRange("AA1").getValue()); } catch (e) {}
  if (!rate) rate = 1;
  const isClosed = function (r) { return cleanText(rowVal_(r, map, "status")) === "סגור"; };
  const open = all.filter(function (r) { return !isClosed(r); });
  const closed = all.filter(isClosed);
  const cryptoTk = { "IBIT": 1, "ETHA": 1, "BSOL": 1, "MSTR": 1 };
  let totCost = 0, totVal = 0, totFeeIls = 0, usdVal = 0, firstDate = null;
  const byTicker = {}, byPlat = {};
  open.forEach(function (r) {
    const t = cleanText(rowVal_(r, map, "ticker")), p = cleanText(rowVal_(r, map, "platform"));
    const ci = parseNum(rowVal_(r, map, "costIls")), vi = parseNum(rowVal_(r, map, "valueIls"));
    const tp = cleanText(rowVal_(r, map, "type")), cur = cleanText(rowVal_(r, map, "currency"));
    const qty = parseNum(rowVal_(r, map, "quantity")), fee = parseNum(rowVal_(r, map, "fee"));
    totCost += ci; totVal += vi; totFeeIls += (cur === "USD" ? fee * rate : fee);
    if (cur === "USD") usdVal += vi;
    if (!byTicker[t]) byTicker[t] = { qty: 0, cost: 0, val: 0, type: tp };
    byTicker[t].qty += qty; byTicker[t].cost += ci; byTicker[t].val += vi;
    if (!byPlat[p]) byPlat[p] = { cost: 0, val: 0, dep: 0 };
    byPlat[p].cost += ci; byPlat[p].val += vi;
    const d = normalizeDateOnly_(rowVal_(r, map, "purchaseDate"));
    if (d && (!firstDate || d < firstDate)) firstDate = d;
  });
  let realized = 0; const closedAgg = {};
  closed.forEach(function (r) {
    const t = cleanText(rowVal_(r, map, "ticker"));
    const ci = parseNum(rowVal_(r, map, "costIls")), vi = parseNum(rowVal_(r, map, "valueIls"));
    const cur = cleanText(rowVal_(r, map, "currency")), fee = parseNum(rowVal_(r, map, "fee"));
    realized += (vi - ci); totFeeIls += (cur === "USD" ? fee * rate : fee);
    if (!closedAgg[t]) closedAgg[t] = { cost: 0, val: 0 };
    closedAgg[t].cost += ci; closedAgg[t].val += vi;
  });
  let deposits = 0; const depByPlat = {};
  try { (readManualDeposits_("live").rows || []).forEach(function (d) { deposits += parseNum(d.Manual_Deposit_ILS); depByPlat[cleanText(d.Platform)] = parseNum(d.Manual_Deposit_ILS); }); } catch (e) {}
  Object.keys(depByPlat).forEach(function (p) { if (byPlat[p]) byPlat[p].dep = depByPlat[p]; });
  const cash = deposits - totCost, totalAccount = totVal + cash, netPL = totVal - totCost, ret = totCost ? netPL / totCost : 0;
  let cryptoVal = 0, equityVal = 0;
  Object.keys(byTicker).forEach(function (t) { if (byTicker[t].type === "קריפטו" || cryptoTk[t]) cryptoVal += byTicker[t].val; else equityVal += byTicker[t].val; });
  const tks = Object.keys(byTicker).sort(function (a, b) { return byTicker[b].val - byTicker[a].val; });
  let top3 = 0; tks.slice(0, 3).forEach(function (t) { top3 += byTicker[t].val; });
  const ranked = tks.filter(function (t) { return byTicker[t].cost > 0; })
    .map(function (t) { return { t: t, y: (byTicker[t].val - byTicker[t].cost) / byTicker[t].cost, pl: byTicker[t].val - byTicker[t].cost }; })
    .sort(function (a, b) { return b.y - a.y; });
  let days = 0; if (firstDate) { try { days = Math.round((new Date() - new Date(firstDate + "T00:00:00")) / 86400000); } catch (e) {} }
  return { rate: rate, deposits: deposits, totCost: totCost, totVal: totVal, cash: cash, totalAccount: totalAccount, netPL: netPL, ret: ret, realized: realized, totFeeIls: totFeeIls, usdVal: usdVal, cryptoVal: cryptoVal, equityVal: equityVal, byTicker: byTicker, byPlat: byPlat, tks: tks, ranked: ranked, closedAgg: closedAgg, conc: totVal ? top3 / totVal : 0, days: days, openCount: open.length };
}

function answerPortfolioQuestion_(text) {
  const s = (text || "").toLowerCase();
  const has = function () { for (var i = 0; i < arguments.length; i++) { if (s.indexOf(arguments[i]) >= 0) return true; } return false; };
  const S = computePortfolioStats_();

  if (has("עזרה", "מה אתה יכול", "מה אתה יודע", "/help", "/start", "פקודות")) return tgHelp_();

  // specific ticker (Latin symbol or Hebrew alias)
  const upper = " " + text.toUpperCase().replace(/[^A-Z]/g, " ") + " ";
  const aliases = { "ביטקוין": "BTC", "ביטקואין": "BTC", "אתריום": "ETH", "את'ריום": "ETH", "סולנה": "SOL", "אית'ריום": "ETH" };
  let tk = null;
  S.tks.forEach(function (t) { if (upper.indexOf(" " + t + " ") >= 0) tk = t; });
  Object.keys(aliases).forEach(function (k) { if (s.indexOf(k) >= 0 && S.byTicker[aliases[k]]) tk = aliases[k]; });
  if (tk) return tgTicker_(S, tk);

  // period returns
  if (has("שבוע")) return tgPeriod_(S, 7, "שבועית");
  if (has("חודש")) return tgPeriod_(S, 30, "חודשית");
  if (has("שנתי") || (has("שנה") && has("תשוא"))) return tgPeriod_(S, 365, "שנתית");
  if (has("מתחיל", "מההתחלה", "כל הזמן", "מאז")) return tgSinceStart_(S);

  // platform
  const platNames = { "אקסלנס": "אקסלנס", "אקסלנט": "אקסלנס", "הורייזון": "הורייזון", "הוריזון": "הורייזון", "horizon": "הורייזון", "bit2c": "Bit2C", "ביט2c": "Bit2C", "ביטטוסי": "Bit2C", "ביט": "Bit2C" };
  let askPlat = null;
  Object.keys(platNames).forEach(function (k) { if (s.indexOf(k) >= 0) askPlat = platNames[k]; });
  if (askPlat && S.byPlat[askPlat]) return tgPlatform_(S, askPlat);
  if (has("פלטפורמ", "פילוח", "התפלגות")) return tgAllPlatforms_(S);

  if (has("הקצא", "חלוק", "אלוקצ", "קריפטו מול", "כמה קריפטו", "כמה מניות")) return tgAllocation_(S);
  if (has("מנצח", "הכי טוב", "מוביל", "הרוויח הכי", "ביצועים הכי")) return tgMovers_(S, true);
  if (has("מפסיד", "הכי גרוע", "הכי הפסיד", "הכי ירד")) return tgMovers_(S, false);
  if (has("מזומן", "נזיל", "כסף פנוי", "פנוי")) return "💵 מזומן פנוי: <b>" + tgMoney_(S.cash) + "</b>\n(הפקדות " + tgMoney_(S.deposits) + " − עלות רכישות " + tgMoney_(S.totCost) + ")";
  if (has("הפקד", "השקעתי", "הכנסתי כסף")) return tgDeposits_(S);
  if (has("עמלה", "עמלות")) return "💸 סך העמלות ששולמו: <b>" + tgMoney_(S.totFeeIls) + "</b>\n(מחושב על כל העסקאות, פתוחות וסגורות)";
  if (has("ממומש", "מכרתי", "מכירות", "סגרתי")) return tgRealized_(S);
  if (has("דולר", "מטבע", "שער", "fx", "מט\"ח", "מטח")) return "💱 שער דולר/שקל: <b>" + S.rate.toFixed(4) + "</b>\nחשיפת מט\"ח (USD) בתיק: " + (S.totVal ? (S.usdVal / S.totVal * 100).toFixed(1) : 0) + "%";
  if (has("שווי", "ערך", "כמה יש", "כמה שווה", "גודל התיק")) return "📊 שווי התיק (אחזקות): <b>" + tgMoney_(S.totVal) + "</b>\n🏦 שווי חשבון כולל (כולל מזומן): <b>" + tgMoney_(S.totalAccount) + "</b>\n💵 מזומן: " + tgMoney_(S.cash);
  if (has("תשוא", "רווח", "הפסד", "הרווחתי", "הפסדתי", "עליתי", "ירדתי")) return tgTotalReturn_(S);

  // default: full summary
  return tgSummary_(S);
}

function tgSummary_(S) {
  const tz = SpreadsheetApp.getActiveSpreadsheet().getSpreadsheetTimeZone() || Session.getScriptTimeZone();
  const big = S.tks.slice(0, 3).map(function (t) { return t + " " + (S.totVal ? (S.byTicker[t].val / S.totVal * 100).toFixed(0) : 0) + "%"; }).join(" · ");
  return "📊 <b>סיכום התיק</b>  (" + Utilities.formatDate(new Date(), tz, "dd/MM/yyyy HH:mm") + ")\n\n" +
    "🏦 שווי חשבון כולל: <b>" + tgMoney_(S.totalAccount) + "</b>\n" +
    "📊 שווי אחזקות: " + tgMoney_(S.totVal) + "\n" +
    "💵 מזומן פנוי: " + tgMoney_(S.cash) + "\n" +
    "💰 הפקדות בפועל: " + tgMoney_(S.deposits) + "\n\n" +
    tgDot_(S.netPL) + " רווח/הפסד לא ממומש: <b>" + tgMoney_(S.netPL) + "</b> (" + tgPct_(S.ret) + ")\n" +
    tgDot_(S.realized) + " רווח/הפסד ממומש: " + tgMoney_(S.realized) + "\n\n" +
    "🥇 הגדולות: " + big + "\n" +
    "₿ קריפטו " + (S.totVal ? (S.cryptoVal / S.totVal * 100).toFixed(0) : 0) + "% · 📈 מניות " + (S.totVal ? (S.equityVal / S.totVal * 100).toFixed(0) : 0) + "%\n" +
    "💱 דולר/שקל " + S.rate.toFixed(3);
}

function tgTotalReturn_(S) {
  return "🎯 <b>תשואה כוללת</b> (מתחילת ההשקעה)\n" +
    tgDot_(S.ret) + " <b>" + tgPct_(S.ret) + "</b>\n" +
    "רווח/הפסד לא ממומש: " + tgMoney_(S.netPL) + "\n" +
    "רווח/הפסד ממומש: " + tgMoney_(S.realized) + "\n" +
    "שווי " + tgMoney_(S.totVal) + " מול עלות " + tgMoney_(S.totCost);
}

function tgTicker_(S, t) {
  const o = S.byTicker[t]; const pl = o.val - o.cost, y = o.cost ? pl / o.cost : 0, w = S.totVal ? o.val / S.totVal : 0;
  return "🔎 <b>" + t + "</b>\n" +
    "כמות: " + (Math.round(o.qty * 10000) / 10000) + "\n" +
    "שווי: <b>" + tgMoney_(o.val) + "</b> (" + (w * 100).toFixed(1) + "% מהתיק)\n" +
    "עלות: " + tgMoney_(o.cost) + "\n" +
    tgDot_(pl) + " רווח/הפסד: <b>" + tgMoney_(pl) + "</b> (" + tgPct_(y) + ")";
}

function tgAllPlatforms_(S) {
  let out = "🏦 <b>פילוח לפי פלטפורמה</b>\n\n";
  Object.keys(S.byPlat).sort(function (a, b) { return S.byPlat[b].val - S.byPlat[a].val; }).forEach(function (p) {
    const o = S.byPlat[p], pl = o.val - o.cost, y = o.cost ? pl / o.cost : 0;
    out += "<b>" + p + "</b>: " + tgMoney_(o.val) + "  " + tgDot_(pl) + " " + tgPct_(y) + "\n";
  });
  out += "\nסה\"כ: " + tgMoney_(S.totVal) + "  " + tgDot_(S.netPL) + " " + tgPct_(S.ret);
  return out;
}

function tgPlatform_(S, p) {
  const o = S.byPlat[p], pl = o.val - o.cost, y = o.cost ? pl / o.cost : 0;
  return "🏦 <b>" + p + "</b>\n" +
    "הפקדה: " + tgMoney_(o.dep) + "\n" +
    "עלות רכישות: " + tgMoney_(o.cost) + "\n" +
    "שווי נוכחי: <b>" + tgMoney_(o.val) + "</b>\n" +
    tgDot_(pl) + " רווח/הפסד: " + tgMoney_(pl) + " (" + tgPct_(y) + ")";
}

function tgAllocation_(S) {
  const c = S.totVal ? S.cryptoVal / S.totVal : 0;
  return "🍩 <b>הקצאת נכסים</b>\n" +
    "₿ קריפטו (כולל קרנות סל): <b>" + (c * 100).toFixed(1) + "%</b>  (" + tgMoney_(S.cryptoVal) + ")\n" +
    "📈 מניות / ETF רחב: <b>" + ((1 - c) * 100).toFixed(1) + "%</b>  (" + tgMoney_(S.equityVal) + ")\n" +
    "⚖️ ריכוז 3 הגדולות: " + (S.conc * 100).toFixed(1) + "%";
}

function tgMovers_(S, winners) {
  const list = winners ? S.ranked.slice(0, 3) : S.ranked.slice(-3).reverse();
  let out = winners ? "🥇 <b>המנצחים</b>\n" : "🔻 <b>המפסידים</b>\n";
  list.forEach(function (r) { out += (winners ? "🟢 " : "🔴 ") + "<b>" + r.t + "</b>  " + tgPct_(r.y) + "  (" + tgMoney_(r.pl) + ")\n"; });
  return out;
}

function tgDeposits_(S) {
  let out = "💰 <b>הפקדות בפועל</b>: " + tgMoney_(S.deposits) + "\n\n";
  Object.keys(S.byPlat).forEach(function (p) { if (S.byPlat[p].dep) out += p + ": " + tgMoney_(S.byPlat[p].dep) + "\n"; });
  return out;
}

function tgRealized_(S) {
  const keys = Object.keys(S.closedAgg);
  if (!keys.length) return "אין עדיין פוזיציות סגורות.";
  let out = "✅ <b>רווח ממומש (פוזיציות שנסגרו)</b>: " + tgMoney_(S.realized) + "\n\n";
  keys.forEach(function (t) { const o = S.closedAgg[t], pl = o.val - o.cost, y = o.cost ? pl / o.cost : 0; out += "<b>" + t + "</b>: " + tgDot_(pl) + " " + tgMoney_(pl) + " (" + tgPct_(y) + ")\n"; });
  return out;
}

// ---- value history + period returns ----
function ensureValueLogSheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let ws = ss.getSheetByName(VALUE_LOG_SHEET);
  if (!ws) {
    ws = ss.insertSheet(VALUE_LOG_SHEET);
    ws.getRange(1, 1, 1, 6).setValues([["תאריך", "שווי אחזקות (₪)", "עלות (₪)", "מזומן (₪)", "שווי כולל (₪)", "תשואה"]])
      .setBackground("#2C5282").setFontColor("white").setFontWeight("bold").setHorizontalAlignment("center");
    ws.setFrozenRows(1);
    ws.setColumnWidth(1, 110);
  }
  return ws;
}

function logPortfolioValue_() {
  const ws = ensureValueLogSheet_();
  const S = computePortfolioStats_();
  const tz = SpreadsheetApp.getActiveSpreadsheet().getSpreadsheetTimeZone() || Session.getScriptTimeZone();
  const today = Utilities.formatDate(new Date(), tz, "yyyy-MM-dd");
  const row = [today, Math.round(S.totVal), Math.round(S.totCost), Math.round(S.cash), Math.round(S.totalAccount), S.ret];
  const last = ws.getLastRow();
  let target = last + 1;
  if (last >= 2 && String(ws.getRange(last, 1).getDisplayValue()).indexOf(today) >= 0) target = last; // overwrite today
  ws.getRange(target, 1, 1, 6).setValues([row]);
  ws.getRange(target, 2, 1, 4).setNumberFormat("#,##0");
  ws.getRange(target, 6).setNumberFormat("0.00%");
}

// Daily trigger target (non-underscore so it can be selected/run from the editor).
function logPortfolioValueDaily() { logPortfolioValue_(); }

function tgPeriod_(S, days, label) {
  const ws = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(VALUE_LOG_SHEET);
  if (!ws || ws.getLastRow() < 2) {
    try { logPortfolioValue_(); } catch (e) {}
    return "📈 <b>תשואה " + label + "</b>\nהתחלתי לתעד את שווי התיק רק עכשיו, אז אין עדיין היסטוריה להשוואה. המדד יתמלא בימים הקרובים.\n\nבינתיים — תשואה מתחילת ההשקעה (" + S.days + " ימים): " + tgDot_(S.ret) + " <b>" + tgPct_(S.ret) + "</b>";
  }
  const rows = ws.getRange(2, 1, ws.getLastRow() - 1, 6).getValues();
  const nowMs = new Date().getTime(), targetMs = nowMs - days * 86400000;
  let pick = null, earliest = null;
  rows.forEach(function (r) {
    const ds = String(r[0]); const dms = new Date(ds.length <= 10 ? ds + "T00:00:00" : ds).getTime();
    if (isNaN(dms)) return;
    if (!earliest || dms < earliest.ms) earliest = { ms: dms, acc: parseNum(r[4]), ret: parseNum(r[5]) };
    if (dms <= targetMs && (!pick || dms > pick.ms)) pick = { ms: dms, acc: parseNum(r[4]), ret: parseNum(r[5]) };
  });
  const base = pick || earliest;
  const back = Math.round((nowMs - base.ms) / 86400000);
  if (back < 1) return "📈 <b>תשואה " + label + "</b>\nעדיין אין מספיק היסטוריה (תיעוד התחיל היום). נסה שוב בעוד יום-יומיים.\nתשואה מתחילת ההשקעה: " + tgDot_(S.ret) + " " + tgPct_(S.ret);
  const nowAcc = S.totalAccount, thenAcc = base.acc;
  const chg = thenAcc ? (nowAcc - thenAcc) / thenAcc : 0;
  const note = pick ? "" : "\n<i>(אין נתון מלפני " + days + " ימים — מציג מאז תחילת התיעוד, לפני " + back + " ימים)</i>";
  return "📈 <b>תשואה " + label + "</b> (" + back + " ימים)\n" +
    "שווי חשבון: " + tgMoney_(thenAcc) + " → " + tgMoney_(nowAcc) + "\n" +
    tgDot_(chg) + " שינוי: <b>" + tgPct_(chg) + "</b>" + note;
}

function tgSinceStart_(S) {
  return "📈 <b>תשואה מתחילת ההשקעה</b> (" + S.days + " ימים)\n" +
    tgDot_(S.ret) + " <b>" + tgPct_(S.ret) + "</b>\n" +
    "רווח/הפסד: " + tgMoney_(S.netPL) + " (לא ממומש) + " + tgMoney_(S.realized) + " (ממומש)";
}

// One-time setup — RUN THIS ONCE FROM THE APPS SCRIPT EDITOR to authorize the
// bot (UrlFetchApp for Telegram + a daily value-logging trigger). Safe to re-run.
function authorizeBot() {
  // 1) daily trigger to log portfolio value (for weekly/monthly returns)
  ScriptApp.getProjectTriggers().forEach(function (t) {
    if (t.getHandlerFunction() === "logPortfolioValueDaily") ScriptApp.deleteTrigger(t);
  });
  ScriptApp.newTrigger("logPortfolioValueDaily").timeBased().everyDays(1).atHour(22).create();
  logPortfolioValue_();
  // 2) point Telegram at this web app (idempotent)
  const url = ScriptApp.getService().getUrl();
  const wh = tgApi_("setWebhook", { url: url, allowed_updates: ["message", "edited_message"], drop_pending_updates: true });
  // 3) confirm to the owner
  const chat = PropertiesService.getScriptProperties().getProperty("TELEGRAM_CHAT_ID");
  if (chat) tgSend_(chat, "✅ הבוט מחובר ופעיל 24/7 (גם כשהמחשב כבוי). שלח 'סיכום' כדי לנסות.");
  return { webhookUrl: url, setWebhook: wh };
}

function buildDashboard() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const mainSheet = ss.getSheetByName("תמונת מצב");
  let homeSheet = ss.getSheetByName("דף הבית");
  if (!homeSheet) homeSheet = ss.insertSheet("דף הבית", 0);
  homeSheet.clear();
  homeSheet.getRange(1, 1, homeSheet.getMaxRows(), homeSheet.getMaxColumns()).clearDataValidations();
  homeSheet.setRightToLeft(true);
  const data = mainSheet.getDataRange().getValues();
  const map = buildSnapshotHeaderIndexMap_(data[0] || []);
  const rows = data.slice(1).filter(function (r) {
    return cleanText(rowVal_(r, map, "ticker")) !== "" && cleanText(rowVal_(r, map, "status")) !== "סגור";
  });
  const summary = {};
  let totalCostILS = 0;
  let totalValILS = 0;
  rows.forEach(function (r) {
    const t = cleanText(rowVal_(r, map, "ticker"));
    if (!summary[t]) summary[t] = { qty: 0, costILS: 0, valILS: 0, costOrig: 0, valOrig: 0 };
    const qty = parseNum(rowVal_(r, map, "quantity"));
    const costIls = parseNum(rowVal_(r, map, "costIls"));
    const valueIls = parseNum(rowVal_(r, map, "valueIls"));
    const costOrig = parseNum(rowVal_(r, map, "costOrigin")) + parseNum(rowVal_(r, map, "fee"));
    const valueOrig = cleanText(rowVal_(r, map, "currency")) === "USD" ? parseNum(rowVal_(r, map, "valueUsd")) : valueIls;
    summary[t].qty += qty;
    summary[t].costILS += costIls;
    summary[t].valILS += valueIls;
    summary[t].costOrig += costOrig;
    summary[t].valOrig += valueOrig;
    totalCostILS += costIls;
    totalValILS += valueIls;
  });
  // ── KPI strip: deposits / value / cash / total account / P&L / return ──
  let totalDeposits = 0;
  try { (readManualDeposits_("live").rows || []).forEach(function (dr) { totalDeposits += parseNum(dr.Manual_Deposit_ILS); }); } catch (e) {}
  const netPL = totalValILS - totalCostILS;
  const totalYield = totalCostILS ? netPL / totalCostILS : 0;
  const cashEst = totalDeposits - totalCostILS;
  const totalAccount = totalValILS + cashEst;
  const nis = function (v) { return "₪" + Math.round(v).toLocaleString("en-US"); };

  homeSheet.getRange("B2:H2").merge().setValue("💼 לוח בקרה — תיק ההשקעות של אוריאל").setFontSize(20).setFontWeight("bold").setHorizontalAlignment("center").setVerticalAlignment("middle").setBackground("#1A365D").setFontColor("white");
  const kpiLabels = [["💰 הפקדות בפועל", "📊 שווי תיק", "💵 מזומן", "🏦 שווי חשבון כולל", "📈 רווח/הפסד שוק", "🎯 תשואה"]];
  const kpiValues = [[nis(totalDeposits), nis(totalValILS), nis(cashEst), nis(totalAccount), nis(netPL), (totalYield * 100).toFixed(2) + "%"]];
  homeSheet.getRange(3, 2, 1, 6).setValues(kpiLabels).setFontWeight("bold").setFontSize(10).setBackground("#2B6CB0").setFontColor("white").setHorizontalAlignment("center").setVerticalAlignment("middle");
  homeSheet.getRange(4, 2, 1, 6).setValues(kpiValues).setFontWeight("bold").setFontSize(15).setHorizontalAlignment("center").setVerticalAlignment("middle").setBackground("#EBF8FF");
  homeSheet.getRange(4, 6).setFontColor(netPL >= 0 ? "#276749" : "#C53030");      // P/L cell (F4)
  homeSheet.getRange(4, 7).setFontColor(totalYield >= 0 ? "#276749" : "#C53030"); // return cell (G4)
  homeSheet.getRange("H3:H4").setBackground("#EBF8FF");
  homeSheet.getRange("B2:H4").setBorder(true, true, true, true, true, true, "#1A365D", SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
  homeSheet.setRowHeight(4, 32);
  homeSheet.getRange("I2").setValue("🔄 רענון").setFontSize(12).setFontWeight("bold").setHorizontalAlignment("center").setBackground("#4A5568").setFontColor("white");
  homeSheet.getRange("I3").insertCheckboxes().setBackground("#F7FAFC").setHorizontalAlignment("center").setVerticalAlignment("middle");
  homeSheet.getRange("I2:I3").setBorder(true, true, true, true, false, false, "black", SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
  const summaryHeader = [["נכס (טיקר)", "כמות פעילה", "עלות כוללת (₪)", "שווי עדכני (₪)", "רווח/הפסד נטו (₪)", "תשואה נטו (מקור)", "תשואה נטו (₪)"]];
  homeSheet.getRange(5, 2, 1, 7).setValues(summaryHeader).setFontWeight("bold").setBackground("#2B6CB0").setFontColor("white").setHorizontalAlignment("center").setVerticalAlignment("middle");
  const body = Object.keys(summary).sort().map(function (t) {
    const s = summary[t];
    const yieldOrig = s.costOrig ? (s.valOrig - s.costOrig) / s.costOrig : 0;
    const yieldILS = s.costILS ? (s.valILS - s.costILS) / s.costILS : 0;
    return [t, s.qty, s.costILS, s.valILS, s.valILS - s.costILS, yieldOrig, yieldILS];
  });
  if (body.length > 0) {
    homeSheet.getRange(6, 2, body.length, 7).setValues(body).setHorizontalAlignment("center").setVerticalAlignment("middle").setFontSize(12);
    homeSheet.getRange(6, 3, body.length, 1).setNumberFormat("#,##0.00000000");
    homeSheet.getRange(6, 4, body.length, 3).setNumberFormat("#,##0.00");
    homeSheet.getRange(6, 7, body.length, 2).setNumberFormat("0.00%");
    for (let i = 0; i < body.length; i++) {
      const rowNum = 6 + i;
      if (i % 2 === 0) homeSheet.getRange(rowNum, 2, 1, 7).setBackground("#F7FAFC");
      homeSheet.getRange(rowNum, 2).setFontWeight("bold");
      homeSheet.getRange(rowNum, 6).setFontColor(body[i][4] >= 0 ? "#38A169" : "#E53E3E").setFontWeight("bold");
      homeSheet.getRange(rowNum, 7).setFontColor(body[i][5] >= 0 ? "#38A169" : "#E53E3E").setFontWeight("bold");
      homeSheet.getRange(rowNum, 8).setFontColor(body[i][6] >= 0 ? "#38A169" : "#E53E3E").setFontWeight("bold");
    }
    homeSheet.getRange(5, 2, body.length + 1, 7).setBorder(true, true, true, true, true, true, "black", SpreadsheetApp.BorderStyle.SOLID);
  }
  const nextRow = 6 + Math.max(body.length, 1) + 2;
  homeSheet.getRange(nextRow, 2).setValue("🔍 בחר נכס להצגת פירוט:").setFontSize(12).setFontWeight("bold").setBackground("#EDF2F7").setHorizontalAlignment("right");
  const drillDownOptions = Object.keys(summary).sort();
  drillDownOptions.unshift("בחר הכל");
  drillDownOptions.push("- נקה בחירה -");
  const tickRule = SpreadsheetApp.newDataValidation().requireValueInList(drillDownOptions, true).setAllowInvalid(false).build();
  homeSheet.getRange(nextRow, 3).setDataValidation(tickRule).setBackground("#FFF2CC").setFontWeight("bold").setHorizontalAlignment("center").setValue("");
  homeSheet.getRange(nextRow, 2, 1, 2).setBorder(true, true, true, true, true, true, "black", SpreadsheetApp.BorderStyle.SOLID);
  drawReportsMenu(homeSheet);
}

function drawReportsMenu(homeSheet) {
  homeSheet.setColumnWidth(2, 160);
  for (let i = 3; i <= 8; i++) homeSheet.setColumnWidth(i, 130);
  homeSheet.setColumnWidth(9, 120);
  homeSheet.setColumnWidth(11, 240);
  homeSheet.setColumnWidth(12, 220);
  homeSheet.setColumnWidth(13, 140);
  homeSheet.getRange("K4").setValue("📊 בחר דוח להצגה:").setFontSize(12).setFontWeight("bold").setBackground("#EDF2F7").setHorizontalAlignment("right");
  const reportOptions = ["חלוקת קריפטו בתיק (אחוזים)", "ריכוז נכסים (מטבעות מול קרנות)", "המנצח והמפסיד (ממוצע לנכס)", "סך השקעה נטו (הפקדות)", "שערי מטבע חיים", "הצג את כל הדוחות", "- נקה דוח -"];
  const rule = SpreadsheetApp.newDataValidation().requireValueInList(reportOptions, true).setAllowInvalid(false).build();
  homeSheet.getRange("L4").setDataValidation(rule).setBackground("#FFF2CC").setFontWeight("bold").setHorizontalAlignment("center").setValue("");
  homeSheet.getRange("K4:L4").setBorder(true, true, true, true, true, true, "black", SpreadsheetApp.BorderStyle.SOLID);
}

function paintReportTable_(sheet, startRow, startCol, rows, cols) {
  if (rows <= 0 || cols <= 0) return;
  sheet.getRange(startRow, startCol, rows, cols).setBorder(true, true, true, true, true, true, "black", SpreadsheetApp.BorderStyle.SOLID);
}

function onEdit(e) {
  if (!e || !e.range) return;
  const sheet = e.source.getActiveSheet();
  const sheetName = sheet.getName();

  // Freeze Sell_Date when a row is manually marked closed in תמונת מצב.
  if (sheetName === PORTFOLIO_SHEET && e.range.getRow() > 1) {
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0].map(cleanText);
    const map = buildSnapshotHeaderIndexMap_(headers);
    const editedCol = e.range.getColumn() - 1;
    if (editedCol === map.status && map.sellDate >= 0) {
      const rowNum = e.range.getRow();
      const statusVal = cleanText(sheet.getRange(rowNum, map.status + 1).getValue());
      if (statusVal === "סגור") {
        const sellDateCell = sheet.getRange(rowNum, map.sellDate + 1);
        const currentSellDate = cleanText(sellDateCell.getValue());
        if (!currentSellDate || currentSellDate === "עדיין פתוח") {
          sellDateCell.setValue(new Date());
        }
      } else {
        sheet.getRange(rowNum, map.sellDate + 1).setValue("עדיין פתוח");
      }
    }
  }

  // Allow manual/additional rows in תגובות לטופס 1 to flow into תמונת מצב.
  if (sheetName === AUDIT_SHEET) {
    syncAuditRowToPortfolio_(sheet, e.range.getRow());
    return;
  }

  if (sheetName !== "דף הבית") return;
  const row = e.range.getRow();
  const col = e.range.getColumn();
  if (col === 9 && row === 3 && e.value === "TRUE") {
    e.source.toast("מרענן נתונים...", "🔄", 3);
    e.range.setValue(false);
    SpreadsheetApp.flush();
    RefreshAllData();
    return;
  }
  if (col === 3 && row >= 8) {
    const label = sheet.getRange(row, 2).getValue();
    if (label === "🔍 בחר נכס להצגת פירוט:") {
      const ticker = e.value;
      const maxRows = sheet.getMaxRows();
      if (maxRows > row) sheet.getRange(row + 1, 1, maxRows - row, 10).clear().clearDataValidations();
      if (ticker && ticker !== "- נקה בחירה -") showDrillDown(ticker, sheet, row + 2);
    }
  }
  if (col === 12 && row === 4) renderReport(e.value, sheet);
}

function onFormSubmit(e) {
  if (!e || !e.range) return;
  const sheet = e.range.getSheet();
  if (!sheet) return;
  if (sheet.getName() !== AUDIT_SHEET) return;
  syncAuditRowToPortfolio_(sheet, e.range.getRow());
}

function renderReport(reportName, homeSheet) {
  homeSheet.getRange(6, 11, homeSheet.getMaxRows() - 5, 8).clear().clearDataValidations();
  if (!reportName || reportName === "- נקה דוח -") return;
  const mainSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("תמונת מצב");
  const data = mainSheet.getDataRange().getValues();
  const map = buildSnapshotHeaderIndexMap_(data[0] || []);
  const rows = data.slice(1).filter(function (r) {
    return cleanText(rowVal_(r, map, "ticker")) !== "" && cleanText(rowVal_(r, map, "status")) !== "סגור";
  });
  let totalVal = 0, totalCost = 0, cryptoVal = 0, btcVal = 0;
  const platformCosts = {}, tickerSummary = {};
  const assetData = { BTC: { realQty: 0, realVal: 0, etfQty: 0, etfVal: 0 }, ETH: { realQty: 0, realVal: 0, etfQty: 0, etfVal: 0 }, SOL: { realQty: 0, realVal: 0, etfQty: 0, etfVal: 0 } };
  rows.forEach(function (r) {
    const platform = cleanText(rowVal_(r, map, "platform"));
    const type = cleanText(rowVal_(r, map, "type"));
    const ticker = cleanText(rowVal_(r, map, "ticker"));
    const qty = parseNum(rowVal_(r, map, "quantity"));
    const costILS = parseNum(rowVal_(r, map, "costIls"));
    const valILS = parseNum(rowVal_(r, map, "valueIls"));
    const costOrig = parseNum(rowVal_(r, map, "costOrigin")) + parseNum(rowVal_(r, map, "fee"));
    const valOrig = cleanText(rowVal_(r, map, "currency")) === "USD" ? parseNum(rowVal_(r, map, "valueUsd")) : valILS;
    totalVal += valILS; totalCost += costILS;
    if (platform) platformCosts[platform] = (platformCosts[platform] || 0) + costILS;
    if (!tickerSummary[ticker]) tickerSummary[ticker] = { cost: 0, val: 0, costOrig: 0, valOrig: 0 };
    tickerSummary[ticker].cost += costILS; tickerSummary[ticker].val += valILS;
    tickerSummary[ticker].costOrig += costOrig; tickerSummary[ticker].valOrig += valOrig;
    if (type === "קריפטו" || CRYPTO_ETFS.indexOf(ticker) >= 0) cryptoVal += valILS;
    if (ticker === "BTC") { btcVal += valILS; assetData.BTC.realQty += qty; assetData.BTC.realVal += valILS; }
    if (ticker === "IBIT") { btcVal += valILS; assetData.BTC.etfQty += qty; assetData.BTC.etfVal += valILS; }
    if (ticker === "ETH") { assetData.ETH.realQty += qty; assetData.ETH.realVal += valILS; }
    if (ticker === "ETHA") { assetData.ETH.etfQty += qty; assetData.ETH.etfVal += valILS; }
    if (ticker === "SOL") { assetData.SOL.realQty += qty; assetData.SOL.realVal += valILS; }
    if (ticker === "BSOL") { assetData.SOL.etfQty += qty; assetData.SOL.etfVal += valILS; }
  });
  let reportRow = 6;
  const reportsToRun = reportName === "הצג את כל הדוחות" ? ["חלוקת קריפטו בתיק (אחוזים)", "ריכוז נכסים (מטבעות מול קרנות)", "המנצח והמפסיד (ממוצע לנכס)", "סך השקעה נטו (הפקדות)", "שערי מטבע חיים"] : [reportName];
  reportsToRun.forEach(function (rep) {
    let cols = 2; if (rep === "ריכוז נכסים (מטבעות מול קרנות)") cols = 6; if (rep === "המנצח והמפסיד (ממוצע לנכס)") cols = 3;
    homeSheet.getRange(reportRow, 11, 1, cols).merge().setValue("📋 " + rep).setFontSize(14).setFontWeight("bold").setBackground("#2C5282").setFontColor("white").setHorizontalAlignment("center");
    reportRow++;
    if (rep === "חלוקת קריפטו בתיק (אחוזים)") {
      const tbl = [["אחוז קריפטו מכלל התיק (%)", totalVal ? cryptoVal / totalVal : 0], ["אחוז ביטקוין מכלל התיק (%)", totalVal ? btcVal / totalVal : 0], ["אחוז ביטקוין מסך הקריפטו (%)", cryptoVal ? btcVal / cryptoVal : 0]];
      homeSheet.getRange(reportRow, 11, tbl.length, 2).setValues(tbl).setBackground("#F7FAFC").setHorizontalAlignment("center");
      homeSheet.getRange(reportRow, 11, tbl.length, 1).setFontWeight("bold");
      homeSheet.getRange(reportRow, 12, tbl.length, 1).setNumberFormat("0.00%").setFontWeight("bold");
      paintReportTable_(homeSheet, reportRow - 1, 11, tbl.length + 1, 2);
      reportRow += tbl.length + 2;
    } else if (rep === "ריכוז נכסים (מטבעות מול קרנות)") {
      const headers = [["מטבע יעד", "אחזקה ישירה (כמות)", "אחזקה ישירה (₪)", "דרך קרן סל (יחידות)", "דרך קרן סל (₪)", "סה\"כ חשיפה (₪)"]];
      homeSheet.getRange(reportRow, 11, 1, 6).setValues(headers).setBackground("#4A5568").setFontColor("white").setFontWeight("bold");
      reportRow++;
      const tbl = [
        ["Bitcoin (BTC/IBIT)", assetData.BTC.realQty, assetData.BTC.realVal, assetData.BTC.etfQty, assetData.BTC.etfVal, assetData.BTC.realVal + assetData.BTC.etfVal],
        ["Ethereum (ETH/ETHA)", assetData.ETH.realQty, assetData.ETH.realVal, assetData.ETH.etfQty, assetData.ETH.etfVal, assetData.ETH.realVal + assetData.ETH.etfVal],
        ["Solana (SOL/BSOL)", assetData.SOL.realQty, assetData.SOL.realVal, assetData.SOL.etfQty, assetData.SOL.etfVal, assetData.SOL.realVal + assetData.SOL.etfVal]
      ];
      homeSheet.getRange(reportRow, 11, tbl.length, 6).setValues(tbl).setBackground("#F7FAFC").setHorizontalAlignment("center");
      homeSheet.getRange(reportRow, 12, tbl.length, 1).setNumberFormat("#,##0.00000000");
      homeSheet.getRange(reportRow, 13, tbl.length, 4).setNumberFormat("#,##0.00");
      homeSheet.getRange(reportRow, 11, tbl.length, 1).setFontWeight("bold");
      paintReportTable_(homeSheet, reportRow - 1, 11, tbl.length + 1, 6);
      reportRow += tbl.length + 2;
    } else if (rep === "המנצח והמפסיד (ממוצע לנכס)") {
      let topYield = -Infinity, bottomYield = Infinity, topTicker = "", bottomTicker = "";
      Object.keys(tickerSummary).forEach(function (t) {
        const s = tickerSummary[t];
        if (s.costOrig > 0) {
          const y = (s.valOrig - s.costOrig) / s.costOrig;
          if (y > topYield) { topYield = y; topTicker = t; }
          if (y < bottomYield) { bottomYield = y; bottomTicker = t; }
        }
      });
      homeSheet.getRange(reportRow, 11, 2, 3).setValues([["המרוויח הגדול", topTicker, topYield], ["המפסיד הגדול", bottomTicker, bottomYield]]).setBackground("#F7FAFC").setHorizontalAlignment("center");
      homeSheet.getRange(reportRow, 11, 2, 2).setFontWeight("bold");
      homeSheet.getRange(reportRow, 13, 2, 1).setNumberFormat("0.00%").setFontWeight("bold");
      paintReportTable_(homeSheet, reportRow - 1, 11, 3, 3);
      reportRow += 4;
    } else if (rep === "סך השקעה נטו (הפקדות)") {
      // Uses REAL deposits (money transferred in) from the manual-deposits sheet —
      // NOT invested cost. Shows deposits, invested cost, est. cash, value, P/L.
      const tbl = [];
      let totDep = 0;
      try {
        const deps = readManualDeposits_("live").rows || [];
        deps.forEach(function (dRow) {
          const amt = parseNum(dRow.Manual_Deposit_ILS);
          if (amt > 0) { tbl.push(["הפקדה בפועל - " + cleanText(dRow.Platform), amt]); totDep += amt; }
        });
      } catch (e) {}
      if (totDep <= 0) {
        Object.keys(platformCosts).sort().forEach(function (p) { if (platformCosts[p] > 0) { tbl.push(["עלות - " + p, platformCosts[p]]); totDep += platformCosts[p]; } });
      }
      const cashEst = totDep - totalCost;
      tbl.push(["סה\"כ הפקדות בפועל", totDep]);
      tbl.push(["עלות מושקעת (כולל עמלות)", totalCost]);
      tbl.push(["מזומן משוער (לא מושקע)", cashEst]);
      tbl.push(["שווי תיק נוכחי", totalVal]);
      tbl.push(["שווי חשבון כולל (תיק + מזומן)", totalVal + cashEst]);
      tbl.push(["רווח/הפסד שוק (₪)", totalVal - totalCost]);
      homeSheet.getRange(reportRow, 11, tbl.length, 2).setValues(tbl).setBackground("#F7FAFC").setHorizontalAlignment("center");
      homeSheet.getRange(reportRow, 11, tbl.length, 1).setFontWeight("bold");
      homeSheet.getRange(reportRow, 12, tbl.length, 1).setNumberFormat("#,##0.00").setFontWeight("bold");
      paintReportTable_(homeSheet, reportRow - 1, 11, tbl.length + 1, 2);
      reportRow += tbl.length + 2;
    } else if (rep === "שערי מטבע חיים") {
      homeSheet.getRange(reportRow, 11, 1, 2).setValues([["מטבע", "שער נוכחי"]]).setBackground("#4A5568").setFontColor("white").setFontWeight("bold").setHorizontalAlignment("center");
      reportRow++;
      const tbl = [["דולר שקל (USD/ILS)", '=GOOGLEFINANCE("CURRENCY:USDILS")'], ["ביטקוין דולר (BTC/USD)", '=GOOGLEFINANCE("CURRENCY:BTCUSD")'], ["אתריום דולר (ETH/USD)", '=GOOGLEFINANCE("CURRENCY:ETHUSD")'], ["סולאנה דולר (SOL/USD)", '=GOOGLEFINANCE("CURRENCY:SOLUSD")']];
      homeSheet.getRange(reportRow, 11, tbl.length, 2).setValues(tbl).setBackground("#F7FAFC").setHorizontalAlignment("center");
      homeSheet.getRange(reportRow, 11, tbl.length, 1).setFontWeight("bold");
      homeSheet.getRange(reportRow, 12, tbl.length, 1).setNumberFormat("#,##0.000000");
      paintReportTable_(homeSheet, reportRow - 1, 11, tbl.length + 1, 2);
      reportRow += tbl.length + 2;
    }
  });
}

function showDrillDown(ticker, homeSheet, startRow) {
  const data = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("תמונת מצב").getDataRange().getValues();
  const map = buildSnapshotHeaderIndexMap_(data[0] || []);
  const allRows = data.slice(1).filter(function (r) { return cleanText(rowVal_(r, map, "ticker")) !== ""; });
  if (allRows.length === 0) return;
  let currentRow = startRow;
  if (ticker === "בחר הכל") {
    const allTickers = {};
    allRows.forEach(function (r) { allTickers[cleanText(rowVal_(r, map, "ticker"))] = true; });
    Object.keys(allTickers).sort().forEach(function (t) { currentRow = drawSingleAssetTable(t, allRows, homeSheet, currentRow); });
  } else {
    drawSingleAssetTable(ticker, allRows, homeSheet, currentRow);
  }
}

function drawSingleAssetTable(ticker, allRows, homeSheet, startRow) {
  const data = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("תמונת מצב").getDataRange().getValues();
  const map = buildSnapshotHeaderIndexMap_(data[0] || []);
  const history = allRows.filter(function (r) { return cleanText(rowVal_(r, map, "ticker")) === ticker; });
  if (history.length === 0) return startRow;
  homeSheet.getRange(startRow, 2, 1, 8)
    .merge()
    .setValue("🔎 פירוט כל הפעולות עבור: " + ticker)
    .setFontSize(14)
    .setFontWeight("bold")
    .setBackground("#EDF2F7")
    .setHorizontalAlignment("center");

  homeSheet.getRange(startRow + 1, 2, 1, 8)
    .setValues([["תאריך רכישה", "פלטפורמה", "כמות", "שער קנייה", "עלות (₪)", "שווי עדכני (₪)", "תשואה (מקור)", "סטטוס"]])
    .setBackground("#4A5568")
    .setFontColor("white")
    .setFontWeight("bold")
    .setHorizontalAlignment("center");

  const body = history.map(function (r) {
    return [
      rowVal_(r, map, "purchaseDate"),
      rowVal_(r, map, "platform"),
      parseNum(rowVal_(r, map, "quantity")),
      parseNum(rowVal_(r, map, "buyPrice")),
      parseNum(rowVal_(r, map, "costIls")),
      parseNum(rowVal_(r, map, "valueIls")),
      parseNum(rowVal_(r, map, "yieldOrigin")),
      rowVal_(r, map, "status")
    ];
  });
  const bodyRange = homeSheet.getRange(startRow + 2, 2, body.length, 8);
  bodyRange
    .setValues(body)
    .setHorizontalAlignment("center")
    .setVerticalAlignment("middle")
    .setFontSize(11);

  // Number/date formats aligned with the rest of Home tables.
  homeSheet.getRange(startRow + 2, 2, body.length, 1).setNumberFormat("dd/MM/yyyy");
  homeSheet.getRange(startRow + 2, 4, body.length, 1).setNumberFormat("#,##0.00000000");
  homeSheet.getRange(startRow + 2, 5, body.length, 2).setNumberFormat("#,##0.00");
  homeSheet.getRange(startRow + 2, 8, body.length, 1).setNumberFormat("0.00%");

  for (let i = 0; i < body.length; i++) {
    const rr = startRow + 2 + i;
    if (i % 2 === 0) homeSheet.getRange(rr, 2, 1, 8).setBackground("#F7FAFC");

    // Yield color coding.
    homeSheet.getRange(rr, 8)
      .setFontColor(body[i][6] >= 0 ? "#38A169" : "#E53E3E")
      .setFontWeight("bold");

    // Closed status emphasized in red.
    if (String(body[i][7]).trim() === "סגור") {
      homeSheet.getRange(rr, 9).setFontColor("#E53E3E").setFontWeight("bold");
    }
  }

  // Outer + inner borders for the full section.
  homeSheet.getRange(startRow + 1, 2, body.length + 1, 8)
    .setBorder(true, true, true, true, true, true, "black", SpreadsheetApp.BorderStyle.SOLID);

  return startRow + body.length + 4;
}

function distributeToPlatformSheets() {
  // Clean, read-only per-platform VIEW sheets. We copy computed VALUES (not the
  // snapshot formulas, which referenced $AA$1 / per-row infra that doesn't exist
  // here and broke to 0 / -100%). Curated columns + header + per-platform totals.
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const mainSheet = ss.getSheetByName("תמונת מצב");
  if (!mainSheet) return;
  const data = mainSheet.getDataRange().getValues();
  const map = buildSnapshotHeaderIndexMap_(data[0] || []);
  const cols = [
    { k: "ticker", h: "טיקר", fmt: null },
    { k: "type", h: "סוג נכס", fmt: null },
    { k: "purchaseDate", h: "תאריך רכישה", fmt: null },
    { k: "quantity", h: "כמות", fmt: "#,##0.00000000" },
    { k: "buyPrice", h: "שער קנייה", fmt: "#,##0.00" },
    { k: "currency", h: "מטבע", fmt: null },
    { k: "costOrigin", h: "עלות (מקור)", fmt: "#,##0.00" },
    { k: "fee", h: "עמלה", fmt: "#,##0.00" },
    { k: "costIls", h: "עלות (₪)", fmt: "#,##0.00" },
    { k: "valueIls", h: "שווי נוכחי (₪)", fmt: "#,##0.00" },
    { k: "yieldOrigin", h: "תשואה (מקור)", fmt: "0.00%" },
    { k: "yieldIls", h: "תשואה (₪)", fmt: "0.00%" },
    { k: "sellPrice", h: "שער מכירה", fmt: "#,##0.00" },
    { k: "status", h: "סטטוס", fmt: null }
  ];
  const nc = cols.length;
  const idxOf = function (key) { for (let i = 0; i < cols.length; i++) if (cols[i].k === key) return i; return -1; };
  ["אקסלנס", "Bit2C", "הורייזון"].forEach(function (platform) {
    const ts = ss.getSheetByName(platform);
    if (!ts) return;
    ts.clear();
    ts.getBandings().forEach(function (b) { b.remove(); });
    ts.setRightToLeft(true);
    ts.getRange(1, 1, 1, nc).merge().setValue("📂 " + platform + " — פירוט אחזקות").setFontSize(16).setFontWeight("bold").setBackground("#1A365D").setFontColor("white").setHorizontalAlignment("center").setVerticalAlignment("middle");
    ts.getRange(2, 1).setValue("🏠 חזור לראשי").setFontColor("#2B6CB0").setFontWeight("bold");
    ts.getRange(3, 1, 1, nc).setValues([cols.map(function (c) { return c.h; })]).setFontWeight("bold").setBackground("#2B6CB0").setFontColor("white").setHorizontalAlignment("center").setVerticalAlignment("middle");
    const rows = data.filter(function (r, idx) { return idx > 0 && cleanText(rowVal_(r, map, "platform")) === platform && cleanText(rowVal_(r, map, "ticker")) !== ""; });
    let totCostIls = 0, totValIls = 0;
    const body = rows.map(function (r) {
      if (cleanText(rowVal_(r, map, "status")) !== "סגור") {
        totCostIls += parseNum(rowVal_(r, map, "costIls"));
        totValIls += parseNum(rowVal_(r, map, "valueIls"));
      }
      return cols.map(function (c) { const v = rowVal_(r, map, c.k); return (v === undefined || v === null) ? "" : v; });
    });
    if (body.length > 0) {
      ts.getRange(4, 1, body.length, nc).setValues(body).setHorizontalAlignment("center").setVerticalAlignment("middle");
      cols.forEach(function (c, ci) { if (c.fmt) ts.getRange(4, ci + 1, body.length, 1).setNumberFormat(c.fmt); });
      const yo = idxOf("yieldOrigin"), yi = idxOf("yieldIls");
      for (let i = 0; i < body.length; i++) {
        const rr = 4 + i;
        ts.getRange(rr, yo + 1).setFontColor(parseNum(body[i][yo]) >= 0 ? "#276749" : "#C53030").setFontWeight("bold");
        ts.getRange(rr, yi + 1).setFontColor(parseNum(body[i][yi]) >= 0 ? "#276749" : "#C53030").setFontWeight("bold");
        if (i % 2 === 1) ts.getRange(rr, 1, 1, nc).setBackground("#F7FAFC");
        ts.getRange(rr, 1).setFontWeight("bold");
      }
      const totRow = 4 + body.length;
      const ci = idxOf("costIls"), vi = idxOf("valueIls"), yit = idxOf("yieldIls");
      const totLine = []; for (let q = 0; q < nc; q++) totLine.push("");
      totLine[0] = "סה\"כ פעיל";
      totLine[ci] = totCostIls; totLine[vi] = totValIls;
      totLine[yit] = totCostIls ? (totValIls - totCostIls) / totCostIls : 0;
      ts.getRange(totRow, 1, 1, nc).setValues([totLine]).setFontWeight("bold").setBackground("#EBF8FF");
      ts.getRange(totRow, ci + 1, 1, 1).setNumberFormat("#,##0.00");
      ts.getRange(totRow, vi + 1, 1, 1).setNumberFormat("#,##0.00");
      ts.getRange(totRow, yit + 1, 1, 1).setNumberFormat("0.00%").setFontColor(totLine[yit] >= 0 ? "#276749" : "#C53030");
      ts.getRange(3, 1, body.length + 2, nc).setBorder(true, true, true, true, true, true, "#CBD5E0", SpreadsheetApp.BorderStyle.SOLID);
    }
    ts.setFrozenRows(3);
    ts.autoResizeColumns(1, nc);
  });
}


