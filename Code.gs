// =========================================================================
// Portfolio Manager OS - Google Sheets Backend (Final)
// Preserves prior dashboard behavior + adds reconciled transaction fixes.
// =========================================================================

const CRYPTO_ETFS = ["IBIT", "ETHA", "BSOL", "MSTR"];
const PORTFOLIO_SHEET = "תמונת מצב";
const AUDIT_SHEET = "תגובות לטופס 1";
const DEPOSITS_SHEET = "הפקדות ידניות";

function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu("🚀 אפליקציית השקעות")
    .addItem("רענן נתונים ודף הבית", "RefreshAllData")
    .addItem("🎨 עצב את מסד הנתונים", "formatMainSheet")
    .addSeparator()
    .addItem("🛡️ העברה לארנק קר (ללא כפילויות)", "TransferToColdWallet")
    .addSeparator()
    .addItem("🛠️ רופא המערכת - ניקוי ופיצול עמלות", "SystemDoctor")
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

function normalizeDateOnly_(val) {
  if (Object.prototype.toString.call(val) === "[object Date]") {
    return Utilities.formatDate(val, Session.getScriptTimeZone(), "yyyy-MM-dd");
  }
  const s = cleanText(val);
  if (!s) return "";
  const d = new Date(s);
  if (String(d) !== "Invalid Date") {
    return Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
  }
  const parts = s.split("/");
  if (parts.length < 3) return s;
  const dd = ("0" + parts[0]).slice(-2);
  const mm = ("0" + parts[1]).slice(-2);
  const yyyy = parts[2].length === 2 ? "20" + parts[2] : parts[2].slice(0, 4);
  return yyyy + "-" + mm + "-" + dd;
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
  if (lastRow < 2) return { mode: currentMode, rows: [] };

  const values = ws.getRange(2, 1, lastRow - 1, 4).getValues();
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
  if (lastRow >= 2) {
    const existingModes = ws.getRange(2, 2, lastRow - 1, 1).getValues();
    for (let i = existingModes.length - 1; i >= 0; i--) {
      if (cleanText(existingModes[i][0]).toLowerCase() === currentMode) {
        ws.deleteRow(i + 2);
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
  ix.tradeId = findAny_(["Trade_ID"]);

  if (ix.tradeId < 0) {
    const col = sheet.getLastColumn() + 1;
    sheet.getRange(1, col).setValue("Trade_ID");
    ix.tradeId = col - 1;
  }
  return ix;
}

function tradeIdFromRow_(row, ix) {
  const raw = [
    cleanText(row[ix.platform]),
    cleanText(row[ix.ticker]).toUpperCase(),
    normalizeDateOnly_(row[ix.purchaseDate]),
    parseNum(row[ix.quantity]),
    parseNum(row[ix.cost])
  ].join("|");
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_1, raw, Utilities.Charset.UTF_8);
  return digest.map(function (b) {
    const v = (b + 256) % 256;
    return ("0" + v.toString(16)).slice(-2);
  }).join("").slice(0, 16);
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

  const dedup = dedupeRows_(rows, ix);
  const patched = applyCrossCheckPatches_(rows, ix);

  // Preserve every trade row. We only normalize / patch values in-place.
  ws.getRange(2, 1, Math.max(lastRow - 1, 1), width).setValues(patched.rows);

  // Restore formulas/formatting expected by your original dashboard logic.
  InstallSystem();

  return {
    rows: patched.rows.length,
    removedDuplicates: dedup.duplicates,
    updated: patched.updates,
    added: patched.adds
  };
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

    const result = action === "delete" ? deleteTrade_(trade.Trade_ID) : upsertTrade_(action, trade);
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
  const rows = values.slice(1).filter(function (r) {
    return cleanText(r[3]) !== "";
  });
  return { headers: headers, rows: rows };
}

function sanitizeTrade_(trade) {
  const out = {};
  Object.keys(trade).forEach(function (k) { out[k] = trade[k]; });
  out.Platform = cleanText(out.Platform || out["פלטפורמה"] || "");
  out.Type = cleanText(out.Type || out["סוג נכס"] || "קריפטו");
  out.Ticker = cleanText(out.Ticker || out["טיקר"] || "").toUpperCase();
  out.Purchase_Date = cleanText(out.Purchase_Date || out["תאריך רכישה"] || "");
  out.Quantity = parseNum(out.Quantity || out["כמות"]);
  out.Origin_Buy_Price = parseNum(out.Origin_Buy_Price || out["שער קנייה"]);
  out.Cost_Origin = parseNum(out.Cost_Origin || out["עלות כוללת"]);
  out.Origin_Currency = cleanText(out.Origin_Currency || out["מטבע"] || "ILS").toUpperCase();
  out.Commission = parseNum(out.Commission || out["עמלה"]);
  out.Status = cleanText(out.Status || out["סטטוס"] || "פתוח");
  out.Trade_ID = cleanText(out.Trade_ID || "");

  if (!out.Trade_ID) {
    const raw = [out.Platform, out.Ticker, normalizeDateOnly_(out.Purchase_Date), out.Quantity, out.Cost_Origin].join("|");
    const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_1, raw, Utilities.Charset.UTF_8);
    out.Trade_ID = digest.map(function (b) {
      const v = (b + 256) % 256;
      return ("0" + v.toString(16)).slice(-2);
    }).join("").slice(0, 16);
  }
  return out;
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

  const result = action === "delete" ? deleteTrade_(trade.Trade_ID) : upsertTrade_(action, trade);
  const statusCell = headers.indexOf("Status") >= 0 ? headers.indexOf("Status") + 1 : -1;
  if (statusCell > 0) {
    sheet.getRange(rowNum, statusCell).setValue(result.ok ? "OK" : "ERROR");
  }
  appendAudit_("audit_sheet_" + action, trade.Trade_ID, result.ok ? "OK" : "ERROR", JSON.stringify(result));
}

function findTradeRowById_(ws, ix, tradeId) {
  const last = ws.getLastRow();
  if (last < 2) return -1;
  const values = ws.getRange(2, ix.tradeId + 1, last - 1, 1).getValues();
  for (let i = 0; i < values.length; i++) {
    if (cleanText(values[i][0]) === cleanText(tradeId)) return i + 2;
  }
  return -1;
}

function upsertTrade_(action, trade) {
  const ws = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(PORTFOLIO_SHEET);
  const ix = getColumnMap_(ws);
  const width = ws.getLastColumn();
  const rowIndex = findTradeRowById_(ws, ix, trade.Trade_ID);

  if (action === "add" && rowIndex > 0) return { ok: false, error: "Trade already exists" };
  if (action === "edit" && rowIndex < 0) return { ok: false, error: "Trade not found" };

  if (action === "add") {
    const target = ws.getLastRow() + 1;
    const row = new Array(width).fill("");
    row[ix.platform] = trade.Platform;
    row[ix.type] = trade.Type;
    row[ix.ticker] = trade.Ticker;
    row[ix.purchaseDate] = trade.Purchase_Date;
    row[ix.quantity] = trade.Quantity;
    row[ix.buyPrice] = trade.Origin_Buy_Price;
    row[ix.cost] = trade.Cost_Origin;
    row[ix.currency] = trade.Origin_Currency;
    row[ix.fee] = trade.Commission;
    row[ix.status] = trade.Status;
    row[ix.tradeId] = trade.Trade_ID;
    ws.getRange(target, 1, 1, width).setValues([row]);
    if (target > 2) {
      ws.getRange(target - 1, 12, 1, 12).copyTo(ws.getRange(target, 12, 1, 12), SpreadsheetApp.CopyPasteType.PASTE_FORMULA, false);
      ws.getRange(target - 1, 1, 1, width).copyTo(ws.getRange(target, 1, 1, width), SpreadsheetApp.CopyPasteType.PASTE_FORMAT, false);
    }
    return { ok: true, message: "Added", row: target };
  }

  const existing = ws.getRange(rowIndex, 1, 1, width).getValues()[0];
  existing[ix.platform] = trade.Platform;
  existing[ix.type] = trade.Type;
  existing[ix.ticker] = trade.Ticker;
  existing[ix.purchaseDate] = trade.Purchase_Date;
  existing[ix.quantity] = trade.Quantity;
  existing[ix.buyPrice] = trade.Origin_Buy_Price;
  existing[ix.cost] = trade.Cost_Origin;
  existing[ix.currency] = trade.Origin_Currency;
  existing[ix.fee] = trade.Commission;
  existing[ix.status] = trade.Status;
  existing[ix.tradeId] = trade.Trade_ID;
  ws.getRange(rowIndex, 1, 1, width).setValues([existing]);
  return { ok: true, message: "Updated", row: rowIndex };
}

function deleteTrade_(tradeId) {
  const ws = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(PORTFOLIO_SHEET);
  const ix = getColumnMap_(ws);
  const rowIndex = findTradeRowById_(ws, ix, tradeId);
  if (rowIndex < 0) return { ok: false, error: "Trade not found" };
  ws.deleteRow(rowIndex);
  return { ok: true, message: "Deleted", row: rowIndex };
}

function validateToken_(token) {
  const required = PropertiesService.getScriptProperties().getProperty("API_TOKEN");
  if (required && token !== required) throw new Error("Unauthorized");
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
  mainSheet.getRange("L:AE").clear();
  const formulaHeaders = [["שער נוכחי USD", "שווי נוכחי USD", "עלות USD", "עלות ILS", "שווי USD", "שווי ILS", "שער קנייה USD", "שער קנייה ILS", "שער נוכחי USD (כפילות)", "שער נוכחי ILS", "תשואה מקור", "תשואה שקלית"]];
  mainSheet.getRange("L1:W1").setValues(formulaHeaders).setBackground("#2C5282").setFontColor("white").setFontWeight("bold").setHorizontalAlignment("center");
  mainSheet.getRange("Y1").setFormula('=GOOGLEFINANCE("CURRENCY:USDILS")').setBackground("#FFF2CC").setFontWeight("bold").setHorizontalAlignment("center");
  mainSheet.getRange("X1").setValue("שער USD/ILS עדכני:").setFontWeight("bold");
  const numRows = Math.max(mainSheet.getLastRow(), 2) - 1;
  const formulasToApply = [];
  for (let i = 0; i < numRows; i++) {
    const r = i + 2;
    const hRate = "IFERROR(IFNA(INDEX(GOOGLEFINANCE(\"CURRENCY:USDILS\", \"price\", E" + r + "), 2, 2), IFNA(INDEX(GOOGLEFINANCE(\"CURRENCY:USDILS\", \"price\", E" + r + "-1), 2, 2), $Y$1)), $Y$1)";
    formulasToApply.push([
      '=IF(K' + r + '="סגור", "נמכר", IF(D' + r + '="","", IF(C' + r + '="קריפטו", GOOGLEFINANCE("CURRENCY:"&D' + r + '&"USD"), IF(C' + r + '="שוק ההון", GOOGLEFINANCE(D' + r + ', "price"), 0))))',
      '=IF(OR(F' + r + '="", L' + r + '="נמכר"), 0, F' + r + '*L' + r + ')',
      '=IF(H' + r + '="","", (H' + r + '+IF(J' + r + '="",0,J' + r + ')) / IF($I' + r + '="USD", 1, ' + hRate + '))',
      '=IF(H' + r + '="","", (H' + r + '+IF(J' + r + '="",0,J' + r + ')) * IF($I' + r + '="ILS", 1, ' + hRate + '))',
      '=IF(M' + r + '="","", M' + r + ')',
      '=IF(M' + r + '="","", M' + r + ' * $Y$1)',
      '=IF(G' + r + '="","", IF($I' + r + '="USD", $G' + r + ', $G' + r + ' / ' + hRate + '))',
      '=IF(G' + r + '="","", IF($I' + r + '="ILS", $G' + r + ', $G' + r + ' * ' + hRate + '))',
      '=IF(L' + r + '="","", L' + r + ')',
      '=IF(L' + r + '="","", L' + r + ' * $Y$1)',
      '=IF(OR($H' + r + '="", $H' + r + '=0, K' + r + '="סגור"), 0, (IF($I' + r + '="USD", $P' + r + ', $Q' + r + ') - ($H' + r + '+IF(J' + r + '="",0,J' + r + '))) / ($H' + r + '+IF(J' + r + '="",0,J' + r + ')))',
      '=IF(OR($O' + r + '="", $O' + r + '=0, K' + r + '="סגור"), 0, ($Q' + r + '-$O' + r + ')/$O' + r + ')'
    ]);
  }
  mainSheet.getRange(2, 12, numRows, 12).setFormulas(formulasToApply);

  // Normalize numeric formats to avoid mixed text/currency exports in CSV.
  if (numRows > 0) {
    mainSheet.getRange(2, 6, numRows, 1).setNumberFormat("#,##0.00000000"); // כמות
    mainSheet.getRange(2, 7, numRows, 1).setNumberFormat("#,##0.00");       // שער קנייה
    mainSheet.getRange(2, 8, numRows, 1).setNumberFormat("#,##0.00");       // עלות כוללת
    mainSheet.getRange(2, 10, numRows, 1).setNumberFormat("#,##0.00");      // עמלה

    // L:W = formula columns
    mainSheet.getRange(2, 12, numRows, 10).setNumberFormat("#,##0.00");     // L:U numeric values
    mainSheet.getRange(2, 22, numRows, 2).setNumberFormat("0.00%");          // V:W yields
  }

  sortSheetByDate();
  formatMainSheet();
}

function formatMainSheet() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("תמונת מצב");
  if (!sheet) return;
  const lastRow = Math.max(sheet.getLastRow(), 2);
  sheet.setRightToLeft(true);
  sheet.setFrozenRows(1);
  const fullRange = sheet.getRange(1, 1, lastRow, 23);
  fullRange.setHorizontalAlignment("center").setVerticalAlignment("middle");
  sheet.getBandings().forEach(function (b) { b.remove(); });
  fullRange.applyRowBanding(SpreadsheetApp.BandingTheme.INDIGO, true, false);
  sheet.autoResizeColumns(1, 23);
}

function RefreshAllData() {
  distributeToPlatformSheets();
  buildDashboard();
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
  const rows = data.slice(1).filter(function (r) { return cleanText(r[3]) !== "" && cleanText(r[10]) !== "סגור"; });
  const summary = {};
  let totalCostILS = 0;
  let totalValILS = 0;
  rows.forEach(function (r) {
    const t = cleanText(r[3]);
    if (!summary[t]) summary[t] = { qty: 0, costILS: 0, valILS: 0, costOrig: 0, valOrig: 0 };
    summary[t].qty += parseNum(r[5]);
    summary[t].costILS += parseNum(r[14]);
    summary[t].valILS += parseNum(r[16]);
    summary[t].costOrig += parseNum(r[7]) + parseNum(r[9]);
    summary[t].valOrig += (cleanText(r[8]) === "USD" ? parseNum(r[15]) : parseNum(r[16]));
    totalCostILS += parseNum(r[14]);
    totalValILS += parseNum(r[16]);
  });
  homeSheet.getRange("B2:H2").merge().setValue("💼 לוח בקרה - התיק הפעיל").setFontSize(20).setFontWeight("bold").setHorizontalAlignment("center").setBackground("#1A365D").setFontColor("white");
  const totalYield = totalCostILS ? (totalValILS - totalCostILS) / totalCostILS : 0;
  const summaryText = "שווי פעיל כולל: ₪" + totalValILS.toLocaleString("en-US", { maximumFractionDigits: 0 }) + "  |  עלות (כולל עמלות): ₪" + totalCostILS.toLocaleString("en-US", { maximumFractionDigits: 0 }) + "  |  תשואה נטו: " + (totalYield * 100).toFixed(2) + "%";
  homeSheet.getRange("B3:H3").merge().setValue(summaryText).setFontSize(14).setFontWeight("bold").setHorizontalAlignment("center").setBackground("#EBF8FF").setFontColor(totalYield >= 0 ? "#276749" : "#C53030");
  homeSheet.getRange("B2:H3").setBorder(true, true, true, true, false, false, "black", SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
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
  const rows = data.slice(1).filter(function (r) { return cleanText(r[3]) !== "" && cleanText(r[10]) !== "סגור"; });
  let totalVal = 0, totalCost = 0, cryptoVal = 0, btcVal = 0;
  const platformCosts = {}, tickerSummary = {};
  const assetData = { BTC: { realQty: 0, realVal: 0, etfQty: 0, etfVal: 0 }, ETH: { realQty: 0, realVal: 0, etfQty: 0, etfVal: 0 }, SOL: { realQty: 0, realVal: 0, etfQty: 0, etfVal: 0 } };
  rows.forEach(function (r) {
    const platform = cleanText(r[1]), type = cleanText(r[2]), ticker = cleanText(r[3]);
    const qty = parseNum(r[5]), costILS = parseNum(r[14]), valILS = parseNum(r[16]);
    const costOrig = parseNum(r[7]) + parseNum(r[9]);
    const valOrig = cleanText(r[8]) === "USD" ? parseNum(r[15]) : parseNum(r[16]);
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
      const tbl = [];
      Object.keys(platformCosts).sort().forEach(function (p) { if (platformCosts[p] > 0) tbl.push(["הפקדות - " + p, platformCosts[p]]); });
      tbl.push(["סה\"כ הפקדות בתיק (עלות בפועל)", totalCost], ["שווי התיק הנוכחי", totalVal], ["רווח / הפסד נטו (₪)", totalVal - totalCost]);
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
      homeSheet.getRange(reportRow, 12, tbl.length, 1).setNumberFormat("#,##0.00");
      paintReportTable_(homeSheet, reportRow - 1, 11, tbl.length + 1, 2);
      reportRow += tbl.length + 2;
    }
  });
}

function showDrillDown(ticker, homeSheet, startRow) {
  const allRows = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("תמונת מצב").getDataRange().getValues().slice(1).filter(function (r) { return cleanText(r[3]) !== ""; });
  if (allRows.length === 0) return;
  let currentRow = startRow;
  if (ticker === "בחר הכל") {
    const allTickers = {};
    allRows.forEach(function (r) { allTickers[r[3]] = true; });
    Object.keys(allTickers).sort().forEach(function (t) { currentRow = drawSingleAssetTable(t, allRows, homeSheet, currentRow); });
  } else {
    drawSingleAssetTable(ticker, allRows, homeSheet, currentRow);
  }
}

function drawSingleAssetTable(ticker, allRows, homeSheet, startRow) {
  const history = allRows.filter(function (r) { return r[3] === ticker; });
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

  const body = history.map(function (r) { return [r[4], r[1], parseNum(r[5]), parseNum(r[6]), parseNum(r[14]), parseNum(r[16]), parseNum(r[21]), r[10]]; });
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
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const mainSheet = ss.getSheetByName("תמונת מצב");
  if (!mainSheet) return;
  const data = mainSheet.getDataRange().getValues();
  ["אקסלנס", "Bit2C", "הורייזון"].forEach(function (platform) {
    const targetSheet = ss.getSheetByName(platform);
    if (!targetSheet) return;
    if (targetSheet.getLastRow() > 1) targetSheet.getRange(2, 1, targetSheet.getLastRow(), 23).clearContent();
    const filteredData = data.filter(function (row) { return cleanText(row[1]) === platform; });
    if (filteredData.length > 0) {
      targetSheet.getRange(2, 1, filteredData.length, filteredData[0].length).setValues(filteredData);
      const formulas = mainSheet.getRange(2, 1, 1, 23).getFormulas()[0];
      for (let col = 0; col < 23; col++) {
        if (formulas[col] !== "") {
          mainSheet.getRange(2, col + 1).copyTo(targetSheet.getRange(2, col + 1, filteredData.length, 1), SpreadsheetApp.CopyPasteType.PASTE_FORMULA, false);
        }
      }
      mainSheet.getRange(2, 1, 1, 23).copyTo(targetSheet.getRange(2, 1, filteredData.length, 23), SpreadsheetApp.CopyPasteType.PASTE_FORMAT, false);
    }
  });
}


