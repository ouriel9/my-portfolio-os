export const meta = {
  name: 'portfolio-os-design-audit',
  description: 'STREAMLIT-ONLY design audit (phone + desktop, he/en × light/dark) — desktop/phone reviewers + English-Dark, Translation, and Completeness specialists → verify → manager work plan',
  phases: [
    { title: 'Review', detail: 'design experts per surface review EVERY screenshot meticulously' },
    { title: 'Verify', detail: 'independent skeptics refute/confirm each medium+ finding against the actual screenshot' },
    { title: 'Manage', detail: 'manager dedups CONFIRMED findings + scores design /1000 + emits a prioritized design work plan' },
  ],
}

const SHOTS = 'C:/Users/ourie/Desktop/Portfolio OS/my-portfolio-os/_design_shots'
const P = {
  streamlit: 'C:/Users/ourie/Desktop/Portfolio OS/my-portfolio-os/app.py',
  streamlitCfg: 'C:/Users/ourie/Desktop/Portfolio OS/my-portfolio-os/.streamlit/config.toml',
  nativeWeb: 'C:/Users/ourie/Desktop/Portfolio OS/PortfolioOS-Native/web',
  nativeMobile: 'C:/Users/ourie/Desktop/Portfolio OS/PortfolioOS-Native/mobile',
}

// Surfaces are DISTINCT (no duplicates): the two EXEs share native web/; the generic native APK
// is native mobile/; the personalized APK is the Streamlit phone in a WebView. So fixing a surface
// fixes BOTH products in its pair.
const PRODUCT_MAP =
  `Streamlit DESKTOP = Streamlit on a PC browser. Streamlit PHONE = Streamlit on a phone browser AND the personalized ` +
  `APK (a WebView of it) — same UI. Native WEB (web/) = BOTH the owner EXE and the generic EXE (identical UI). Native ` +
  `MOBILE (mobile/) = the generic native APK. So a fix to web/ lands on both EXEs; a fix to mobile/ lands on the APK; ` +
  `a fix to app.py lands on Streamlit desktop+phone AND the personalized APK.`

const BRIEFING =
  `This is a DESIGN-ONLY review. Judge what ACTUALLY RENDERS in the screenshots — layout, spacing, alignment, visual ` +
  `hierarchy, typography, color/contrast, RTL Hebrew correctness, dark/light theme correctness, consistency across pages ` +
  `and conditions, empty/loading states, overflow/truncation/clipping, density, polish, and anything that looks unfinished ` +
  `or off. The owner says STREAMLIT is the most problematic (desktop AND phone) — scrutinize it hardest.\n` +
  `DO NOT deduct for these (intentional / already fixed in prior rounds — verify present, don't re-flag):\n` +
  `- The native apps are Hebrew-only by design (no English) — only Streamlit has he/en.\n` +
  `- The native theme system is a multi-theme "design language" picker (nova=dark default, daylight=light, etc.) — richer\n` +
  `  than Streamlit's light/dark+System; that's intentional, not a defect.\n` +
  `- Already fixed: empty-state pills/tables now show a neutral em-dash / centered '.empty' message (not a red 0%);\n` +
  `  modals have a Close button + focus styling; the lock 🔒 FAB sits bottom-inline-end (off the add-trade/agent composer);\n` +
  `  the st.dataframe grid theme is correct in all 4 device×app combos; the loading bar completes (no 90% park).\n` +
  `- The app lock is a deterrent; demo data is intentional showcase content.\n` +
  `ALREADY FIXED in design round 1 (verify PRESENT in the screenshots; do NOT re-flag as open):\n` +
  `- Streamlit sidebar Settings tabs (Sync/Connect/Lock/General) now render FULLY (no 'Sy/C/Loc/Ge' truncation).\n` +
  `- Streamlit Hebrew dashboard status-line numbers now attach to the correct labels (bidi-isolated).\n` +
  `- Streamlit demo hero card + Connect Web-App-URL/API-Token labels are translated to Hebrew.\n` +
  `- Native RTL negative sign now LEADS ('-20.00%' / '-745', not '20.00%-') in web+mobile incl. the treemap.\n` +
  `- Native-mobile bottom stack destacked: the demo 'יציאה' exit button is fully visible and the lock FAB lifts above it.\n` +
  `- Native-web donut by-ticker legend no longer clips rows; treemap small tiles are labeled; rebalance no longer shows 0/0/0.\n` +
  `- Native-mobile risk health bars are visible + show %, app-bar de-crowded (text 'דמו' chip not emoji), avatar shows full ticker.\n` +
  `ALREADY FIXED in design round 2 (verify PRESENT; do NOT re-flag):\n` +
  `- Streamlit sidebar Settings tabs now WRAP to a 2x2 grid with FULL labels (no 'Sy/C/Loc/Ge' / 'Con'/'Gener' clip) — white-space:normal, no ellipsis.\n` +
  `- Streamlit KPI cards are EQUAL height (the Closed-Positions 'N open' delta no longer makes that card taller); in LIGHT mode the non-hero cards have a defined surface (border+shadow) so they don't vanish into the #f6f8fd page. The TOTAL VALUE indigo-gradient card is an INTENTIONAL hero-metric (one premium headline figure) — do NOT deduct it as a 'selected' card.\n` +
  `- Streamlit Crypto-Concentration table: the long 'Coin qty (incl. ETF)' header was shortened + given a wider column → header renders in full (no mid-word clip).\n` +
  `- Streamlit allocation donut: sub-2.5% slices fold into one 'Other' wedge, %-only sits INSIDE the ring, and the legend lists EVERY wedge (no 6-of-10 legend, no crowded outside leader-lines jammed at the right edge).\n` +
  `- Streamlit Hebrew Connect: 'Spreadsheet URL or ID' is now Hebrew ('כתובת או מזהה גיליון'); the 'Force pull' label was shortened ('שליפה כפויה מהענן') so it fits the ~250px sidebar.\n` +
  `- Native web + mobile: the demo banner no longer overlaps scroll content (reserved bottom padding on .view-wrap / .m-scroll) — donut legend, simulator hero, risk/report tails all clear it.\n` +
  `- Native-mobile: the add '+' FAB is HIDDEN in demo (editing is disabled there) so it is never half-clipped by the banner; the lock FAB has a solid opaque backplate so it never bleeds over card rows; the Risk health-gauge is capped (≤112px) so the part-% values no longer clip off the left edge.\n` +
  `- Native-web dashboard 'by asset-class' donut card: the legend is content-sized + centered with the donut (no stretched-legend dead gap).\n` +
  `- Native web + mobile: data-completeness % is threshold-colored (green ≥90 / amber 70-90 / red <70) — 94.1% now reads GREEN, not alarm-red.\n` +
  `- Native web + mobile rebalance: the no-target state shows an em-dash header + BLANK placeholder inputs (not a literal '0') + a 'set targets' message.\n` +
  `ALREADY FIXED in design round 3 (verify PRESENT; do NOT re-flag):\n` +
  `- native-web LIGHT active sidebar nav pill is now a saturated indigo gradient with white text on every view (was washed-out gray post-navigation).\n` +
  `- native-mobile demo lock FAB moved to the START gutter (physical RIGHT in RTL) so it no longer sits over the left-aligned value column (gain%/concentration%/legend-% are no longer clipped); the add '+' FAB stays hidden in demo.\n` +
  `- native-mobile agent chat composer is hidden in demo (it used to sit buried behind the demo banner).\n` +
  `- native-web add '+' FAB is hidden in demo (no longer overlaps the demo banner).\n` +
  `- native-mobile demo scroll padding raised to var(--tabh)+240px so the last holding row + simulator sub-pills clear the banner; the 'מקומי' build-id chip is hidden in demo to de-crowd the app bar.\n` +
  `- Streamlit Hebrew Crypto-Concentration headers: HE labels shortened ('ישיר (כמות)','קרן סל (יח׳)') + HE report columns widened (medium/large) → no left-edge header clip.\n` +
  `- Streamlit phone EN dashboard 4th sub-tab is now 'Activity' (was 'Transactions' clipped to 'Transaction').\n` +
  `- Streamlit phone: the three non-hero KPI cards (P&L/Return/Closed) now share ONE defined surface (border+shadow, white in light) — only TOTAL VALUE is the gradient hero (no more EN light 'double hero'), and they no longer vanish on the #f6f8fd page.\n` +
  `- Streamlit Net-P/L bar chart: y-axis uses automargin so the HE title 'רווח/הפסד (₪)' isn't clipped; axis currency is ₪ (not 'ILS'); EN nav label is Title-Case 'Agent AI'.\n` +
  `ALREADY FIXED in design round 4 (verify PRESENT; do NOT re-flag):\n` +
  `- Streamlit Hebrew Crypto-Concentration table shows ALL 7 columns again (the r40 'force HE medium' that dropped 3 columns was reverted to small-default).\n` +
  `- The ₪ (shekel) glyph renders correctly on KPI labels + table headers — a Heebo webfont covering ₪ is now loaded app-wide.\n` +
  `- Streamlit light-mode sidebar text inputs now have a visible border + faint fill (were white-on-white).\n` +
  `- Native (web + mobile) demo banner is now a FLOW element (web: appended to .main after .view-wrap; mobile: inserted above the tab bar) so it is ALWAYS visible and NEVER overlaps scroll content. NO floating FABs are shown in demo (add '+' and lock FAB both hidden) — so nothing floats over values/avatars/donut.\n` +
  `- Native-mobile: simulator hero value clamps to fit (no digit clip); the Risk score gauge is separated from the health bars (own column, gap); Holdings gain ▲/% stays on one line.\n` +
  `CRITICAL CAPTURE CAVEAT for the NATIVE apps (web + mobile): they use an INTERNAL scroll container (.view-wrap / .m-scroll) inside a fixed 100vh shell, so a full-page screenshot only shows the FIRST viewport — content below the fold is NOT in the image. Do NOT deduct for a 'last card / donut legend / FIFO table / projection chart appearing cut at the bottom of the viewport' on native — that is the capture not scrolling the inner container, NOT an app defect (the user simply scrolls to see it). Only flag a native clipping issue if an element is overlapped by a FIXED element WITHIN the visible viewport. (Streamlit uses normal document scroll, so its full-page captures ARE complete — judge Streamlit clipping normally.)\n` +
  `ALREADY FIXED in design round 5 (verify PRESENT; do NOT re-flag):\n` +
  `- Streamlit Allocation-tab donut has an explicit height so its 2-row legend always seats (no clip); on phone the legend font is smaller, sub-4% slices fold into 'Other', and % that can't fit inside a wedge is HIDDEN (uniformtext) rather than ejected as gray outside text — so 'NVDA' isn't clipped.\n` +
  `- Streamlit Build-Up chart: the value badge is xanchor=right (no '(+46.1%)' overflow) and on phone the legend is centered + small (no clipped 2nd series name).\n` +
  `- Streamlit TREEMAP (מפת חום): per OWNER REQUEST every tile shows its yield % even the tiniest (uniformtext mode=show + compact ticker+% label) — do NOT flag small-tile labels as 'clipping/overflow'; showing the % on every tile is INTENTIONAL. On phone the title is compact ('מפת חום של התיק') with a caption.\n` +
  `- Streamlit dark KPI: P&L green/red are brighter on the dark cards (no longer desaturated).\n` +
  `- native-mobile hero VALUE is white in the light/daylight theme (was dark-on-blue); holdings rows have a comfortable value-column gutter.\n` +
  `- native simulator capital-gains-tax negative sign now leads adjacent to ₪ (bidi-isolated), web + mobile.\n` +
  `- native-web agent chat composer is hidden in demo (matches mobile); the agent quick-prompt chips are equal-height.\n` +
  `- native dashboard growth chart (area2) now has x-axis date ticks + already had a 2-series legend (web + mobile).\n` +
  `- The NEW app logo (ascending bars + growth arrow on an indigo→violet→cyan gradient) is the brand mark on every surface — do not flag it.\n` +
  `ALREADY FIXED in design round 6 (verify PRESENT; do NOT re-flag):\n` +
  `- native-web DARK below-the-fold white-bleed: .view-wrap now paints background:var(--bg) so every card/table at any scroll depth has the dark backdrop (no white-on-dark bleed).\n` +
  `- Streamlit Hebrew sidebar is RIGHT-aligned (widget labels, captions, checkbox rows flip the box to the right, version line) — no longer LTR-hugging-left. (Latin URL/Token/JSON-path INPUT values intentionally stay LTR.)\n` +
  `- Streamlit HE: the settings 2x2 grid + main dashboard tablists are direction:rtl so the default/Sync tab anchors top-RIGHT and tabs read R→L.\n` +
  `- Streamlit settings 2x2 tabs use a smaller font so the (wider Heebo) Connect/General · חיבור/כללי labels fit the cells on desktop without clipping.\n` +
  `- native-mobile FIFO realized empty rows render a NEUTRAL muted '—' (not success-green); the rebalance drift chip shows a clean muted '₪0' for zero drift (no '+0-₪') with spaced slash '32% / 32%'; the holdings 'מקור %' / units / share segments are each nowrap <bdi> (no mid-segment wrap); the first summary chip seats fully (scroll-padding).\n` +
  `- native-web Allocation 'לפי סוג נכס' donut+legend is centered (donut-wrap--center), no stretched gap.\n` +
  `ALREADY FIXED in design round 7 (verify PRESENT; do NOT re-flag):\n` +
  `- Streamlit PRIMARY buttons (Save-code / 'סנכרן עכשיו' / form submits) now render with the indigo→violet gradient + white text (were default/washed-out) on both themes.\n` +
  `- Streamlit phone overflow-prone tables (exposure, transactions/activity) render as STACKED CARDS (one card per row, label:value pairs) instead of a horizontally-scrolling grid — that is the INTENTIONAL mobile layout, NOT a 'missing/clipped table'. Do NOT flag the phone tables as cut off.\n` +
  `- Streamlit KPI metric VALUES share a baseline: the metric label has a min-height so a 2-line label (e.g. 'פוזיציות סגורות') no longer pushes its number lower than the single-line cards — the four big figures align on one row.\n` +
  `- native (web + mobile) money() puts a thin space after the ₪ glyph for kerning (₪ 58,071) — that small gap is INTENTIONAL typographic kerning, NOT a spacing bug. The RTL money string is bidi-isolated so the sign/₪/digits never reorder.\n` +
  `ALREADY FIXED in design round 8 (verify PRESENT; do NOT re-flag):\n` +
  `- Streamlit Risk 'Scenario Lab' (מעבדת תרחישים) table column HEADERS are now localized in the Hebrew view (תרחיש/הלם/שווי משוער (₪)/רווח/הפסד (₪)) — they used to read 'Scenario/Shock/Estimated Value (ILS)/Estimated P/L (ILS)' in English over Hebrew rows.\n` +
  `NEW CAPTURE NOTE: the Streamlit captures are now TRUE full-page top→bottom (the prior rounds' phone/desktop captures were clipped at the first viewport, hiding ~70-90% of the deep tabs). So you are seeing the treemap, crypto-concentration cards, exposure/transactions tables, the scenario lab, the advanced-analytics gauge + drawdown chart, and the ENTIRE FIFO list for the FIRST time — judge this below-the-fold content normally and thoroughly (it is the richest source of remaining real defects). The 'analytics summary' Metric/Value table with English metric names is EXPORT-ONLY (a download CSV, not rendered on screen) — do NOT flag it as a visible defect.\n` +
  `Carry-forward INTENTIONAL design decisions (do NOT deduct):\n` +
  `- OWNER PREFERENCE (partly overrides the round-7 'mobile card stack' decision): on the Streamlit PHONE, the ` +
  `EXPOSURE, TRANSACTIONS/Activity and FIFO tables render as REAL st.dataframe TABLES that SCROLL HORIZONTALLY — ` +
  `NOT per-row card stacks. The owner prefers the scrollable table and is fine that not every column is visible ` +
  `without scrolling sideways. So do NOT flag those phone tables as 'overflowing / clipped at the right / not all ` +
  `columns visible / should be cards' — horizontal scroll with off-screen columns is INTENTIONAL. EXCEPTION: the ` +
  `REPORTS tab (crypto-concentration / winner-loser / net-investment-by-platform) STILL renders as per-row CARDS on ` +
  `phone by owner request ('it was nice there') — do NOT flag the Reports cards as 'should be a table' either. ` +
  `(None of this applies to CHARTS — donut legend / treemap tile / bar tick clipping are still real defects to fix.)\n` +
  `- INTENTIONAL (owner request, do NOT flag): the Streamlit MAIN dashboard sub-tabs (and the phone top-nav) ` +
  `read LEFT→RIGHT with 'סקירה'/Overview as the LEFTMOST/active tab EVEN in Hebrew. The owner explicitly asked ` +
  `for this (he rejected the RTL tab flip because it reversed the swipe). Do NOT flag 'HE tabs should be RTL / ` +
  `active should anchor right' — LTR tabs with Overview leftmost is correct and deliberate here.\n` +
  `- INTENTIONAL (owner request): in the HEBREW Streamlit tables, Latin platform/location VALUES are ` +
  `transliterated to Hebrew (e.g. 'Bit2C'→'ביטוסי', 'Global Prime'→'גלובל פריים', 'Cold Wallet'→'ארנק קר') ` +
  `so a column mixing Hebrew + Latin lines up to one side instead of alternating and leaving dead space. ` +
  `Do NOT flag these Hebraized names as 'wrong/inconsistent/should be English' — it is deliberate. TICKER ` +
  `columns (AAPL/VOO/BTC) correctly STAY Latin. (English view keeps the Latin values.)\n` +
  `- The native-mobile 'מקור' (origin-yield) holdings segment appears ONLY on foreign-currency rows — that is correct (ILS rows have no separate origin yield), not an inconsistency.\n` +
  `- The treemap shows a % on EVERY tile including tiny ones by owner request — tiny-tile text that is small is intentional, not a defect.\n` +
  `- English data tables may use the ISO code 'ILS' in column headers while headline KPI/charts use the '₪' glyph — acceptable (symbol for headlines, ISO code for detailed tables).\n` +
  `- The 'Worksheet name' Connect field defaults to the Hebrew 'תמונת מצב' even in English because that is the ACTUAL Google-Sheet tab name (changing the displayed value would break sync).\n` +
  `- The Streamlit sidebar hamburger stays on the LEFT in Hebrew because the Streamlit drawer slides in from the left; moving only the trigger to the right would break the 'opens-from' mental model.\n` +
  `- 'Apps Script Web App URL' and 'Service Account JSON path' correctly keep the brand/tech terms LTR with a Hebrew lead word — that is correct RTL+brand handling.\n` +
  `GOAL: an HONEST design score /1000 per surface; only deduct for a real visual defect a careful designer would fix. ` +
  `If a screen is genuinely polished, score it high. Report EVERY genuine issue — there is NO limit; be exhaustive and precise.`

const VISUAL =
  `You MUST look at the apps. FIRST run \`ls "${SHOTS}"\` (Bash) to list every screenshot. Filenames encode the condition: ` +
  `streamlit_<desktop|phone>_<he|en>_<light|dark>_<NN>_<view>.png and native_<web|mobile>_<light|dark>_<NN>_<view>.png ` +
  `(each is a TRUE FULL-PAGE capture, the whole scroll top→bottom). Read EVERY screenshot for your surface — every page, every ` +
  `language, both themes. Do NOT sample or skim; the owner requires that everything is actually looked at. Cite the exact ` +
  `screenshot filename in each finding.\n` +
  `CRITICAL — TALL IMAGES: these full-page captures are VERY TALL (Streamlit phone tabs and the Risk/Simulator pages are ` +
  `8,000–16,000 px). If you Read such an image whole, it is downscaled so far that small text (table headers, axis labels, ` +
  `legend %, fine spacing) becomes unreadable and you WILL miss real defects (e.g. an English column header sitting over ` +
  `Hebrew rows, a mislabeled axis, a clipped cell). So for any image taller than ~2000px you MUST CROP IT INTO VERTICAL ` +
  `SECTIONS and Read each crop at full resolution before judging. Use Bash + PIL, e.g.:\n` +
  `  python -c "from PIL import Image; im=Image.open(r'${SHOTS}/<file>.png'); H=im.size[1];\\n` +
  `  [im.crop((0,y,im.size[0],min(y+1600,H))).save(rf'${SHOTS}/_c_{y}.png') for y in range(0,H,1600)]"\n` +
  `then Read each _c_*.png. Inspect the WHOLE height this way — the below-the-fold tables/charts (treemap, crypto ` +
  `concentration, exposure, transactions, scenario lab, advanced analytics, the full FIFO list) are exactly where the ` +
  `remaining defects live, because earlier rounds only ever saw the top viewport. Clean up your _c_*.png crops when done.\n` +
  `If you need a condition that's missing, capture it via Bash (_design_capture.py / _diag_*.py boot the Streamlit app and ` +
  `serve native web/ + mobile/ in real Chrome).`

const FINDINGS_SCHEMA = {
  type: 'object', additionalProperties: false,
  required: ['surface', 'findings', 'scores', 'screenshots_reviewed', 'notes'],
  properties: {
    surface: { type: 'string' },
    findings: { type: 'array', description: 'EVERY genuine design issue — no cap', items: {
      type: 'object', additionalProperties: false,
      required: ['screenshot', 'condition', 'area', 'severity', 'issue', 'how_to_fix', 'file'],
      properties: {
        screenshot: { type: 'string', description: 'exact filename evidencing it' },
        condition: { type: 'string', description: 'e.g. phone/he/dark' },
        area: { type: 'string', description: 'page/component' },
        severity: { type: 'string', enum: ['low', 'medium', 'high', 'critical'] },
        issue: { type: 'string', description: 'what looks wrong, concretely' },
        how_to_fix: { type: 'string' },
        file: { type: 'string', description: 'likely file to fix (app.py / web/js/app.js / mobile/app.js / styles.css / config.toml)' },
      },
    } },
    scores: { type: 'array', items: {
      type: 'object', additionalProperties: false,
      required: ['condition', 'score', 'rationale'],
      properties: { condition: { type: 'string' }, score: { type: 'number' }, rationale: { type: 'string' } },
    } },
    screenshots_reviewed: { type: 'array', items: { type: 'string' } },
    notes: { type: 'string' },
  },
}

const SURFACES = [
  // STREAMLIT-ONLY (owner redirected all effort here; native EXE/APK dropped). Five reviewers: two
  // per-breakpoint + three cross-cutting specialists (English+Dark, Translations, Completeness).
  { key: 'streamlit-desktop', who: 'a senior UI designer reviewing the Streamlit app on DESKTOP', prefix: 'streamlit_desktop_', file: P.streamlit + ' (+ ' + P.streamlitCfg + ')', extra: `Scrutinize EVERY desktop page in he AND en, light AND dark: dashboard + all sub-tabs + the Trade/Risk/Simulator/Data/Agent nav pages + Settings. Sidebar layout/width, KPI cards, charts, dataframe tables, spacing/density on a wide screen, he-vs-en text fit, light-vs-dark correctness.` },
  { key: 'streamlit-phone', who: 'a senior mobile UI designer reviewing the Streamlit app on PHONE', prefix: 'streamlit_phone_', file: P.streamlit + ' (+ ' + P.streamlitCfg + ')', extra: `Scrutinize EVERY phone page in he AND en, light AND dark: collapsed sidebar/overlay, RTL Hebrew, tap targets, horizontal overflow, chart/table fit on a narrow screen, the loading bar, he-vs-en, light-vs-dark. Covers the personalized APK too (same UI).` },
  { key: 'streamlit-dark-en', who: 'a reviewer auditing the ENGLISH + DARK Streamlit surface specifically (owner flagged this)', prefix: 'streamlit_', file: P.streamlit, extra: `FOCUS on the *_en_dark_* screenshots (desktop AND phone), EVERY page. Hunt for anything wrong in English-dark: low-contrast/illegible text, a control/card/table/header/axis that stayed light/white in dark mode, white-on-white or black-on-dark labels, an un-themed widget, a chart with dark-on-dark text, a tooltip/badge/pill/legend with bad contrast. Compare en-dark vs en-light and vs he-dark to catch dark-mode-only or English-only regressions.` },
  { key: 'streamlit-translation', who: 'a bilingual (Hebrew/English) localization reviewer of the Streamlit app', prefix: 'streamlit_', file: P.streamlit, extra: `FOCUS on TRANSLATION CORRECTNESS across every page, he AND en. Flag: untranslated keys/labels (raw snake_case or English leaking into the HEBREW view, or Hebrew leaking into the ENGLISH view); a chart AXIS TITLE / legend / table HEADER / button / caption / metric label / tab name in the WRONG language; mixed-language in one label; a column header still English in HE (or vice-versa); 'ILS' vs '₪' mismatches; any string localized in one view but not the other. Quote the exact wrong text, the view it appears in, and what it should say.` },
  { key: 'streamlit-completeness', who: 'a data-completeness reviewer of the Streamlit app (owner: "everything must show in every table & every chart")', prefix: 'streamlit_', file: P.streamlit, extra: `FOCUS on COMPLETENESS — does every TABLE and every CHART render ALL of its data/labels with nothing missing/empty/blank/cut/unlabeled? TABLES: all expected columns present (none silently dropped), all cells populated (no stray all-empty / all-0 / all-'—' column), headers fully shown, no horizontal clip that hides a column without a scroll affordance. CHARTS/figures: ALL series drawn, legend lists EVERY series, x AND y axis ticks + titles present and unclipped, every bar/wedge/tile labeled with its value/%, no empty/blank plot, no '0.0000'/'NaN'/'None' artifact. Go page by page (dashboard tabs + Trade/Risk/Simulator/Data/Agent) in he/en × light/dark and list anything missing or blank.` },
]

phase('Review')
const reviews = (await parallel(SURFACES.map(s => () =>
  agent(
    `You are ${s.who}.\nProduct mapping: ${PRODUCT_MAP}\n\n${BRIEFING}\n\n${VISUAL}\n\n` +
    `YOUR SURFACE: ${s.key}. ${s.extra}\n` +
    (s.prefix ? `Focus on screenshots named "${s.prefix}*" (read ALL of them — every page, language, theme).` : `Read a representative set across ALL surfaces/conditions.`) +
    `\nLikely fix file(s): ${s.file}.\n\nReport EVERY genuine design issue (no limit) with the exact screenshot filename, the condition, the area, severity, what looks wrong, and how to fix it (+ the file). Give a 0-100 design score per condition you reviewed (100 = genuinely polished, ship-ready). Be exhaustive and precise — the owner wants design perfection, Streamlit especially.`,
    { label: 'design:' + s.key, phase: 'Review', schema: FINDINGS_SCHEMA }
  ).then(r => r ? { ...r, dim: s.key } : null)
))).filter(Boolean)

const totalFindings = reviews.reduce((n, r) => n + (r.findings ? r.findings.length : 0), 0)
log(`Design review: ${reviews.length} surfaces, ${totalFindings} design findings (no cap)`)

// ───────── Verify: an independent skeptic looks at the cited screenshot and tries to REFUTE each
// medium+ finding. This filters phantom nitpicks / by-design / capture-artifact claims so the score
// is HONEST and the work plan only contains real, fixable defects. Low-severity pass through unverified.
phase('Verify')
const VERDICT_SCHEMA = {
  type: 'object', additionalProperties: false,
  required: ['verdict', 'reason', 'real_severity'],
  properties: {
    verdict: { type: 'string', enum: ['CONFIRMED', 'REFUTED'] },
    reason: { type: 'string', description: 'cite what you actually see in the screenshot' },
    real_severity: { type: 'string', enum: ['low', 'medium', 'high', 'critical'], description: 'severity you would assign if CONFIRMED (may be lower than claimed)' },
  },
}
const flat = []
for (const r of reviews) for (const f of (r.findings || [])) flat.push({ ...f, surface: r.dim })
const toVerify = flat.filter(f => f.severity === 'medium' || f.severity === 'high' || f.severity === 'critical')
const passthrough = flat.filter(f => f.severity === 'low')
const verified = (await parallel(toVerify.map(f => () =>
  agent(
    `You are a SKEPTICAL design verifier. A reviewer claims a design defect. Your job: look at the ACTUAL screenshot and ` +
    `decide if it is a REAL, currently-present visual defect — or REFUTED (by-design, already-fixed, a native inner-scroll ` +
    `capture artifact, an intentional decision, or simply not visible/true in the image).\n\n${PRODUCT_MAP}\n\n${BRIEFING}\n\n` +
    `Open the screenshot with the Read tool: "${SHOTS}/${f.screenshot}". Look carefully at the cited area.\n\n` +
    `CLAIM (surface=${f.surface}, condition=${f.condition}, area=${f.area}, claimed severity=${f.severity}):\n` +
    `"${f.issue}"\n\nProposed fix: ${f.how_to_fix}\n\n` +
    `Return CONFIRMED only if you can SEE the defect in this screenshot and it is not covered by the 'do NOT deduct' / ` +
    `already-fixed / intentional list above. Return REFUTED otherwise. Default to REFUTED when the claim is vague, subjective ` +
    `taste, not visible, or explained by the native inner-scroll capture caveat. Give the real severity you'd assign.`,
    { label: 'verify:' + (f.screenshot || f.surface).slice(0, 28), phase: 'Verify', schema: VERDICT_SCHEMA }
  ).then(v => v ? { ...f, _verdict: v.verdict, _reason: v.reason, _real_severity: v.real_severity } : { ...f, _verdict: 'CONFIRMED', _reason: 'verifier unavailable — kept', _real_severity: f.severity })
))).filter(Boolean)
const confirmed = verified.filter(f => f._verdict === 'CONFIRMED').map(f => ({ ...f, severity: f._real_severity }))
const refuted = verified.filter(f => f._verdict === 'REFUTED')
log(`Verify: ${confirmed.length} CONFIRMED, ${refuted.length} REFUTED of ${toVerify.length} medium+ findings (+${passthrough.length} low passed through)`)

// Rebuild per-surface confirmed findings for the manager (confirmed medium+ ∪ low passthrough)
const confirmedAll = [...confirmed, ...passthrough]
const bySurface = {}
for (const r of reviews) bySurface[r.dim] = { surface: r.dim, findings: [], scores: r.scores, notes: r.notes }
for (const f of confirmedAll) { (bySurface[f.surface] || (bySurface[f.surface] = { surface: f.surface, findings: [], scores: [], notes: '' })).findings.push(f) }
const confirmedReviews = Object.values(bySurface)

phase('Manage')
const plan = await agent(
  `You are the DESIGN MANAGER. ${reviews.length} per-surface design experts reviewed the Portfolio OS surfaces from full-page ` +
  `screenshots in every condition (he/en × light/dark). Independent skeptics then VERIFIED every medium+ finding against the ` +
  `actual screenshot: ${confirmed.length} were CONFIRMED, ${refuted.length} were REFUTED (phantom/by-design/capture-artifact) and ` +
  `DROPPED. Only CONFIRMED findings (+ minor low-severity passthrough) are given to you below. Product mapping: ${PRODUCT_MAP}\n\n${BRIEFING}\n\n` +
  `Your job: (1) DEDUP/reconcile (a defect visible on a shared surface affects BOTH products in its pair — say so; merge duplicates ` +
  `across the per-surface and cross-consistency reviewers). (2) Anything by-design/already-fixed was already filtered — but if any ` +
  `slipped through, record it under unjustified_deductions. (3) Score DESIGN /1000 overall + per-surface (streamlit-desktop, ` +
  `streamlit-phone) — this is a STREAMLIT-ONLY audit (native dropped); also call out the worst TRANSLATION and COMPLETENESS and ` +
  `ENGLISH-DARK issues explicitly. Score based ONLY on the CONFIRMED defects — if a surface has no confirmed defects it is ` +
  `genuinely ship-ready and should score 980+. Be honest: do NOT invent a gap to justify a sub-1000 score; a polished surface with ` +
  `only nitpicks scores high. (4) Produce a PRIORITIZED design WORK PLAN — ordered concrete fixes, each with the screenshot ` +
  `evidence, the file, and a clear how-to-fix, grouped for efficient execution, weighting Streamlit highest. Be decisive and complete.\n\n` +
  `## CONFIRMED design findings (post-verification)\n${JSON.stringify(confirmedReviews.map(r => ({ surface: r.surface, findings: r.findings, scores: r.scores, notes: r.notes })))}`,
  { label: 'design-manager', phase: 'Manage', schema: {
    type: 'object', additionalProperties: false,
    required: ['overall_design_1000', 'surface_scores', 'work_plan', 'unjustified_deductions', 'summary'],
    properties: {
      overall_design_1000: { type: 'number' },
      surface_scores: { type: 'array', items: { type: 'object', additionalProperties: false, required: ['surface', 'score_1000', 'top_issue'], properties: { surface: { type: 'string' }, score_1000: { type: 'number' }, top_issue: { type: 'string' } } } },
      work_plan: { type: 'array', items: { type: 'object', additionalProperties: false, required: ['priority', 'title', 'problem', 'how_to_fix', 'file', 'surfaces', 'screenshot'], properties: { priority: { type: 'string' }, title: { type: 'string' }, problem: { type: 'string' }, how_to_fix: { type: 'string' }, file: { type: 'string' }, surfaces: { type: 'string' }, screenshot: { type: 'string' } } } },
      unjustified_deductions: { type: 'array', items: { type: 'string' } },
      summary: { type: 'string' },
    },
  } }
)
return { surfaces: reviews.length, total_findings: totalFindings, confirmed: confirmed.length, refuted: refuted.length, refuted_examples: refuted.slice(0, 12).map(f => ({ screenshot: f.screenshot, issue: f.issue, reason: f._reason })), plan }
