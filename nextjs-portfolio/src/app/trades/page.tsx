'use client';

import { useMemo, useState, useCallback } from 'react';
import { useApp } from '@/components/providers/AppProvider';
import { useSnapshot } from '@/components/hooks/useSnapshot';
import { PageHeader } from '@/components/ui/PageHeader';
import { LoadingBlock, ErrorBlock, EmptyBlock } from '@/components/ui/StateBanners';
import { formatCurrency, formatNumber } from '@/lib/format';
import type { Trade } from '@/lib/types';

// ── Types ──────────────────────────────────────────────────────────────────

type Mode = 'add' | 'edit' | 'delete';
type Side = 'BUY' | 'SELL';

interface TradeForm {
  currentLocation: string;
  platform: string;
  type: string;
  ticker: string;
  currency: string;
  purchaseDate: string;
  quantity: string;
  buyPrice: string;
  costOrigin: string;
  commission: string;
  status: string;
  sellDate: string;
  sellPrice: string;
  action: Side;
}

const EMPTY_FORM: TradeForm = {
  currentLocation: '',
  platform: '',
  type: 'קריפטו',
  ticker: '',
  currency: 'USD',
  purchaseDate: new Date().toISOString().slice(0, 10),
  quantity: '',
  buyPrice: '',
  costOrigin: '',
  commission: '0',
  status: 'פתוח',
  sellDate: new Date().toISOString().slice(0, 10),
  sellPrice: '',
  action: 'BUY',
};

function tradeToForm(t: Trade): TradeForm {
  // Normalize every closed-status variant (English or Hebrew — same set as
  // lib/snapshot.ts isClosedStatus) to the canonical 'סגור' the status select
  // and Code.gs sell-date logic expect. Without this, editing a trade whose
  // sheet status is 'closed'/'sold'/'close'/'נמכר' sent the raw value through,
  // Code.gs saw nowClosed=false and silently reset its sell date to 'עדיין פתוח'.
  const closed = ['סגור', 'closed', 'close', 'sold', 'נמכר'].includes(
    (t.status ?? '').toLowerCase().trim(),
  );
  return {
    currentLocation: t.currentLocation ?? '',
    platform: t.platform ?? '',
    type: t.type ?? 'קריפטו',
    ticker: t.ticker ?? '',
    currency: t.currency ?? 'USD',
    purchaseDate: t.purchaseDate ?? new Date().toISOString().slice(0, 10),
    quantity: String(t.quantity ?? ''),
    buyPrice: String(t.buyPrice ?? ''),
    costOrigin: String(t.costOrigin ?? ''),
    commission: String(t.commission ?? '0'),
    status: closed ? 'סגור' : 'פתוח',
    sellDate: t.sellDate || new Date().toISOString().slice(0, 10),
    sellPrice: String(t.sellPrice ?? ''),
    action: closed ? 'SELL' : 'BUY',
  };
}

// ── Small helpers ──────────────────────────────────────────────────────────

function unique(arr: string[]): string[] {
  return [...new Set(arr.filter(Boolean))].sort();
}

function parseNum(v: string): number {
  const n = parseFloat(v.replace(/,/g, ''));
  return isNaN(n) ? 0 : n;
}

function today(): string {
  return new Date().toISOString().slice(0, 10);
}

// ── Sub-components ─────────────────────────────────────────────────────────

function FieldLabel({ children }: { children: React.ReactNode }) {
  return (
    <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
      {children}
    </label>
  );
}

function TextInput({
  value,
  onChange,
  placeholder,
  list,
  className,
  type = 'text',
  step,
  min,
}: {
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  list?: string;
  className?: string;
  type?: string;
  step?: string;
  min?: string;
}) {
  return (
    <input
      type={type}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      list={list}
      step={step}
      min={min}
      className={`h-9 w-full rounded-lg border border-slate-200 bg-white px-3 text-sm text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 focus:outline-none focus:ring-2 focus:ring-emerald-500/30 ${className ?? ''}`}
    />
  );
}

function SelectInput({
  value,
  onChange,
  options,
}: {
  value: string;
  onChange: (v: string) => void;
  options: { value: string; label: string }[];
}) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className="h-9 w-full rounded-lg border border-slate-200 bg-white px-3 text-sm text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 focus:outline-none focus:ring-2 focus:ring-emerald-500/30"
    >
      {options.map((o) => (
        <option key={o.value} value={o.value}>
          {o.label}
        </option>
      ))}
    </select>
  );
}

// ── Add / Edit form ────────────────────────────────────────────────────────

function TradeFormPanel({
  isHe,
  form,
  setForm,
  platforms,
  types,
  tickers,
  currencies,
  locations,
  openTrades,
  mode,
  onSubmit,
  submitting,
  submitError,
}: {
  isHe: boolean;
  form: TradeForm;
  setForm: React.Dispatch<React.SetStateAction<TradeForm>>;
  platforms: string[];
  types: string[];
  tickers: string[];
  currencies: string[];
  locations: string[];
  openTrades: Trade[];
  mode: 'add' | 'edit';
  onSubmit: () => void;
  submitting: boolean;
  submitError: string;
}) {
  const [sellFromLot, setSellFromLot] = useState(false);
  const [sourceLotId, setSourceLotId] = useState('');

  const openLotIds = useMemo(() => openTrades.map((t) => t.tradeId).filter(Boolean), [openTrades]);

  const set = useCallback(
    (field: keyof TradeForm) => (v: string) => setForm((f) => ({ ...f, [field]: v })),
    [setForm],
  );

  const isEdit = mode === 'edit';
  const isSell = form.action === 'SELL';

  // When sell-from-lot is chosen, auto-fill fields from source lot
  const handleLotChange = (lotId: string) => {
    setSourceLotId(lotId);
    const lot = openTrades.find((t) => t.tradeId === lotId);
    if (lot) {
      setForm((f) => ({
        ...f,
        currentLocation: lot.currentLocation ?? '',
        platform: lot.platform ?? '',
        type: lot.type ?? f.type,
        ticker: lot.ticker ?? '',
        currency: lot.currency ?? 'USD',
        purchaseDate: lot.purchaseDate ?? f.purchaseDate,
        quantity: String(lot.quantity ?? ''),
        buyPrice: String(lot.buyPrice ?? ''),
        costOrigin: String(lot.costOrigin ?? ''),
        commission: String(lot.commission ?? '0'),
        status: 'סגור',
        sellDate: today(),
      }));
    }
  };

  return (
    <div className="rounded-xl border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-950">
      <div className="mb-4 text-sm font-semibold text-slate-800 dark:text-slate-100">
        {isEdit
          ? isHe ? '✏ עריכת עסקה' : '✏ Edit Trade'
          : isHe ? '➕ הוספת עסקה' : '➕ Add Trade'}
      </div>

      {/* Side selector (Add only) */}
      {!isEdit && (
        <div className="mb-4">
          <div className="mb-1.5 text-xs font-medium text-slate-500 dark:text-slate-400">
            {isHe ? 'צד העסקה' : 'Trade side'}
          </div>
          <div className="flex gap-2">
            {(['BUY', 'SELL'] as Side[]).map((s) => (
              <button
                key={s}
                onClick={() => setForm((f) => ({ ...f, action: s, status: s === 'BUY' ? 'פתוח' : 'סגור' }))}
                className={`flex-1 rounded-lg py-1.5 text-sm font-semibold transition-all ${
                  form.action === s
                    ? s === 'BUY'
                      ? 'bg-emerald-500 text-white shadow-sm'
                      : 'bg-rose-500 text-white shadow-sm'
                    : 'border border-slate-200 text-slate-500 hover:bg-slate-50 dark:border-slate-700 dark:text-slate-400 dark:hover:bg-slate-800'
                }`}
              >
                {s === 'BUY' ? (isHe ? '📈 קנייה' : '📈 Buy') : (isHe ? '📉 מכירה' : '📉 Sell')}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Sell from existing lot (Add SELL only) */}
      {!isEdit && isSell && (
        <div className="mb-4 rounded-lg border border-amber-200 bg-amber-50 p-3 dark:border-amber-800/50 dark:bg-amber-900/20">
          <label className="flex cursor-pointer items-center gap-2 text-sm text-slate-700 dark:text-slate-300">
            <input
              type="checkbox"
              checked={sellFromLot}
              onChange={(e) => {
                setSellFromLot(e.target.checked);
                if (!e.target.checked) setSourceLotId('');
              }}
              disabled={openLotIds.length === 0}
              className="rounded"
            />
            {isHe ? 'מכירה מתוך לוט פתוח קיים' : 'Sell from existing open lot'}
          </label>
          {sellFromLot && openLotIds.length > 0 && (
            <div className="mt-2">
              <SelectInput
                value={sourceLotId}
                onChange={handleLotChange}
                options={[
                  { value: '', label: isHe ? '— בחר לוט —' : '— Select lot —' },
                  ...openLotIds.map((id) => ({ value: id, label: id })),
                ]}
              />
            </div>
          )}
        </div>
      )}

      {/* Row 1: Location / Platform / Type */}
      <div className="mb-3 grid grid-cols-1 gap-3 sm:grid-cols-3">
        <div>
          <FieldLabel>{isHe ? 'מיקום נוכחי' : 'Current Location'}</FieldLabel>
          <TextInput value={form.currentLocation} onChange={set('currentLocation')} placeholder="Bit2C" list="loc-list" />
          <datalist id="loc-list">{locations.map((l) => <option key={l} value={l} />)}</datalist>
        </div>
        <div>
          <FieldLabel>{isHe ? 'פלטפורמה' : 'Platform'}</FieldLabel>
          <TextInput value={form.platform} onChange={set('platform')} placeholder="Bit2C" list="plat-list" />
          <datalist id="plat-list">{platforms.map((p) => <option key={p} value={p} />)}</datalist>
        </div>
        <div>
          <FieldLabel>{isHe ? 'סוג נכס' : 'Asset Type'}</FieldLabel>
          <SelectInput
            value={form.type}
            onChange={set('type')}
            options={
              types.length
                ? types.map((t) => ({ value: t, label: t }))
                : [{ value: 'קריפטו', label: isHe ? 'קריפטו' : 'Crypto' }, { value: 'שוק ההון', label: isHe ? 'שוק ההון' : 'Equities' }]
            }
          />
        </div>
      </div>

      {/* Row 2: Ticker / Currency */}
      <div className="mb-3 grid grid-cols-2 gap-3 sm:grid-cols-4">
        <div className="sm:col-span-2">
          <FieldLabel>{isHe ? 'טיקר' : 'Ticker'}</FieldLabel>
          <TextInput
            value={form.ticker}
            onChange={(v) => set('ticker')(v.toUpperCase())}
            placeholder="BTC"
            list="ticker-list"
            className="uppercase"
          />
          <datalist id="ticker-list">{tickers.map((t) => <option key={t} value={t} />)}</datalist>
        </div>
        <div>
          <FieldLabel>{isHe ? 'מטבע מקור' : 'Currency'}</FieldLabel>
          <SelectInput
            value={form.currency}
            onChange={set('currency')}
            options={
              currencies.length
                ? currencies.map((c) => ({ value: c, label: c }))
                : [{ value: 'USD', label: 'USD' }, { value: 'ILS', label: 'ILS' }]
            }
          />
        </div>
        <div>
          <FieldLabel>{isHe ? 'סטטוס' : 'Status'}</FieldLabel>
          <SelectInput
            value={form.status}
            onChange={set('status')}
            options={[
              { value: 'פתוח', label: isHe ? 'פתוח' : 'Open' },
              { value: 'סגור', label: isHe ? 'סגור' : 'Closed' },
            ]}
          />
        </div>
      </div>

      {/* Row 3: Dates */}
      <div className="mb-3 grid grid-cols-1 gap-3 sm:grid-cols-2">
        <div>
          <FieldLabel>{isHe ? 'תאריך רכישה' : 'Purchase Date'}</FieldLabel>
          <TextInput type="date" value={form.purchaseDate} onChange={set('purchaseDate')} />
        </div>
        {(isSell || form.status === 'סגור') && (
          <div>
            <FieldLabel>{isHe ? 'תאריך מכירה' : 'Sell Date'}</FieldLabel>
            <TextInput type="date" value={form.sellDate} onChange={set('sellDate')} />
          </div>
        )}
      </div>

      {/* Row 4: Quantity / Buy Price / Cost / Commission */}
      <div className="mb-3 grid grid-cols-2 gap-3 sm:grid-cols-4">
        <div>
          <FieldLabel>{isHe ? 'כמות' : 'Quantity'}</FieldLabel>
          <TextInput type="number" step="any" min="0" value={form.quantity} onChange={set('quantity')} placeholder="0.00000000" />
        </div>
        <div>
          <FieldLabel>{isHe ? 'שער קנייה' : 'Buy Price'}</FieldLabel>
          <TextInput type="number" step="any" min="0" value={form.buyPrice} onChange={set('buyPrice')} placeholder="0.00" />
        </div>
        <div>
          <FieldLabel>{isHe ? 'עלות מקור' : 'Origin Cost'}</FieldLabel>
          <TextInput type="number" step="any" min="0" value={form.costOrigin} onChange={set('costOrigin')} placeholder="0.00" />
        </div>
        <div>
          <FieldLabel>{isHe ? 'עמלה' : 'Commission'}</FieldLabel>
          <TextInput type="number" step="any" min="0" value={form.commission} onChange={set('commission')} placeholder="0" />
        </div>
      </div>

      {/* Sell price (sell side) */}
      {(isSell || (isEdit && form.status === 'סגור')) && (
        <div className="mb-3 grid grid-cols-2 gap-3">
          <div>
            <FieldLabel>{isHe ? 'מחיר מכירה' : 'Sell Price'}</FieldLabel>
            <TextInput type="number" step="any" min="0" value={form.sellPrice} onChange={set('sellPrice')} placeholder="0.00" />
          </div>
        </div>
      )}

      {submitError && (
        <div className="mb-3 rounded-lg bg-rose-50 px-3 py-2 text-sm text-rose-700 dark:bg-rose-900/30 dark:text-rose-300">
          {submitError}
        </div>
      )}

      <button
        onClick={onSubmit}
        disabled={submitting}
        className="h-9 rounded-lg bg-emerald-600 px-5 text-sm font-semibold text-white shadow-sm transition-all hover:bg-emerald-700 disabled:opacity-60"
      >
        {submitting
          ? isHe ? 'שומר…' : 'Saving…'
          : isEdit
            ? isHe ? 'עדכן' : 'Update'
            : isHe ? 'שמור' : 'Save'}
      </button>
    </div>
  );
}

// ── Main page ──────────────────────────────────────────────────────────────

export default function TradesPage() {
  const { t, locale, demoMode } = useApp();
  const isHe = locale === 'he';
  const { views, error, isLoading, refresh } = useSnapshot();

  const [mode, setMode] = useState<Mode>('add');
  const [search, setSearch] = useState('');
  const [statusFilter, setStatusFilter] = useState<'all' | 'open' | 'closed'>('all');
  const [selectedId, setSelectedId] = useState('');
  const [form, setForm] = useState<TradeForm>({ ...EMPTY_FORM });
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState('');
  const [successMsg, setSuccessMsg] = useState('');

  // Derived lists for datalists / selects
  const allTrades = useMemo(() => views?.trades ?? [], [views]);
  const platforms = useMemo(() => unique(allTrades.map((t) => t.platform)), [allTrades]);
  const types = useMemo(() => unique(allTrades.map((t) => t.type)), [allTrades]);
  const tickers = useMemo(() => unique(allTrades.map((t) => t.ticker)), [allTrades]);
  const currencies = useMemo(() => unique(allTrades.map((t) => t.currency)), [allTrades]);
  const locations = useMemo(() => unique(allTrades.map((t) => t.currentLocation)), [allTrades]);
  const openTrades = useMemo(() => views?.openTrades ?? [], [views]);

  // Filtered table rows
  const rows = useMemo(() => {
    let base = allTrades;
    if (statusFilter === 'open') base = views?.openTrades ?? [];
    else if (statusFilter === 'closed') base = views?.closedTrades ?? [];
    if (!search) return base;
    const q = search.toLowerCase();
    return base.filter(
      (r) =>
        r.ticker.toLowerCase().includes(q) ||
        (r.platform || '').toLowerCase().includes(q) ||
        (r.tradeId || '').toLowerCase().includes(q),
    );
  }, [allTrades, views, search, statusFilter]);

  // Select a row for edit/delete
  const handleRowClick = useCallback(
    (tradeId: string) => {
      if (mode === 'add') return;
      if (selectedId === tradeId) {
        setSelectedId('');
        setForm({ ...EMPTY_FORM });
      } else {
        setSelectedId(tradeId);
        const trade = allTrades.find((t) => t.tradeId === tradeId);
        if (trade) setForm(tradeToForm(trade));
      }
    },
    [mode, selectedId, allTrades],
  );

  // Validation
  function validate(): string {
    const qty = parseNum(form.quantity);
    const price = parseNum(form.buyPrice);
    if (qty <= 0) return isHe ? 'כמות חייבת להיות גדולה מאפס.' : 'Quantity must be > 0.';
    if (price <= 0) return isHe ? 'שער קנייה חייב להיות גדול מאפס.' : 'Buy price must be > 0.';
    if (form.action === 'SELL') {
      if (form.purchaseDate && form.sellDate && form.sellDate < form.purchaseDate)
        return isHe ? 'תאריך מכירה לא יכול להיות לפני תאריך הרכישה.' : 'Sell date cannot be before purchase date.';
      if (parseNum(form.sellPrice) <= 0)
        return isHe ? 'מחיר מכירה חייב להיות חיובי.' : 'Sell price must be > 0.';
    }
    if (!form.ticker.trim()) return isHe ? 'חובה להזין טיקר.' : 'Ticker is required.';
    return '';
  }

  async function callApi(action: 'add' | 'edit' | 'delete', payload: Record<string, unknown>) {
    const res = await fetch('/api/trades', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action, trade: payload }),
    });
    const json = (await res.json()) as { ok: boolean; error?: string };
    if (!json.ok) throw new Error(json.error ?? 'Unknown error');
  }

  async function handleSubmit() {
    setSubmitError('');
    setSuccessMsg('');
    // Demo mode is read-only: never write synthetic trades to the real sheet.
    if (demoMode) {
      setSubmitError(isHe ? 'מצב הדגמה פעיל — כתיבה מושבתת. כבה אותו כדי לערוך עסקאות אמיתיות.' : 'Demo mode is active — writes are disabled. Turn it off to edit real trades.');
      return;
    }
    const err = validate();
    if (err) { setSubmitError(err); return; }

    setSubmitting(true);
    try {
      const qty = parseNum(form.quantity);
      const buyPrice = parseNum(form.buyPrice);
      const costOrigin = parseNum(form.costOrigin) || qty * buyPrice;
      const sellPrice = parseNum(form.sellPrice);

      const payload: Record<string, unknown> = {
        Current_Location: form.currentLocation,
        Platform: form.platform,
        Type: form.type,
        Ticker: form.ticker.toUpperCase(),
        Origin_Currency: form.currency,
        Purchase_Date: form.purchaseDate,
        Quantity: qty,
        Origin_Buy_Price: buyPrice,
        Cost_Origin: costOrigin,
        Commission: parseNum(form.commission),
        Status: form.status,
        Action: form.action,
        Event_Type: 'TRADE',
        Sell_Date: form.sellDate || '',
        Sell_Price_Origin: sellPrice,
        Cost_ILS: 0,
        Current_Value_ILS: 0,
      };

      if (mode === 'edit') payload.Trade_ID = selectedId;

      await callApi(mode === 'edit' ? 'edit' : 'add', payload);
      setSuccessMsg(isHe ? '✅ נשמר בהצלחה' : '✅ Saved successfully');
      setForm({ ...EMPTY_FORM });
      setSelectedId('');
      await refresh();
    } catch (e) {
      setSubmitError(e instanceof Error ? e.message : 'Error');
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete() {
    if (!selectedId) return;
    setSubmitError('');
    setSuccessMsg('');
    setSubmitting(true);
    try {
      await callApi('delete', { Trade_ID: selectedId });
      setSuccessMsg(isHe ? '✅ נמחק בהצלחה' : '✅ Deleted successfully');
      setSelectedId('');
      await refresh();
    } catch (e) {
      setSubmitError(e instanceof Error ? e.message : 'Error');
    } finally {
      setSubmitting(false);
    }
  }

  if (isLoading) return <LoadingBlock label={t('loading')} />;
  if (error) return <ErrorBlock message={error} />;
  if (!views) return <EmptyBlock message={t('no_data')} />;

  const selectedTrade = allTrades.find((t) => t.tradeId === selectedId);

  return (
    <div className="space-y-5">
      <PageHeader title={t('nav_trades')} accent="emerald" />

      {/* Action mode selector */}
      <div className="flex gap-2">
        {(['add', 'edit', 'delete'] as Mode[]).map((m) => {
          const labels: Record<Mode, string> = {
            add: isHe ? '➕ הוספה' : '➕ Add',
            edit: isHe ? '✏ עריכה' : '✏ Edit',
            delete: isHe ? '🗑 מחיקה' : '🗑 Delete',
          };
          const active = mode === m;
          return (
            <button
              key={m}
              onClick={() => { setMode(m); setSelectedId(''); setForm({ ...EMPTY_FORM }); setSubmitError(''); setSuccessMsg(''); }}
              className={`rounded-xl px-4 py-2 text-sm font-semibold transition-all ${
                active
                  ? m === 'delete'
                    ? 'bg-rose-100 text-rose-700 dark:bg-rose-500/20 dark:text-rose-300'
                    : 'bg-emerald-100 text-emerald-700 dark:bg-emerald-500/20 dark:text-emerald-300'
                  : 'border border-slate-200 text-slate-500 hover:bg-slate-50 dark:border-slate-700 dark:text-slate-400 dark:hover:bg-slate-800'
              }`}
            >
              {labels[m]}
            </button>
          );
        })}
      </div>

      {successMsg && (
        <div className="rounded-xl bg-emerald-50 px-4 py-3 text-sm font-medium text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300">
          {successMsg}
        </div>
      )}

      {/* Add form (shown above table in add mode) */}
      {mode === 'add' && (
        <TradeFormPanel
          isHe={isHe}
          form={form}
          setForm={setForm}
          platforms={platforms}
          types={types}
          tickers={tickers}
          currencies={currencies}
          locations={locations}
          openTrades={openTrades}
          mode="add"
          onSubmit={handleSubmit}
          submitting={submitting}
          submitError={submitError}
        />
      )}

      {/* Trade table */}
      <div className="card">
        <div className="mb-4 flex flex-wrap items-center gap-3">
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder={t('search')}
            className="h-9 flex-1 min-w-[180px] rounded-lg border border-slate-200 bg-white px-3 text-sm dark:border-slate-700 dark:bg-slate-900"
          />
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value as 'all' | 'open' | 'closed')}
            className="h-9 rounded-lg border border-slate-200 bg-white px-3 text-sm dark:border-slate-700 dark:bg-slate-900"
          >
            <option value="all">{t('all')}</option>
            <option value="open">{t('open')}</option>
            <option value="closed">{t('closed')}</option>
          </select>
          <span className="text-xs text-slate-500 dark:text-slate-400">
            {rows.length} {t('rows')}
          </span>
        </div>

        {(mode === 'edit' || mode === 'delete') && (
          <p className="mb-3 text-xs text-slate-500 dark:text-slate-400">
            {isHe ? 'לחץ על שורה כדי לבחור אותה' : 'Click a row to select it'}
          </p>
        )}

        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead className="border-b border-slate-200 dark:border-slate-700">
              <tr className="text-slate-600 dark:text-slate-400">
                <th className="px-2 py-2 text-start font-semibold">{t('ticker')}</th>
                <th className="px-2 py-2 text-start font-semibold">{t('platform')}</th>
                <th className="px-2 py-2 text-start font-semibold">{t('purchase_date')}</th>
                <th className="px-2 py-2 text-start font-semibold">{t('status')}</th>
                <th className="px-2 py-2 text-end font-semibold">{t('quantity')}</th>
                <th className="px-2 py-2 text-end font-semibold">{t('cost')}</th>
                <th className="px-2 py-2 text-end font-semibold">{t('value')}</th>
                <th className="px-2 py-2 text-end font-semibold">{t('profit')}</th>
                <th className="px-2 py-2 text-end font-semibold">{t('return')}</th>
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 300).map((r, i) => {
                const profit = r.valueIls - r.costIls;
                const ret = r.costIls > 0 ? profit / r.costIls : 0;
                const positive = profit >= 0;
                const isSelected = r.tradeId === selectedId;
                return (
                  <tr
                    key={`${r.tradeId}-${i}`}
                    onClick={() => handleRowClick(r.tradeId)}
                    className={`border-b border-slate-100 dark:border-slate-800 transition-colors ${
                      mode !== 'add' ? 'cursor-pointer hover:bg-emerald-50/50 dark:hover:bg-emerald-900/10' : ''
                    } ${isSelected ? 'bg-emerald-50 dark:bg-emerald-900/20' : ''}`}
                  >
                    <td className="px-2 py-2 font-mono font-bold text-slate-900 dark:text-slate-100">
                      {r.ticker}
                    </td>
                    <td className="px-2 py-2 text-slate-600 dark:text-slate-400">{r.platform}</td>
                    <td className="px-2 py-2 text-slate-500 dark:text-slate-400 tabular-nums">{r.purchaseDate}</td>
                    <td className="px-2 py-2">
                      <span className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-medium ${
                        r.status === 'פתוח' || r.status === 'open'
                          ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300'
                          : 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-400'
                      }`}>
                        {r.status}
                      </span>
                    </td>
                    <td className="px-2 py-2 text-end font-mono tabular-nums">
                      {formatNumber(r.quantity, locale, 4)}
                    </td>
                    <td className="px-2 py-2 text-end font-mono tabular-nums">
                      {formatCurrency(r.costIls, 'ILS', locale)}
                    </td>
                    <td className="px-2 py-2 text-end font-mono tabular-nums">
                      {formatCurrency(r.valueIls, 'ILS', locale)}
                    </td>
                    <td className={`px-2 py-2 text-end font-mono font-semibold tabular-nums ${positive ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-600 dark:text-rose-400'}`}>
                      {formatCurrency(profit, 'ILS', locale)}
                    </td>
                    <td className={`px-2 py-2 text-end font-mono font-semibold tabular-nums ${positive ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-600 dark:text-rose-400'}`}>
                      {(ret * 100).toFixed(2)}%
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Edit form (shown below table) */}
      {mode === 'edit' && selectedId && (
        <TradeFormPanel
          isHe={isHe}
          form={form}
          setForm={setForm}
          platforms={platforms}
          types={types}
          tickers={tickers}
          currencies={currencies}
          locations={locations}
          openTrades={openTrades}
          mode="edit"
          onSubmit={handleSubmit}
          submitting={submitting}
          submitError={submitError}
        />
      )}

      {mode === 'edit' && !selectedId && (
        <div className="rounded-xl border border-dashed border-slate-200 p-6 text-center text-sm text-slate-400 dark:border-slate-700 dark:text-slate-500">
          {isHe ? 'בחר שורה בטבלה למעלה כדי לערוך' : 'Select a row in the table above to edit'}
        </div>
      )}

      {/* Delete confirmation */}
      {mode === 'delete' && selectedId && selectedTrade && (
        <div className="rounded-xl border border-rose-200 bg-rose-50 p-5 dark:border-rose-800/50 dark:bg-rose-900/20">
          <div className="mb-3 text-sm font-semibold text-rose-800 dark:text-rose-200">
            {isHe ? '🗑 מחיקת עסקה' : '🗑 Delete Trade'}
          </div>
          <div className="mb-4 text-sm text-rose-700 dark:text-rose-300">
            {isHe
              ? `למחוק את העסקה ${selectedTrade.ticker} (${selectedId})? פעולה זו אינה ניתנת לביטול.`
              : `Delete trade ${selectedTrade.ticker} (${selectedId})? This cannot be undone.`}
          </div>
          {submitError && (
            <div className="mb-3 text-sm text-rose-700 dark:text-rose-400">{submitError}</div>
          )}
          <div className="flex gap-3">
            <button
              onClick={handleDelete}
              disabled={submitting}
              className="rounded-lg bg-rose-600 px-5 py-2 text-sm font-semibold text-white hover:bg-rose-700 disabled:opacity-60"
            >
              {submitting ? (isHe ? 'מוחק…' : 'Deleting…') : (isHe ? 'מחק' : 'Delete')}
            </button>
            <button
              onClick={() => { setSelectedId(''); setSubmitError(''); }}
              className="rounded-lg border border-slate-200 px-5 py-2 text-sm text-slate-600 hover:bg-slate-50 dark:border-slate-700 dark:text-slate-400"
            >
              {isHe ? 'ביטול' : 'Cancel'}
            </button>
          </div>
        </div>
      )}

      {mode === 'delete' && !selectedId && (
        <div className="rounded-xl border border-dashed border-slate-200 p-6 text-center text-sm text-slate-400 dark:border-slate-700 dark:text-slate-500">
          {isHe ? 'בחר שורה בטבלה למעלה כדי למחוק' : 'Select a row in the table above to delete'}
        </div>
      )}
    </div>
  );
}
