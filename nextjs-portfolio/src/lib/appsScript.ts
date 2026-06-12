/**
 * Thin server-side wrapper around the Google Apps Script Web App that backs
 * the spreadsheet. Mirrors the action contract in `Code.gs` (read_snapshot,
 * read_manual_deposits, save_manual_deposits, add/edit/delete).
 *
 * NEVER import this module from a client component — it carries the API token.
 */
import { getAppsScriptConfig } from './config';

interface AppsScriptPayload {
  action: string;
  [key: string]: unknown;
}

const DEFAULT_TIMEOUT_MS = 25_000;

async function postJson<T>(payload: AppsScriptPayload): Promise<T> {
  const cfg = getAppsScriptConfig();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);
  try {
    const res = await fetch(cfg.url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token: cfg.token, ...payload }),
      cache: 'no-store',
      signal: controller.signal,
    });
    if (!res.ok) {
      throw new Error(`Apps Script HTTP ${res.status}`);
    }
    const text = await res.text();
    return JSON.parse(text) as T;
  } finally {
    clearTimeout(timer);
  }
}

export interface RawSnapshotPayload {
  ok: boolean;
  error?: string;
  data?: {
    headers: string[];
    rows: Array<Array<string | number | null>>;
  };
}

export function readSnapshot(): Promise<RawSnapshotPayload> {
  return postJson<RawSnapshotPayload>({ action: 'read_snapshot' });
}

export interface ManualDepositRow {
  Platform: string;
  Manual_Deposit_ILS: number;
}

export interface ManualDepositsResponse {
  ok: boolean;
  error?: string;
  data?: { mode: string; rows: ManualDepositRow[] };
}

export function readManualDeposits(mode: 'live' | 'demo' = 'live'): Promise<ManualDepositsResponse> {
  return postJson<ManualDepositsResponse>({ action: 'read_manual_deposits', mode });
}

export interface SaveManualDepositsResponse {
  ok: boolean;
  error?: string;
  count?: number;
}

export function saveManualDeposits(
  mode: 'live' | 'demo',
  rows: ManualDepositRow[],
): Promise<SaveManualDepositsResponse> {
  return postJson<SaveManualDepositsResponse>({
    action: 'save_manual_deposits',
    mode,
    rows,
  });
}

export interface TradeMutationResponse {
  ok: boolean;
  error?: string;
  trade_id?: string;
  row?: number;
}

export function upsertTrade(
  action: 'add' | 'edit' | 'delete',
  trade: Record<string, unknown>,
): Promise<TradeMutationResponse> {
  return postJson<TradeMutationResponse>({ action, trade });
}
