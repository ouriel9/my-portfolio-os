/**
 * Snapshot proxy. Fetches the raw spreadsheet payload from the Apps Script
 * web app, normalises it, and returns the `CoreViews` shape consumed by the
 * client pages. Cached for 30 seconds to keep round-trips cheap.
 */
import { NextResponse } from 'next/server';
import { readSnapshot } from '@/lib/appsScript';
import { normalizeSnapshot, prepareCoreViews } from '@/lib/snapshot';
import { DEMO_TRADES } from '@/lib/demo';
import type { CoreViews } from '@/lib/types';

export const revalidate = 30;
export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

interface CacheEntry {
  expires: number;
  body: { ok: true; views: CoreViews };
}

let memoryCache: CacheEntry | null = null;
const CACHE_TTL_MS = 30_000;

export async function GET(request: Request): Promise<NextResponse> {
  // Demo mode: serve the synthetic dataset — readable numbers, no real
  // portfolio data, no Apps Script round-trip. Mirrors app.py demo_mode.
  const url = new URL(request.url);
  if (url.searchParams.get('demo') === '1') {
    return NextResponse.json(
      { ok: true as const, views: prepareCoreViews(DEMO_TRADES) },
      { headers: { 'Cache-Control': 'private, max-age=300' } },
    );
  }

  const now = Date.now();
  if (memoryCache && memoryCache.expires > now) {
    return NextResponse.json(memoryCache.body, {
      headers: { 'Cache-Control': 'private, max-age=30, stale-while-revalidate=60' },
    });
  }

  try {
    const raw = await readSnapshot();
    if (!raw.ok || !raw.data) {
      return NextResponse.json(
        { ok: false, error: raw.error ?? 'Empty snapshot from Apps Script' },
        { status: 502 },
      );
    }
    const trades = normalizeSnapshot(raw.data.headers, raw.data.rows);
    const views = prepareCoreViews(trades);
    const body = { ok: true as const, views };
    memoryCache = { expires: now + CACHE_TTL_MS, body };
    return NextResponse.json(body, {
      headers: { 'Cache-Control': 'private, max-age=30, stale-while-revalidate=60' },
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Unknown error';
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
