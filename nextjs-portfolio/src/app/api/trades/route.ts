import { NextRequest, NextResponse } from 'next/server';
import { upsertTrade } from '@/lib/appsScript';

export const runtime = 'nodejs';

interface TradePayload {
  action: 'add' | 'edit' | 'delete';
  trade: Record<string, unknown>;
}

export async function POST(req: NextRequest): Promise<NextResponse> {
  let body: TradePayload;
  try {
    body = (await req.json()) as TradePayload;
  } catch {
    return NextResponse.json({ ok: false, error: 'Invalid JSON' }, { status: 400 });
  }

  const { action, trade } = body;
  if (!action || !trade || typeof trade !== 'object' || Array.isArray(trade)) {
    return NextResponse.json({ ok: false, error: 'Missing action or trade' }, { status: 400 });
  }
  if (!['add', 'edit', 'delete'].includes(action)) {
    return NextResponse.json({ ok: false, error: 'Unknown action' }, { status: 400 });
  }

  try {
    const result = await upsertTrade(action, trade);
    return NextResponse.json(result);
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Unknown error';
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
