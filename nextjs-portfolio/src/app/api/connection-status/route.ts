import { NextResponse } from 'next/server';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

function maskUrl(raw: string): string {
  if (!raw) return '';
  try {
    const u = new URL(raw);
    // Show scheme + host only — hide the /macros/s/<id>/exec path
    const path = u.pathname;
    if (path.length > 20) {
      // Never include deployment-id characters — show only the generic tail.
      const tail = path.endsWith('/exec') ? '/exec' : path.endsWith('/dev') ? '/dev' : '';
      return `${u.protocol}//${u.host}/…${tail}`;
    }
    return `${u.protocol}//${u.host}${path}`;
  } catch {
    return raw.length > 30 ? raw.slice(0, 30) + '…' : raw;
  }
}

export async function GET(): Promise<NextResponse> {
  const url = process.env.APPS_SCRIPT_URL ?? '';
  const token = process.env.APPS_SCRIPT_TOKEN ?? '';
  const configured = Boolean(url && token);

  return NextResponse.json({
    configured,
    urlPreview: maskUrl(url),
    hasToken: Boolean(token),
  });
}
