'use client';

import { useEffect, useState } from 'react';
import useSWR from 'swr';
import { useApp } from '@/components/providers/AppProvider';
import type { CoreViews } from '@/lib/types';

export interface SnapshotApiBody {
  ok: boolean;
  error?: string;
  views?: CoreViews;
}

const fetcher = async (url: string): Promise<SnapshotApiBody> => {
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status} ${text}`.trim());
  }
  return (await res.json()) as SnapshotApiBody;
};

export function useSnapshot() {
  // Demo mode swaps the data source to the synthetic dataset; the SWR key
  // change triggers an automatic refetch when the toggle flips.
  const { demoMode } = useApp();
  const { data, error, isLoading, mutate } = useSWR<SnapshotApiBody>(
    demoMode ? '/api/snapshot?demo=1' : '/api/snapshot',
    fetcher,
    {
      revalidateOnFocus: false,
      dedupingInterval: 30_000,
      refreshInterval: 60_000,
    },
  );
  // Hydration gate: the localStorage-persisted SWR cache makes `data` available
  // SYNCHRONOUSLY on the first client render, but the server-rendered HTML was
  // the loading state — exposing cached data during hydration causes React #418
  // (hydration mismatch) on every reload. First paint mirrors the server; the
  // cached data appears immediately after mount (still instant, no network).
  const [hydrated, setHydrated] = useState(false);
  useEffect(() => setHydrated(true), []);

  return {
    views: hydrated ? data?.views : undefined,
    error: !hydrated
      ? undefined
      : error instanceof Error
        ? error.message
        : data?.ok === false
          ? data.error
          : undefined,
    isLoading: hydrated ? isLoading : true,
    refresh: mutate,
  };
}
