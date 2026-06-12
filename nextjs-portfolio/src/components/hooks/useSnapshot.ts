'use client';

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
  return {
    views: data?.views,
    error: error instanceof Error ? error.message : data?.ok === false ? data.error : undefined,
    isLoading,
    refresh: mutate,
  };
}
