import { cn } from '@/lib/cn';

export function LoadingBlock({ label }: { label?: string }) {
  return (
    <div className="card flex items-center justify-center py-12 text-sm text-slate-500 dark:text-slate-400">
      <div className="flex items-center gap-3">
        <span className="inline-block h-3 w-3 animate-pulse-soft rounded-full bg-brand-500" />
        <span>{label ?? 'Loading...'}</span>
      </div>
    </div>
  );
}

export function ErrorBlock({ message, onRetry }: { message: string; onRetry?: () => void }) {
  return (
    <div className="card border-rose-200 bg-rose-50/60 dark:border-rose-900/40 dark:bg-rose-950/40">
      <div className="text-sm font-semibold text-rose-700 dark:text-rose-300">
        {message}
      </div>
      {onRetry && (
        <button
          type="button"
          onClick={onRetry}
          className={cn('btn-secondary mt-3 h-9 px-4 text-xs')}
        >
          Retry
        </button>
      )}
    </div>
  );
}

export function EmptyBlock({ message }: { message: string }) {
  return (
    <div className="card text-center text-sm text-slate-500 dark:text-slate-400">
      {message}
    </div>
  );
}
