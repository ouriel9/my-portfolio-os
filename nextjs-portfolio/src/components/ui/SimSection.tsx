'use client';

import { useState, type ReactNode } from 'react';

interface BaseProps {
  title: string;
  icon?: string;
  children: ReactNode;
}

/** Always-open section card with amber accent left bar + title row. */
export function SimSection({ title, icon, children }: BaseProps) {
  return (
    <section className="card border-l-4 border-l-amber-400/80">
      <h2 className="mb-3 flex items-center gap-2 text-sm font-bold text-slate-700 dark:text-slate-200">
        {icon && <span aria-hidden>{icon}</span>}
        <span>{title}</span>
      </h2>
      {children}
    </section>
  );
}

/**
 * Collapsible section — same look as SimSection but starts collapsed and
 * toggles via a header button. Mirrors Streamlit's `st.expander(expanded=False)`.
 */
export function SimExpander({
  title,
  icon,
  children,
  defaultOpen = false,
}: BaseProps & { defaultOpen?: boolean }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <section className="card border-l-4 border-l-amber-400/60 transition-all">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        aria-expanded={open}
        className="flex w-full items-center justify-between gap-2 text-start text-sm font-bold text-slate-700 transition-colors hover:text-amber-700 dark:text-slate-200 dark:hover:text-amber-300"
      >
        <span className="flex items-center gap-2">
          {icon && <span aria-hidden>{icon}</span>}
          <span>{title}</span>
        </span>
        <span
          aria-hidden
          className={`text-slate-400 transition-transform ${open ? 'rotate-90' : ''}`}
        >
          ▶
        </span>
      </button>
      {open && <div className="mt-4">{children}</div>}
    </section>
  );
}
