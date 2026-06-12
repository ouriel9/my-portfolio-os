import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

/** `clsx` + tailwind-merge — the canonical class combiner. */
export function cn(...inputs: ClassValue[]): string {
  return twMerge(clsx(inputs));
}
