const fr = (opts: Intl.DateTimeFormatOptions) => new Intl.DateTimeFormat("fr-FR", { timeZone: "UTC", ...opts });

/** « 8 oct. 2026, 17:11 UTC » : les heures sont toujours en UTC, dites comme telles. */
export function formatUtc(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "n/d";
  return `${fr({ day: "numeric", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" }).format(d)} UTC`;
}

export function formatDay(iso: string): string {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? "n/d" : fr({ day: "numeric", month: "short", year: "numeric" }).format(d);
}

export function formatNumber(n: number | null | undefined, digits = 0): string {
  if (n === null || n === undefined || Number.isNaN(n)) return "n/d";
  return new Intl.NumberFormat("fr-FR", { minimumFractionDigits: digits, maximumFractionDigits: digits }).format(n);
}

export function sourceDomain(url: string | null): string | null {
  if (!url) return null;
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return null;
  }
}

/** Âge lisible entre deux instants ISO (« 3 h 12 min »). */
export function ageBetween(fromIso: string, to: Date): string {
  const ms = to.getTime() - new Date(fromIso).getTime();
  if (Number.isNaN(ms) || ms < 0) return "n/d";
  const min = Math.floor(ms / 60000);
  if (min < 60) return `${min} min`;
  const h = Math.floor(min / 60);
  if (h < 48) return `${h} h ${String(min % 60).padStart(2, "0")} min`;
  return `${Math.floor(h / 24)} j`;
}
