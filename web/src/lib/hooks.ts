import { useEffect, useRef, useState, type RefObject } from "react";

function useMedia(query: string, initial = false): boolean {
  const [match, setMatch] = useState(() =>
    typeof window !== "undefined" && window.matchMedia ? window.matchMedia(query).matches : initial,
  );
  useEffect(() => {
    if (!window.matchMedia) return;
    const mq = window.matchMedia(query);
    const on = () => setMatch(mq.matches);
    on();
    mq.addEventListener("change", on);
    return () => mq.removeEventListener("change", on);
  }, [query]);
  return match;
}

export const useReducedMotion = () => useMedia("(prefers-reduced-motion: reduce)");
export const useNarrow = (maxWidthPx: number) => useMedia(`(max-width: ${maxWidthPx}px)`);

/** Vrai dès que l'élément approche de l'écran (une seule fois : il ne repasse pas à faux). */
export function useInView<T extends Element>(rootMargin = "0px 0px -10% 0px"): [RefObject<T | null>, boolean] {
  const ref = useRef<T | null>(null);
  const [seen, setSeen] = useState(() => typeof IntersectionObserver === "undefined");
  useEffect(() => {
    const el = ref.current;
    if (seen || !el) return;
    const io = new IntersectionObserver(
      (entries) => {
        if (entries.some((e) => e.isIntersecting)) {
          setSeen(true);
          io.disconnect();
        }
      },
      { rootMargin },
    );
    io.observe(el);
    return () => io.disconnect();
  }, [seen, rootMargin]);
  return [ref, seen];
}
