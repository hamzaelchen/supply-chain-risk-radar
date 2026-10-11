import type { ElementType, ReactNode } from "react";
import { useInView } from "../../lib/hooks";

/** Apparition au défilement : visible d'emblée sans IntersectionObserver ou avec prefers-reduced-motion (voir styles.css). */
export function Reveal({
  as: Tag = "div",
  className = "",
  delay = 0,
  children,
  ...rest
}: {
  as?: ElementType;
  className?: string;
  delay?: number;
  children: ReactNode;
  [key: string]: unknown;
}) {
  const [ref, seen] = useInView<HTMLElement>();
  return (
    <Tag
      ref={ref}
      className={`reveal${seen ? " is-in" : ""} ${className}`.trim()}
      style={delay ? { transitionDelay: `${delay}ms` } : undefined}
      {...rest}
    >
      {children}
    </Tag>
  );
}
