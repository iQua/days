import type { ComponentPropsWithoutRef } from 'react';
import { useEffect, useId, useRef, useState } from 'react';

type MermaidTheme = 'default' | 'dark';

const THEME_ATTRIBUTE_NAMES = ['class', 'data-theme', 'data-color-mode', 'data-mode'] as const;
const DARK_THEME_PATTERN = /dark|night|black|dim/i;

export interface MermaidProps extends ComponentPropsWithoutRef<'div'> {
  chart: string;
}

function resolveTheme(): MermaidTheme {
  if (typeof window === 'undefined') {
    return 'default';
  }

  const root = document.documentElement;
  const candidates = [
    root.className,
    root.getAttribute('data-theme'),
    root.getAttribute('data-color-mode'),
    root.getAttribute('data-mode'),
    root.dataset.theme,
    root.dataset.colorMode,
    root.dataset.mode,
  ];

  if (candidates.some((value) => Boolean(value && DARK_THEME_PATTERN.test(value)))) {
    return 'dark';
  }

  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'default';
}

function normalizeChart(chart: string): string {
  return chart.replaceAll('\\n', '\n');
}

export function Mermaid({ chart, className, ...props }: MermaidProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [theme, setTheme] = useState<MermaidTheme>('default');
  const [error, setError] = useState<string | null>(null);
  const renderNonce = useRef(0);
  const id = useId().replace(/:/g, '-');

  useEffect(() => {
    const updateTheme = () => {
      setTheme(resolveTheme());
    };

    updateTheme();

    const root = document.documentElement;
    const observer = new MutationObserver(() => {
      updateTheme();
    });

    observer.observe(root, {
      attributes: true,
      attributeFilter: [...THEME_ATTRIBUTE_NAMES],
    });

    const media = window.matchMedia('(prefers-color-scheme: dark)');
    const onMediaChange = () => {
      updateTheme();
    };

    if (typeof media.addEventListener === 'function') {
      media.addEventListener('change', onMediaChange);
    } else {
      media.addListener(onMediaChange);
    }

    return () => {
      observer.disconnect();

      if (typeof media.removeEventListener === 'function') {
        media.removeEventListener('change', onMediaChange);
      } else {
        media.removeListener(onMediaChange);
      }
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    const request = ++renderNonce.current;

    const render = async () => {
      try {
        const { default: mermaid } = await import('mermaid');

        mermaid.initialize({
          startOnLoad: false,
          securityLevel: 'strict',
          theme,
          fontFamily: 'inherit',
        });

        const { svg, bindFunctions } = await mermaid.render(
          `mermaid-${id}-${request}`,
          normalizeChart(chart),
        );

        if (cancelled || request !== renderNonce.current) {
          return;
        }

        const container = containerRef.current;
        if (!container) {
          return;
        }

        container.innerHTML = svg;
        bindFunctions?.(container);
        setError(null);
      } catch (cause) {
        if (cancelled || request !== renderNonce.current) {
          return;
        }

        const container = containerRef.current;
        if (container) {
          container.innerHTML = '';
        }

        setError(cause instanceof Error ? cause.message : 'Failed to render Mermaid diagram.');
      }
    };

    void render();

    return () => {
      cancelled = true;
    };
  }, [chart, id, theme]);

  return (
    <div className={className} {...props}>
      <div ref={containerRef} />
      {error ? <pre className="mt-2 text-sm text-red-600 dark:text-red-400">{error}</pre> : null}
    </div>
  );
}
