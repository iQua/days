# Days documentation site

This is the documentation website for **Days**, a Rust-powered discrete-event network simulator.

- Docs content lives under `content/docs/` (MDX).
- The site is built with **Fumadocs** on **TanStack Start** and served under `/docs`.

## Development

```bash
bun install
bun run dev
```

Then open:

- http://localhost:3000/ (home)
- http://localhost:3000/docs (docs)

## Build

```bash
bun run build
```

## Notes

The original Days repository previously used MkDocs (`days/docs/mkdocs.yml`). This site migrates that content into MDX and adds additional reference pages derived from the current codebase (CLI + trace formats + Nexosim internals).
