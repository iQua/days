# Conformance Verification (LeanGuard)

Days ships with an optional Lean-based conformance checker suite called **LeanGuard**.
LeanGuard is meant to answer a narrow, protocol-level question:

> Given a simulator-produced *per-transition event trace* (CSV), does every logged transition match an executable reference model?

If the trace is consistent with the model, the checker prints `ACCEPT`.
Otherwise it prints `REJECT: ...` with the first failing row and a diagnostic.

This section covers:

- [Use guide](leanguard.md): how to emit traces from Days and run the Lean checkers.
- [Design sketch](design.md): how the trace-checking pipeline is structured and how to extend it to new protocols.

