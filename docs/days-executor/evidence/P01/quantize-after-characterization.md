# P01 legacy `quantize_after` characterization

This note freezes the behavior of the existing Days time helper. It does not
propose a replacement policy. The machine-readable cases are in
`quantize-after-characterization.toml`, and
`tests/quantize_after_characterization.rs` evaluates those cases against the
production `quantize_after` function.

## Existing operation

With `time_quantum_ns` absent, the helper returns
`max(0, base_seconds + delta_seconds)` as an `f64`. With a quantum `q`, it:

1. adds `base_seconds + delta_seconds` in `f64`;
2. multiplies the absolute deadline by \(10^9\) and rounds it to the nearest
   integer nanosecond;
3. if that integer is not divisible by `q`, advances it to the next multiple
   of `q`;
4. converts the integer back to `f64` seconds.

Thus the operation is absolute-deadline quantization. It is not equivalent in
general to quantizing a duration once and repeatedly adding that duration.
The `rem == 0` path leaves exact quantum edges unchanged: with `q = 10 ns`,
deadlines of 10 ns and 20 ns remain 10 ns and 20 ns.

Base residue also matters. For `q = 10 ns` and a 5 ns delta (for example,
10-byte packets at 16 Gbit/s), the frozen cases are:

| Base (ns) | Absolute `quantize_after` (ns) | Base + independently quantized duration (ns) |
|---:|---:|---:|
| 0 | 10 | 10 |
| 4 | 10 | 14 |
| 5 | 10 | 15 |
| 6 | 20 | 16 |
| 9 | 20 | 19 |

## Repeated 5.5 ns serialization

An 11-byte packet at 16 Gbit/s has a raw serialization duration of exactly
\(11/2\) ns as a rational value. The port scheduler chains each service using
the preceding absolute departure as the next `service_start`.

With the legacy default (`time_quantum_ns` absent), the absolute chain retains
the fractional deadline. Independently rounding 5.5 ns to 6 ns before each
addition drifts:

| Services | Legacy absolute deadline (ns) | Sum of rounded durations (ns) | Divergence (ns) |
|---:|---:|---:|---:|
| 1 | 5.5 | 6 | 0.5 |
| 3 | 16.5 | 18 | 1.5 |
| 10 | 55 | 60 | 5 |
| 20 | 110 | 120 | 10 |
| 100 | 550 | 600 | 50 |

With `time_quantum_ns = 1`, the same tested chain produces 6, 18, 60, 120,
and 600 ns at those service counts. That coincidence does not admit the
fractional-input family into the exact domain below; only the finite domain
actually exhaustively checked by P01 is frozen as exact-ledger evidence.

## Sub-nanosecond and large-time boundaries

With quantization disabled, a 0.25 ns delta remains 0.25 ns. With a 1 ns
quantum, both 0.25 ns (1 byte at 32 Gbit/s) and 0.4 ns (1 byte at 20 Gbit/s)
round to zero. Such a transmission produces no positive serialization
lookahead. A 0.8 ns delta (1 byte at 10 Gbit/s) advances to 1 ns.

At a base of \(10^{18}\) ns, `f64` no longer resolves a 1 ns addition. With
quantization disabled the observed deadline remains \(10^{18}\) ns. With a
1 ns quantum, the legacy integer-to-`f64` conversion yields an observed
\(10^{18} + 128\) ns. Both cases are semantic-migration inputs.

## Frozen comparison boundary

P01 admits this finite aligned exact domain for full-ledger comparison:

- `time_quantum_ns = 1`;
- packet size exactly 1000 bytes;
- port rate exactly 1,000,000,000 bit/s;
- raw serialization duration exactly 8000 ns;
- every integer-nanosecond base from 0 through 2,000,000 ns, inclusive.

The characterization test exhaustively checks every base in that interval and
observes `departure_ns = base_ns + 8000`. These values cover the three bounded
P01 FIFO/TailDrop migration fixtures.

The frozen semantic-migration domain consists of the explicitly labeled
fractional, sub-nanosecond, and large-time rows in the TOML artifact:

- 11-byte/16-Gbit/s serialization (5.5 ns), with the quantum absent or 1 ns;
- the recorded 0.25 ns and 0.4 ns deltas under a 1 ns quantum, which become
  zero;
- a 1-byte/8-Gbit/s delta (1 ns) at a \(10^{18}\)-ns base, where mantissa precision is
  insufficient.

Inputs outside the finite aligned exact domain are not granted full-key ledger
equality by this P01 artifact. A later integer time policy must either add and
prove another exact domain or label the fixture as a versioned semantic
migration and compare terminal observations only.
