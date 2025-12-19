15 examples have been provided in `day/examples/`. One can run each of these examples using:

```
RUST_LOG=debug cargo run --example fattree
```

The `time` command in UNIX can be used to measure the total running time of a run without any logs:

```
time cargo run -- configs/simple.toml
```

DCQCN example (requires the `dcqcn` and `l2_pfc` features):

```
RUST_LOG=debug cargo run --features dcqcn,l2_pfc -- configs/dcqcn_simple.toml
```
