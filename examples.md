Five examples have also been provided in `day/examples/`. One can run each of these examples using:

```
RUST_LOG=debug cargo run --example fattree
```

The `time` command in UNIX can be used to measure the total running time of a run without any logs:

```
time cargo run -- configs/simple.toml
```
