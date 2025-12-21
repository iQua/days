RUST_LOG=info cargo run --release -- configs/exp_tcp_fattree.toml \
  2>&1 | tee logs/exp/tcp_fattree/t<T>_m<M>/run.log

grep -E "Concurrency:|Elapsed wall-clock time:|Total packets processed:|Average one-way delay:" \
  logs/exp/tcp_fattree/t<T>_m<M>/run.log
