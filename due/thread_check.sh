#!/bin/bash

# builds and runs the process
cargo build --example new_core
./target/debug/examples/new_core &

# gets the PID
PID=$!
echo "Monitoring PID: $PID"

# monitors the number of threads 
while sleep 0.0001; do
  if [ -d /proc/$PID ]; then
    THREADS=$(ls /proc/$PID/task | wc -l)
    echo "Number of threads: $THREADS"
  else
    echo "Process $PID has finished."
    break
  fi
done
