#!/bin/bash

# builds and runs the process
cargo build --example fattree
./target/debug/examples/fattree &

# gets the PID
PID=$!
echo "Monitoring PID: $PID"

# gets the start time of running
START_TIME=$(date +%s)

# monitors the number of threads 
while sleep 1; do
  if [ -d /proc/$PID ]; then
    THREADS=$(ls /proc/$PID/task | wc -l)
    echo "Number of threads: $THREADS"
  else
    echo "Process $PID has finished."
    break
  fi
done

# prints the total running time
END_TIME=$(date +%s)
TOTAL_TIME=$((END_TIME - START_TIME))
echo "Total running time: $TOTAL_TIME seconds."