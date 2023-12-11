#!/bin/bash

# builds and runs the process
cargo build --example fattree
./target/debug/examples/fattree &

# gets the PID
PID=$!
echo "Monitoring PID: $PID"

# gets the start time of running
START_TIME=$(date +%s.%N)
TASK_DIR="/proc/$PID/task"

# monitors the number of threads 
while sleep 0.01; do
  if [ -d $TASK_DIR ]; then
    FORMATTED_TIME=$(printf "%.3f" $(echo "$(date +%s.%N) - $START_TIME" | bc))
    THREADS=$(ls $TASK_DIR | wc -l)
    echo "Number of threads at time $FORMATTED_TIME: $THREADS"
  else
    echo "Process $PID has finished."
    break
  fi
done

# prints the total running time
FORMATTED_TOTAL_TIME=$(printf "%.3f" $(echo "$(date +%s.%N) - $START_TIME" | bc))
echo "Total running time: $FORMATTED_TOTAL_TIME seconds."