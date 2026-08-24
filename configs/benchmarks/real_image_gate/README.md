# Real-image GPU gate corpus

These fixtures extend the frozen baseline progression without changing it. The flow count follows
the existing `f = k^3 / 8` sequence exactly: k4/f8, k8/f64, k16/f512, k32/f4096, and
k64/f32768.

The T13d fixture stays inside the Days executor v1 model: open-loop constant packet generation,
FIFO service, TailDrop, fixed packet size, and fixed link rate. `threading = "single"` controls
only the legacy runner; T13d lowers the same file once and selects scalar or CPU execution after
lowering.
