RingAllReduce what-if: oversubscription sweep.
- Fixed: N=128, size=512MB, port_rate=200G, FIFO+TailDrop, ECMP routing.
- oversub 1:1: CCT=47.983509s, slowdown vs 1:1 = 1.00x
- oversub 2:1: CCT=51.392640s, slowdown vs 1:1 = 1.07x
- oversub 4:1: CCT=58.594427s, slowdown vs 1:1 = 1.22x

Takeaway: As oversubscription increases, uplink contention creates non-linear queueing and phase synchronization effects, causing CCT to degrade faster than linearly.
