RingAllReduce what-if: topology swap.
- Fixed: N=128, size=512MB, port_rate=200G, FIFO+TailDrop, ECMP routing.
- FatTree(k=16): CCT=44.616993s (baseline)
- Ring(1D Torus, n=128): CCT=114.205866s, slowdown=2.56x (+156.0%)

Takeaway: Topologies with lower bisection bandwidth / longer average path length amplify contention in the all-to-all phases of RingAllReduce, increasing completion time even when link rate is unchanged.
