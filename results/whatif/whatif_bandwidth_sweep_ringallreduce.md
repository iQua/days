RingAllReduce what-if: bandwidth sweep (N=128, size=512MB).
- 100G: CCT=0.426141s, speedup vs 100G = 1.00x
- 200G: CCT=44.616993s, speedup vs 100G = 0.01x
- 400G: CCT=0.106535s, speedup vs 100G = 4.00x

Takeaway: 100G→200G improves CCT by -10370.0%, while 200G→400G improves by 99.8% (diminishing returns).
Interpretation: as link bandwidth increases, RingAllReduce becomes less dominated by pure serialization and more by dependency/phase synchronization and shared bottlenecks.
