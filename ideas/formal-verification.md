### Formal verification

The repo includes a new discrete-event network simulator that uses Rust coroutines to model a process in each simulation, and uses process-based simulation. For a specific network protocol (such as ECN in RFC 3168), is there a formal way of verifying that the implementation is correctly reflecting the protocol specifications, using the Lean programming language, for example? Any existing work in the literature on using Lean to proof that the simulator's implementation strictly adheres to the protocol specs? If not, how can I proceed with this objective?

Answer (GPT 5.2 xhigh):

Yes—in principle you can verify “RFC compliance” in Lean, but there’s no turnkey “Lean proves my Rust simulator implements RFC 3168” pipeline.

  What exists (Lean-specific)

  - I couldn’t find published work that formalizes RFC 3168 ECN and then proves a simulator/implementation is compliant in Lean.
  - There is realistic Lean protocol verification work for distributed protocols (e.g., a full Lean 4 safety proof for two‑phase commit, using a TLA+-style inductive-invariant workflow and Apalache to discover the invariant). (protocols-made-fun.com (https://protocols-made-fun.com/lean/2025/05/10/lean-two-phase-proofs.html))
  - There’s also precedent for “Lean model + Rust implementation side-by-side” as a way to keep a spec/implementation pair aligned (e.g., risc0/risc0-lean4 maintains executable Lean models alongside Rust counterparts). (github.com (https://github.com/risc0/risc0-lean4))

  What’s more common in the literature (non-Lean)

  - Refinement-based verification of protocol implementations (e.g., IronFleet in Dafny). (microsoft.com (https://www.microsoft.com/en-us/research/publication/ironfleet-proving-practical-distributed-systems-correct/))
  - Coq frameworks for verified distributed systems (e.g., Verdi). (github.com (https://github.com/uwplse/verdi))
  - Automated noncompliance finding via learned models / blackbox checking (common for real protocol stacks; e.g., QUICtester, Prognosis). (arxiv.org (https://arxiv.org/abs/2505.12690))

  How to proceed with your objective (Lean + Rust simulator)

  1. Define “adheres to the RFC” precisely: usually either (a) trace refinement (“every observable behavior of the simulator is allowed by the spec”) or (b) a checklist of RFC “MUST/SHOULD” properties as invariants/temporal properties.
  2. Formalize the RFC subset you implement as a transition system in Lean (endpoints + network events). For ECN, this includes the IP ECN field behavior and TCP signaling/negotiation rules as you model them. (datatracker.ietf.org (https://datatracker.ietf.org/doc/html/rfc3168))
  3. Prove compliance properties in Lean (invariants, and possibly liveness under explicit fairness assumptions). A practical workflow is “model check to find inductive invariants, then prove them in Lean,” as in the 2PC Lean work. (protocols-made-fun.com (https://protocols-made-fun.com/lean/2025/05/10/lean-two-phase-proofs.html))
  4. Connect Lean ↔ Rust via one of these realistic bridges:
      - Twin model + differential testing: keep a Lean executable spec and a Rust implementation; compare traces on randomized/bounded scenarios (good bug-finder; not a full proof).
      - Lean-verified trace checker: make the simulator emit an event trace; prove in Lean that “any trace accepted by the checker is RFC-compliant”; then treat “simulator emits only accepted traces” as a testable/monitorable contract.
      - Shrink the trusted core: isolate ECN logic into a small, pure “step function” and aim to verify that (much easier than verifying the whole coroutine scheduler + simulator).
  5. If you truly need a machine-checked proof about the Rust code itself, you’ll likely need an additional Rust-verification toolchain (Lean doesn’t currently have a mature, standard end-to-end story for reasoning about Rust semantics the way Coq/F*/Isabelle ecosystems do).

  If you tell me which ECN behaviors you’ve implemented (e.g., just IP marking + TCP ECE/CWR handshake, or full congestion-control reactions), I can help sketch a minimal Lean state machine + a concrete “conformance theorem” that matches your simulator’s notion of events.


---
To answer Q1, I wanted (a0 trace refinement. To answer the question on which ECN behaviours I have implemented, I believe a full congestion-control reaction (in DCQCN) has been implemented. Proceed to sketch a minimal Lean statement + a
  concrete "conformance theorem" that matches my simulator's notion of events.
---
Here’s a minimal (but concrete) Lean 4 sketch where “the protocol spec” is an executable trace-acceptor, and “conformance” is plain trace refinement (DaysTraces ⊆ SpecTraces). The event vocabulary is chosen to line up with what Days actually
  does for DCQCN:

  - srcSend: each DcqcnPacketSource.output.send(packet)
  - sinkRecv: each DcqcnPacketSink.process(packet, now)
  - sinkSendCnp: each DcqcnPacketSink.output.send(cnp)
  - srcRecv: each PacketSource.packet_received(..) delivering a CNP to DcqcnPacketSource.packet_received(..)
  - srcTimer: each PacketSource::periodic_timer_event calling DcqcnPacketSource.timer_tick(now)

  import Std

  namespace Days.Spec

  abbrev Time := Nat          -- e.g. nanoseconds (monotone in the trace)
  abbrev EndpointId := Nat
  abbrev FlowId := Nat

  inductive EcnField | notEct | ect0 | ect1 | ce
  deriving DecidableEq, Repr

  inductive Control | dcqcnCnp
  deriving DecidableEq, Repr

  structure Packet where
    flowId : FlowId
    pktId  : Nat
    sizeB  : Nat
    ecn    : EcnField
    control : Option Control
  deriving DecidableEq, Repr

  structure Timed (α : Type) where
    t : Time
    ev : α
  deriving Repr

  inductive Event
  | srcSend (src : EndpointId) (pkt : Packet)
  | srcRecv (src : EndpointId) (pkt : Packet)       -- includes CNP delivery
  | srcTimer (src : EndpointId)
  | sinkRecv (sink : EndpointId) (pkt : Packet)
  | sinkSendCnp (sink : EndpointId) (cnp : Packet)
  deriving Repr

  abbrev Trace := List (Timed Event)

  def Refines (Impl Spec : Set Trace) : Prop :=
    ∀ tr, Impl tr → Spec tr

  structure SrcState where
    alpha : Rat := 0
    rateBps : Rat
    lastCnp : Time := 0
    cnpSeen : Bool := false
    g : Rat
    miFactor : Rat
    minRateBps : Rat
    maxRateBps : Rat
    aiRateBps : Rat
    haiRateBps : Rat
    cnpInterval : Time
  deriving Repr

  def SrcState.onCnp (st : SrcState) (t : Time) : SrcState :=
    if t - st.lastCnp < st.cnpInterval then st else
      let alpha' := (1 - st.g) * st.alpha + st.g
      let rate'  := max (st.rateBps * (1 - st.miFactor * alpha')) st.minRateBps
      { st with alpha := alpha', rateBps := rate', lastCnp := t, cnpSeen := true }

  def SrcState.onTimer (st : SrcState) : SrcState :=
    if st.cnpSeen then { st with cnpSeen := false }
    else
      let alpha' := (1 - st.g) * st.alpha
      let inc    := if alpha' < (1/10 : Rat) then st.haiRateBps else st.aiRateBps
      let rate'  := min (st.rateBps + inc) st.maxRateBps
      { st with alpha := alpha', rateBps := rate', cnpSeen := false }

  structure PendingCnp where
    flowId : FlowId
    pktId : Nat
  deriving DecidableEq, Repr

  structure SinkState where
    lastCnp : Time := 0
    cnpInterval : Time
    pending : Option PendingCnp := none
  deriving Repr

  structure State where
    src : SrcState
    sink : SinkState
    ectSent : List (FlowId × Nat) := []  -- enough to enforce “CE only on ECT”
  deriving Repr

  def step (s : State) (te : Timed Event) : Option State :=
    match te.ev with
    | .srcSend _ pkt =>
        -- matches `dcqcn_source.rs`: data packets are sent ECT0
        match pkt.control, pkt.ecn with
        | none, .ect0 =>
            some { s with ectSent := (pkt.flowId, pkt.pktId) :: s.ectSent }
        | none, _ => none
        | some _, _ => some s
    | .sinkRecv _ pkt =>
        -- matches `dcqcn_sink.rs`: only CE-marked data can trigger a CNP;
        -- plus an RFC3168-style sanity: anything CE must have been ECT when sent.
        if pkt.control = none ∧ pkt.ecn = .ce then
          if (pkt.flowId, pkt.pktId) ∈ s.ectSent then
            if te.t - s.sink.lastCnp < s.sink.cnpInterval then some s else
              some { s with sink := { s.sink with pending := some ⟨pkt.flowId, pkt.pktId⟩ } }
          else none
        else
          some s
    | .sinkSendCnp _ cnp =>
        match s.sink.pending with
        | none => none
        | some p =>
            -- matches `dcqcn_sink.rs`: CNP is Not-ECT, 64B, and tied to the marked packet id.
            if cnp.control = some .dcqcnCnp ∧ cnp.ecn = .notEct ∧ cnp.sizeB = 64
               ∧ cnp.flowId = p.flowId ∧ cnp.pktId = p.pktId
               ∧ ¬ (te.t - s.sink.lastCnp < s.sink.cnpInterval) then
              some { s with sink := { s.sink with lastCnp := te.t, pending := none } }
            else none
    | .srcRecv _ pkt =>
        -- matches `dcqcn_source.rs`: only CNP affects the rate state
        if pkt.control = some .dcqcnCnp then
          some { s with src := s.src.onCnp te.t }
        else
          some s
    | .srcTimer _ =>
        some { s with src := s.src.onTimer }

  def SpecTraces (init : State) : Set Trace :=
    fun tr => (tr.foldlM step init).isSome

  theorem days_dcqcn_trace_refines
    (DaysTraces : Set Trace) (init : State) :
    Refines DaysTraces (SpecTraces init) := by
    -- Goal: show any logged Days trace is accepted by the DCQCN+ECN spec.
    -- Proof strategy in practice:
    --   (i) define DaysTraces via a Rust→trace extractor you trust (or a trace logger),
    --  (ii) prove per-event correspondence lemmas for:
    --       `DcqcnPacketSource.on_cnp`, `DcqcnPacketSource.timer_tick`,
    --       `DcqcnPacketSink.maybe_send_cnp`, and ECN marking/drop behavior,
    -- (iii) induct over the trace, showing each logged event preserves acceptance.
    admit

  end Days.Spec

  How you proceed from here (to make this more than a sketch):

  - Decide what “time” and “rate” are in the trace (if you keep f64 in Rust, strict equality becomes painful; for strict refinement, strongly consider logging fixed-point integers, e.g. time_ns : u64, rate_bps_fp : u64).
  - Add a trace logger in Rust at the five call sites above so DaysTraces is a real set of traces.
  - Prove local correspondence first: show each Rust state update is exactly the onCnp/onTimer/maybe_send_cnp function (or refactor Rust to call a shared pure function so the lemma is trivial).
  - Then prove the refinement theorem by induction over the logged trace (the step function is set up for that style of proof).

  If you paste a sample “event trace” format you’d be willing to log from Days (JSON lines, CSV, etc.), I can tailor the Event/Packet types and the step rules so they match exactly what you can realistically emit.

---
I have four different .csv formats for event traces: PFC (L2) traces, sources, sinks, and switches. They have different columns. All four are available in `logs/pfc`.
---
• What you have in logs/pfc/ are periodic interval reports, not per-packet/per-event traces:

  - logs/pfc/sources.csv matches PacketSourceReport (src/flows/source.rs:31) with columns id,flow_id,start_time,end_time,sent_packets,packet_sizes,ack_bytes
  - logs/pfc/sinks.csv matches PacketSinkReport (src/flows/sink.rs:31)
  - logs/pfc/switches.csv matches SchedulerReport (src/schedulers/mod.rs:15)
  - logs/pfc/pfc.csv matches PfcPortReport (src/l2/pfc.rs:46)

  That’s a perfectly valid observable trace—but it’s too lossy to express “DCQCN adheres to its spec” via trace refinement, because none of these logs expose the DCQCN-relevant events/state (CNP sent/received, CE-mark observations, alpha,
  rate_bps, etc.; see src/flows/dcqcn_source.rs:139 / src/flows/dcqcn_sink.rs:104).

  ## 1) Minimal Lean model that matches your current CSV “events”

  Treat each CSV row as an observation over an interval; the run’s observable trace is a record of four lists:

  import Std

  namespace Days.Obs

  abbrev Time := Rat  -- parse CSV f64 strings as exact decimals → Rat

  structure SourceRow where
    id : Nat
    flowId : Nat
    startTime : Time
    endTime : Time
    sentPackets : Nat
    packetSizes : Nat
    ackBytes : Nat

  structure SinkRow where
    id : Nat
    flowId : Nat
    startTime : Time
    endTime : Time
    recvPackets : Nat
    recvSizes : Nat
    qDelayMean : Time
    oneWayDelayMean : Time

  structure SwitchRow where
    id : Nat
    startTime : Time
    endTime : Time
    recvPackets : Nat
    droppedPackets : Nat
    fwdPackets : Nat
    queueLengthB : Nat
    recvSizesB : Nat
    fwdSizesB : Nat
    throughputMean : Time
    qDelayMean : Time

  structure PfcRow where
    id : Nat
    startTime : Time
    endTime : Time
    pauseFrames : Nat
    resumeFrames : Nat
    droppedPackets : Nat
    maxOccB : Nat
    curOccB : Nat

  structure RunObs where
    sources : List SourceRow
    sinks : List SinkRow
    switches : List SwitchRow
    pfc : List PfcRow

  def Refines (Impl Spec : Set RunObs) : Prop :=
    ∀ o, Impl o → Spec o

  Now define a report-level spec as “self-consistency + accounting invariants” (this is what your current logs can support). Example: switch queue-length accounting across intervals (matches how queue_length is maintained in src/schedulers/port.rs:267 and never reset in reset_stats):

  namespace SwitchSpec

  structure St where
    lastQ : Std.HashMap Nat Nat := {}     -- port id ↦ last reported queue_length
    lastEnd : Std.HashMap Nat Time := {}  -- port id ↦ last end_time

  def step (ε : Time) (st : St) (r : SwitchRow) : Option St := do
    if r.startTime > r.endTime then none else
    let q0 := st.lastQ.findD r.id 0
    -- queue_length_end = queue_length_start + recvSizes - fwdSizes
    if q0 + r.recvSizesB ≠ r.fwdSizesB + r.queueLengthB then none else
    -- (optional) interval chaining within epsilon:
    match st.lastEnd.find? r.id with
    | none => some { st with lastQ := st.lastQ.insert r.id r.queueLengthB
                            lastEnd := st.lastEnd.insert r.id r.endTime }
    | some prevEnd =>
        if (r.startTime - prevEnd).abs > ε then none else
        some { st with lastQ := st.lastQ.insert r.id r.queueLengthB
                      lastEnd := st.lastEnd.insert r.id r.endTime }

  def Accepts (ε : Time) (rows : List SwitchRow) : Prop :=
    (rows.foldlM (step ε) {}).isSome

  end SwitchSpec

  Then your report-level “conformance theorem” (trace refinement) is:

  theorem days_reports_refine_report_spec
    (DaysRuns : Set RunObs) (ε : Time) :
    Refines DaysRuns (fun o => SwitchSpec.Accepts ε o.switches ∧ True) := by
    -- you’d add analogous Accepts predicates for sources/sinks/pfc
    admit

  This matches your current notion of “events” (rows), but it’s not DCQCN compliance—it’s “the logs are internally consistent with the accounting semantics.”

  ## 2) Minimal extra trace you need for DCQCN/ECN refinement (otherwise it’s not expressible)

  To state DCQCN trace refinement, you need the observable trace to include (at least):

  - ECN marking occurrences (or counts): happens via DropAction::MarkEcn → Packet.mark_ce() (src/schedulers/port.rs:159, src/flows/packet.rs:119)
  - Sink CE→CNP behavior: maybe_send_cnp (src/flows/dcqcn_sink.rs:104)
  - Source reaction: on_cnp and timer_tick (src/flows/dcqcn_source.rs:139, src/flows/dcqcn_source.rs:160), including alpha and rate_bps updates

  The smallest practical move is: add a fifth log (e.g. dcqcn_events.csv) with one row per DCQCN-relevant transition, and include (time, flow_id, endpoint_id, kind, alpha_before/after, rate_before/after) so Lean can check that each step matches the spec.

  Then the DCQCN conformance theorem becomes the one you originally wanted:

  -- DaysDcqcnTraces ⊆ SpecDcqcnTraces
  theorem days_dcqcn_refines :
    Refines DaysDcqcnTraces (SpecDcqcnTraces init) := by
    admit

  If you want, I can propose an exact dcqcn_events.csv schema that lines up 1:1 with your Rust call sites (so every logged row corresponds to exactly one of: “CNP received”, “timer tick”, “CNP sent”), and then rewrite the Lean Event/step so it is literally a checker for those rows.


---
Yes, proceed to propose an exact dcqcn_events.csv schema that lines up 1:1 with your Rust call sites (so every logged row corresponds to exactly one of: “CNP received”, “timer tick”, “CNP sent”), and then rewrite the Lean Event/step so it is literally a checker for those rows.
---

• Crafting Lean code structure (2m 53s • esc to interrupt)

• dcqcn_events.csv (single-file, 1 row = 1 DCQCN transition)

  Header (exact columns, in order):

  time_ns,kind,endpoint_id,flow_id,pkt_id,pkt_flow_id,trigger_ecn,cnp_priority,cnp_size_b,cnp_ecn,cnp_cwr,cnp_last_packet,cnp_interval_ns,g_ppb
  ,mi_ppb,init_rate_bps,min_rate_bps,max_rate_bps,ai_rate_bps,hai_rate_bps,alpha_ppb,rate_bps,cnp_seen,last_cnp_ns

  Conventions / required fields per kind:

  - kind=cnp_sent (log at src/flows/dcqcn_sink.rs:104 inside maybe_send_cnp, right before self.output.send(cnp)):
      - Required:
        time_ns,endpoint_id,flow_id,pkt_id,pkt_flow_id,trigger_ecn,cnp_priority,cnp_size_b,cnp_ecn,cnp_cwr,cnp_last_packet,cnp_interval_ns
      - Must satisfy (from your Rust): trigger_ecn=CE, cnp_size_b=64, cnp_ecn=NotEct, cnp_cwr=false, cnp_last_packet=false, and
        pkt_flow_id=flow_id.
      - last_cnp_ns must be time_ns (because sink sets last_cnp_time = now when it actually sends).
      - Leave source-state columns empty: alpha_ppb,rate_bps,cnp_seen (or write empty string).
  - kind=cnp_recv (log at src/flows/dcqcn_source.rs:132 in packet_received, only when packet.control == DcqcnCnp, after on_cnp(now) returns):
      - Required packet sanity: pkt_id,pkt_flow_id,cnp_size_b,cnp_ecn,cnp_cwr,cnp_last_packet (these describe the received CNP packet).
      - Required source snapshot (post-state): alpha_ppb,rate_bps,cnp_seen,last_cnp_ns (these are after handling the CNP).
      - Leave trigger_ecn empty.
  - kind=timer_tick (log at src/flows/dcqcn_source.rs:160 at end of timer_tick):
      - Required source snapshot (post-state): alpha_ppb,rate_bps,cnp_seen,last_cnp_ns.
      - Leave all packet fields empty (pkt_id..cnp_last_packet).

  Numeric encodings (so Lean can check deterministically):

  - time_ns, *_bps, *_ns are integers.
  - g_ppb, mi_ppb, alpha_ppb are “parts-per-billion”: value × 1_000_000_000, integer-rounded.
      - Lean checker below assumes the spec arithmetic is done in this integer domain (fixed-point with flooring on divisions).

  ———

  ## Lean 4: “rows are events” checker (no extra Event type)

  This is a literal acceptor for dcqcn_events.csv rows: it folds over rows, maintains per-endpoint DCQCN state, and rejects if any row’s post-
  state doesn’t match the DCQCN step semantics (plus CNP well-formedness + interval gates + “no spurious CNP receives”).

  import Std

  namespace Days.DcqcnEventLog

  def PPB : Nat := 1_000_000_000

  inductive Kind | cnpSent | cnpRecv | timerTick
  deriving DecidableEq, Repr

  inductive Ecn | NotEct | Ect0 | Ect1 | Ce
  deriving DecidableEq, Repr

  /-- 1:1 with one CSV row from `dcqcn_events.csv`. -/
  structure Row where
    timeNs : Nat
    kind : Kind
    endpointId : Nat
    flowId : Nat

    -- Present for `cnpSent`/`cnpRecv`, empty for `timerTick`.
    pktId : Option Nat := none
    pktFlowId : Option Nat := none
    triggerEcn : Option Ecn := none      -- only for `cnpSent`
    cnpPriority : Option Nat := none     -- only for `cnpSent` (and optionally `cnpRecv`)
    cnpSizeB : Option Nat := none
    cnpEcn : Option Ecn := none
    cnpCwr : Option Bool := none
    cnpLastPacket : Option Bool := none

    -- Repeat on every row (so the file is self-contained).
    cnpIntervalNs : Nat
    gPpb : Nat
    miPpb : Nat
    initRateBps : Nat
    minRateBps : Nat
    maxRateBps : Nat
    aiRateBps : Nat
    haiRateBps : Nat

    -- Present for source kinds (`cnpRecv`/`timerTick`) as *post-state* snapshot.
    -- For sink kind (`cnpSent`), only `lastCnpNs` is meaningful; others may be empty.
    alphaPpb : Option Nat := none
    rateBps : Option Nat := none
    cnpSeen : Option Bool := none
    lastCnpNs : Option Nat := none
  deriving Repr

  structure SrcParams where
    flowId : Nat
    cnpIntervalNs : Nat
    gPpb : Nat
    miPpb : Nat
    initRateBps : Nat
    minRateBps : Nat
    maxRateBps : Nat
    aiRateBps : Nat
    haiRateBps : Nat
  deriving DecidableEq, Repr

  structure SrcState where
    p : SrcParams
    alphaPpb : Nat := 0
    rateBps : Nat
    cnpSeen : Bool := false
    lastCnpNs : Option Nat := none
    lastTimeNs : Option Nat := none
  deriving Repr

  structure SinkParams where
    flowId : Nat
    cnpIntervalNs : Nat
    cnpPriority : Nat
  deriving DecidableEq, Repr

  structure SinkState where
    p : SinkParams
    lastCnpNs : Option Nat := none
    lastTimeNs : Option Nat := none
  deriving Repr

  structure Global where
    src : Std.HashMap Nat SrcState
    sink : Std.HashMap Nat SinkState
    pending : Std.HashMap (Nat × Nat) Unit  -- (flowId, pktId)
  deriving Repr

  def Global.empty : Global :=
    { src := Std.HashMap.empty, sink := Std.HashMap.empty, pending := Std.HashMap.empty }

  def requireP (p : Prop) [Decidable p] : Option Unit :=
    if p then some () else none

  def requireB (b : Bool) : Option Unit :=
    if b then some () else none

  def okParams (p : SrcParams) : Prop :=
    p.gPpb ≤ PPB ∧ p.miPpb ≤ PPB ∧
    p.minRateBps ≤ p.initRateBps ∧ p.initRateBps ≤ p.maxRateBps

  def alphaOnCnp (gPpb alphaPpb : Nat) : Nat :=
    ((PPB - gPpb) * alphaPpb) / PPB + gPpb

  def alphaOnTimer (gPpb alphaPpb : Nat) : Nat :=
    ((PPB - gPpb) * alphaPpb) / PPB

  def srcAfterCnp (s : SrcState) (t : Nat) : SrcState :=
    let p := s.p
    let applied :=
      match s.lastCnpNs with
      | none => true
      | some last => last + p.cnpIntervalNs ≤ t
    if applied then
      let α := alphaOnCnp p.gPpb s.alphaPpb
      let decPpb := PPB - (p.miPpb * α) / PPB
      let rDec := (s.rateBps * decPpb) / PPB
      let r := Nat.max rDec p.minRateBps
      { s with alphaPpb := α, rateBps := r, cnpSeen := true,
               lastCnpNs := some t, lastTimeNs := some t }
    else
      { s with lastTimeNs := some t }

  def srcAfterTimer (s : SrcState) (t : Nat) : SrcState :=
    let p := s.p
    if s.cnpSeen then
      { s with cnpSeen := false, lastTimeNs := some t }
    else
      let α := alphaOnTimer p.gPpb s.alphaPpb
      let inc := if α < PPB / 10 then p.haiRateBps else p.aiRateBps
      let r := Nat.min (s.rateBps + inc) p.maxRateBps
      { s with alphaPpb := α, rateBps := r, cnpSeen := false, lastTimeNs := some t }

  def cnpPacketOk (r : Row) : Option Nat := do
    let pktId <- r.pktId
    let pktFlowId <- r.pktFlowId
    requireP (pktFlowId = r.flowId)

    let sz <- r.cnpSizeB
    let ecn <- r.cnpEcn
    let cwr <- r.cnpCwr
    let last <- r.cnpLastPacket
    requireP (sz = 64)
    requireP (ecn = Ecn.NotEct)
    requireP (cwr = false)
    requireP (last = false)
    pure pktId

  def rowParams (r : Row) : SrcParams :=
    { flowId := r.flowId
      cnpIntervalNs := r.cnpIntervalNs
      gPpb := r.gPpb
      miPpb := r.miPpb
      initRateBps := r.initRateBps
      minRateBps := r.minRateBps
      maxRateBps := r.maxRateBps
      aiRateBps := r.aiRateBps
      haiRateBps := r.haiRateBps }

  def step (g : Global) (r : Row) : Option Global := do
    match r.kind with
    | Kind.cnpSent => do
        let pktId <- cnpPacketOk r
        let trig <- r.triggerEcn
        requireP (trig = Ecn.Ce)
        let prio <- r.cnpPriority

        let sinkSt :=
          match g.sink.find? r.endpointId with
          | some s => s
          | none => { p := { flowId := r.flowId, cnpIntervalNs := r.cnpIntervalNs, cnpPriority := prio } }

        requireP (sinkSt.p.flowId = r.flowId)
        requireP (sinkSt.p.cnpIntervalNs = r.cnpIntervalNs)
        requireP (sinkSt.p.cnpPriority = prio)

        match sinkSt.lastTimeNs with
        | some prev => requireP (prev ≤ r.timeNs)
        | none => pure ()

        match sinkSt.lastCnpNs with
        | some last => requireP (last + sinkSt.p.cnpIntervalNs ≤ r.timeNs)
        | none => pure ()

        requireP (r.lastCnpNs = some r.timeNs)

        requireB ((g.pending.find? (r.flowId, pktId)).isNone)

        let sinkSt' := { sinkSt with lastCnpNs := some r.timeNs, lastTimeNs := some r.timeNs }
        pure
          { g with
            sink := g.sink.insert r.endpointId sinkSt'
            pending := g.pending.insert (r.flowId, pktId) () }

    | Kind.cnpRecv => do
        let pktId <- cnpPacketOk r
        requireB ((g.pending.find? (r.flowId, pktId)).isSome)

        let p := rowParams r
        requireP (okParams p)

        let srcSt :=
          match g.src.find? r.endpointId with
          | some s => s
          | none => { p := p, rateBps := p.initRateBps }

        requireP (srcSt.p = p)

        match srcSt.lastTimeNs with
        | some prev => requireP (prev ≤ r.timeNs)
        | none => pure ()

        let srcSt' := srcAfterCnp srcSt r.timeNs

        let α <- r.alphaPpb;  requireP (α = srcSt'.alphaPpb)
        let rb <- r.rateBps;  requireP (rb = srcSt'.rateBps)
        let seen <- r.cnpSeen; requireP (seen = srcSt'.cnpSeen)
        requireP (r.lastCnpNs = srcSt'.lastCnpNs)

        pure
          { g with
            src := g.src.insert r.endpointId srcSt'
            pending := g.pending.erase (r.flowId, pktId) }

    | Kind.timerTick => do
        let p := rowParams r
        requireP (okParams p)

        let srcSt :=
          match g.src.find? r.endpointId with
          | some s => s
          | none => { p := p, rateBps := p.initRateBps }

        requireP (srcSt.p = p)

        match srcSt.lastTimeNs with
        | some prev => requireP (prev ≤ r.timeNs)
        | none => pure ()

        let srcSt' := srcAfterTimer srcSt r.timeNs

        let α <- r.alphaPpb;  requireP (α = srcSt'.alphaPpb)
        let rb <- r.rateBps;  requireP (rb = srcSt'.rateBps)
        let seen <- r.cnpSeen; requireP (seen = srcSt'.cnpSeen)
        requireP (r.lastCnpNs = srcSt'.lastCnpNs)

        pure { g with src := g.src.insert r.endpointId srcSt' }

  def accepts (rows : List Row) : Bool :=
    (rows.foldlM step Global.empty).isSome

  end Days.DcqcnEventLog

If you want, I can also (a) implement the Rust logging + new CSV file plumbing (preferably by adding a Report::DcqcnEvent and
dcqcn_events.csv to src/utils/logger.rs), and (b) add a tiny Lean/CLI “parse CSV → List Row → accepts” harness so you can run the checker on
real simulation output.

---
Yes, proceed to do (a).
---

• - Added a lean Cargo feature (Cargo.toml:13) and gated DCQCN event logging behind --features dcqcn,lean.
  - Extended the CSV logger with Report::DcqcnEventRow + dcqcn_events.csv output (src/utils/logger.rs:57, src/utils/logger.rs:209) and added
    CsvLogger::try_log_report so unit tests don’t panic when logging isn’t initialized (src/utils/logger.rs:282).
  - Emitted 1-row-per-transition DCQCN events:
      - cnp_sent at DcqcnPacketSink::maybe_send_cnp (src/flows/dcqcn_sink.rs:144)
      - cnp_recv + timer_tick at DcqcnPacketSource (src/flows/dcqcn_source.rs:157, src/flows/dcqcn_source.rs:216)

  To generate the file: cargo run --features dcqcn,lean -- configs/dcqcn_simple.toml (writes dcqcn_events.csv under the config’s log_path).

  Tests run: cargo test --features test -- --show-output, cargo test --features "test,dcqcn,lean" -- --show-output.

---
Now proceed to complete (b), (b) add a tiny Lean/CLI “parse CSV → List Row → accepts” harness so you can run the checker on
  real simulation output.
---

- Added a self-contained Lean 4 “trace accepts” harness under lean/ (checker logic in lean/LeanGuard/DcqcnEventLog.lean:1, CLI in lean/LeanGuard/Main.lean:1) that parses dcqcn_events.csv and prints ACCEPT / REJECT: line ….

- The checker simulates the same floating-point DCQCN update logic as src/flows/dcqcn_source.rs:1 and checks the logged *_ppb / *_bps snapshots using the same rounding as the Rust logger.

- Updated run instructions in docs/lean_dcqcn_checker.md:1 and ignored Lean build artifacts via .gitignore:1.

To run it on real output:

```
cargo run --features dcqcn,lean -- configs/dcqcn_simple.toml
cd lean
lake build
lake exe dcqcn_check ../logs/dcqcn_simple/dcqcn_events.csv
```
