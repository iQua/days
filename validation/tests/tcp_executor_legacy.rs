use days_executor::{TcpCongestionControl, TcpPhase};
use days_legacy::flows::cc::{AckEvent, CongestionControl, CongestionEvent};
use days_legacy::flows::cubic::TCPCubic;
use days_legacy::flows::reno::TCPReno;

const MSS: u64 = 512;
const RTT_NS: u64 = 100_000_000;

fn legacy_ack(control: &mut dyn CongestionControl, acknowledgment: u64, now_ns: u64, bytes: u64) {
    control.ack_received(AckEvent::new_basic(
        acknowledgment as usize,
        RTT_NS as f64 / 1e9,
        now_ns as f64 / 1e9,
        bytes as usize,
    ));
}

#[test]
fn executor_reno_matches_legacy_slow_start_then_documents_f64_ca_divergence() {
    let mut legacy = TCPReno::new();
    let mut executor = TcpCongestionControl::reno(MSS);
    let mut acknowledgment = 0;

    for round in 1..=8 {
        acknowledgment += MSS;
        legacy_ack(&mut legacy, acknowledgment, round * RTT_NS, MSS);
        executor.on_new_ack(
            MSS,
            round * RTT_NS,
            RTT_NS,
            executor.cwnd_bytes(MSS),
            acknowledgment,
        );
        assert_eq!(executor.cwnd_bytes(MSS) as usize, legacy.get_cwnd());
    }

    let mut round = 8;
    while executor.phase() == TcpPhase::SlowStart {
        round += 1;
        acknowledgment += MSS;
        legacy_ack(&mut legacy, acknowledgment, round * RTT_NS, MSS);
        executor.on_new_ack(
            MSS,
            round * RTT_NS,
            RTT_NS,
            executor.cwnd_bytes(MSS),
            acknowledgment,
        );
    }
    assert_eq!(executor.cwnd_bytes(MSS), 65_535);
    assert_eq!(legacy.get_cwnd(), 65_535);

    let mut first_divergence = None;
    for ack_index in 1..=256 {
        round += 1;
        acknowledgment += MSS;
        legacy_ack(&mut legacy, acknowledgment, round * RTT_NS, MSS);
        executor.on_new_ack(
            MSS,
            round * RTT_NS,
            RTT_NS,
            executor.cwnd_bytes(MSS),
            acknowledgment,
        );
        if executor.cwnd_bytes(MSS) as usize != legacy.get_cwnd() {
            first_divergence = Some((ack_index, executor.cwnd_bytes(MSS), legacy.get_cwnd()));
            break;
        }
    }
    let (ack_index, exact, float) = first_divergence.expect("the CA trajectories must diverge");
    assert_eq!(ack_index, 1);
    assert_eq!(exact, 65_535);
    assert_eq!(float, 65_539);
    println!(
        "reno first_ca_divergence_ack={ack_index} executor={exact} legacy={float} divergence={} (integer ABC versus legacy f64 accumulator)",
        float as i64 - exact as i64
    );
}

#[test]
fn executor_reno_uses_real_flight_while_legacy_standalone_recovery_sees_zero() {
    let mut legacy = TCPReno::new();
    let mut executor = TcpCongestionControl::reno(MSS);

    legacy.consecutive_dupacks_received();
    executor.on_fast_retransmit(8 * MSS, 0);

    assert_eq!(legacy.get_cwnd(), 5 * MSS as usize);
    assert_eq!(executor.cwnd_bytes(MSS), 7 * MSS);
    println!(
        "reno fast_recovery executor={} legacy={} (executor flight=4096; legacy controller packet_sent hook is a no-op, flight=0)",
        executor.cwnd_bytes(MSS),
        legacy.get_cwnd()
    );
}

#[test]
fn executor_cubic_matches_legacy_slow_start_and_records_fixed_point_divergence() {
    let mut legacy = TCPCubic::new();
    let mut executor = TcpCongestionControl::cubic(MSS);
    let mut acknowledgment = 0;
    let mut now_ns = 0;

    while executor.phase() == TcpPhase::SlowStart {
        now_ns += RTT_NS;
        acknowledgment += MSS;
        legacy_ack(&mut legacy, acknowledgment, now_ns, MSS);
        executor.on_new_ack(
            MSS,
            now_ns,
            RTT_NS,
            executor.cwnd_bytes(MSS),
            acknowledgment,
        );
        assert!(
            (executor.cwnd_bytes(MSS) as i64 - legacy.get_cwnd() as i64).abs() <= 1,
            "the shared slow-start trajectory may differ only at the fixed-point byte boundary"
        );
    }

    let flight = executor.cwnd_bytes(MSS);
    legacy.congestion_event(CongestionEvent {
        now: now_ns as f64 / 1e9,
        flight_size_bytes: flight as usize,
    });
    executor.on_loss(flight, now_ns);
    legacy.dupack_over();
    executor.on_recovery_exit();

    let mut first_divergence = None;
    for _ in 0..10_000 {
        now_ns += RTT_NS;
        acknowledgment += MSS;
        legacy_ack(&mut legacy, acknowledgment, now_ns, MSS);
        executor.on_new_ack(
            MSS,
            now_ns,
            RTT_NS,
            executor.cwnd_bytes(MSS),
            acknowledgment,
        );
        if executor.cwnd_bytes(MSS) as usize != legacy.get_cwnd() {
            first_divergence = Some((now_ns, executor.cwnd_bytes(MSS), legacy.get_cwnd()));
            break;
        }
    }
    let (time_ns, exact, float) =
        first_divergence.expect("integer CUBIC must expose a lattice divergence from legacy f64");
    println!(
        "cubic first_divergence_ns={time_ns} executor={exact} legacy={float} delta={}",
        float as i64 - exact as i64
    );
}
