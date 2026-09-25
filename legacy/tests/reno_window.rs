use days_legacy::flows::cc::{AckEvent, CongestionControl};
use days_legacy::flows::reno::TCPReno;

#[test]
fn sustained_successful_delivery_grows_reno_cwnd_past_initial_ssthresh() {
    let mut reno = TCPReno::new();
    let mss = 512;
    let mut acknowledgment = 0;

    for round in 1..=127 {
        reno.packet_sent(mss, round as f64 * 0.1);
        acknowledgment += mss;
        reno.ack_received(AckEvent::new_basic(
            acknowledgment,
            0.1,
            round as f64 * 0.1,
            mss,
        ));
    }

    println!("reno_cwnd_bytes={}", reno.get_cwnd());
    assert!(
        reno.get_cwnd() > 65_535,
        "sustained successful delivery left cwnd capped at {} bytes",
        reno.get_cwnd()
    );
}
