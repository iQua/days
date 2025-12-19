#![cfg(all(feature = "test", feature = "dcqcn"))]

use futures::executor::block_on;
use nexosim::ports::EventSlot;
use rand::SeedableRng;
use rand::rngs::SmallRng;

use days::flows::dcqcn_sink::DcqcnPacketSink;
use days::flows::dcqcn_source::DcqcnPacketSource;
use days::flows::packet::{ControlPacket, EcnField, Packet};
use days::flows::{DcqcnCharacteristics, DistributionInfo, TrafficCharacteristics};

fn make_dcqcn_traffic() -> TrafficCharacteristics {
    let mut traffic = TrafficCharacteristics::new(
        0.0,
        None,
        Some(10_000),
        DistributionInfo::Uniform {
            low: 1.0,
            high: 1.0,
        },
        DistributionInfo::DiscreteUniform {
            low: 1000,
            high: 1000,
        },
        None,
    );

    traffic.dcqcn = Some(DcqcnCharacteristics {
        rate_gbps: 10.0,
        min_rate_gbps: 1.0,
        max_rate_gbps: 10.0,
        g: 0.5,
        ai_rate_gbps: 0.5,
        hai_rate_gbps: 1.0,
        mi_factor: 0.5,
        rtt_ns: Some(100_000.0),
        cnp_interval_ns: Some(10_000.0),
        pacing_interval_ns: Some(1_000.0),
        cnp_priority: Some(0),
    });

    traffic
}

#[test]
fn test_cnp_reduces_rate() {
    let traffic = make_dcqcn_traffic();
    let rng = SmallRng::seed_from_u64(1);
    let mut source = DcqcnPacketSource::new(0, Vec::new(), traffic, 0, rng);

    let initial_rate = source.current_rate_bps();

    let mut cnp = Packet::new(64, 0, 0, 0.0);
    cnp.control = Some(ControlPacket::DcqcnCnp);

    source.packet_received(cnp, 0.0);
    let new_rate = source.current_rate_bps();

    assert!(new_rate < initial_rate);
}

#[test]
fn test_cnp_generated_on_ce() {
    let traffic = make_dcqcn_traffic();
    let mut sink = DcqcnPacketSink::new(0, &traffic);

    let mut slot = EventSlot::new();
    sink.output.connect_sink(&slot);

    let mut packet = Packet::new(1200, 1, 0, 0.0);
    packet.ecn = EcnField::Ce;

    block_on(sink.process(packet, 0.0));

    let cnp = slot.next().expect("expected CNP packet");
    assert_eq!(cnp.control, Some(ControlPacket::DcqcnCnp));
}
