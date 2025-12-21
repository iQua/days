#![cfg(all(feature = "test", feature = "dcqcn", feature = "lean"))]

use std::fs;

use csv::ReaderBuilder;
use rand::SeedableRng;
use tempfile::tempdir;

use days::flows::dcqcn_sink::DcqcnPacketSink;
use days::flows::dcqcn_source::DcqcnPacketSource;
use days::flows::packet::{ControlPacket, EcnField, Packet};
use days::flows::{DcqcnCharacteristics, DistributionInfo, TrafficCharacteristics};
use days::utils::logger::CsvLogger;

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
fn dcqcn_events_include_event_id() {
    let tmp = tempdir().expect("create temp dir");
    let logger = CsvLogger::get_instance();
    logger
        .init(tmp.path().to_str().expect("temp dir path"))
        .expect("init logger");

    let traffic = make_dcqcn_traffic();

    let mut sink = DcqcnPacketSink::new(0, &traffic);
    let mut packet = Packet::new(1200, 1, 0, 0.0);
    packet.ecn = EcnField::Ce;
    futures::executor::block_on(sink.process(packet, 0.0));

    let rng = rand::rngs::SmallRng::seed_from_u64(1);
    let mut source = DcqcnPacketSource::new(0, Vec::new(), traffic, 0, rng);

    let mut cnp = Packet::new(64, 1, 0, 0.0);
    cnp.control = Some(ControlPacket::DcqcnCnp);
    cnp.ecn = EcnField::NotEct;
    source.packet_received(cnp, 0.0);
    source.timer_tick(0.0);

    logger.flush_reports();

    let path = tmp.path().join("dcqcn_events.csv");
    let content = fs::read_to_string(&path).expect("read dcqcn_events.csv");

    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(content.as_bytes());

    let headers = reader.headers().expect("read headers").clone();
    assert_eq!(headers.get(0), Some("time_ns"));
    assert_eq!(headers.get(1), Some("event_id"));

    let mut event_ids: Vec<u64> = Vec::new();
    for row in reader.records() {
        let row = row.expect("read record");
        let event_id: u64 = row
            .get(1)
            .expect("event_id column")
            .parse()
            .expect("parse event_id");
        event_ids.push(event_id);
    }

    event_ids.sort_unstable();
    event_ids.dedup();
    assert_eq!(event_ids, vec![0, 1, 2]);
}
