//! P16 ecnbytes and ecnramp (user rulings, Oct 8): the SimAI fidelity lowers SimAI.conf's ECN rows
//! to the ECN ramp in bytes of queue, over a byte capacity of `BUFFER_SIZE`, as SimAI and real
//! switches count queue depth.
//!
//! Each egress rate's row is SimAI.conf's own ramp: KMIN and KMAX in KB times 1,000, PMAX as an
//! exact fraction: 400,000 / 1,600,000 B at 1/5 on a 100 Gb/s egress and 800,000 / 3,200,000 B at
//! 1/5 on a 400 Gb/s one, against SimAI's 32 MiB `BUFFER_SIZE`.

#[path = "support/aicb.rs"]
mod aicb;

use std::collections::BTreeMap;

use days_executor::{DropMarkPolicy, EcnRampPolicy, SimulationImage};

const G: u64 = 1_000_000_000;

/// Every switch queue's ECN ramp, by its egress link's rate, with its queue count.
fn ecn_policies(image: &SimulationImage) -> BTreeMap<(u64, String), usize> {
    let mut policies = BTreeMap::new();
    for queue in image.switch_states.iter().flat_map(|state| &state.queues) {
        let DropMarkPolicy::EcnRamp(policy) = queue.drop_mark else {
            panic!("an AICB switch queue marks on the ECN ramp")
        };
        let link = queue
            .egress_link
            .expect("a switch queue has an egress link");
        let rate = image.links[link.0 as usize].rate_bps;
        *policies.entry((rate, format!("{policy:?}"))).or_default() += 1;
    }
    policies
}

fn expected(rate: u64) -> EcnRampPolicy {
    let (kmin_bytes, kmax_bytes) = match rate {
        rate if rate == 100 * G => (400_000, 1_600_000),
        rate if rate == 400 * G => (800_000, 3_200_000),
        other => panic!("unexpected egress rate {other}"),
    };
    EcnRampPolicy {
        capacity_bytes: 33_554_432,
        kmin_bytes,
        kmax_bytes,
        pmax_numerator: 1,
        pmax_denominator: 5,
    }
}

fn assert_simai_ecn(name: &str) {
    let image = aicb::lower(name);
    let policies = ecn_policies(&image);
    assert!(!policies.is_empty(), "{name}: switch queues");
    for ((rate, policy), queues) in policies {
        assert_eq!(
            policy,
            format!("{:?}", expected(rate)),
            "{name}: {queues} queues at {rate} b/s"
        );
    }
}

#[test]
fn b4_marks_on_simais_byte_rows() {
    assert_simai_ecn("b4-simai.toml");
}

#[test]
fn the_smoke_marks_on_simais_byte_rows() {
    assert_simai_ecn("smoke-simai.toml");
}

#[test]
fn the_reduced_traces_mark_on_simais_byte_rows() {
    for name in [
        "reduced-dense-simai.toml",
        "reduced-dense-megatron.toml",
        "reduced-moe-simai.toml",
        "reduced-moe-imbalanced.toml",
    ] {
        assert_simai_ecn(name);
    }
}
