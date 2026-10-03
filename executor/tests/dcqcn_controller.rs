//! The Mellanox-form DCQCN reaction point, one test per rule of the spec
//! (`days-gpu/evidence/P16/simai-dcqcn-spec.md` section 7; design note §10.1).

use days_executor::{DCQCN_ALPHA_ONE, DcqcnController, DcqcnControllerConfig};

const G_2_8: u64 = DCQCN_ALPHA_ONE >> 8;

/// SimAI's and HPCC's shipped block at 100 Gb/s.
fn shipped() -> DcqcnControllerConfig {
    DcqcnControllerConfig {
        initial_rate_bps: 100_000_000_000,
        minimum_rate_bps: 100_000_000,
        maximum_rate_bps: 100_000_000_000,
        additive_rate_bps: 50_000_000,
        hyper_rate_bps: 100_000_000,
        g_q63: G_2_8,
        alpha_interval_ns: 1_000,
        decrease_interval_ns: 4_000,
        increase_interval_ns: 900_000,
        fast_recovery_steps: 1,
        clamp_target_rate: false,
    }
}

#[test]
fn a_new_controller_is_pristine() {
    let controller = DcqcnController::new(shipped()).unwrap();
    assert_eq!(controller.alpha_q63, DCQCN_ALPHA_ONE);
    assert_eq!(controller.current_rate_bps, 100_000_000_000);
    assert_eq!(controller.target_rate_bps, 100_000_000_000);
    assert!(!controller.armed && !controller.alpha_pending && !controller.decrease_pending);
    assert!(!controller.increase_armed);
    assert_eq!(
        (
            controller.next_alpha_ns,
            controller.next_decrease_ns,
            controller.next_increase_ns,
            controller.stage
        ),
        (0, 0, 0, 0)
    );
    // Nothing is due before the first feedback, at any time.
    assert_eq!(controller.due_ns(), u64::MAX);
}

#[test]
fn the_first_check_halves_the_rate_when_feedback_continues() {
    // Feedback in every alpha window keeps alpha at one (spec section 7: the first CNP does not
    // count, each later window's does), so the first cut, at 4,001 ns, is R - ceil(R/2).
    let mut controller = DcqcnController::new(shipped()).unwrap();
    for time in [0, 84, 1_500, 2_500, 3_500] {
        controller.on_feedback(time);
    }
    assert_eq!(controller.next_decrease_ns, 4_001);
    controller.materialize(4_002);
    assert_eq!(controller.current_rate_bps, 50_000_000_000);
    assert_eq!(controller.next_increase_ns, 904_001);
}

#[test]
fn the_first_check_cuts_by_one_decayed_alpha_without_more_feedback() {
    let mut controller = DcqcnController::new(shipped()).unwrap();
    controller.on_feedback(0);
    controller.materialize(4_002);
    // Four alpha ticks (1..4 us) decay alpha to (255/256)^4, exact in Q63.
    let alpha = DCQCN_ALPHA_ONE / 256_u64.pow(4) * 255_u64.pow(4);
    assert_eq!(controller.alpha_q63, alpha);
    let rate = 100_000_000_000_u128;
    let expected = rate - (rate * u128::from(alpha)).div_ceil(1 << 64);
    assert_eq!(u128::from(controller.current_rate_bps), expected);
}

#[test]
fn alpha_decays_exactly_while_the_q63_grid_holds_it() {
    // (255/256)^k * 2^63 is an integer for 8k <= 63: the floors are exact for seven steps.
    let mut controller = DcqcnController::new(shipped()).unwrap();
    controller.on_feedback(0);
    for k in 1..=7_u32 {
        // A feedback at k us + 1 ns applies the ticks before it; the flag it sets is consumed by
        // the next tick, so alternate observations read pure decays from a fresh controller.
        let mut probe = controller;
        probe.settle(u64::from(k) * 1_000 + 1);
        let exact = DCQCN_ALPHA_ONE / 256_u64.pow(k) * 255_u64.pow(k);
        assert_eq!(probe.alpha_q63, exact, "k = {k}");
    }
}

#[test]
fn a_cut_floors_at_the_minimum_rate() {
    let mut config = shipped();
    config.initial_rate_bps = 150_000_000;
    let mut controller = DcqcnController::new(config).unwrap();
    controller.on_feedback(0);
    controller.on_feedback(84);
    controller.materialize(4_002);
    assert_eq!(controller.current_rate_bps, 100_000_000);
}

#[test]
fn the_target_clamps_only_off_stage_zero_unless_configured() {
    for (clamp, stage_before_cut, expect_clamped) in [
        (false, false, false),
        (false, true, true),
        (true, false, true),
    ] {
        let mut config = shipped();
        config.clamp_target_rate = clamp;
        let mut controller = DcqcnController::new(config).unwrap();
        controller.on_feedback(0);
        controller.on_feedback(84);
        controller.materialize(4_002); // first cut; the target stays 100 Gb/s
        if stage_before_cut {
            controller.materialize(904_002); // one increase: stage 1, current 75 Gb/s
            assert_eq!(controller.stage, 1);
        }
        let current = controller.current_rate_bps;
        let target = controller.target_rate_bps;
        let now = controller.next_decrease_ns + 1;
        controller.on_feedback(now);
        controller.materialize(controller.next_decrease_ns + 1);
        let expected_target = if expect_clamped { current } else { target };
        assert_eq!(
            controller.target_rate_bps, expected_target,
            "clamp {clamp}, stage moved {stage_before_cut}"
        );
        assert_eq!(controller.stage, 0);
    }
}

#[test]
fn increases_run_fast_recovery_then_one_additive_step_then_hyper() {
    let mut config = shipped();
    config.initial_rate_bps = 60_000_000_000;
    config.maximum_rate_bps = 60_000_000_000;
    let mut controller = DcqcnController::new(config).unwrap();
    controller.on_feedback(0);
    controller.on_feedback(84);
    controller.materialize(4_002); // first cut; target 60 Gb/s
    let mut fire = controller.next_increase_ns;
    let mut targets = Vec::new();
    for _ in 0..4 {
        controller.materialize(fire + 1);
        targets.push((controller.stage, controller.target_rate_bps));
        fire += 900_000;
    }
    // Stage 0: fast recovery; stage 1 = F: AI once; stage 2 > F: HAI, clamped at the maximum; the
    // stage saturates at F + 1.
    assert_eq!(
        targets,
        [
            (1, 60_000_000_000),
            (2, 60_000_000_000),
            (2, 60_000_000_000),
            (2, 60_000_000_000)
        ]
    );
    config.maximum_rate_bps = 100_000_000_000;
    let mut controller = DcqcnController::new(config).unwrap();
    controller.on_feedback(0);
    controller.on_feedback(84);
    controller.materialize(4_002);
    let mut fire = controller.next_increase_ns;
    let mut targets = Vec::new();
    for _ in 0..3 {
        controller.materialize(fire + 1);
        targets.push(controller.target_rate_bps);
        fire += 900_000;
    }
    assert_eq!(
        targets,
        [60_000_000_000, 60_050_000_000, 60_150_000_000],
        "FR keeps the target, AI adds 50 Mb/s once, HAI adds 100 Mb/s"
    );
}

#[test]
fn zero_fast_recovery_steps_start_at_the_additive_step() {
    let mut config = shipped();
    config.fast_recovery_steps = 0;
    config.initial_rate_bps = 60_000_000_000;
    let mut controller = DcqcnController::new(config).unwrap();
    controller.on_feedback(0);
    controller.on_feedback(84);
    controller.materialize(4_002);
    controller.materialize(controller.next_increase_ns + 1);
    assert_eq!(controller.target_rate_bps, 60_050_000_000);
    assert_eq!(controller.stage, 1);
}

#[test]
fn the_average_truncates_each_half_and_never_falls_below_the_floor() {
    // Odd current and target: floor(R/2) + floor(T/2) = (R + T)/2 - 1, unlike floor((R+T)/2).
    let mut config = shipped();
    config.initial_rate_bps = 99_999_999_999;
    config.maximum_rate_bps = 99_999_999_999;
    config.minimum_rate_bps = 3;
    let mut controller = DcqcnController::new(config).unwrap();
    controller.on_feedback(0);
    controller.on_feedback(84);
    controller.materialize(4_002);
    let (current, target) = (controller.current_rate_bps, controller.target_rate_bps);
    assert!(current % 2 == 0 || target % 2 == 1);
    controller.materialize(controller.next_increase_ns + 1);
    assert_eq!(controller.current_rate_bps, current / 2 + target / 2);
    // Random feedback against tiny minimums: the rate never leaves
    // [2 * floor((minimum - 1) / 2), maximum].
    for minimum in [3_u64, 4, 5, 6, 7] {
        let mut config = shipped();
        config.minimum_rate_bps = minimum;
        config.initial_rate_bps = minimum + 4;
        config.maximum_rate_bps = minimum + 9;
        config.additive_rate_bps = 1;
        config.hyper_rate_bps = 1;
        config.increase_interval_ns = 5_000;
        let floor = 2 * ((minimum - 1) / 2);
        let mut controller = DcqcnController::new(config).unwrap();
        let mut state = minimum;
        let mut now = 0;
        for _ in 0..5_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            now += (state >> 33) % 9_000;
            controller.on_feedback(now);
            controller.materialize(now + 1);
            assert!(
                (floor..=config.maximum_rate_bps).contains(&controller.current_rate_bps)
                    && (floor..=config.maximum_rate_bps).contains(&controller.target_rate_bps),
                "minimum {minimum}: {controller:?}"
            );
        }
    }
}

#[test]
fn the_decrease_grid_keeps_its_one_nanosecond_offset() {
    let mut controller = DcqcnController::new(shipped()).unwrap();
    controller.on_feedback(1_000);
    let mut cuts = Vec::new();
    for time in [1_100, 5_100, 9_100, 13_100] {
        let before = controller.next_decrease_ns;
        controller.on_feedback(time);
        let advance = controller.materialize(before + 1);
        cuts.push((before, advance.decrease_cuts));
    }
    assert_eq!(
        cuts,
        [(5_001, 1), (9_001, 1), (13_001, 1), (17_001, 1)],
        "checks at t0 + 4,001 + m * 4,000"
    );
}

#[test]
fn a_feedback_at_a_check_instant_precedes_the_check() {
    // Phase order: an arrival at t precedes the controller instants at t, so a feedback that
    // reopens the decrease gate exactly at a grid instant is cut at that instant.
    let mut controller = DcqcnController::new(shipped()).unwrap();
    controller.on_feedback(0);
    controller.materialize(4_002); // first cut at 4,001; the next grid instant is 8,001
    assert!(!controller.decrease_pending);
    controller.on_feedback(8_001);
    assert_eq!(controller.next_decrease_ns, 8_001);
    let advance = controller.materialize(8_002);
    assert_eq!(advance.decrease_cuts, 1);
}

#[test]
fn settle_brings_alpha_and_the_grid_to_the_bound_and_then_holds() {
    let mut controller = DcqcnController::new(shipped()).unwrap();
    controller.on_feedback(0);
    controller.materialize(4_002);
    controller.settle(1_000_001);
    assert_eq!(controller.next_alpha_ns, 1_001_000);
    assert_eq!(controller.next_decrease_ns, 1_000_001);
    let frozen = controller;
    // A second settle at the same bound is the identity.
    controller.settle(1_000_001);
    assert_eq!(controller, frozen);
}

#[test]
fn configurations_out_of_range_are_rejected() {
    let reject = |edit: fn(&mut DcqcnControllerConfig)| {
        let mut config = shipped();
        edit(&mut config);
        DcqcnController::new(config).unwrap_err().to_string()
    };
    assert!(reject(|config| config.minimum_rate_bps = 2).contains("at least 3 bit/s"));
    assert!(reject(|config| config.initial_rate_bps = 99).contains("minimum <= initial"));
    assert!(reject(|config| config.g_q63 = DCQCN_ALPHA_ONE + 1).contains("0..=1"));
    assert!(reject(|config| config.alpha_interval_ns = 0).contains("must be positive"));
    assert!(reject(|config| config.decrease_interval_ns = 0).contains("must be positive"));
    assert!(reject(|config| config.increase_interval_ns = 0).contains("must be positive"));
    assert!(reject(|config| config.fast_recovery_steps = u32::MAX).contains("fast recovery"));
    let mut config = shipped();
    config.g_q63 = 0;
    config.clamp_target_rate = true;
    assert!(DcqcnController::new(config).is_ok(), "g = 0 is legal");
}

/// HPCC's arithmetic as compiled (`-O0`, SSE2, no FMA: `evidence/P16/dcqcn-design-probe/
/// hpcc-updatealphamlx-sim.txt`), as a test-only reference: alpha a binary64, the cut
/// `(uint64)(double(R) * (1 - alpha / 2))`, the average `R/2 + T/2` in integers.
struct HpccDouble {
    alpha: f64,
    current: u64,
    target: u64,
}

#[test]
fn the_shipped_block_matches_hpcc_double_arithmetic_on_a_saturated_stream() {
    // The design note's probe found no Q63 divergence at g = 2^-8 over 11 M cuts; this pins a
    // deterministic stream of 400 cuts: feedback on alternate 84 ns ACKs for 1.6 ms.
    let config = shipped();
    let mut lazy = DcqcnController::new(config).unwrap();
    let g = 1.0_f64 / 256.0;
    let mut hpcc = HpccDouble {
        alpha: 1.0,
        current: config.initial_rate_bps,
        target: config.initial_rate_bps,
    };
    let mut alpha_pending = false;
    let mut decrease_pending = true;
    let mut stage = 0_u32;
    let mut next_alpha = 1_000_u64;
    let mut next_decrease = 4_001_u64;
    let mut next_increase: Option<u64> = None;
    let mut cuts = 0;
    lazy.on_feedback(0);
    let mut time = 168;
    while time < 1_600_000 {
        // Eager HPCC timers before the feedback at `time`.
        loop {
            let instant = next_alpha
                .min(next_decrease)
                .min(next_increase.unwrap_or(u64::MAX));
            if instant >= time {
                break;
            }
            if instant == next_alpha {
                hpcc.alpha = if alpha_pending {
                    (1.0 - g) * hpcc.alpha + g
                } else {
                    (1.0 - g) * hpcc.alpha
                };
                alpha_pending = false;
                next_alpha += 1_000;
            } else if Some(instant) == next_increase {
                next_increase = Some(instant + 900_000);
                if stage == 1 {
                    hpcc.target = (hpcc.target + 50_000_000).min(config.maximum_rate_bps);
                } else if stage > 1 {
                    hpcc.target = (hpcc.target + 100_000_000).min(config.maximum_rate_bps);
                }
                hpcc.current = hpcc.current / 2 + hpcc.target / 2;
                stage += 1;
            } else {
                next_decrease = instant + 4_000;
                if decrease_pending {
                    if stage != 0 {
                        hpcc.target = hpcc.current;
                    }
                    let cut = (hpcc.current as f64 * (1.0 - hpcc.alpha / 2.0)) as u64;
                    hpcc.current = cut.max(config.minimum_rate_bps);
                    stage = 0;
                    decrease_pending = false;
                    next_increase = Some(instant + 900_000);
                    cuts += 1;
                }
            }
        }
        alpha_pending = true;
        decrease_pending = true;
        lazy.on_feedback(time);
        assert_eq!(
            (lazy.current_rate_bps, lazy.target_rate_bps),
            (hpcc.current, hpcc.target),
            "at {time} ns after {cuts} cuts"
        );
        time += 168;
    }
    assert!(cuts >= 399, "{cuts} cuts");
}
