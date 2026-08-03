use days_executor::{
    DCQCN_FRACTION_SCALE, DcqcnController, DcqcnControllerConfig, DcqcnIncreaseStage,
};

fn legacy_anchor_config() -> DcqcnControllerConfig {
    DcqcnControllerConfig {
        initial_rate_bps: 10_000_000_000,
        minimum_rate_bps: 1_000_000_000,
        maximum_rate_bps: 10_000_000_000,
        additive_rate_bps: 500_000_000,
        hyper_rate_bps: 1_000_000_000,
        g_ppb: 500_000_000,
        decrease_ppb: 500_000_000,
        cnp_interval_ns: 10_000,
        control_interval_ns: 100_000,
        increase_byte_threshold: 10_000,
    }
}

#[test]
fn exact_alpha_and_repeated_cnp_anchor() {
    assert_eq!(DCQCN_FRACTION_SCALE, 1_000_000_000);
    let mut controller = DcqcnController::new(legacy_anchor_config(), 100_000).unwrap();

    let expected = [
        (500_000_000, 7_500_000_000),
        (750_000_000, 4_687_500_000),
        (875_000_000, 2_636_718_750),
        (937_500_000, 1_400_756_835),
        (968_750_000, 1_000_000_000),
    ];
    for (index, (alpha, rate)) in expected.into_iter().enumerate() {
        let time_ns = u64::try_from(index).unwrap() * 10_000;
        assert!(controller.on_cnp(time_ns).unwrap());
        assert_eq!(controller.alpha_ppb, alpha);
        assert_eq!(controller.current_rate_bps, rate);
    }
}

#[test]
fn legacy_f64_trajectory_has_the_documented_one_bps_divergence() {
    let mut alpha = 0.0_f64;
    let mut rate = 10_000_000_000.0_f64;
    let mut legacy_rounded = Vec::new();
    for _ in 0..5 {
        alpha = 0.5 * alpha + 0.5;
        rate = (rate * (1.0 - 0.5 * alpha)).max(1_000_000_000.0);
        legacy_rounded.push(rate.round() as u64);
    }
    assert_eq!(
        legacy_rounded,
        [
            7_500_000_000,
            4_687_500_000,
            2_636_718_750,
            1_400_756_836,
            1_000_000_000,
        ]
    );

    let mut exact = DcqcnController::new(legacy_anchor_config(), 100_000).unwrap();
    for index in 0..4 {
        exact.on_cnp(index * 10_000).unwrap();
    }
    assert_eq!(exact.current_rate_bps, 1_400_756_835);
    assert_eq!(legacy_rounded[3] - exact.current_rate_bps, 1);
}

#[test]
fn cnp_interval_is_inclusive_and_early_feedback_is_a_noop() {
    let mut controller = DcqcnController::new(legacy_anchor_config(), 100_000).unwrap();
    assert!(controller.on_cnp(0).unwrap());
    let after_first = controller;
    assert!(!controller.on_cnp(9_999).unwrap());
    assert_eq!(controller, after_first);
    assert!(controller.on_cnp(10_000).unwrap());
}

#[test]
fn timer_clears_seen_then_runs_five_fast_recovery_steps() {
    let mut config = legacy_anchor_config();
    config.maximum_rate_bps = 20_000_000_000;
    let mut controller = DcqcnController::new(config, 100_000).unwrap();
    controller.on_cnp(0).unwrap();

    assert!(!controller.on_control_timer(100_000).unwrap());
    assert_eq!(controller.alpha_ppb, 500_000_000);
    assert_eq!(controller.current_rate_bps, 7_500_000_000);

    let rates = [
        8_750_000_000,
        9_375_000_000,
        9_687_500_000,
        9_843_750_000,
        9_921_875_000,
    ];
    for (index, expected) in rates.into_iter().enumerate() {
        let time_ns = 200_000 + u64::try_from(index).unwrap() * 100_000;
        assert!(controller.on_control_timer(time_ns).unwrap());
        assert_eq!(controller.current_rate_bps, expected);
    }
    assert_eq!(controller.stage, DcqcnIncreaseStage::Additive);
    assert_eq!(controller.stage_steps, 0);
    assert_eq!(controller.alpha_ppb, 15_625_000);
}

#[test]
fn additive_then_hyper_sequence_is_exact() {
    let config = DcqcnControllerConfig {
        initial_rate_bps: 8_000,
        minimum_rate_bps: 1_000,
        maximum_rate_bps: 20_000,
        additive_rate_bps: 1_000,
        hyper_rate_bps: 4_000,
        g_ppb: 500_000_000,
        decrease_ppb: 500_000_000,
        cnp_interval_ns: 0,
        control_interval_ns: 10,
        increase_byte_threshold: 100,
    };
    let mut controller = DcqcnController::new(config, 10).unwrap();
    controller.on_cnp(0).unwrap();
    controller.on_control_timer(10).unwrap();
    for time in [20, 30, 40, 50, 60] {
        controller.on_control_timer(time).unwrap();
    }
    assert_eq!(controller.stage, DcqcnIncreaseStage::Additive);

    let mut additive = Vec::new();
    for time in [70, 80, 90, 100, 110] {
        controller.on_control_timer(time).unwrap();
        additive.push((controller.target_rate_bps, controller.current_rate_bps));
    }
    assert_eq!(
        additive,
        vec![
            (9_000, 8_468),
            (10_000, 9_234),
            (11_000, 10_117),
            (12_000, 11_058),
            (13_000, 12_029)
        ]
    );
    assert_eq!(controller.stage, DcqcnIncreaseStage::Hyper);

    controller.on_control_timer(120).unwrap();
    assert_eq!(
        (controller.target_rate_bps, controller.current_rate_bps),
        (17_000, 14_514)
    );
}

#[test]
fn byte_counter_is_an_independent_increase_opportunity() {
    let mut config = legacy_anchor_config();
    config.maximum_rate_bps = 20_000_000_000;
    let mut controller = DcqcnController::new(config, 100_000).unwrap();
    controller.on_cnp(0).unwrap();

    assert!(!controller.on_bytes_emitted(9_999).unwrap());
    assert_eq!(controller.bytes_since_increase, 9_999);
    assert!(!controller.on_bytes_emitted(1).unwrap());
    assert_eq!(controller.bytes_since_increase, 10_000);

    controller.on_control_timer(100_000).unwrap();
    assert!(controller.on_bytes_emitted(1).unwrap());
    assert_eq!(controller.bytes_since_increase, 0);
    assert_eq!(controller.current_rate_bps, 8_750_000_000);
    assert_eq!(controller.next_control_time_ns, 200_000);
}

#[test]
fn invalid_or_unrepresentable_controller_configuration_is_rejected() {
    let mut bad = legacy_anchor_config();
    bad.g_ppb = DCQCN_FRACTION_SCALE + 1;
    assert!(DcqcnController::new(bad, 100_000).is_err());

    let mut bad = legacy_anchor_config();
    bad.minimum_rate_bps = bad.initial_rate_bps + 1;
    assert!(DcqcnController::new(bad, 100_000).is_err());

    let mut bad = legacy_anchor_config();
    bad.control_interval_ns = 0;
    assert!(DcqcnController::new(bad, 100_000).is_err());

    let bad_deadline = DcqcnController::new(legacy_anchor_config(), u64::MAX);
    assert!(bad_deadline.is_err());
}
