//! P16 D1 condition 1: the lazy Mellanox-form controller against the eager oracle, in lockstep on
//! random feedback and observation streams. Feedback times and observation times are drawn so that
//! many fall exactly on alpha, rate-increase and rate-decrease instants; the configurations include
//! nanosecond-granular intervals whose grids collide. The controller state must be identical at
//! every feedback and at the freeze, and the rate-relevant state at every observation.

#[path = "support/dcqcn_oracle.rs"]
mod oracle;

use days_executor::mellanox::{DCQCN_ALPHA_ONE, DcqcnController, DcqcnControllerConfig};
use oracle::{EagerDcqcn, rate_state_matches};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // SplitMix64: deterministic across runs and platforms.
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

fn config(
    alpha: u64,
    decrease: u64,
    increase: u64,
    steps: u32,
    clamp: bool,
    g_q63: u64,
) -> DcqcnControllerConfig {
    DcqcnControllerConfig {
        initial_rate_bps: 100_000_000_000,
        minimum_rate_bps: 100_000_000,
        maximum_rate_bps: 100_000_000_000,
        additive_rate_bps: 50_000_000,
        hyper_rate_bps: 100_000_000,
        g_q63,
        alpha_interval_ns: alpha,
        decrease_interval_ns: decrease,
        increase_interval_ns: increase,
        fast_recovery_steps: steps,
        clamp_target_rate: clamp,
    }
}

fn configs() -> Vec<DcqcnControllerConfig> {
    let g8 = DCQCN_ALPHA_ONE >> 8;
    vec![
        // SimAI / HPCC shipped block and the P15 custody block.
        config(1_000, 4_000, 900_000, 1, false, g8),
        config(1_000, 4_000, 300_000, 1, false, g8),
        // Collisions: the first check meets an alpha tick (A = D + 1); RP = 2D meets every check.
        config(1_001, 1_000, 2_000, 1, false, g8),
        config(1_001, 1_000, 2_000, 0, true, DCQCN_ALPHA_ONE >> 4),
        // RP = D (the ns-3 order departure is a definition here, not a divergence of the models).
        config(1_000, 1_000, 1_000, 3, false, g8),
        // Nanosecond-granular grids that collide often; extreme gains.
        config(3, 5, 10, 2, false, DCQCN_ALPHA_ONE / 2),
        config(2, 4, 8, 1, true, DCQCN_ALPHA_ONE),
        config(7, 3, 21, 5, false, 0),
    ]
}

#[derive(Clone, Copy, Debug)]
enum Step {
    Feedback,
    /// A phase-0 transition that reads the controller (an ACK without echo, a RESUME).
    ObserveArrival,
    /// A phase-1 transition that reads the controller (a pacing tick, a timeout).
    ObserveTimer,
}

/// Next transition time: often exactly on a pending timer instant of the oracle, otherwise a
/// random gap (including zero, so several transitions share an instant).
fn next_time(rng: &mut Rng, now: u64, eager: &EagerDcqcn, scale: u64) -> u64 {
    let instants = [eager.next_alpha, eager.next_increase, eager.next_decrease];
    if rng.below(3) == 0 {
        if let Some(instant) = instants[rng.below(3) as usize].filter(|&time| time >= now) {
            return instant;
        }
    }
    now + match rng.below(4) {
        0 => 0,
        1 => rng.below(scale),
        2 => rng.below(scale * 20),
        _ => rng.below(scale * 2_000),
    }
}

fn run_stream(config: DcqcnControllerConfig, seed: u64, steps: usize) -> (usize, usize) {
    let mut rng = Rng(seed);
    let mut lazy = DcqcnController::new(config).expect("valid configuration");
    let mut eager = EagerDcqcn::new(config);
    let scale = config.alpha_interval_ns.max(config.decrease_interval_ns);
    let mut now = rng.below(scale * 10);
    let mut feedbacks = 0;
    let mut coincident = 0;
    // Phase order: every phase-0 transition at `t` precedes every phase-1 transition at `t`, so a
    // phase-0 step after a phase-1 step at the same instant moves to the next nanosecond.
    let mut last_timer: Option<u64> = None;
    for step in 0..steps {
        let kind = match rng.below(5) {
            0 | 1 => Step::Feedback,
            2 => Step::ObserveArrival,
            _ => Step::ObserveTimer,
        };
        if !matches!(kind, Step::ObserveTimer) && last_timer == Some(now) {
            now += 1;
        }
        if matches!(kind, Step::ObserveTimer) {
            last_timer = Some(now);
        }
        let on_instant =
            [eager.next_alpha, eager.next_increase, eager.next_decrease].contains(&Some(now));
        match kind {
            Step::Feedback => {
                eager.advance_to(now);
                eager.feedback(now);
                lazy.on_feedback(now);
                feedbacks += 1;
                coincident += usize::from(on_instant);
                assert_eq!(
                    lazy,
                    eager.as_controller(),
                    "feedback at {now} (step {step}, seed {seed}, {config:?})"
                );
            }
            Step::ObserveArrival | Step::ObserveTimer => {
                let bound = if matches!(kind, Step::ObserveArrival) {
                    now
                } else {
                    now + 1
                };
                eager.advance_to(bound);
                if lazy.due_ns() < bound {
                    lazy.materialize(bound);
                }
                assert!(
                    rate_state_matches(&lazy, &eager.as_controller()),
                    "observation {kind:?} at {now} (step {step}, seed {seed}):\nlazy  {lazy:?}\neager {:?}",
                    eager.as_controller()
                );
            }
        }
        now = next_time(&mut rng, now, &eager, scale);
    }
    // The freeze at a phase-1 transition: the whole state matches.
    let bound = now + 1;
    eager.advance_to(bound);
    lazy.settle(bound);
    assert_eq!(
        lazy,
        eager.as_controller(),
        "freeze at {bound} (seed {seed})"
    );
    (feedbacks, coincident)
}

#[test]
fn the_lazy_controller_equals_the_eager_oracle() {
    let mut feedbacks = 0;
    let mut coincident = 0;
    for (index, config) in configs().into_iter().enumerate() {
        for seed in 0..12 {
            let (f, c) = run_stream(config, 1_000 * index as u64 + seed, 400);
            feedbacks += f;
            coincident += c;
        }
    }
    // The streams must exercise feedback exactly on timer instants, not only near them.
    assert!(feedbacks > 10_000, "{feedbacks} feedbacks");
    assert!(
        coincident > 500,
        "{coincident} feedbacks on a timer instant"
    );
}

#[test]
fn a_freeze_before_the_first_feedback_keeps_the_controller_pristine() {
    let config = configs()[0];
    let mut lazy = DcqcnController::new(config).unwrap();
    lazy.settle(5_000_000);
    assert_eq!(lazy, DcqcnController::pristine(config));
    assert_eq!(lazy, EagerDcqcn::new(config).as_controller());
}

#[test]
fn a_long_idle_gap_before_feedback_matches_the_oracle_alpha_bit_for_bit() {
    // 50 ms without feedback after one congestion episode: 50,000 eager alpha ticks against one
    // lazy catch-up, which must stop at alpha = 0 with the oracle.
    let config = configs()[0];
    let mut lazy = DcqcnController::new(config).unwrap();
    let mut eager = EagerDcqcn::new(config);
    for time in [0, 84, 168, 2_000, 2_084] {
        eager.advance_to(time);
        eager.feedback(time);
        lazy.on_feedback(time);
    }
    for time in [50_000_000, 50_000_084] {
        eager.advance_to(time);
        eager.feedback(time);
        lazy.on_feedback(time);
        assert_eq!(lazy, eager.as_controller());
    }
}
