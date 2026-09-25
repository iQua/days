use days_executor::{
    TimeError, default_propagation_ns, link_arrival_time_ns, serialization_time_ns,
};

#[test]
fn serialization_is_exact_when_divisible() {
    assert_eq!(serialization_time_ns(1_500, 12_000_000_000), Ok(1_000));
}

#[test]
fn serialization_rounds_each_non_integral_interval_up() {
    assert_eq!(serialization_time_ns(1, 3_000_000_000), Ok(3));
}

#[test]
fn serialization_has_a_one_nanosecond_minimum_for_nonzero_packets() {
    assert_eq!(serialization_time_ns(1, u64::MAX), Ok(1));
}

#[test]
fn serialization_handles_a_numerator_larger_than_u64() {
    assert!((3_000_000_000_u128 * 8 * 1_000_000_000) > u64::MAX.into());
    assert_eq!(
        serialization_time_ns(3_000_000_000, 8_000_000_000),
        Ok(3_000_000_000)
    );
}

#[test]
fn zero_rate_is_an_explicit_error() {
    assert_eq!(serialization_time_ns(1_500, 0), Err(TimeError::ZeroRate));
    assert_eq!(
        link_arrival_time_ns(100, 1_500, 0, 20),
        Err(TimeError::ZeroRate)
    );
}

#[test]
fn arrival_adds_serialization_and_constant_propagation() {
    assert_eq!(default_propagation_ns(), 0);
    assert_eq!(
        link_arrival_time_ns(50, 1_500, 12_000_000_000, 0),
        Ok(1_050)
    );
    assert_eq!(
        link_arrival_time_ns(50, 1_500, 12_000_000_000, 25),
        Ok(1_075)
    );
}

#[test]
fn arrival_overflow_fails_instead_of_wrapping() {
    assert_eq!(
        link_arrival_time_ns(u64::MAX, 1, u64::MAX, 0),
        Err(TimeError::ArrivalOverflow)
    );
    assert_eq!(
        link_arrival_time_ns(u64::MAX - 1, 1, u64::MAX, 1),
        Err(TimeError::ArrivalOverflow)
    );
}

#[test]
fn serialization_result_overflow_fails_cleanly() {
    assert_eq!(
        serialization_time_ns(u64::MAX, 1),
        Err(TimeError::SerializationOverflow)
    );
}
