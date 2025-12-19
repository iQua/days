#![cfg(feature = "test")]

use days::flows::packet::{EcnField, Packet};

#[test]
fn test_mark_ce_only_for_ect() {
    let mut packet = Packet::new(1000, 0, 0, 0.0);
    assert_eq!(packet.ecn, EcnField::NotEct);
    assert!(!packet.mark_ce());
    assert_eq!(packet.ecn, EcnField::NotEct);

    packet.ecn = EcnField::Ect0;
    assert!(packet.mark_ce());
    assert_eq!(packet.ecn, EcnField::Ce);

    packet.ecn = EcnField::Ect1;
    assert!(packet.mark_ce());
    assert_eq!(packet.ecn, EcnField::Ce);

    packet.ecn = EcnField::Ce;
    assert!(packet.mark_ce());
    assert_eq!(packet.ecn, EcnField::Ce);
}
