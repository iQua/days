use days_executor::{
    CpuConfig, DiagnosticPlanes, HostState, LinkId, NodeDescriptor, NodeId, NodeKind,
    ObservationMode, SimulationImage, run_cpu, run_cpu_with_observations, run_scalar,
    run_scalar_with_observations,
};

fn empty_image() -> SimulationImage {
    SimulationImage {
        stop_time_ns: 0,
        nodes: vec![NodeDescriptor {
            id: NodeId(0),
            kind: NodeKind::Host,
            state_slot: 0,
        }],
        host_states: vec![HostState {
            egress_link: LinkId(0),
            queue: Default::default(),
            in_service: None,
            tx_ready_pending: false,
            generators: Vec::new(),
            tcp_receivers: Vec::new(),
            dcqcn_receivers: Vec::new(),
            next_payload_seq: 0,
            next_origin_seq: 0,
            sourced_packets: 0,
            departed_packets: 0,
            received_packets: 0,
        }],
        switch_states: Vec::new(),
        flows: Vec::new(),
        initial_packets: Vec::new(),
        links: Vec::new(),
        channels: Vec::new(),
        initial_events: Vec::new(),
        seed: 0,
    }
}

#[test]
fn scalar_and_cpu_expose_diagnostics_only_in_full_mode() {
    let image = empty_image();
    let scalar_summary = run_scalar(&image, None).unwrap();
    let cpu_summary = run_cpu(&image, None, CpuConfig::default()).unwrap().result;
    assert!(scalar_summary.diagnostics.is_none());
    assert!(cpu_summary.diagnostics.is_none());
    assert_eq!(cpu_summary, scalar_summary);

    let scalar_full = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let cpu_full =
        run_cpu_with_observations(&image, None, CpuConfig::default(), ObservationMode::Full)
            .unwrap()
            .result;
    assert_eq!(scalar_full.diagnostics, Some(DiagnosticPlanes::default()));
    assert_eq!(cpu_full.diagnostics, scalar_full.diagnostics);
    assert_eq!(cpu_full, scalar_full);
}

#[test]
fn absent_diagnostics_are_distinct_from_present_but_empty_diagnostics() {
    let mut absent = run_scalar(&empty_image(), None).unwrap();
    assert!(absent.diagnostics.is_none());
    let absent_debug = format!("{absent:#?}");

    let mut present = absent.clone();
    present.diagnostics = Some(DiagnosticPlanes::default());
    assert_ne!(present, absent);
    assert_ne!(format!("{present:#?}"), absent_debug);

    absent.diagnostics = Some(DiagnosticPlanes::default());
    assert_eq!(present, absent);
}
