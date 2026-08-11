use days_executor::{DrainProfileChannel, DrainProfileLayout, OutboundDegreeProfile};

#[test]
fn exact_histograms_preserve_every_selected_event_and_lookup_iteration() {
    // Node 0 can expose two heads, node 1 one head, and node 2 three heads.
    let layout = DrainProfileLayout::new(
        &[2, 1, 3],
        &[
            DrainProfileChannel {
                source: 0,
                target: 10,
            },
            DrainProfileChannel {
                source: 1,
                target: 20,
            },
            DrainProfileChannel {
                source: 1,
                target: 30,
            },
            DrainProfileChannel {
                source: 2,
                target: 40,
            },
            DrainProfileChannel {
                source: 2,
                target: 50,
            },
        ],
    )
    .unwrap();
    let mut words = layout.zeroed_words();

    layout.set_head_bin_for_testing(&mut words, 0, 1, 2);
    layout.set_head_bin_for_testing(&mut words, 0, 2, 3);
    layout.set_head_bin_for_testing(&mut words, 1, 1, 5);
    layout.set_head_bin_for_testing(&mut words, 2, 3, 7);
    for (channel, emissions) in [4, 2, 4, 3, 5].into_iter().enumerate() {
        layout.set_channel_emissions_for_testing(&mut words, channel, emissions);
    }

    let profile = layout.decode(&words).unwrap();
    assert_eq!(profile.selected_events, 17);
    assert_eq!(profile.head_visits, 34);
    assert_eq!(
        profile
            .head_visit_histogram
            .iter()
            .map(|row| (row.heads_visited, row.selected_events))
            .collect::<Vec<_>>(),
        vec![(1, 7), (2, 3), (3, 7)]
    );
    assert_eq!(profile.remote_emissions, 18);
    assert_eq!(profile.binary_search_lookups, 14);
    assert_eq!(profile.binary_search_iterations, 28);
    assert_eq!(
        profile.outbound_degrees,
        vec![
            OutboundDegreeProfile {
                outbound_degree: 1,
                producer_count: 1,
                remote_emissions: 4,
                binary_search_lookups: 0,
                binary_search_iterations: 0,
            },
            OutboundDegreeProfile {
                outbound_degree: 2,
                producer_count: 2,
                remote_emissions: 14,
                binary_search_lookups: 14,
                binary_search_iterations: 28,
            },
        ]
    );
}

#[test]
fn decoder_rejects_truncated_or_internally_inconsistent_counters() {
    let layout = DrainProfileLayout::new(
        &[1],
        &[DrainProfileChannel {
            source: 0,
            target: 10,
        }],
    )
    .unwrap();
    assert!(layout.decode(&[]).is_err());

    let mut words = layout.zeroed_words();
    layout.set_node_flag_for_testing(&mut words, 0, 1);
    assert!(
        layout.decode(&words).is_err(),
        "device-side diagnostic invariant failures must reject the profile"
    );
}

#[test]
fn layout_rejects_mismatched_node_shapes_and_zero_head_capacity() {
    assert!(
        DrainProfileLayout::new(
            &[1],
            &[DrainProfileChannel {
                source: 1,
                target: 10,
            }]
        )
        .is_err()
    );
    assert!(DrainProfileLayout::new(&[0], &[]).is_err());
    assert!(
        DrainProfileLayout::new(
            &[1],
            &[
                DrainProfileChannel {
                    source: 0,
                    target: 10,
                },
                DrainProfileChannel {
                    source: 0,
                    target: 10,
                },
            ]
        )
        .is_err(),
        "duplicate source-target channels would make lookup attribution ambiguous"
    );
}
